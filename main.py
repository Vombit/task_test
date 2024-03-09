import requests
from bs4 import BeautifulSoup
import time
import sqlite3


class HabrParser:
    def __init__(self):
        '''
        Создание и инициализация полей базы данных
        '''
        self.base_url = 'https://habr.com'
        self.conn = sqlite3.connect('habr.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        self.cursor.execute(
            '''CREATE TABLE IF NOT EXISTS hubs (
                name TEXT,
                url TEXT UNIQUE,
                round_time INTEGER
            )'''
        )
        
        self.cursor.execute(
            '''CREATE TABLE IF NOT EXISTS articles (
                hub_name TEXT,
                title TEXT,
                date TEXT,
                article_url TEXT UNIQUE,
                author_link TEXT,
                author_name TEXT,
                publication_text TEXT
            )'''
        )
        self.conn.commit()

    def get_hubs(self) -> list:
        '''
        Получение хабов из базы
        '''
        hubs = self.cursor.execute('SELECT * FROM hubs')
        
        return hubs.fetchall()
        
        
    def get_hub_articles(self, hub_url:str) -> list:
        '''
        Получение ссылок со страницы хаба
        '''
        response = requests.get(hub_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        articles = []

        for link in soup.find_all('a', href=True):
            if "/articles/" in link['href'] and "tm-title__link" in link["class"]:
                articles.append(link['href'])
                
        return articles


    def get_article_info(self, article_url:str, hub_name:str) -> dict:
        '''
        Получение данных статей
        '''
        url = self.base_url + article_url
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        title = soup.find('h1').text.strip()
        date = soup.find('time', {'datetime': True})['datetime']
        publication_text = soup.find('div', {'id': 'post-content-body'}).text
        
        author_link = self.base_url + soup.find('a', {'class': 'tm-user-info__username'})['href'] if soup.find('a', {'class': 'tm-user-info__username'}) else ''
        author_name = soup.find('span', {'class': 'tm-user-card__name'}).text.strip() if soup.find('span', {'class': 'tm-user-card__name'}) else ''

        self.cursor.execute(
            'INSERT OR IGNORE INTO articles (hub_name, title, date, article_url, author_link, author_name, publication_text)'
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            (hub_name, title, date, url, author_link, author_name, publication_text)
        )
        self.conn.commit()

        res = {
            'hub_name': hub_name,
            'title': title,
            'date': date,
            'article_url': url,
            'author_link': author_link,
            'author_name': author_name,
            'publication_text': publication_text
        }

        print(res)
        # return res

if __name__ == '__main__':
    hbrp = HabrParser()
    
    while True:
        hubs_url = hbrp.get_hubs()
        for hub_url in hubs_url:
            urls = hbrp.get_hub_articles(hub_url[1])
            for article in urls:
                hbrp.get_article_info(article, hub_url[0])
            
        time.sleep(60 * 10)