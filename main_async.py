'''
    TODO:
        LVL3: Сделать парсер асинхронным, используя библиотеку aiohttp, например, 5 параллельных запросов.
        LVL4: Добавить админку на Django для отображения хабов и управления ими (добавить хаб/удалить хаб/указать период обхода хаба).

    НЕ РЕАЛИЗОВАНО 
'''

from bs4 import BeautifulSoup
import time
import sqlite3

import asyncio
import aiohttp

class HabrParser:
    def __init__(self):
        '''
        Создание и инициализация полей базы данных
        '''
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
        }
        self.session = aiohttp.ClientSession()
        
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
        
    async def async_get_article_info(self, article_url:str, hub_name:str) -> dict:
        '''
        Получение данных статей
        '''
        url = self.base_url + article_url

        async with self.session.get(url=url, headers=self.headers) as response:
            soup = BeautifulSoup(await response.text(), 'html.parser')

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


    async def async_get_hub_articles(self):
        '''
        Получение ссылок со страницы хаба
        '''
        
        hubs_url = self.get_hubs()
        for hub_url in hubs_url:
            response = await self.session.get(url=hub_url[1], headers=self.headers)
            soup = BeautifulSoup(await response.text(), 'html.parser')
            
            tasks = []
            for link in soup.find_all('a', href=True):
                print(link['href'])
                if "/articles/" in link['href'] and "tm-title__link" in link["class"]:
                    task = asyncio.create_task(self.async_get_article_info(link['href'], hub_url[0]))
                    tasks.append(task)

        await asyncio.gather(*tasks)

def main():
    hbrp = HabrParser()
    asyncio.run(hbrp.async_get_hub_articles())
    
if __name__ == "__main__":
    main()