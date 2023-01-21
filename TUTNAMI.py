import requests
import time
import json
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing.pool import ThreadPool
import datetime
import os

def next(url, n_pieces=20):
    '''
    Функция, получающая шаблон для формирования url и возвращающая несколько запросов, сформированных по этому запросу. 
    Позволяет получить информацию о разных кусочках страницы, открывающихся при прокручивании страницы вниз.
    
    :param url: Шаблон для веб-адресов, всегда должен содержать символ '*' там, где надоделать замены (type: str) 
    
    :param n_pieces: число частей, из которых состоит страница, соответствует числу получаемых на выходе адресов.
    Если информации о числе частей нет в самом адресе, берётся значение по умолчанию. Лишние адреса при запросе 
    просто вернут пустые ответы. (type: int, default: 20)
    
    :returns: Адреса, собранные по шаблону (type: list of str)
    
    Example:
    
    next()

    '''
    if 'storeId=' in url:
        n_pieces=int(url[url.rfind('storeId=')+len('storeId='):])

    urls = []
    parts = url.split('*')
    for p in range(1, n_pieces+1):
        urls.append(str(p).join(parts))
    return urls


def getcat(template):
    '''
    Функция, делающая запросы по url-адресам частей страницы и получающая json-файлы этих частей в виде списка.
    :param template: адрес web-страницы (type: strip)
    :returns: сисок частей страницы, загруженых в формате json (type: list)

    '''

    headers = {'Host': 'website-api.ecom-dev.tamimimarkets.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0',
    'Accept': 'application/json',
    'Accept-Language': 'en',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.shop.tamimimarkets.com/',
    'Content-Type': 'application/json',
    'Origin': 'https://www.shop.tamimimarkets.com',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'TE': 'trailers'}
    data = []
    urls = next(template.strip()) # Удаляем крайние пробелы в адресе страницы, 
                                  # с помощью функции next() перебираем все части страницы
    for url in urls:
        resp = requests.get(url,headers=headers)
        data.append(json.loads(resp.text))
    return data


def data_of_website(data_json):
    '''
    Функция, сортирующая информацию с  web-страницы, а также составляющая на основе этого таблицу 
    с нужными данными (характеристиками) посредством библиотеки pandas.
    :param data_json: код web-страницы в формате json.
    :returns: таблица с выбранными данными web-страницы (type: Pandas DataFrame)

    '''
    
    name = [] # Создаём пустые списки для добавления в них нужной нам информации
    id_of_product = []
    fullName = []
    brand = []
    mrp = [] # цена до скидки
    discount = [] # скидка
    stock = [] # наличие
    category = []
    #print(data_json)
    if data_json['data']['product'] is None:
        pass
    else:
        for x in data_json['data']['product']: # Создаём цикл для поиска нужных данных
            name.append(*[x['name']]) # По ключам обращаемся к нужной нам категории данных и добавляем  в соответствующий список
            id_of_product.append(*[x['id']])
            fullName.append(*[x['variants'][0]['fullName']])
            if x['brand'] is not None:
                brand.append(*[x['brand']['name']])
            else:
                brand.append('nodata')
            category.append(x['primaryCategory']['parentCategory']['slug'])
            mrp.append(*[x['variants'][0]['storeSpecificData'][0]['mrp']])
            discount.append(*[x['variants'][0]['storeSpecificData'][0]['discount']])
            stock.append(*[x['variants'][0]['storeSpecificData'][0]['stock']])

    df=pd.DataFrame({'Категория': category, 'Наименование':name, 'id':id_of_product, 'Полное имя':fullName, 
                     'Производитель':brand,  'Цена':mrp, 'Скидка':discount, 
                     'В наличии, шт':stock}) # Имея списки с нужной информацией создаём 
                                             # таблицу pandas (DataFrame), прописывая названия столбцов 
    
    return df


def all_to_df(data_list):
    '''
    Функция, конкатенирующая список c Pandas DataFrame в один Pandas DataFrame, выровненным по оси y.
    :param data_list: список таблиц Pandas DataFrame (type: list)
    :returns: сводная таблица со всех страниц (type: Pandas DataFrame)

    '''
    
    return pd.concat([data_of_website(data) for data in data_list], axis=0).reset_index(drop=True)

def threaded_requests_and_dataframes(templates):
    '''
    Функция, создающая потоки (пулы) из функции getcat(), отправляя несколько запросов друг за другом, 
    не дожидаясь ответов предыдущих запросов

    '''
    
    pool = ThreadPool(processes=len(templates))
    return all_to_df([a for b in pool.map(getcat, templates) for a in b])


if __name__ =='__main__':
    adress=open('Templates.txt', 'r') # Открываем файл с адрессами страниц сайта
    templates = adress.readlines() # Читаем файл построчно
    adress.close() # Закрываем файл 
    dt_now = datetime.datetime.now() # Узнаём сегодняшнюю дату
    threaded_requests_and_dataframes(templates).to_excel(os.path.join('requests_data_archive', '_'.join([str(dt_now.year),
                                                         str(dt_now.month), str(dt_now.day), 'goods-data.xlsx'])), index=False) # Сохраняем таблицу с данными в формате exсel в папку 'requests_data_archive',
                                                                                                                                # учитывая время последнего изменения,
                                                                                                                                # с названием по типу: год_месяц_день_goods-data.xlsx
                                                         
