import pandas as pd
import os
import numpy as np
import time


def sort_and_path(file_list, folder_name):
    '''
    Функция, упорядочивающая файлы в папке в зависимости от дня последних изменений, начиная с самого раннего.
    
    :param file_list: список названий файлов в их исходном порядке (type: list)
    
    :param folder_name: по умолчанию папка с файлами, которые требуется отсортировать
    
    :returns: ??? +-
    
    ''' 
    
    file_list.sort(key=lambda x: int(x.split('_')[2])) # И наконец третью - день
    file_list.sort(key=lambda x: int(x.split('_')[1])) # Потом вторую часть (месяц) по возратсанию
    file_list.sort(key=lambda x: int(x.split('_')[0])) # Упорядочиваем сначала первую часть названия (год) по возратсанию
    
    file_list = list(map(lambda x: os.path.join(folder_name, x), file_list)) # Добавляем путь к файлу (название папки) 
                                                                             # к названию каждого файла из списка (из папки)
    return file_list


def det_price_dict(sorted_paths, last=30):
    '''
    Функция, cоздающая словарь с  id товара и его итоговой ценой.
    
    :param sorted_paths: упорядоченный список файлов (type: list)
    
    :returns: словарь с  id товара и его итоговыми ценами за определённое время (type: dictionary)
    
    ''' 
    
    prod_dict = dict()
    for i in range(-min(last, len(sorted_paths)), 0): # Итерируемся по длине списка, если она не больше 30
        df = pd.read_excel(sorted_paths[i])
        df['Итоговая цена'] = df['Цена'] - df['Скидка'] # Считаем и добавляем итоговую цену
        for j in range(len(df)):
            prod_dict[df.iloc[j]['id']] = prod_dict.get(df.iloc[j]['id'], []) + [df.iloc[j]['Итоговая цена']]
        
    return prod_dict


def get_stat_df(prod_df):
    '''
    Функция, из исходной таблицы с данными создающая DataFrame с минимальной 
    и максимальной ценой на товар, квантилями (5%, 10%, 25%, 50%, 75%, 90%, 95%), вчерашней и сегодняшней ценой.
    
    :param prod_df: таблица с категориями товаров, id и названиями товаров (в качестве индексов) и итоговой ценой 
    (type: Pandas DataFrame))
    
    :returns: таблица с нужными нам характеристиками (type: Pandas DataFrame)
    
    ''' 
    common_df = pd.DataFrame() # Создаём пустой DataFrame
    common_df['Min'] = prod_df.min(axis=1) # Считаем мин. и макс. значения построчно
    common_df['Max'] = prod_df.max(axis=1)
    common_df['Q5'] = prod_df.quantile(q=0.05, axis=1) # Считаем кванили построчно
    common_df['Q10'] = prod_df.quantile(q=0.1, axis=1)
    common_df['Q25'] = prod_df.quantile(q=0.25, axis=1)
    common_df['Q50'] = prod_df.quantile(q=0.5, axis=1)
    common_df['Q75'] = prod_df.quantile(q=0.75, axis=1)
    common_df['Q90'] = prod_df.quantile(q=0.9, axis=1)
    common_df['Q95'] = prod_df.quantile(q=0.95, axis=1)
    common_df['Yesterday'] = prod_df.iloc[:,-2] # Предпоследний столбец - вчерашняя цена
    common_df['Today'] = prod_df.iloc[:,-1] # Последний столбец - цена товара сегодня
        
    return common_df


def det_price_dict_f(sorted_paths, last=30):
    '''
    Функция, cоздающая Pandas DataFrame с id товаров, их категориями, полными именами и его итоговыми ценами.
    
    :param sorted_paths: упорядоченный список файлов (type: list)
    
    :returns: Pandas DataFrame с id товаров, их категориями, полными именами и его итоговыми ценами 
    id, полное имя и категория представлены как индексы (type: Pandas DataFrame)
    
    ''' 
    dfs = list()
    for i in range(-min(last, len(sorted_paths)), 0): # Итерируемся по длине списка, если она не больше 30
        df = pd.read_excel(sorted_paths[i]) # читаем excel
        df['Итоговая цена'] = df['Цена'] - df['Скидка'] # вычисляем итоговую цену
        # Добавляем нужную информацию в список, меняя при этом индексы
        dfs.append(df[['Категория', 'id', 'Полное имя', 'Итоговая цена']].set_index(['Категория', 'id', 'Полное имя']))
        
    return pd.concat(dfs, axis=1)


def index_more(array, lacmus):
    '''
    Функция, сравнивающая по порядку элементы массива с заданным числом
    
    :param array: массив (type: numpy array)
    :param lacmus: число для сравнения (type: float) 
    
    :returns: порядковый номер элемента | -1 (type: integer)
    
    ''' 
    
    for i in range(len(array)):
        if array[i] > lacmus:
            return i
    return -1


def Quant(df_p):
    '''
    Функция, считающая в скольки процентах случаев цена ниже сегодняшней

    :param df_p: таблица с нужными нам характеристиками (type: Pandas DataFrame)
        
    :returns: список, полученный в результате сравнения сегодняшней цены со значениями квантилей (type: list)
    
    ''' 
    
    q_dict = {-1:-1, 0: 5, 1:10, 2:25, 3:50, 4:75, 5:90, 6:95}
    quantiles = list()
    for i in range(len(df_p)):
        quantiles.append(q_dict[index_more(df_p.iloc[i][3:10].to_list(), df_p.iloc[i]['Today'])])
        
        # Если цена на сегодня выше цены, заданной в квантиле, то в список quantiles добавляем -1, если ниже, 
        # то добавляем этот квантиль
        
    return quantiles


    
def process_best_and_worse_prices_pipe(quantile_df):
    '''Функция анализирует pandas DataFrame c квантилями (см. функцию get_stat_df), для каждого товара 
    определяет текущий квантиль (см. функцию Quant), сортирует товары по полученному квантилю с порядке 
    возрастания, отсеивает случаи, где цена не изменилась, а в конце возвращает таблицы с наилучшими ценами 
    (квантиль согласно функции Quant <= 25) и с наихудшими (квантиль согласно функции Quant >= 75)
    
    :param quantile_df: таблица с информанией о минимальной и максимальной ценах, наименовании продуктов и с квантилями
    (см. функцию get_stat_df) (type: pandas DataFrame)
    :returns: таблицы с наилучшими ценами (квантиль согласно функции Quant <= 25) 
    и с наихудшими (квантиль согласно функции Quant >= 75) в указанном порядке 
    (type: list of pandas DataFrame, [df_best, df_worst])
    
   
    :param quantile_df: таблица с информанией о минимальной и максимальной ценах, наименовании продуктов и с квантилями
    (см. функцию get_stat_df) (type: pandas DataFrame)
    :returns: таблицы с наилучшими ценами (квантиль согласно функции Quant <= 25) 
    и с наихудшими (квантиль согласно функции Quant >= 75) в указанном порядке 
    (type: list of pandas DataFrame, [df_best, df_worst])
    
    '''
    # Вычисление квантилей, сортировка и фильтрация
    df_new = quantile_df.assign(Today_quantile=lambda df_: \
                                Quant(df_)).sort_values(by='Today_quantile').query("Today_quantile > -1") 
    best_price = df_new[df_new['Today_quantile'] <= 25][df_new.columns[[0,1,9,10,11]]] # Лучшие цены
    not_today = df_new[df_new['Today_quantile'] >= 75][df_new.columns[[0,1,9,10,11]]] # Худшие цены
    return [best_price, not_today]


if __name__ =='__main__':
    folder_name = 'requests_data_archive'
    # file_list = os.listdir(folder_name)
    # sp = sort_and_path(file_list, folder_name=folder_name)
    # price_df = det_price_dict_f(sp)
    # df_p = get_stat_df(price_df)
    # dfs = process_best_and_worse_prices_pipe(df_p)

    dfs = process_best_and_worse_prices_pipe(get_stat_df(det_price_dict_f(sort_and_path(os.listdir(folder_name), folder_name))))
    xls = pd.ExcelWriter('df_new.xls', engine='openpyxl')
    dfs[0].to_excel(xls, 'Удачные покупки сегодня')
    dfs[1].to_excel(xls, 'Лучше потом')
    xls.save()

