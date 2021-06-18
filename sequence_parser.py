from func import GetTable
import pandas as pd
pd.options.display.max_colwidth = 150
import requests
import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# ОТКЛЮЧАЕТ ВОРНИНГИ НО ВОЗМОЖНО ЗАМЕДЛЯЕТ
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from requests.auth import HTTPBasicAuth
#import timing # запуск модуля timing (нашел в интеренете)
from time import perf_counter
import yaml

t1_start = perf_counter() # запуск начала отсчета

''' ЗАДАИМ ПАРАМЕТРЫ'''
with open('params.yaml', 'r', encoding='UTF-8' ) as f:
    params = yaml.safe_load(f)

'''Endpoint И АКРЕДЫ АУТЕНТИФИКАЦИИ '''
url = params['url']
login = params['login']
password = params['password']
''' СПИСОК GTIN'''
GTIN_LIST_path = params['GTIN_LIST_path']
GTIN_LIST_file = params['GTIN_LIST_file']
''' РЕЗУЛЬТАТ ВЫГРУЗКИ '''
output_folder = params['output_folder']
output_file = params['output_file']
''' ЗАДАИМ СПИСОК АТРИБУТОВ'''
Attributes_list = params['Attributes_list']


''' соберем авторизацию'''
auth = HTTPBasicAuth(login, password)


def cal_multi_col(row):
    out = GetTable(gtin_list=[row['GTIN']], attr_list=[row['GS1Attr']], url=url, auth=auth, batch_size=1)
    return [out]

 #################
""" НИЖЕ ТЕСТОВЫЙ ПРИМЕР"""
'''
df_test = pd.DataFrame({'GTIN':['4620005721227', '4620119120145'], 'GS1Attr':['WEB_90001853','WEB_90001771']})

df_test['GS1_values'] = df_test.apply(cal_multi_col, axis=1, result_type='expand')
try:
    df_test.to_excel('D:/CRPT/2021.05_май/внесение изменений/загруженые файлы импорты/Молочная продукция/concatinated/df_test_result.xlsx', index=True, sheet_name='sheet_1')
    print('файл успешно записался.. наверное..')
except PermissionError:
    print('\nфайл не доступен для записи\n')
'''



 #################
""" НИЖЕ РЕАЛЬНЫЙ РАБОЧИЙ ПРИМЕР  """
df_first_try = pd.read_excel('D:/CRPT/2021.05_май/внесение изменений/загруженые файлы импорты/Молочная продукция/concatinated/concatinated.xlsx', dtype= {'GTIN': object})
df_first_try = df_first_try.loc[:,['GTIN', 'GS1Attr']]
df_first_try.info()

#TODO добавить замену Nan на NULL
df_first_try['value_in_GS1'] = df_first_try.apply(cal_multi_col, axis=1, result_type='expand')

try:
    df_first_try.to_excel('D:/CRPT/2021.05_май/внесение изменений/загруженые файлы импорты/Молочная продукция/concatinated/first_result_2021.06.04_1320.xlsx', index=True, sheet_name='sheet_1')
    print('файл успешно записался.. наверное..')
except PermissionError:
    print('\nфайл не доступен для записи\n')