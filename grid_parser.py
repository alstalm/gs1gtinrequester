# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from df_creating import gs1_requester
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

input_folder_for_grid = params['input_folder_for_grid']
input_file_for_grid= params['input_file_for_grid']
full_input_path_for_grid = input_folder_for_grid + input_file_for_grid


''' соберем авторизацию'''
auth = HTTPBasicAuth(login, password)

''' получим список GTIN '''
GTIN_LIST_full_path = GTIN_LIST_path + GTIN_LIST_file
GTIN_df = pd.read_csv(GTIN_LIST_full_path, delimiter= ';')
GTIN_list= list(GTIN_df['GTIN'])
GTIN_list = list(map(str, GTIN_list))

''' соберем путь к файлу результата выгрузки '''
full_output_path = output_folder + output_file


''' сделаем запрос в ГС1'''
#GTIN_list = ['4601075342390', '4601075342420']
in_df = pd.read_excel(full_input_path_for_grid)
print(in_df.to_string())
gs1_requester_object = gs1_requester(get_valueMap=True, source_df=in_df, verbose_result=True)

df = gs1_requester_object.batch_requester(chunk=50) #url=url, auth=auth,

#print(df)
''' выгрузим в эксель'''



try:
    df.to_excel(full_output_path, index=False, sheet_name='sheet_1')
except Exception as e:
    print('запись не удалась. ошибка {}',e)



''' сделаем отсечку времени окончания'''
t1_stop = perf_counter() # окночание отсчета времение
print("Время выполнения программы : {} \n".format(t1_stop-t1_start)) # посчитаем время выполнения скрипта



#TODO пофиксить в данном файле возможность работыбез EAV и чанками GTIN не более 50-ти.
