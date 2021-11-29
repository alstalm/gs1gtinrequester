# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from df_creating import get_total_df
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

''' получим список GTIN '''
GTIN_LIST_full_path = GTIN_LIST_path + GTIN_LIST_file
GTIN_df = pd.read_csv(GTIN_LIST_full_path, delimiter= ';')
GTIN_list= list(GTIN_df['GTIN'])
GTIN_list = list(map(str, GTIN_list))

''' соберем путь к файлу результата выгрузки '''
full_output_path = output_folder + output_file


''' сделаем запрос в ГС1'''
#GTIN_list = ['4601075342390', '4601075342420']
df = get_total_df(url=url, auth=auth, gtin_list=GTIN_list, attr_list=Attributes_list, batch_size=1)

print(df)
''' выгрузим в эксель'''
try:
    df.to_excel(full_output_path, index=True, sheet_name='sheet_1')
except Exception as e:
    # потом доделать запись с timestamp
#TODO в закомментированных строках  БАГ! с этими строками идет попытка сделать пустой запрос в ГС1.
    '''
    now = datetime.datetime.now()
    full_output_path_without = full_output_path[(len(full_output_path) - 4):]
    full_output_path_with_time_stamp = full_output_path_without + str(now) + '.xlsx'
    df.to_excel(full_output_path_with_time_stamp, index=True, sheet_name='sheet_1')
    '''
    print('------------------\nво время записи в файл произошла ошибка:', e)

''' сделаем отсечку времени окончания'''
t1_stop = perf_counter() # окночание отсчета времение
print("Время выполнения программы : {} \n".format(t1_stop-t1_start)) # посчитаем время выполнения скрипта


'''
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
'''
