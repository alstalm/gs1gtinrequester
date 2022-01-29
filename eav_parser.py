from df_creating import gs1_requester
import pandas as pd
pd.options.display.max_colwidth = 150
import requests

import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# ОТКЛЮЧАЕТ ВОРНИНГИ НО ВОЗМОЖНО ЗАМЕДЛЯЕТ
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from attributes_extractor import AtrrValueParesr

from requests.auth import HTTPBasicAuth
#import timing # запуск модуля timing (нашел в интеренете)
from time import perf_counter
import yaml
#TODO #1 отсортировать и убрать лишнее!

t1_start = perf_counter() # запуск начала отсчета

''' ЗАДАИМ ПАРАМЕТРЫ'''
with open('params.yaml', 'r', encoding='UTF-8' ) as f:
    params = yaml.safe_load(f)

'''Endpoint И АКРЕДЫ АУТЕНТИФИКАЦИИ '''
url = params['url']
login = params['login']
password = params['password']
''' ВХОДНОЙ ФАЙЛ'''
input_path = params['input_path']
input_file = params['input_file']

''' РЕЗУЛЬТАТ ВЫГРУЗКИ '''
output_folder = params['output_folder']
output_file = params['output_file']
''' ЗАДАИМ СПИСОК АТРИБУТОВ'''
Attributes_list = params['Attributes_list']

''' соберем авторизацию'''
auth = HTTPBasicAuth(login, password)


# тест соединения с БД
try:
    print('проверим соединение с БД')

    cash, test_result = AtrrValueParesr.get_value_from_valueMap(cash={}, gs1_attrid ='TEST_CONNECTION', mapping_key ='')
    print('test_result =',test_result)
    if test_result == '':
        print('соединение с БД установлено')
    else:
        print('проблемы с сеоединением с БД НК')

    #TODO os.path.join - чтобы не сломался путь
    full_input_path = input_path + input_file
    input_df = pd.read_excel(full_input_path, dtype= {'GTIN': object})
    input_df = input_df.loc[:,['GTIN', 'GS1Attr']]

    full_output_path = output_folder + output_file

##############################################

    gs1_requester_object = gs1_requester(get_valueMap=True, source_df=input_df, verbose_result=True)

    output_df = gs1_requester_object.batch_requester()  # url=url, auth=auth,









    try:
        output_df.to_excel(full_output_path, sheet_name='sheet_1', index = False)
        print('файл успешно записался.. наверное..')
    except PermissionError:
        print('\nфайл не доступен для записи\n')

except Exception as e:
    print('проблемы с сеоединением с БД НК:',e)

''' сделаем отсечку времени окончания'''
t1_stop = perf_counter() # окночание отсчета времение
print("Время выполнения программы : {} \n".format(t1_stop-t1_start)) # посчитаем время выполнения скрипта