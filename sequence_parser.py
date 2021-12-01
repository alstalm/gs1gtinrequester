from df_creating import get_total_df
from df_creating import splitter
from df_creating import splitter_joiner
import pandas as pd
pd.options.display.max_colwidth = 150
import requests
from attrvalue_from_valueMap_extracting import get_mapping_value
import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# ОТКЛЮЧАЕТ ВОРНИНГИ НО ВОЗМОЖНО ЗАМЕДЛЯЕТ
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

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
    cash, test_result = get_mapping_value(cash={}, gs1_attrid='PROD_NAME', mapping_key='')
    print('соединение с БД установлено')

    #TODO #1 переименовать!
    def cal_multi_col(row):
        out = get_total_df(gtin_list=[row['GTIN']], attr_list=[row['GS1Attr']], url=url, auth=auth, batch_size=1)
        return [out]

     #################
    #TODO с каждым запросом пристыковывать новый датафрейм к существующему, так чтобы если есть два варианта картоек то они оба записались в итоговый датафрейм
    #TODO #1 нужны нормальные названия датафреймов
    #TODO os.path.join - чтобы не сломался путь
    full_input_path = input_path + input_file
    df = pd.read_excel(full_input_path, dtype= {'GTIN': object})
    df = df.loc[:,['GTIN', 'GS1Attr']]


    #TODO лучше использовать pipe
    # разобъем на столбцы
    df['value_in_GS1'] = df.apply(cal_multi_col, axis=1, result_type='expand')
    df['variant']      = df['value_in_GS1'].map(lambda x: list(x.split(' '))[0] if list(x.split(' '))[0] != 'NaN' else '')
    df['errCode']      = df['value_in_GS1'].map(lambda x: splitter(x, 1))
    df['GTIN']         = df['value_in_GS1'].map(lambda x: splitter(x, 2))
    df['value']        = df['value_in_GS1'].map(lambda x: splitter_joiner(x))

    full_output_path = output_folder + output_file

    try:
        df.to_excel(full_output_path, sheet_name='sheet_1', index = False)
        print('файл успешно записался.. наверное..')
    except PermissionError:
        print('\nфайл не доступен для записи\n')

except Exception as e:
    print(e)