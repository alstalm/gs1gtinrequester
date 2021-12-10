from df_creating import get_total_df
from df_creating import get_curent_df
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

    def one_by_one_parser(source_df):
        final_output_df = pd.DataFrame()
        for i in range(len(source_df)):
            gtin = source_df.loc[i,'GTIN']
            gs1attr = source_df.loc[i,'GS1Attr']
            current_output_df = get_curent_df(curent_gtin_list= [gtin],  attr_list=[gs1attr] , url = url, auth= auth  )

            # если есть атрибуты, то перевдем в EAV вид
            cols_list = current_output_df.columns.tolist()
            if len(cols_list) >3:
                GS1Attr_name = cols_list[-1]
                current_output_df.loc[:, 'GS1Attr_name'] = GS1Attr_name
                current_output_df.loc[:, 'GS1Attr_value'] = current_output_df.loc[:, GS1Attr_name]
                current_output_df = current_output_df[['GTIN', 'errorcode', 'variant', 'GS1Attr_name', 'GS1Attr_value']].copy()
            else:
                pass

            if len(final_output_df) == 0:
                final_output_df = current_output_df
            else:
                final_output_df = pd.concat([final_output_df, current_output_df],axis=0)
                print('NV71: final_output_df = ',final_output_df)

        return final_output_df


    #TODO os.path.join - чтобы не сломался путь
    full_input_path = input_path + input_file
    input_df = pd.read_excel(full_input_path, dtype= {'GTIN': object})
    input_df = input_df.loc[:,['GTIN', 'GS1Attr']]

    full_output_path = output_folder + output_file
    output_df = one_by_one_parser(source_df=input_df)

    try:
        output_df.to_excel(full_output_path, sheet_name='sheet_1', index = False)
        print('файл успешно записался.. наверное..')
    except PermissionError:
        print('\nфайл не доступен для записи\n')

except Exception as e:
    print(e)