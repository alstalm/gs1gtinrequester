import xmltodict
import pandas as pd
import requests
from table_bulider import table_bulider
import yaml
from requests.auth import HTTPBasicAuth
from retry import retry  # pip install retry

with open('params.yaml', 'r', encoding='UTF-8') as f:
    params = yaml.safe_load(f)

'''Endpoint И АКРЕДЫ АУТЕНТИФИКАЦИИ '''
url = params['url']
login = params['login']
password = params['password']
auth = HTTPBasicAuth(login, password)


def gtin_list_combiner(GTIN_list):
    row_set = ''
    for gtin in GTIN_list:

        print('func22: gtin_type = ', type(gtin))

        row = '<ns1:GTIN>' + str(gtin) + '</ns1:GTIN>'
        row_set = row_set + row
        # print('row_set after iteration:', row_set)
        # print('')
    return row_set


# TODO ALt+shift+L - beutify
@retry(TimeoutError, tries=5, delay=1, max_delay=180, backoff=3)
def get_curent_df(curent_gtin_list, attr_list, url, auth):
    """
    Собирает один запрос из нескольких гтин. Запрашивает ГС1 по полученнуму на входе списку гтинов и формирует датафрейм
    """

    body_prefix = """<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="urn:org.gs1ru.gs46.intf"><SOAP-ENV:Body><ns1:GetItemByGTIN>"""
    body_postfix = """<ns1:lang>RU</ns1:lang><ns1:showMeta>0</ns1:showMeta><ns1:noCache>0</ns1:noCache><ns1:loadChangeVersion>0</ns1:loadChangeVersion><ns1:noCascade>0</ns1:noCascade><ns1:noGepir>1</ns1:noGepir></ns1:GetItemByGTIN></SOAP-ENV:Body></SOAP-ENV:Envelope>"""

    body_core = gtin_list_combiner(GTIN_list=curent_gtin_list)
    # TODO DONE конкатинацию лучше делать через ''.join()
    full_body = ''.join([body_prefix, body_core, body_postfix])
    # Задаим параметры для Запроса


    # Соберем запрос и распарсим
    resp = requests.post(url=url, data=full_body, auth=auth, verify=False)
    status_code = resp.status_code
    if status_code == 200:
        answer = resp.content
        XML_parsed_to_dict = xmltodict.parse(answer)
        curent_attr_df = table_bulider(XML_parsed_to_dict, attr_list)
    else:

        curent_attr_df = pd.DataFrame({'http_code': [200]})

    return curent_attr_df


''' Запрашивает батчами ГС1 по любому списку гтин'''


def GetTable(gtin_list, attr_list, url=url, auth=auth, batch_size=1):
    """
    GetTable - функция для построения таблицы атрибутов по списку gtin.
    В качестве аргументов функция принимает артументы: gtin_list, attr_list,
    где:
    gtin_list - список gtin в формате ['12345', '2345']
    attr_list - список атрибутов GS1 в формате [['PROD_COVER_GTIN', 'PROD_REGDATE', 'PUBLICATION_DATE']
    batch_size - количество одновременно запрашиваемых в GS1 gtin
    для любого списка атрибутов функция всегда дополнительно возвращает значение варианта карточки товара
    пример записи функции GetTable(gtin_list = GTIN_list, attr_list = Attributes_list)
    """
    full_attr_df = pd.DataFrame()

    if len(gtin_list) >= batch_size:
        while len(gtin_list) >= batch_size:

            curent_gtin_list = gtin_list[:batch_size]
            gtin_list = gtin_list[batch_size:]

            try:
                current_attr_df = get_curent_df(curent_gtin_list=curent_gtin_list, attr_list=attr_list, url=url, auth=auth)
                if len(full_attr_df) < 1:
                    full_attr_df = current_attr_df.copy()
                else:
                    full_attr_df = pd.concat([full_attr_df, current_attr_df], axis=0)
            except:
                #TODO добавить вместо это принт с указанием строки и GTIN на которой или логирование
                #TODO DONE - Добавлен pass
                pass

        # добавим остаток от последнего gtin_listб т.к. он не обрабатывается в цикле while
        # curent_gtin_list = gtin_list
        # current_attr_df = get_curent_df(curent_gtin_list=curent_gtin_list, attr_list=attr_list, url=url, auth=auth)
        # full_attr_df = pd.concat([full_attr_df, current_attr_df], axis = 0)


    else:
        curent_gtin_list = gtin_list

        full_attr_df = get_curent_df(curent_gtin_list=curent_gtin_list, attr_list=attr_list, url=url, auth=auth)

        # TODO ВНИМАНИЕ! СЕЙЧАС GETTABLE ВОЗВРАЩАЕТ STRING. Подэтому необходимо добавить дефолтное значение в функцию, чтоб переключала режимы )
    df_as_a_string = full_attr_df.to_string(index=False, header=False)
    return df_as_a_string

def splitter(x, index):
    try:
        y = list(x.split(' '))[index]
        return y
    except Exception as e:
        y = 'index error'
        return y


def splitter_joiner(x):
    y = ' '.join(list(x.split(' '))[4:])
    if y == '':
        y = 'index error'
    else:
        y = y
    return y