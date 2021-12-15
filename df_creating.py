import xmltodict
import pandas as pd
import requests
from attributes_extractor import AtrrValueParesr

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


def combine_gtin_list(GTIN_list):
    '''
    gtin_list_combiner - возвращает строку для дальнейшей вставки в XML для аформирования ОДНОГО запроса из более чем одного GTIN
    :param GTIN_list: список номеров GTIN. Список может быть как из переменных типа string, так и Integer
    :return: фрагмент XML-объекта в формате string, содержащий массив записей, готовый для вставки в тело запроса
    пример результата: '<ns1:GTIN>4620099582759</ns1:GTIN><ns1:GTIN>4620099582760</ns1:GTIN>'
    '''
    row_set = ''
    for gtin in GTIN_list:
        row = '<ns1:GTIN>' + str(gtin) + '</ns1:GTIN>'
        row_set = row_set + row

    return row_set


# TODO Добавить дефолтный параметр trytoreaddescr=True - с которым функция будем работать как сейчас, а если False, то вычитывать только value
def table_from_dict_builder(XML_parsed_to_dict, attr_list):
    '''
    Данная функция принимает на вход из функции get_curent_df словарь и
    1. ВЫзывает парсеры основных параметров, базовых атрибутов и WEB атрибутов


    '''
    full_df = pd.DataFrame()
    # в случае, если в XML только один рекорд
    try:
        variant_from_second_redord_for_try = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][1]['@variant']
        # TODO здесь надо заменить на вычисление длинны списка вместо попытки распарсить значение @variant для второго рекорда
        # что-то типа этого: len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'])

        if isinstance(variant_from_second_redord_for_try, str):  # просто проверяем что есть второй рекорд у которого есть хоть какое-то значение варинта
            # print('tb25: рекордов больше чем 1. успешно прошли try. идем в цикл парсинга нескольких записей \n')
            # если на входе получили несколько рекордов то для каждого рекорда
            for global_record in range(len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'])):

                #print('xtdp40: \n                          ==============  вошли в цикл парсинга записи № {} ============== \n'.format(global_record))

                errcode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['result']['@errCode']

                if int(errcode) != 0:
                    # просто записываем значение errcode и variant и переходим к следующему рекорду
                    parser = AtrrValueParesr(XML_parsed_to_dict=XML_parsed_to_dict, attr_list=attr_list, errcode=errcode, global_record = global_record)
                    general_parameters_df = parser.general_parameters()
                    current_df = general_parameters_df

                # иначе, (если errorCode = 0)
                else:

                    # запишем в текущий датафрейм основные параметры рекорда (errCode, variant etc)
                    parser = AtrrValueParesr(XML_parsed_to_dict=XML_parsed_to_dict, attr_list=attr_list, errcode=errcode, global_record=global_record)
                    general_parameters_df = parser.general_parameters()
                    base_attribute_df = parser.base_attributes()
                    web_attributes_df = parser.web_attribute_parser()
                    TNVED_codes_df = parser.TNVED_codes()
                    # сконкатинируем по горизонтали датафрейм базовых атрибутов и web-атрибутов
                    #print('xtdp95: объединим базовые и web-атрибуты: df= \n')
                    current_df = pd.concat([general_parameters_df, base_attribute_df, web_attributes_df, TNVED_codes_df], axis=1)
                    #print('tb87: после объединения current_df=\n', current_df.to_string())
                    # print('\n')

                if len(full_df) < 1:
                    full_df = current_df.copy()
                else:
                    full_df = pd.concat([full_df, current_df], axis=0)

    # в случае, если в XML только один рекорд
    except KeyError:

        errcode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode']

        if int(errcode) != 0:
            #  просто записываем значение errcode и variant
            parser = AtrrValueParesr(XML_parsed_to_dict=XML_parsed_to_dict, attr_list=attr_list, errcode=errcode, global_record=None)
            general_parameters_df = parser.general_parameters()
            current_df = general_parameters_df
        else:
            # запишем в текущий датафрейм основные параметры рекорда (errCode, variant etc)
            parser = AtrrValueParesr(XML_parsed_to_dict=XML_parsed_to_dict, attr_list=attr_list, errcode=errcode, global_record=None)
            general_parameters_df = parser.general_parameters()
            base_attribute_df = parser.base_attributes()
            web_attributes_df = parser.web_attribute_parser()
            TNVED_codes_df = parser.TNVED_codes()

            current_df = pd.concat([general_parameters_df, base_attribute_df, web_attributes_df, TNVED_codes_df], axis=1)

        full_df = current_df.copy()

    return full_df


# TODO Reformat Code (beutify) ALt+CTR+L
@retry(TimeoutError, tries=5, delay=1, max_delay=180, backoff=3)
def get_current_df(curent_gtin_list, attr_list, url, auth):
    '''
    Данная функция формирует датафрейм по одному запросу.
    1. С использованием функции gtin_list_combiner фомрирует один запрос для запроса в ГС1 по нескольким номерам GTIN.
    2. По полученнуму на входе списку гтинов делает запрос в ГС1.
    3. При помощи xmltodict пробразует полученноый XML в словарь и передает его в функцию table_from_dict_builder
    4. При помощи table_from_dict_builder парсит полученный словарь в таблицу
    4. Формирует на выходе датафрейм.
    :param curent_gtin_list: string/integer список GTIN для передачи в функцию gtin_list_combiner (см. выше)
    :param attr_list:
    :param url:
    :param auth:
    :return:
    '''

    body_prefix = """<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="urn:org.gs1ru.gs46.intf"><SOAP-ENV:Body><ns1:GetItemByGTIN>"""
    body_postfix = """<ns1:lang>RU</ns1:lang><ns1:showMeta>0</ns1:showMeta><ns1:noCache>0</ns1:noCache><ns1:loadChangeVersion>0</ns1:loadChangeVersion><ns1:noCascade>0</ns1:noCascade><ns1:noGepir>1</ns1:noGepir></ns1:GetItemByGTIN></SOAP-ENV:Body></SOAP-ENV:Envelope>"""

    body_core = combine_gtin_list(GTIN_list=curent_gtin_list)

    full_body = ''.join([body_prefix, body_core, body_postfix])
    # Задаим параметры для Запроса

    # Соберем запрос и распарсим
    resp = requests.post(url=url, data=full_body, auth=auth, verify=False)
    status_code = resp.status_code
    if status_code == 200:
        answer = resp.content
        XML_parsed_to_dict = xmltodict.parse(answer)
        curent_attr_df = table_from_dict_builder(XML_parsed_to_dict, attr_list)
    else:

        curent_attr_df = pd.DataFrame({'http_code': [status_code]})

    return curent_attr_df


def one_by_one_requester(source_df):
    final_output_df = pd.DataFrame()
    for i in range(len(source_df)):
        gtin = source_df.loc[i,'GTIN']
        gs1attr = source_df.loc[i,'GS1Attr']
        current_output_df = get_current_df(curent_gtin_list= [gtin], attr_list=[gs1attr], url = url, auth= auth)

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
            #print('NV71: final_output_df = ',final_output_df)

    return final_output_df


def batch_requester(full_gtin_list, attr_list, url=url, auth=auth, batch_size=1):
    """
    Данная функция суммирует итоговый датафрейм из датафреймов полученных в серии запросов.
    1. Разбивает список GTIN для запроса батчами (по несколько GTIN за один запрос)
    2. По каждому запросу вызывает функцию get_curent_df получает датафрейм
    3. Объединяет запросы в один датафарейм

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

    if len(full_gtin_list) >= batch_size:
        rest_of_gtin_list = full_gtin_list
        while len(rest_of_gtin_list) >= batch_size:

            current_gtin_list = rest_of_gtin_list[:batch_size]
            rest_of_gtin_list = rest_of_gtin_list[batch_size:]

            try:
                current_attr_df = get_current_df(curent_gtin_list=current_gtin_list, attr_list=attr_list, url=url, auth=auth)
                if len(full_attr_df) < 1:
                    full_attr_df = current_attr_df.copy()
                else:
                    full_attr_df = pd.concat([full_attr_df, current_attr_df], axis=0)
            except:

                pass
        # чтобы не потерять остаток от цикла

        current_attr_df = get_current_df(curent_gtin_list=rest_of_gtin_list, attr_list=attr_list, url=url, auth=auth)
        full_attr_df = pd.concat([full_attr_df, current_attr_df], axis=0)

        # добавим остаток от последнего gtin_listб т.к. он не обрабатывается в цикле while
        # curent_gtin_list = gtin_list
        # current_attr_df = get_curent_df(curent_gtin_list=curent_gtin_list, attr_list=attr_list, url=url, auth=auth)
        # full_attr_df = pd.concat([full_attr_df, current_attr_df], axis = 0)


    else:
        current_gtin_list = full_gtin_list

        full_attr_df = get_current_df(curent_gtin_list=current_gtin_list, attr_list=attr_list, url=url, auth=auth)



    return full_attr_df











