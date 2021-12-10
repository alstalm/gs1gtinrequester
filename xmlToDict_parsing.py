import xmltodict
import pandas as pd
import requests
import yaml
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from baseAttribute_parsing import base_attribute_parser
from webAttributes_parsing import web_attribute_parser
from TNVED_parsing import TNVED_codes_parser
from generalParameters_parsing import general_parameter_parser
import numpy as np

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# print('XML_parsed_to_dict = \n', XML_parsed_to_dict)

# TODO Добавить дефолтный параметр trytoreaddescr=True - с которым функция будем работать как сейчас, а если False, то вычитывать только value
def table_from_dict_builder(XML_parsed_to_dict, attr_list):
    '''
    Данная функция принимает на вход из функции get_curent_df словарь и
    1. Сама парсит базовые атрибуты
    2. для ЦУИ атрибутов вызывает функцию web_attribute_parser и формирует датафрейм из базовых и WEB-атрибутов.

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

                print('xtdp40: \n                          ==============  вошли в цикл парсинга записи № {} ============== \n'.format(global_record))

                errcode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['result']['@errCode']

                if int(errcode) != 0:
                    # просто записываем значение errcode и variant и переходим к следующему рекорду
                    general_parameters_df = general_parameter_parser(XML_parsed_to_dict=XML_parsed_to_dict, errcode=errcode, global_record=global_record)
                    current_df = general_parameters_df

                # иначе, (если errorCode = 0)
                else:

                    # запишем в текущий датафрейм основные параметры рекорда (errCode, variant etc)
                    general_parameters_df = general_parameter_parser(XML_parsed_to_dict=XML_parsed_to_dict, errcode=errcode, global_record=global_record)
                    base_attribute_df = base_attribute_parser(XML_parsed_to_dict=XML_parsed_to_dict, basic_attr_list=attr_list, global_record=global_record)
                    web_attributes_df = web_attribute_parser(XML_parsed_to_dict=XML_parsed_to_dict, global_record=global_record, web_attr_list=attr_list)
                    TNVED_codes_df = TNVED_codes_parser(XML_parsed_to_dict=XML_parsed_to_dict, global_record=global_record, tnved_attr_list=attr_list)
                    # сконкатинируем по горизонтали датафрейм базовых атрибутов и web-атрибутов
                    print('xtdp95: объединим базовые и web-атрибуты: df= \n')
                    current_df = pd.concat([general_parameters_df, base_attribute_df, web_attributes_df, TNVED_codes_df], axis=1)
                    print('tb87: после объединения current_df=\n', current_df.to_string())
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
            general_parameters_df = general_parameter_parser(XML_parsed_to_dict=XML_parsed_to_dict, errcode=int(errcode), global_record=None)
            current_df = general_parameters_df
        else:
            # запишем в текущий датафрейм основные параметры рекорда (errCode, variant etc)
            general_parameters_df = general_parameter_parser(XML_parsed_to_dict=XML_parsed_to_dict, errcode=errcode, global_record=None)
            base_attribute_df = base_attribute_parser(XML_parsed_to_dict=XML_parsed_to_dict, basic_attr_list=attr_list, global_record=None)
            web_attributes_df = web_attribute_parser(XML_parsed_to_dict=XML_parsed_to_dict, web_attr_list=attr_list, global_record=None)
            TNVED_codes_df = TNVED_codes_parser(XML_parsed_to_dict=XML_parsed_to_dict, tnved_attr_list=attr_list, global_record=None)

            current_df = pd.concat([general_parameters_df, base_attribute_df, web_attributes_df, TNVED_codes_df], axis=1)

        full_df = current_df.copy()

    # изменим порядок первых двух столбцов
    #cols = full_df.columns.tolist()
    #newcols = []
    #y = cols.pop(0)
    #x = cols.pop(0)
    #newcols.append(x)
    #newcols.append(y)
    #newcols.extend(cols)

    #full_df = full_df[newcols].copy()
    # cols = cols[-1:] + cols[:-1]
    return full_df


if __name__ == '__main__':
    print('start testing table_bulider function \n')

    ''' ЗАДАИМ ПАРАМЕТРЫ'''
    with open('params.yaml', 'r', encoding='UTF-8') as f:
        params = yaml.safe_load(f)

    url = params['url']
    login = params['login']
    password = params['password']
    auth = HTTPBasicAuth(login, password)
    Attributes_list = params['Attributes_list']

    full_body = '''<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="urn:org.gs1ru.gs46.intf">
    	<SOAP-ENV:Body>
    		<ns1:GetItemByGTIN>
    		    <!--<<ns1:GTIN>886059754862</ns1:GTIN>-->
    		    <!--<ns1:GTIN>4605865487766</ns1:GTIN>-->
    		    <!--<ns1:GTIN>4620065184307</ns1:GTIN>-->
    			
    			<ns1:GTIN>4600840899077</ns1:GTIN>
    			<!--<ns1:GTIN>4620065184307</ns1:GTIN>-->

    			<ns1:lang>RU</ns1:lang>
    			<ns1:showMeta>0</ns1:showMeta>
    			<ns1:noCache>0</ns1:noCache>
    			<ns1:loadChangeVersion>0</ns1:loadChangeVersion>
    			<ns1:noCascade>0</ns1:noCascade>
    			<ns1:noGepir>0</ns1:noGepir>
    		</ns1:GetItemByGTIN>
    	</SOAP-ENV:Body>
    </SOAP-ENV:Envelope>'''

    resp = requests.post(url=url, data=full_body, auth=auth, verify=False)
    answer = resp.content
    XML_parsed_to_dict = xmltodict.parse(answer)

    df = table_from_dict_builder(XML_parsed_to_dict=XML_parsed_to_dict, attr_list=Attributes_list)
    print('\ndf = \n {}'.format(df.to_string()))
    try:
        df.to_excel('parsed_attributes.xlsx', index=True, sheet_name='sheet_1')
    except Exception as e:
        print('------------------\nво время записи в файл произошла ошибка:', e)
