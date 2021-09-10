import xmltodict
import pandas as pd
import requests
import yaml
from pprint import pprint
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from attrvalue_extractor_from_mapping_table import get_mapping_value

def web_attribute_parser(XML_parsed_to_dict, web_attr_list, global_record=None):
    if global_record is None:
        common_part = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']
        global_record = 0
    else:
        common_part = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]

    ########################################################################################################################
    # распасим WEB-атрибуты
    cash = {}

    df = pd.DataFrame()
    print('\n++++++++++++++++ ПАРСИНГ WEB-АТРИБУТОВ ++++++++++++++++\n')
    for infotype_record in range(len(common_part['InfoTypeRecords']['record'])):
        try: # если в infotype_record есть AttributeValues и это или список или словарь
            if  isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'], list): # если в InfoTypeRecords/record/AttributeValues несколько value
                print('wap28: в infotype_record N={} в /AttributeValues содержится несколько записей value'.format(infotype_record)) # !!!!!!!!!!!!

                for value_number in range(len(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'])):
                    try: # проверяем если есть вложенный мультиатрибут
                        if     isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@extAttrId'], str) \
                            or isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][0]['@extAttrId'], str):
                            # предположим, что вложенный мультиатрибут есть, тогда:
                            print('\nwap35: в infotype_record N={} в /AttributeValues в записи  value N={} есть один или несколько вложеных MultValue'.format(infotype_record, value_number))
                            if isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@extAttrId'], str): # если multivalue одно
                                web_attr_id =    common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@extAttrId']
                                if web_attr_id not in web_attr_list:
                                    continue
                                else:
                                 #   try:
                                 #       web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@descr']
                                 #       df.loc[global_record, web_attr_id] = web_attr_descr
                                 #       print('wap46: текущее значение df= \n {}\n'.format(df))
                                 #   except KeyError:
                                    web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@value']
                                    cash, mapping_value = get_mapping_value(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value)
                                    df.loc[global_record, web_attr_id] = mapping_value
                                    print('wap49: текущее значение df= \n {}\n'.format(df))

                            else: # если multivalue  НЕ одно
                                print('\nwap52: в infotype_record N={} в /AttributeValues в записи  value N={} ВОЗМОЖНО несколько записей MultValue. проверим.'.format(infotype_record, value_number))

                                web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][MultValue_N]['@extAttrId']
                                if web_attr_id not in web_attr_list:
                                    continue
                                else:
                                    multiattrlist = []
                                    for MultValue_N in range(len(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][MultValue_N])):
                                        #try:
                                        #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][MultValue_N]['@descr']
                                        #    multiattrlist.append(web_attr_descr)

                                        #except KeyError:
                                        web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][MultValue_N]['@value']
                                        cash, mapping_value = get_mapping_value(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value)
                                        multiattrlist.append(mapping_value)

                                    df.loc[global_record, web_attr_id] = multiattrlist
                    except KeyError: # ЗДЕСЬ  РАЗБИРАЕМ ВЕБ-АТРИБУТЫ

                        #for value_number in range(len(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'])):
                        # следующие строки были на 7-м отступе и строка 70 не былазакоментирована
                        web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['@extAttrId']
                        print('wap75: в infotype_record={} value_number={} парсим web_attr_id={}'.format(infotype_record, value_number, web_attr_id))

                        if web_attr_id not in web_attr_list:
                            print('wap78: web_attr_id = {} НЕ в списке заданных атрибутов :(\n'.format(web_attr_id))
                            continue
                        else:
                            #try:
                            #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['@descr']
                            #    print('wap79: текущее значение web_attr_descr=', web_attr_descr)
                            #    df.loc[global_record, web_attr_id] = web_attr_descr
                            #    print('wap93: текущее значение df= \n {}\n'.format(df))

                            #except KeyError:
                            #    print('wap85:  !!! в web_attr_id={} параметра @descr НЕТ '.format(web_attr_id))
                            web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['@value']
                            cash, mapping_value = get_mapping_value(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value)
                            df.loc[global_record, web_attr_id] = mapping_value

                            print('wap93: текущее значение df= \n {}\n'.format(df))
                            #except:
                            #    print('wap92: в infotype_record N={} в /AttributeValues в записи  value N={} для web_attr_id {} что-то НЕОЖИДАННОЕ'.format(infotype_record, value_number, web_attr_id))
                    except : #
                        print('wap97: в infotype_record N={} в /AttributeValues в записи  value N={} что-то НЕОЖИДАННОЕ'.format(infotype_record, value_number))

            if isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'], dict): # если только одна запись value в AttributeValues
                print('wap100: в infotype_record N={} в AttributeValues/value только одна запись value.'.format(infotype_record))
                try:  # проверяем если мультиатрибут один
                    if isinstance(       common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@extAttrId'], str) \
                            or isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][0]['@extAttrId'], str) :
                        print('wap104: в записи infotype_record N={} в /AttributeValues в единственной записи value есть одна или несколько MultValue'.format(infotype_record))
                        if isinstance(       common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@extAttrId'], str):
                            web_attr_id =    common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@extAttrId']

                            #try:
                            #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@descr']
                            #    # if isinstance(web_attr_descr, str):
                            #    if web_attr_id not in web_attr_list:
                            #        print('wap123: web_attr_id {} not in web_attr_list web_attr_list'.format(web_attr_id))
                            #    else:
                            #        df.loc[global_record, web_attr_id] = web_attr_descr
                            #        print('wap126: текущее значение df= \n {}\n'.format(df))

                            #except KeyError:

                            if web_attr_id not in web_attr_list:
                                print('wap120: web_attr_id {} НЕ в web_attr_list'.format(web_attr_id))
                            else:
                                web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@value']
                                cash, mapping_value = get_mapping_value(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value)
                                df.loc[global_record, web_attr_id] = mapping_value
                                print('wap125: текущее значение df= \n {}\n'.format(df))


                        else:
                            print('\nwap129: в infotype_record N={} в /AttributeValues в единственной записи value НЕСКОЛЬКО записей MultValue'.format(infotype_record))
                            multiattrlist = []
                            for MultValue_N in range(len(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][MultValue_N])):
                                web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][MultValue_N]['@extAttrId']

                                #try:
                                #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][MultValue_N]['@descr']
                                #    # if isinstance(web_attr_descr, str):
                                #    if web_attr_id not in web_attr_list:
                                #        continue
                                #    else:
                                #        multiattrlist.append(web_attr_descr)
                                #        print('wap150: текущее значение df= \n {}\n'.format(df))

                                #except KeyError:
                                web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][MultValue_N]['@value']
                                cash, mapping_value = get_mapping_value(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value)
                                if web_attr_id not in web_attr_list:
                                    continue
                                else:
                                    multiattrlist.append(mapping_value)

                                    print('wap150: текущее значение df= \n {}\n'.format(df))
                            df.loc[global_record, web_attr_id] = multiattrlist

                except KeyError:

                    web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['@extAttrId']

                    #try:
                    #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['@descr']
                    #    # if isinstance(web_attr_descr, str):
                    #    if web_attr_id not in web_attr_list:
                    #        print('wap170: web_attr_id {} not in web_attr_list web_attr_list'.format(web_attr_id))
                    #    else:
                    #        df.loc[global_record, web_attr_id] = web_attr_descr
                    #        print('wap173: текущее значение df= \n {}\n'.format(df))

                    #except KeyError:
                    web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['@value']
                    cash, mapping_value = get_mapping_value(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value)
                    if web_attr_id not in web_attr_list:
                        print('wap170: web_attr_id {} не в web_attr_list'.format(web_attr_id))
                    else:
                        df.loc[global_record, web_attr_id] = mapping_value

                        print('wap174: текущее значение df= \n {}\n'.format(df))

                except :  #
                    print('wap177: в infotype_record N={} в /AttributeValues в записи  value N={} что-то НЕОЖИДАННОЕ'.format(infotype_record, value_number))
        except TypeError: #  нет  пути InfoTypeRecords/record/AttributeValues нет или там только одно value
            print('wap179: в infotype_record {} НЕ содержится AttributeValues'.format(infotype_record))

    return df

if __name__ == '__main__':
    ''' ЗАДАИМ ПАРАМЕТРЫ'''
    with open('params.yaml', 'r', encoding='UTF-8') as f:
        params = yaml.safe_load(f)

    url = params['url']
    login = params['login']
    password = params['password']
    auth = HTTPBasicAuth(login, password)


    full_body_test = '''<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="urn:org.gs1ru.gs46.intf">
    	<SOAP-ENV:Body>
    		<ns1:GetItemByGTIN>
    			<ns1:GTIN>4605865487766</ns1:GTIN>

    			<ns1:lang>RU</ns1:lang>
    			<ns1:showMeta>0</ns1:showMeta>
    			<ns1:noCache>0</ns1:noCache>
    			<ns1:loadChangeVersion>0</ns1:loadChangeVersion>
    			<ns1:noCascade>0</ns1:noCascade>
    			<ns1:noGepir>0</ns1:noGepir>
    		</ns1:GetItemByGTIN>
    	</SOAP-ENV:Body>
    </SOAP-ENV:Envelope>'''

    resp = requests.post(url=url, data=full_body_test, auth=auth, verify=False)
    answer = resp.content
    XML_parsed_to_dict = xmltodict.parse(answer)

    print('start testing table_bulider function \n')
    web_attr_list = ['PROD_COVER_GTIN', 'PROD_DESC', 'PROD_COUNT', 'WEB_90001854', 'WEB_90001722', 'WEB_90001723', 'WEB_90001731', 'WEB_90001709', 'WEB_90000196', 'WEB_90001809', 'WEB_90000626']

    df = web_attribute_parser(XML_parsed_to_dict=XML_parsed_to_dict,
                              global_record=0,
                              web_attr_list=web_attr_list)
    print('\ndf = \n {}'.format(df.to_string()))
