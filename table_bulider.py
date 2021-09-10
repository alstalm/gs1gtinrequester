import xmltodict
import pandas as pd
import requests
import yaml
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from web_attributes_parser import web_attribute_parser
import numpy as np

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# print('XML_parsed_to_dict = \n', XML_parsed_to_dict)

#TODO Добавить дефолтный параметр trytoreaddescr=True - с которым функция будем работать как сейчас, а если False, то вычитывать только value
def table_bulider(XML_parsed_to_dict, attr_list):
    full_df = pd.DataFrame()
    # errCode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['Result']['@errCode']
    # if errCode == '0': # Добавлено 21.01.2021
    print('\n================= вошли в table_bulider ================= \n')
    print('XML_parsed_to_dict\n',XML_parsed_to_dict)
    try:
        # проверим есть ли второй рекорд. для этого попытаемся найти в нем значение варианта
        variant_from_second_redord_for_try = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][1]['@variant']
        #TODO здесь надо заменить на вычисление длинны списка вместо попытки распарсить значение @variant для второго рекорда
        # что-то типа этого: len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'])

        if isinstance(variant_from_second_redord_for_try, str):  # просто проверяем что есть второй рекорд у которого есть хоть какое-то значение варинта
            print('tb25: рекордов больше чем 1. успешно прошли try. идем в цикл парсинга нескольких записей \n')
            # если на входе получили несколько рекордов то для каждого рекорда
            for global_record in range(len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'])):
                # если датафрейм не пустой, то объединим по вериткали

                current_df = pd.DataFrame()
                print('tb31: \n                          ==============  вошли в цикл парсинга записи № {} ============== \n'.format(global_record))
                # если errCode не равен 0, то
                errorcode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['result']['@errCode']
                if int(errorcode) != 0:
                    # просто записываем значение errcode и variant и переходим к следующему рекорду
                    current_df.loc[global_record, 'errorcode'] = errorcode
                    variant = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['@variant']
                    current_df.loc[global_record, 'variant'] = variant
                    basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['baseKey']
                    current_df.loc[global_record, 'basekey'] = basekey
                    global_record_id = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['@idRecord']
                    current_df.loc[global_record, 'idRecord'] = global_record_id
                    print('tb43: текущий датафрейм выглядит так: \n', current_df.to_string())
                    print('=' * 40)

                # иначе, (если errorCode = 0)
                else:
                    print('tb48: для глобального рекорда {} errorCode=0'.format(global_record))
                    # записываем текущее значение errCode
                    current_df.loc[global_record, 'errorcode'] = errorcode
                    variant = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['@variant']
                    current_df.loc[global_record, 'variant'] = variant
                    basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['baseKey']
                    current_df.loc[global_record, 'basekey'] = basekey
                    global_record_id = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['@idRecord']
                    current_df.loc[global_record, 'idRecord'] = global_record_id
                    print('tb57: записали основные параметры глобального рекорда и сохранили в датафрейм. текущий df = \n', current_df.to_string())

                    # и начинаем парсить параметры
                    for base_attr_val_record in range(
                            len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['BaseAttributeValues']['value'])):
                        attrName = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['BaseAttributeValues']['value'][base_attr_val_record][
                            '@baseAttrId']

                        # print('attrName', attrName)
                        # print('attrValue', attrValue)
                        if attrName in attr_list:
                            try:
                                descr = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['BaseAttributeValues']['value'][
                                    base_attr_val_record]['@descr']
                                current_df.loc[global_record, attrName] = descr
                            except:
                                attrValue = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]['BaseAttributeValues']['value'][base_attr_val_record]['@value']
                                current_df.loc[global_record, attrName] = attrValue
                        else:
                            print('tb76: attrName {} не в списке искомых атрибутов')
                    ###############
                    # здесь начинать парсинг веб-атрибутов
                    # вызовем парсер web-атрибутов
                    print('tb80: прошлись циклом по базовые атрибутам глобального рекорда {}'. format(global_record))
                    print('tb81: текущее значение датафрейма с глобальными атрибутами: current_df = \n', current_df.to_string())
                    print('вызовем функцию web_attribute_parser')
                    current_df2 = web_attribute_parser(XML_parsed_to_dict=XML_parsed_to_dict, global_record=global_record, web_attr_list=attr_list)
                    # сконкатинируем по горизонтали датафрейм базовых атрибутов и web-атрибутов
                    print('tb85: объединим базовые и web-атрибуты: df= \n')
                    current_df = pd.concat([current_df,current_df2], axis = 1)
                    print('tb87: после объединения current_df=\n', current_df.to_string())
                    print('\n')

                if len(full_df) < 1:
                    full_df = current_df.copy()
                else:
                    full_df = pd.concat([full_df, current_df], axis=0)


    except KeyError:
        # если на входе только один рекорд
        print('tb98: второго рекорда не существует')

        # если errCode не равен 0, то
        errcode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode']
        print('tb102: errcode =', errcode)

        current_df = pd.DataFrame()

        if int(errcode) != 0:
            #  просто записываем значение errcode и variant
            print('tb108: в текущем глобалрекорде errcode !=0')

            current_df.loc[0, 'errorcode'] = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode']
            try:
                variant =                      XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['@variant']
            except KeyError:
                variant = np.nan
            current_df.loc[0, 'variant'] = variant
            try:
                basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['baseKey']
            except KeyError:
                basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['ReqValues']['reqValue']


            current_df.loc[0, 'basekey'] = basekey

            try:
                global_record_id = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['@idRecord']
            except KeyError:
                global_record_id = np.nan
            current_df.loc[0, 'idRecord'] = global_record_id



            print('tb132: errCode =', XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode'])
            print('tb133: variant =', variant)

        # иначе
        else:
            # записываем значение errcode и variant
            current_df.loc[0, 'errorcode'] = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode']
            variant = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['@variant']
            current_df.loc[0, 'variant'] = variant
            basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['baseKey']
            print('tb142:: baseKey =', basekey)
            current_df.loc[0, 'basekey'] = basekey
            global_record_id = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['@idRecord']
            current_df.loc[0, 'idRecord'] = global_record_id

            # и начинаем парсить параметры
            print('переходим к циклу')
            for base_attr_val_record in range(len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['BaseAttributeValues']['value'])):
                print('tb150: зашли в цикл')
                attrName = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['BaseAttributeValues']['value'][base_attr_val_record]['@baseAttrId']
                if attrName in attr_list:
                    try:
                        descr = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['BaseAttributeValues']['value'][base_attr_val_record]['@descr']
                        current_df.loc[0, attrName] = descr
                        print('для value {} есть параметр @descr={}'.format(base_attr_val_record,descr))
                    except:
                        attrValue = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['BaseAttributeValues']['value'][base_attr_val_record][
                            '@value']
                        current_df.loc[0, attrName] = attrValue
                else:
                    print('tb162: attrName {} не в списке искомых атрибутов'.format(attrName))


            #########################
            # здесь так же надо вызвать парсер web-атрибутов
            current_df2 = web_attribute_parser(XML_parsed_to_dict=XML_parsed_to_dict, global_record=None, web_attr_list=attr_list)
            # сконкатинируем датафреймы по горизонтали axis=1
            print('tb169: а сейчас посмотрим почему не конкатинируется датафреймы..')
            print('tb170: current_df2: \n', current_df2.to_string())
            print('tb171: current_df: \n', current_df.to_string())
            current_df = pd.concat([current_df, current_df2], axis=1)
            print('tb173: после конкатинации \n', current_df.to_string())
            print('\n')

        full_df = current_df.copy()
    ##########################################################

    # except Exception as e:
    #    print('Exception =', e)

    ###########################
    #   df=df.copy().astype({'variant':'int32'}) # поменяем тип столбца variant
    ############################
    # изменим порядок первых двух столбцов

    cols = full_df.columns.tolist()
    print('\ntb188: cols = ', cols)

    newcols = []

    y = cols.pop(0)
    x = cols.pop(0)
    newcols.append(x)
    newcols.append(y)
    newcols.extend(cols)
    full_df = full_df[newcols].copy()
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

    df = table_bulider(XML_parsed_to_dict=XML_parsed_to_dict, attr_list=Attributes_list)
    print('\ndf = \n {}'.format(df.to_string()))
    try:
        df.to_excel('parsed_attributes.xlsx', index=True, sheet_name='sheet_1' )
    except Exception as e:
        print('------------------\nво время записи в файл произошла ошибка:', e)

