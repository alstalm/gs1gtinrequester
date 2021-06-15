import xmltodict
import pandas as pd
import requests
import yaml
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

''' ЗАДАИМ ПАРАМЕТРЫ'''
with open('params.yaml', 'r', encoding='UTF-8' ) as f:
    params = yaml.safe_load(f)

url = params['url']
login = params['login']
password = params['password']
auth = HTTPBasicAuth(login, password)

Attributes_list = params['Attributes_list']

full_body = '''<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="urn:org.gs1ru.gs46.intf">
	<SOAP-ENV:Body>
		<ns1:GetItemByGTIN>
		    <ns1:GTIN>4605865487766</ns1:GTIN>
			<ns1:GTIN>4620065184307</ns1:GTIN>
			<!--<ns1:GTIN>886059754862</ns1:GTIN>-->
			<!--<ns1:GTIN>4620065184291</ns1:GTIN>-->
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
#print('XML_parsed_to_dict = \n', XML_parsed_to_dict)


def table_bulider(XML_parsed_to_dict, attr_list):
    df = pd.DataFrame()
    # errCode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['Result']['@errCode']
    # if errCode == '0': # Добавлено 21.01.2021

    try:
        # проверим есть ли второй рекорд. для этого попытаемся найти в нем значение варианта
        variant_from_second_redord_for_try =            XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][1]['@variant']
        if isinstance(variant_from_second_redord_for_try,str) : # просто проверяем что есть второй рекорд у которого есть хоть какое-то значение варинта
            print('рекордов больше чем 1. успешно прошли try. идем в цикл парсинга нескольких записей \n')
            # если на входе получили несколько рекордов то для каждого рекорда
            for record in range(len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'])):
                print('вошли в цикл парсинга записи № {}'.format(record))
                # если errCode не равен 0, то
                errorcode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['result']['@errCode']
                if int(errorcode) != 0:
                    # просто записываем значение errcode и variant и переходим к следующему рекорду
                    df.loc[record, 'errorcode'] = errorcode
                    variant = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['@variant']
                    df.loc[record, 'variant'] = variant
                    basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['baseKey']
                    df.loc[record, 'basekey'] = basekey
                    idRecord = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['@idRecord']
                    df.loc[record, 'idRecord'] = idRecord
                    print('строка 67: текущий датафрейм выглядит так: \n', df.to_string())
                    print('='*40)

                # иначе, (если errorCode = 0)
                else:
                    print('строка 75: вошли в else')
                    # записываем текущее значение errCode
                    df.loc[record, 'errorcode'] = errorcode
                    variant = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['@variant']
                    df.loc[record, 'variant'] = variant
                    basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['baseKey']
                    df.loc[record, 'basekey'] = basekey
                    idRecord = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['@idRecord']
                    df.loc[record, 'idRecord'] = idRecord
                    print('строка 85: теукщий df = \n', df.to_string())

                    # и начинаем парсить параметры
                    for subrecord in range(len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['BaseAttributeValues']['value'])):
                        attrName = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['BaseAttributeValues']['value'][subrecord]['@baseAttrId']
                        attrValue = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][record]['BaseAttributeValues']['value'][subrecord]['@value']
                        # print('attrName', attrName)
                        # print('attrValue', attrValue)
                        if attrName in attr_list:
                            df.loc[record, attrName] = attrValue


    except KeyError:
    # если на входе только один рекорд
        print('Рекордов не больше 2-х')

        # если errCode не равен 0, то
        errcode = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode']
        print('errcode на строке 89 =', errcode)
        print(type(errcode))

        if int(errcode) !=0:
            #  просто записываем значение errcode и variant
            print('провалились в If на строке 91')

            df.loc[0, 'errCode'] = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode']
            variant = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['@variant']
            df.loc[0, 'variant'] = variant
            basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['baseKey']
            df.loc[0, 'basekey'] = basekey
            idRecord = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['@idRecord']
            df.loc[record, 'idRecord'] = idRecord

            print('errCode =',XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode'])
            print('variant =', variant)

        # иначе
        else:
            # записываем значение errcode и variant
            df.loc[0, 'errCode'] = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['result']['@errCode']
            variant = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['@variant']
            df.loc[0, 'variant'] = variant
            basekey = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['baseKey']
            print('строка 118: baseKey =', basekey)
            df.loc[0, 'basekey'] = basekey
            idRecord = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['@idRecord']
            df.loc[record, 'idRecord'] = idRecord

            # и начинаем парсить параметры
            print('переходим к циклу')
            for subrecord in range(len(XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['BaseAttributeValues']['value'])):
                print('зашли в цикл')
                attrName = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['BaseAttributeValues']['value'][subrecord]['@baseAttrId']
                attrValue = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']['BaseAttributeValues']['value'][subrecord]['@value']
                print('текущее значение subrecord', subrecord)
                print('текущее значение attrName', attrName)
                print('текущее значение attrValue', attrValue)
                if attrName in attr_list:
                    df.loc[0, attrName] = attrValue

    ##########################################################
    '''
    на этом уровне сперва try тип list (несколько записей в InfoTypeRecords) except dict (одна запись в InfoTypeRecords)
     и внутри try/except все что в test2 насиная со строки 52
    '''
    #except Exception as e:
    #    print('Exception =', e)

    ###########################
    #   df=df.copy().astype({'variant':'int32'}) # поменяем тип столбца variant
    ############################
    # изменим порядок первых двух столбцов

    cols = df.columns.tolist()
    print('\nстрока 149: cols = ',cols )

    newcols = []

    y = cols.pop(0)
    x = cols.pop(0)
    newcols.append(x)
    newcols.append(y)
    newcols.extend(cols)
    df = df[newcols].copy()
    # cols = cols[-1:] + cols[:-1]
    return df

if __name__ == '__main__':
    print('start testing table_bulider function \n')

    df = table_bulider(XML_parsed_to_dict=XML_parsed_to_dict, attr_list=Attributes_list)
    print('\ndf = \n {}'.format(df.to_string()))
