import pandas as pd
import numpy as np
from requests.auth import HTTPBasicAuth
import yaml
import requests
import xmltodict

def general_parameter_parser(XML_parsed_to_dict, errcode, global_record=None):
    '''
    Данная функция вызывается из table_from_dict_builder и парсит из переданного словаря ОСНВНЫЕ параметры рекорда (карточки).
    :param XML_parsed_to_dict:
    :param errcode:
    :param global_record:
    :return:
    '''
    print('gpp16: проверим что с globalrecord')
    if global_record is None:
        common_part = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']
        global_record = 0
        print('gpp20:  globalrecord = {}, common_part = {}'.format(global_record,common_part))
    else:
        common_part = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]
        print('gpp23:  globalrecord = {}, common_part = {}'.format(global_record, common_part))
    df = pd.DataFrame()
    print('gpp25:  df = {}'.format(df))
    df.loc[global_record, 'errorcode'] = errcode
    print('gpp27:  df = {}'.format(df))
    try:
        df.loc[global_record, 'variant'] = common_part['@variant']
    except KeyError:
        df.loc[global_record, 'variant'] = np.nan

    try:
        df.loc[global_record, 'basekey'] = common_part['baseKey']
    except KeyError:
        df.loc[global_record, 'basekey'] = common_part['ReqValues']['reqValue']

    try:
        df.loc[global_record, 'idRecord'] = common_part['@idRecord']
    except KeyError:
        df.loc[global_record, 'idRecord'] = np.nan

    print('=' * 40)

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
    			<ns1:GTIN>4660085970269</ns1:GTIN>

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
    web_attr_list = ['PROD_DESC']

    df = general_parameter_parser(XML_parsed_to_dict=XML_parsed_to_dict, global_record=None, errcode= 0)

    print('\ndf = \n {}'.format(df.to_string()))