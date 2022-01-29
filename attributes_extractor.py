import pandas as pd
import numpy as np
from requests.auth import HTTPBasicAuth
import yaml
import requests
import xmltodict
import pymysql
from retry import retry


with open ('params.yaml', 'r', encoding='UTF-8') as f:
    params = yaml.safe_load(f)

host = params['DB_host']
user = params['DB_user']
port = params['DB_port']
password = params['DB_password']
database = params['DB_database']



class AtrrValueParesr:

    def __init__(self, XML_parsed_to_dict, errcode, attr_list, global_record=None, get_valueMap=True, verbose_result=False):
        self.XML_parsed_to_dict = XML_parsed_to_dict
        self.errcode = errcode
        self.global_record = global_record
        self.attr_list = attr_list
        self.get_valueMap = get_valueMap
        self.verbose_result = verbose_result


    def general_parameters(self):
        '''
        Данная функция вызывается из table_from_dict_builder и парсит из переданного словаря ОСНВНЫЕ параметры рекорда (карточки).
        :param XML_parsed_to_dict:
        :param errcode:
        :param global_record:
        :return:
        '''


        #print('gpp16: проверим что с globalrecord')
        if self.global_record is None:
            common_part = self.XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']
            global_record = 0
        else:
            common_part = self.XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][self.global_record]
            #print('gpp23:  globalrecord = {}, common_part = {}'.format(self.global_record, common_part))

        df = pd.DataFrame()

        try:
            df.loc[self.global_record, 'GTIN'] = common_part['baseKey']
        except KeyError:
            df.loc[self.global_record, 'GTIN'] = common_part['ReqValues']['reqValue']

        df.loc[self.global_record, 'errorcode'] = self.errcode

        try:
            df.loc[self.global_record, 'variant'] = common_part['@variant']
        except KeyError:
            df.loc[self.global_record, 'variant'] = np.nan

        df = df[['GTIN', 'errorcode', 'variant']].copy()
        if self.verbose_result:
            print('GTIN = {}, errorcode = {}, variant = {} '.format(df['GTIN'].to_string(index=False),df['errorcode'].to_string(index=False),df['variant'].to_string(index=False) ))


        #print('=' * 40)

        return df


    def base_attributes_parser(self):
        '''
        Данная функция вызывается из table_from_dict_builder и парсит из переданного словаря БАЗОВЫЕ атрибуты.
        :param XML_parsed_to_dict:
        :param web_attr_list:
        :param global_record:
        :return:
        '''
        if self.global_record is None:
            common_part = self.XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']
            global_record = 0
        else:
            common_part = self.XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][self.global_record]

        df = pd.DataFrame()
        #print('\n++++++++++++++++ ПАРСИНГ БАЗОВЫХ АТРИБУТОВ ++++++++++++++++\n')

        for base_attr_val_record in range(len(common_part['BaseAttributeValues']['value'])):
            attrName = common_part['BaseAttributeValues']['value'][base_attr_val_record]['@baseAttrId']

            if attrName in self.attr_list:
                #print('bap27: attrName {} в списке искомых атрибутов !!! '.format(attrName))
                try:
                    descr = common_part['BaseAttributeValues']['value'][base_attr_val_record]['@descr']
                    df.loc[self.global_record, attrName] = descr
                except:
                    attrValue = common_part['BaseAttributeValues']['value'][base_attr_val_record]['@value']
                    df.loc[self.global_record, attrName] = attrValue
            else:
                #print('bap35: attrName {} не в списке искомых атрибутов'.format(attrName))
                pass
        return df


    def TNVED_codes_parser(self):
        #print('\n++++++++++++++++ ПАРСИНГ ТНВЭДов ++++++++++++++++\n')
        '''
        Данная функция вызывается из xmlToDict_parsing.table_from_dict_builder и парсит атрибуты только в рекордах узла SubDataObjectRecords с dataObjectId = "PROD_CLASS"

        :param XML_parsed_to_dict:
        :param web_attr_list:
        :param global_record:
        :return:
        '''
        if self.global_record is None:
            #print('global_record is None, поэтому ')

            common_part = self.XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']
            global_record = 0

        else:

            common_part = self.XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][self.global_record]

        df = pd.DataFrame()

        if isinstance(common_part['SubDataObjectRecords']['record'], list):

            for SubDataObjectRecord in range(len(common_part['SubDataObjectRecords']['record'])):
                #print('\ntnved36: зашли в {}-й SubDataObjectRecord'.format(SubDataObjectRecord))
                try:
                    if isinstance(common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'], list) \
                            and common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['@dataObjectId'] == 'PROD_CLASS':

                        for value_number in range(len(common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'])):
                            try:  # ЗДЕСЬ  РАЗБИРАЕМ ТНВЭДЫ
                                tnved_attr_id = common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'][value_number]['@baseAttrId']
                                if tnved_attr_id not in self.attr_list:
                                    #print('tnved_attr_id = {} НЕ в списке исомых атрибутов'.format(tnved_attr_id))
                                    pass
                                else:
                                    tnved_value = common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'][value_number]['@value']
                                    #print('tnved_attr_id = {} В списке исомых атрибутов и tnved_value = {}'.format(tnved_attr_id, tnved_value))
                                    df.loc[self.global_record, tnved_attr_id] = tnved_value
                            except:  #
                                print('tnved33: в SubDataObjectRecord N={} в /BaseAttributeValues в записи  value_number={} что-то НЕОЖИДАННОЕ'.format(SubDataObjectRecord, value_number))

                    elif isinstance(common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'], dict) \
                            and common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['@dataObjectId'] == 'PROD_CLASS':
                        print('tnved39:в BaseAttributeValues есть ТОЛЬКО ОДНА запись')
                        try:
                            tnved_attr_id = common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value']['@baseAttrId']
                            tnved_value = common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value']['@value']
                            if tnved_attr_id not in self.attr_list:
                                #print('tnved40: web_attr_id {} не в web_attr_list'.format(tnved_attr_id))
                                pass
                            else:
                                df.loc[self.global_record, tnved_attr_id] = tnved_value
                        except:  #
                            print('wap177: вв SubDataObjectRecord N={}  /BaseAttributeValues в записи  value_number={} что-то НЕОЖИДАННОЕ'.format(SubDataObjectRecord, value_number))



                except TypeError:  # нет  пути SubDataObjectRecords/record/AttributeValues нет или там только одно value
                    #print('tnved46: в SubDataObjectRecord = {} НЕ содержится TNVED'.format(SubDataObjectRecord))
                    pass
        else:
            #print('запись всего одна')

            try:
                if isinstance(common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'], list) and common_part['SubDataObjectRecords']['record']['@dataObjectId'] == 'PROD_CLASS':
                    #'@dataObjectId = ', common_part['SubDataObjectRecords']['record']['@dataObjectId'])
                    for value_number in range(len(common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'])):
                        try:  # ЗДЕСЬ  РАЗБИРАЕМ ТНВЭДЫ
                            tnved_attr_id = common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'][value_number]['@baseAttrId']
                            if tnved_attr_id not in self.attr_list:
                                #print('tnved_attr_id = {} НЕ в списке исомых атрибутов'.format(tnved_attr_id))
                                pass
                            else:
                                tnved_value = common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'][value_number]['@value']
                                #print('tnved_attr_id = {} В списке исомых атрибутов и tnved_value = {}'.format(tnved_attr_id, tnved_value))
                                df.loc[self.global_record, tnved_attr_id] = tnved_value
                        except:  #
                            print('tnved33: в SubDataObjectRecord в BaseAttributeValues в записи  value_number={} что-то НЕОЖИДАННОЕ'.format(value_number))

                elif isinstance(common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'], dict) and common_part['SubDataObjectRecords']['record']['@dataObjectId'] == 'PROD_CLASS':
                    #print('tnved39:в BaseAttributeValues есть ТОЛЬКО ОДНА запись')
                    try:
                        tnved_attr_id = common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value']['@baseAttrId']
                        tnved_value = common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value']['@value']
                        if tnved_attr_id not in self.attr_list:
                            pass

                        else:
                            df.loc[self.global_record, tnved_attr_id] = tnved_value
                    except:  #
                        print('tnved100: вв SubDataObjectRecord в BaseAttributeValues в единственной записи что-то НЕОЖИДАННОЕ')


            except TypeError:  # нет  пути SubDataObjectRecords/record/AttributeValues нет или там только одно value
                #print('tnved106: в SubDataObjectRecord  НЕ содержится TNVED')
                pass
        #print('tnved108 df =:', df.to_string())

        return df


    @staticmethod
    @retry(TimeoutError, tries=3, delay=3, max_delay=10, backoff=3)
    
    def test_connection():
        """Tests the connection by executing a select 1 query"""
        con = pymysql.connect(host=host, user=user, port=port, password=password, database=database)
        status, message = False, ''
        try:
            with con:
                cur = con.cursor()
                cur.execute("select 1")
                if cur.fetchone():
                    status = True
                    message = 'Connection successfully tested'
        except Exception as e:
            status = False
            message = str(e)

        return status, message

    def get_value_from_valueMap(cash: object, gs1_attrid: object, mapping_key: object, get_valueMap=False) -> object:

        def db_request(host, user, port, password, database):
            con = pymysql.connect(host=host, user=user, port=port, password=password, database=database)
            with con:
                cur = con.cursor()
                query = ''' SELECT TRIM(BOTH '"' FROM (ValueMap->'$."{}"'))
                                            FROM Lst_AttrToGS1Attr latga
                                            WHERE Deleted = 0 AND Operation = 'import' AND GS1AttrId = '{}'
                                        '''.format(str(mapping_key), str(gs1_attrid))
                cur.execute(query)
                DBresponse = cur.fetchone()
            return  DBresponse



        if get_valueMap == False:
            mapping_value = mapping_key

        else:
            if cash.get(gs1_attrid, None) == None or cash.get(gs1_attrid).get(mapping_key, None) == None:
                #print('attfmap 20: запрашиваемое по ключу gs1_attrid= \'{}\' и mapping_key=\'{}\' ЕЩЕ НЕТ в кэше'.format(gs1_attrid, mapping_key))
                DBresponse = db_request(host, user, port, password, database)
                if DBresponse == None or DBresponse[0] == None:  # None получим если нет в принципе такой записи с указанным GS1AttrId, а 'NULL' если GS1AttrId но для него нет valueMap
                    mapping_value = None  # т.к. маппинга нет, тогда функция вернет attrId из эксемеля (в mapping_key подается attrId)
                else:
                    mapping_value = DBresponse[0]
                    # поскольку в базе что-то есть, запишем в кэш полученное значение, но сперва проверим, есть ли там хотя бы атрибут
                    if cash.get(gs1_attrid, None) == None:  # если нет еще
                        cash[gs1_attrid] = {mapping_key: mapping_value}
                    else:
                        cash[gs1_attrid][mapping_key] = mapping_value

            # в этом случае и gs1_attrid и ключ-значение для него есть

            elif cash.get(gs1_attrid, None) != None and cash.get(gs1_attrid).get(mapping_key, None) != None:
                mapping_value = cash[gs1_attrid][mapping_key]

            elif cash.get(gs1_attrid).get(mapping_key, None) == None:
                DBresponse = db_request(host, user, port, password, database)
                if DBresponse == None or DBresponse[0] == None:  # None получим если нет в принципе такой записи с указанным GS1AttrId, а 'NULL' если GS1AttrId но для него нет valueMap
                    mapping_value = None  # т.к. маппинга нет, тогда функция вернет attrId из эксемеля (в mapping_key подается attrId)
                else:
                    mapping_value = DBresponse[0]
                    cash[gs1_attrid][mapping_key] = mapping_value




            if mapping_value == None:  # если в базе нет маппинга
                mapping_value = mapping_key
            else:
                #print('attfmap 66: на выход функции отдаем mapping_value=', mapping_value)
                pass
        return cash, mapping_value


    def web_attribute_parser(self):
        '''
        Данная функция вызывается из table_from_dict_builder и парсит из переданного словаря WEB - атрибуты.
        Если для WEB атрибута находит ключ (без descr) то вызывает get_mapping_value для получения ключа из базы по ValueMap.
        Если ключа в базе не находится то забирает value как оно указано в XML
        :param XML_parsed_to_dict:
        :param web_attr_list:
        :param global_record:
        :return:
        '''
        if self.global_record is None:
            common_part = self.XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']

        else:
            common_part = self.XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][self.global_record]

        ########################################################################################################################
        cash = {}

        df = pd.DataFrame()
        #print('\n++++++++++++++++ ПАРСИНГ WEB-АТРИБУТОВ ++++++++++++++++\n')
        for infotype_record in range(len(common_part['InfoTypeRecords']['record'])):
            try:  # если в infotype_record есть AttributeValues и это или список или словарь
                if isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'], list):  # если в InfoTypeRecords/record/AttributeValues несколько value
                    # print('wap28: в infotype_record N={} в /AttributeValues содержится несколько записей value'.format(infotype_record)) # !!!!!!!!!!!!

                    for value_number in range(len(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'])):
                        try:  # проверяем если есть вложенный мультиатрибут
                            if isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@extAttrId'], str) \
                                    or isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][0]['@extAttrId'], str):
                                # предположим, что вложенный мультиатрибут есть, тогда:
                                # print('\nwap35: в infotype_record N={} в /AttributeValues в записи  value N={} есть один или несколько вложеных MultValue'.format(infotype_record, value_number))
                                if isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@extAttrId'],
                                              str):  # если multivalue одно
                                    web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@extAttrId']
                                    if web_attr_id not in self.attr_list:
                                        continue
                                    else:
                                        #   try:
                                        #       web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@descr']
                                        #       df.loc[global_record, web_attr_id] = web_attr_descr
                                        #       print('wap46: текущее значение df= \n {}\n'.format(df))
                                        #   except KeyError:
                                        web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue']['@value']
                                        #print('wap154: вызовем get_mapping_value')
                                        cash, mapping_value = AtrrValueParesr.get_value_from_valueMap(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value, get_valueMap=self.get_valueMap)
                                        df.loc[self.global_record, web_attr_id] = mapping_value
                                        # print('wap49: текущее значение df= \n {}\n'.format(df))

                                else:  # если multivalue  НЕ одно
                                    # print('\nwap52: в infotype_record N={} в /AttributeValues в записи  value N={} ВОЗМОЖНО несколько записей MultValue. проверим.'.format(infotype_record, value_number))

                                    web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][MultValue_N]['@extAttrId']
                                    if web_attr_id not in self.attr_list:
                                        continue
                                    else:
                                        multiattrlist = []
                                        for MultValue_N in range(
                                                len(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][MultValue_N])):
                                            # try:
                                            #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][MultValue_N]['@descr']
                                            #    multiattrlist.append(web_attr_descr)

                                            # except KeyError:
                                            web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['ns0:MultValue'][MultValue_N]['@value']
                                            #print('wap74: вызовем get_mapping_value')
                                            cash, mapping_value = AtrrValueParesr.get_value_from_valueMap(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value, get_valueMap=self.get_valueMap)
                                            multiattrlist.append(mapping_value)

                                        df.loc[self.global_record, web_attr_id] = multiattrlist
                        except KeyError:  # ЗДЕСЬ  РАЗБИРАЕМ ВЕБ-АТРИБУТЫ

                            # for value_number in range(len(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'])):
                            # следующие строки были на 7-м отступе и строка 70 не былазакоментирована
                            web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['@extAttrId']
                            # ('wap75: в infotype_record={} value_number={} парсим web_attr_id={}'.format(infotype_record, value_number, web_attr_id))

                            if web_attr_id not in self.attr_list:
                                # print('wap78: web_attr_id = {} НЕ в списке заданных атрибутов :(\n'.format(web_attr_id))
                                continue
                            else:
                                # try:
                                #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['@descr']
                                #    print('wap79: текущее значение web_attr_descr=', web_attr_descr)
                                #    df.loc[global_record, web_attr_id] = web_attr_descr
                                #    print('wap93: текущее значение df= \n {}\n'.format(df))

                                # except KeyError:
                                #    print('wap85:  !!! в web_attr_id={} параметра @descr НЕТ '.format(web_attr_id))
                                web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'][value_number]['@value']
                                #print('wap99: вызовем get_mapping_value')
                                cash, mapping_value = AtrrValueParesr.get_value_from_valueMap(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value, get_valueMap=self.get_valueMap)
                                df.loc[self.global_record, web_attr_id] = mapping_value

                                # print('wap93: текущее значение df= \n {}\n'.format(df))
                                # except:
                                #    print('wap92: в infotype_record N={} в /AttributeValues в записи  value N={} для web_attr_id {} что-то НЕОЖИДАННОЕ'.format(infotype_record, value_number, web_attr_id))
                        except:  #
                            print('wap107: в infotype_record N={} в /AttributeValues в записи  value N={} что-то НЕОЖИДАННОЕ'.format(infotype_record, value_number))

                if isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value'], dict):  # если только одна запись value в AttributeValues
                    # print('wap100: в infotype_record N={} в AttributeValues/value только одна запись value.'.format(infotype_record))
                    try:  # проверяем если мультиатрибут один
                        if isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@extAttrId'], str) \
                                or isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][0]['@extAttrId'], str):
                            # print('wap104: в записи infotype_record N={} в /AttributeValues в единственной записи value есть одна или несколько MultValue'.format(infotype_record))
                            if isinstance(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@extAttrId'], str):
                                web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@extAttrId']

                                # try:
                                #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@descr']
                                #    # if isinstance(web_attr_descr, str):
                                #    if web_attr_id not in web_attr_list:
                                #        print('wap123: web_attr_id {} not in web_attr_list web_attr_list'.format(web_attr_id))
                                #    else:
                                #        df.loc[global_record, web_attr_id] = web_attr_descr
                                #        print('wap126: текущее значение df= \n {}\n'.format(df))

                                # except KeyError:

                                if web_attr_id not in self.attr_list:
                                    #print('wap127: web_attr_id {} НЕ в web_attr_list'.format(web_attr_id))
                                    pass
                                else:
                                    web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue']['@value']
                                    #print('wap133: вызовем get_mapping_value')
                                    cash, mapping_value = AtrrValueParesr.get_value_from_valueMap(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value, get_valueMap=self.get_valueMap)
                                    df.loc[self.global_record, web_attr_id] = mapping_value
                                    # print('wap125: текущее значение df= \n {}\n'.format(df))


                            else:
                                # print('\nwap129: в infotype_record N={} в /AttributeValues в единственной записи value НЕСКОЛЬКО записей MultValue'.format(infotype_record))
                                multiattrlist = []
                                for MultValue_N in range(len(common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][MultValue_N])):
                                    web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][MultValue_N]['@extAttrId']

                                    # try:
                                    #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][MultValue_N]['@descr']
                                    #    # if isinstance(web_attr_descr, str):
                                    #    if web_attr_id not in web_attr_list:
                                    #        continue
                                    #    else:
                                    #        multiattrlist.append(web_attr_descr)
                                    #        print('wap150: текущее значение df= \n {}\n'.format(df))

                                    # except KeyError:
                                    web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['ns0:MultValue'][MultValue_N]['@value']
                                    #print('wap156: вызовем get_mapping_value')
                                    cash, mapping_value = AtrrValueParesr.get_value_from_valueMap(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value, get_valueMap=self.get_valueMap)
                                    if web_attr_id not in self.attr_list:
                                        continue
                                    else:
                                        multiattrlist.append(mapping_value)

                                        #print('wap150: текущее значение df= \n {}\n'.format(df))
                                    df.loc[self.global_record, web_attr_id] = multiattrlist

                    except KeyError:

                        web_attr_id = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['@extAttrId']

                        # try:
                        #    web_attr_descr = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['@descr']
                        #    # if isinstance(web_attr_descr, str):
                        #    if web_attr_id not in web_attr_list:
                        #        print('wap170: web_attr_id {} not in web_attr_list web_attr_list'.format(web_attr_id))
                        #    else:
                        #        df.loc[global_record, web_attr_id] = web_attr_descr
                        #        print('wap173: текущее значение df= \n {}\n'.format(df))

                        # except KeyError:
                        web_attr_value = common_part['InfoTypeRecords']['record'][infotype_record]['AttributeValues']['value']['@value']
                        #print('wap181: вызовем get_mapping_value')
                        cash, mapping_value = AtrrValueParesr.get_value_from_valueMap(cash, gs1_attrid=web_attr_id, mapping_key=web_attr_value, get_valueMap=self.get_valueMap)
                        if web_attr_id not in self.attr_list:
                            #print('wap170: web_attr_id {} не в web_attr_list'.format(web_attr_id))
                            pass
                        else:
                            df.loc[self.global_record, web_attr_id] = mapping_value

                            # print('wap174: текущее значение df= \n {}\n'.format(df))

                    except:  #
                        print('wap177: в infotype_record N={} в /AttributeValues в записи  что-то НЕОЖИДАННОЕ'.format(infotype_record))
            except TypeError:  # нет  пути InfoTypeRecords/record/AttributeValues нет или там только одно value
                #print('wap179: в infotype_record {} НЕ содержится AttributeValues'.format(infotype_record))
                pass

        return df







if __name__ == '__main__':
    ''' ЗАДАИМ ПАРАМЕТРЫ'''
    with open('params.yaml', 'r', encoding='UTF-8') as f:
        params = yaml.safe_load(f)

    url = params['url']
    login = params['login']
    password = params['password']
    auth = HTTPBasicAuth(login, password)


    full_body_test = '''
    <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="urn:org.gs1ru.gs46.intf">
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

