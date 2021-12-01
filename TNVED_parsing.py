import pandas as pd


def TNVED_codes_parser(XML_parsed_to_dict, tnved_attr_list, global_record=None):
    '''
    Данная функция вызывается из xmlToDict_parsing.table_from_dict_builder и парсит атрибуты только в рекордах узла SubDataObjectRecords с dataObjectId = "PROD_CLASS"

    :param XML_parsed_to_dict:
    :param web_attr_list:
    :param global_record:
    :return:
    '''
    if global_record is None:
        print('global_record is None, поэтому ')

        common_part = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']
        global_record = 0

    else:

        common_part = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]



    df = pd.DataFrame()

    print('\n++++++++++++++++ ПАРСИНГ ТНВЭДов ++++++++++++++++\n')

    if isinstance(common_part['SubDataObjectRecords']['record'], list):

        for SubDataObjectRecord in range(len(common_part['SubDataObjectRecords']['record'])):
            print('\ntnved36: зашли в {}-й SubDataObjectRecord'.format(SubDataObjectRecord))
            try:
                if  isinstance(common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'], list) \
                        and common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['@dataObjectId']=='PROD_CLASS':

                    for value_number in range(len(common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'])):
                        try: # ЗДЕСЬ  РАЗБИРАЕМ ТНВЭДЫ
                            tnved_attr_id = common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'][value_number]['@baseAttrId']
                            if tnved_attr_id not in tnved_attr_list:
                                print('tnved_attr_id = {} НЕ в списке исомых атрибутов'.format(tnved_attr_id))
                                pass
                            else:
                                tnved_value = common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'][value_number]['@value']
                                print('tnved_attr_id = {} В списке исомых атрибутов и tnved_value = {}'.format(tnved_attr_id, tnved_value))
                                df.loc[global_record, tnved_attr_id] = tnved_value
                        except : #
                            print('tnved33: в SubDataObjectRecord N={} в /BaseAttributeValues в записи  value_number={} что-то НЕОЖИДАННОЕ'.format(SubDataObjectRecord, value_number))

                elif isinstance(common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value'], dict)\
                        and common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['@dataObjectId']=='PROD_CLASS':
                    print('tnved39:в BaseAttributeValues есть ТОЛЬКО ОДНА запись')
                    try:
                        tnved_attr_id = common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value']['@baseAttrId']
                        tnved_value = common_part['SubDataObjectRecords']['record'][SubDataObjectRecord]['BaseAttributeValues']['value']['@value']
                        if tnved_attr_id not in tnved_attr_list:
                            print('tnved40: web_attr_id {} не в web_attr_list'.format(tnved_attr_id))
                            pass
                        else:
                            df.loc[global_record, tnved_attr_id] = tnved_value
                    except :  #
                        print('wap177: вв SubDataObjectRecord N={}  /BaseAttributeValues в записи  value_number={} что-то НЕОЖИДАННОЕ'.format(SubDataObjectRecord, value_number))



            except TypeError: #  нет  пути SubDataObjectRecords/record/AttributeValues нет или там только одно value
                print('tnved46: в SubDataObjectRecord = {} НЕ содержится TNVED'.format(SubDataObjectRecord))

    else:
        print('запись всего одна')

        try:
            if  isinstance(common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'], list) and common_part['SubDataObjectRecords']['record']['@dataObjectId']=='PROD_CLASS':
                print('@dataObjectId = ', common_part['SubDataObjectRecords']['record']['@dataObjectId'])
                for value_number in range(len(common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'])):
                    try: # ЗДЕСЬ  РАЗБИРАЕМ ТНВЭДЫ
                        tnved_attr_id = common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'][value_number]['@baseAttrId']
                        if tnved_attr_id not in tnved_attr_list:
                            print('tnved_attr_id = {} НЕ в списке исомых атрибутов'.format(tnved_attr_id))
                            pass
                        else:
                            tnved_value = common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'][value_number]['@value']
                            print('tnved_attr_id = {} В списке исомых атрибутов и tnved_value = {}'.format(tnved_attr_id, tnved_value))
                            df.loc[global_record, tnved_attr_id] = tnved_value
                    except : #
                        print('tnved33: в SubDataObjectRecord в BaseAttributeValues в записи  value_number={} что-то НЕОЖИДАННОЕ'.format(value_number))

            elif isinstance(common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value'], dict) and common_part['SubDataObjectRecords']['record']['@dataObjectId']=='PROD_CLASS':
                print('tnved39:в BaseAttributeValues есть ТОЛЬКО ОДНА запись')
                try:
                    tnved_attr_id = common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value']['@baseAttrId']
                    tnved_value = common_part['SubDataObjectRecords']['record']['BaseAttributeValues']['value']['@value']
                    if tnved_attr_id not in tnved_attr_list:
                        pass

                    else:
                        df.loc[global_record, tnved_attr_id] = tnved_value
                except :  #
                    print('tnved100: вв SubDataObjectRecord в BaseAttributeValues в единственной записи что-то НЕОЖИДАННОЕ')


        except TypeError: #  нет  пути SubDataObjectRecords/record/AttributeValues нет или там только одно value
            print('tnved106: в SubDataObjectRecord  НЕ содержится TNVED')

    print('tnved108 df =:',df.to_string())

    return df