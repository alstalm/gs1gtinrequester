import pandas as pd


def TNVED_codes_parser(XML_parsed_to_dict, tnved_attr_list, global_record=None):
    '''
    Данная функция вызывается из table_from_dict_builder и парсит из переданного словаря коды TNVED.

    :param XML_parsed_to_dict:
    :param web_attr_list:
    :param global_record:
    :return:
    '''
    if global_record is None:
        common_part = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record']
        global_record = 0
    else:
        common_part = XML_parsed_to_dict['S:Envelope']['S:Body']['ns0:GetItemByGTINResponse']['ns0:GS46Item']['DataRecord']['record'][global_record]

    df = pd.DataFrame()
    print('\n++++++++++++++++ ПАРСИНГ ТНВЭДов ++++++++++++++++\n')
    for SubDataObjectRecord in range(len(common_part['InfoTypeRecords']['record'])):
        try:
            if  isinstance(common_part['InfoTypeRecords']['record'][SubDataObjectRecord]['record']['BaseAttributeValues'], list): # если в SubDataObjectRecords/record/BaseAttributeValues несколько value
                for value_number in range(len(common_part['InfoTypeRecords']['record'][SubDataObjectRecord]['record']['BaseAttributeValues'])):
                    try: # ЗДЕСЬ  РАЗБИРАЕМ ТНВЭДЫ
                        tnved_attr_id = common_part['InfoTypeRecords']['record'][SubDataObjectRecord]['record']['BaseAttributeValues'][value_number]['@baseAttrId']
                        if tnved_attr_id not in tnved_attr_list:
                            continue
                        else:
                            tnved_value = common_part['InfoTypeRecords']['record'][SubDataObjectRecord]['record']['BaseAttributeValues'][value_number]['@value']
                            df.loc[global_record, tnved_attr_id] = tnved_value
                    except : #
                        print('tnved33: в SubDataObjectRecord N={} в /BaseAttributeValues в записи  value_number={} что-то НЕОЖИДАННОЕ'.format(SubDataObjectRecord, value_number))

            if isinstance(common_part['InfoTypeRecords']['record'][SubDataObjectRecord]['record']['BaseAttributeValues']['value'], dict): # если только одна запись value в BaseAttributeValues
                try:
                    tnved_attr_id = common_part['InfoTypeRecords']['record'][SubDataObjectRecord]['record']['BaseAttributeValues']['value']['@baseAttrId']
                    tnved_value = common_part['InfoTypeRecords']['record'][SubDataObjectRecord]['record']['BaseAttributeValues']['value']['@value']
                    if tnved_attr_id not in tnved_attr_list:
                        print('tnved40: web_attr_id {} не в web_attr_list'.format(tnved_attr_id))
                        continue
                    else:
                        df.loc[global_record, tnved_attr_id] = tnved_value
                except :  #
                    print('wap177: вв SubDataObjectRecord N={}  /BaseAttributeValues в записи  value_number={} что-то НЕОЖИДАННОЕ'.format(SubDataObjectRecord, value_number))
        except TypeError: #  нет  пути InfoTypeRecords/record/AttributeValues нет или там только одно value
            print('tnved46: в SubDataObjectRecord = {} НЕ содержится TNVED'.format(SubDataObjectRecord))
    return df