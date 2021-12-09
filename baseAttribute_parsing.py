import pandas as pd


def base_attribute_parser(XML_parsed_to_dict, basic_attr_list, global_record=None):
    '''
    Данная функция вызывается из table_from_dict_builder и парсит из переданного словаря БАЗОВЫЕ атрибуты.
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
    print('\n++++++++++++++++ ПАРСИНГ БАЗОВЫХ АТРИБУТОВ ++++++++++++++++\n')

    for base_attr_val_record in range(len(common_part['BaseAttributeValues']['value'])):
        attrName = common_part['BaseAttributeValues']['value'][base_attr_val_record]['@baseAttrId']

        if attrName in basic_attr_list:
            print('bap27: attrName {} в списке искомых атрибутов !!! '.format(attrName))
            try:
                descr = common_part['BaseAttributeValues']['value'][base_attr_val_record]['@descr']
                df.loc[global_record, attrName] = descr
            except:
                attrValue = common_part['BaseAttributeValues']['value'][base_attr_val_record]['@value']
                df.loc[global_record, attrName] = attrValue
        else:
            print('bap35: attrName {} не в списке искомых атрибутов'.format(attrName))

    return df