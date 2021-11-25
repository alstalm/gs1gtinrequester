import pymysql # pip install pymysql
import yaml
from retry import retry

with open ('params.yaml', 'r', encoding='UTF-8') as f:
    params = yaml.safe_load(f)

host = params['DB_host']
user = params['DB_user']
port = params['DB_port']
password = params['DB_password']
database = params['DB_database']

''' СОЗДАДИМ ОБЪЕКТ ПОДКЛЮЧЕНИЯ и ЗАПРОС В ОБЩЕМ ВИДЕ'''
@retry(TimeoutError, tries = 5, delay=1, max_delay = 180, backoff = 3 )
def get_mapping_value(cash, gs1_attrid, mapping_key):
    # если нет или GS1AttrId или одного из его ключей, то идем в базу

    if cash.get(gs1_attrid, None) == None or cash.get(gs1_attrid).get(mapping_key, None) == None:
        print('attfmap 20: запрашиваемое по ключу gs1_attrid= \'{}\' и mapping_key=\'{}\' ЕЩЕ НЕТ в кэше'.format(gs1_attrid, mapping_key))
        con = pymysql.connect(host=host, user=user, port=port, password=password, database=database)
        with con:
            cur = con.cursor()
            query = ''' SELECT TRIM(BOTH '"' FROM (ValueMap->'$."{}"'))
                        FROM Lst_AttrToGS1Attr latga
                        WHERE Deleted = 0 AND Operation = 'import' AND GS1AttrId = '{}'
                    '''.format(str(mapping_key),str(gs1_attrid))
            cur.execute(query)

            DBresponse = cur.fetchone()

        if DBresponse == None or DBresponse[0] == None: # None получим если нет в принципе такой записи с указанным GS1AttrId, а 'NULL' если GS1AttrId но для него нет valueMap
            mapping_value = None # т.к. маппинга нет, тогда функция вернет attrId из эксемеля (в mapping_key подается attrId)

        else:
            mapping_value = DBresponse[0]
            # поскольку в базе что-то есть, запишем в кэш полученное значение, но сперва проверим, есть ли там хотя бы атрибут
            if cash.get(gs1_attrid, None) == None:  # если нет еще
                cash[gs1_attrid] = {mapping_key: mapping_value}

            else:
                cash[gs1_attrid][mapping_key] = mapping_value

        print('attfmap 52: по итогу запроса в базу, текущее значение mapping_value=', mapping_value)

    # в этом случае и gs1_attrid и ключ-значение для него есть
    else:
        mapping_value = cash[gs1_attrid][mapping_key]

    if mapping_value == None: # если в базе нет маппинга
        mapping_value = mapping_key
    else:
        print('attfmap 66: на выход функции отдаем mapping_value=', mapping_value)

    return cash, mapping_value

if __name__ == '__main__':
    ''' ЗАДАИМ ПАРАМЕТРЫ'''
    cash = {}

    gs1_attrid_1 = 'WEB_90001771'
    mappingkey_1 = '45 % - 50 %'

    gs1_attrid_2 = 'WEB_90001854'
    mappingkey_2 = '0.1-10'

    cash,mapping_value = get_mapping_value(cash, gs1_attrid = gs1_attrid_1, mapping_key = mappingkey_1)
    print('attfmap 81: mapping_value =', mapping_value)
    print('attfmap 81: cash={}'.format(cash))