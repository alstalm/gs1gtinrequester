import pymysql # pip install pymysql
import yaml
import numpy as np

with open ('params.yaml', 'r', encoding='UTF-8') as f:
    params = yaml.safe_load(f)

host = params['DB_host']
user = params['DB_user']
port = params['DB_port']
password = params['DB_password']
database = params['DB_database']

def get_mapping_from_attrtogs1attr(AccountId, gtin,  AttrId,  AttrIsValueOrType):


    con = pymysql.connect(host=host, user=user, port=port, password=password, database=database)
    print('atmr 19: попробуем найти GS1Attr в маппинге экспорта.  gtin={} AttrId={} AttrIsValueOrType={}'.format(gtin, AttrId, AttrIsValueOrType))
    with con:
        cur = con.cursor()
        #query = "SELECT  GS1AttrId FROM Lst_AttrToGS1Attr latga WHERE Deleted = 0 AND Operation = 'export' AND CatId IN (SELECT lgtc.CatId FROM  Lst_Goods lg JOIN Lst_GoodsToCat lgtc ON lg.GoodId = lgtc.GoodId WHERE lg.Barcode = '{}' ) AND latga.AttrId = {} AND AttrIsValueOrType = '{}'".format(str(gtin),str(AttrId), str(AttrIsValueOrType))
        query = '''
        SET @AttrIsValueOrType := {};
        SELECT lg.MainAccountId, lg.Barcode, lg.GoodId,  latga.MarkGroupId, lgtc.CatId, lav.AttrId
        ,latga.GS1AttrId
        ,lav.Value, lav.`Type` 
        ,(CASE WHEN @AttrIsValueOrType = 'value' THEN lav.Value WHEN @AttrIsValueOrType = 'type'THEN lav.Type END) as comparedValue
        ,latga.TypeCondition
        ,latga.AttrIsValueOrType -- вспомогательный
        ,latga.DefaultType
        ,latga.GS1AttrName 
        ,latga.GS1AttrIsDictionary -- НЕ обязательный
        FROM Lst_AttrValues lav 
        JOIN Lst_Goods lg ON lav.GoodId = lg.GoodId  AND (lg.Status = 'published' or lg.Status = 'archived') AND lg.Deleted = 0
        LEFT JOIN Lst_GoodsToCat lgtc ON lgtc.GoodId = lg.GoodId  AND lgtc.Deleted = 0 
        LEFT JOIN Lst_AttrToGS1Attr latga ON latga.CatId =  lgtc.CatId AND latga.AttrId = lav.AttrId  AND latga.Operation = 'export' AND latga.GS1AttrId NOT LIKE '%BRICK'   AND latga.Deleted = 0  
             AND (CASE WHEN (lav.Type = 'Артикул' OR  lav.Type = 'Модель') THEN latga.TypeCondition = lav.Type ELSE (latga.TypeCondition <=> NULL OR latga.TypeCondition = '') END)
        WHERE lav.Deleted = 0   AND AttrIsValueOrType = @AttrIsValueOrType
        AND lg.MainAccountId = {}  
        AND lg.Barcode = {}  
        AND lav.AttrId = {}    
        '''.format(str(AttrIsValueOrType), str(AccountId), str(gtin), str(AttrId))

        cur.execute(query)
        DBresponse = cur.fetchone()
    print('atmr 47: DBresponse={}'.format(DBresponse))

    if DBresponse != None: #or DBresponse[0] != None:
        # начинаем считать количество полученных записей и складывать в датафрейм

        GS1AttrId = DBresponse[0]
    else:
        print('atmr 30: в маппинге экспорта GS1AttrId для gtin={} AttrId={} AttrIsValueOrType={} не найдено'.format(gtin, AttrId, AttrIsValueOrType))

    return GS1AttrId


if __name__ == '__main__':
    gtin = '4630079437153'
    AttrId = '16272'
    AttrIsValueOrType = 'value'

    answer = get_mapping_from_attrtogs1attr(gtin, AttrId, AttrIsValueOrType)
    print('answer is: \" {} \"'.format(answer))





