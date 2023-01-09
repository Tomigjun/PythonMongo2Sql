#宗表  5 表
import re

from pymongo import MongoClient
import pymysql

subfix = "_"
tableName = 'public_energy_consume'
db_name = 'ggj-working'
collection_sum_name = 'ggj-working.stats_working_integrate_task'
collection_base_name = 'ggj-working.stats_working_gathered_respondent_task'
collectionReportName = 'ggj-working.stats_working_data'
collectionBaseName = 'ggj-working.stats_working_data101'
client2 = MongoClient(host='192.168.6.204', port=27017)
database = client2[db_name]
# database.authenticate(name = 'admin', password= '123456')
collectionTaskSum = database[collection_sum_name]
collectionTaskBase = database[collection_base_name]
collectionData = database[collectionReportName]
collectionData101 = database[collectionBaseName]
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql2 = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')
sql = 'INSERT INTO {} (record_id,p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

table2Dict= {
    '009':'p0',
    '101':'p1',
    '102':'p2',
    '1021':'p3',
    '103':'p4',
    '1031':'p5',
    '1032':'p6',
    '1033':'p7',
    '110':'p8',
    '111':'p9',
    '120':'p10',
    '121':'p11',
    '130':'p12',
    '131':'p13',
    '140':'p14',
    '141':'p15',
    '150':'p16',
    '151':'p17',
    '1501':'p18',
    '1511':'p19',
    '1502':'p20',
    '1512':'p21',
    '160':'p22',
    '161':'p23',
    '1601':'p24',
    '1611':'p25',
    '1602':'p26',
    '1612':'p27',
    '104':'p28',
    '1041':'p29',
    '170':'p30',
    '171':'p31',
    '180':'p32',
    '181':'p33',
    '105':'p34',
    # 'None':'p35',
    '190':'p36',
    '191':'p37',
    '192':'p38',
    'None':'p39',
    '402':'p40',
    '401':'p41',
    '403':'p42',
    '404':'p43',
    '405':'p44',
    '406':'p45',
    # 'None':'p46',
    # 'None':'p47',
    # 'None':'p48',
    # 'None':'p49',
    # 'None':'p50',
    # 'None':'p51',
    # 'None':'p52',
    # 'None':'p53',
    # 'None':'p54',
    # 'None':'p55',
    # 'None':'p56',
}
table2KeyList = table2Dict.keys()
p35= None
p46 =None
p47 =None
p48=None
p49=None
p50=None
p51=None
p52=None
p53=None
p54=None
p55=None
p56=None
p57=None
p58=None
p58=None
p59=None
p60=None
p61=None
p62=None
p63=None
p64=None
p65=None
p66=None


#通过任务表中的周期和组织获取monggo宗表中的数据
def queryDocByPeriodAndOrg2(periodId,orgId,recordId,periodYear):
    res = []
    hisTableName = tableName+subfix+periodYear
    cursor2 = clientSql2.cursor()
    query = {"periodId": periodId, "respondentId": orgId,'reportCode': 'NY201_2020'}
    collectionData = database[collectionReportName]
    # count = collectionData.count(query)#.sort("info.usage_count_180_day", -1).limit(number)
    # if(count == 0):
    #     return
    res_cussor2 = collectionData.find(query)
    # res_data2 = list(res_cussor2[:])
    for i in res_cussor2:
        keyNotExistValueSetParam(i['rowData'])
        par = (recordId,p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39,p40,p41,p42,p43,p44,p45,p46,p47,p48,p49,p50,p51,p52,p53,p54,p55,p56,p57,p58,p59,p60,p61,p62,p63,p64,p65,p66)
        try:
            cursor2.execute(sql.format(hisTableName),par)
        except Exception as e:
            print(e)
    try:
        clientSql2.commit()
    except Exception as e:
        clientSql2.rollback()
        # cursor2.close()
        # client2.close()
        print(e)
    cursor2.close()
    # client2.close()

#获取当前doc中的所有key
def keysList(doc):
    keys = []
    for key in doc:
        keys.append(key)
    return keys
#设置插入数据库对应字段的tuple,如果keylist中没有就赋值为Null
def keyNotExistValueSetParam(doc):
    keysExist = keysList(doc)
    keysNotExist = list(set(table2KeyList).difference(set(keysExist)))
    #如果key不存在那么对应的mysql中的字段就为空
    for keysNotExistEach in keysNotExist:
        valNotExistEach  = table2Dict.get(keysNotExistEach)
        # paramNotExist.append(valNotExistEach)
        try:
            globals()[valNotExistEach] = None
        except Exception as e:
            print(valNotExistEach)
            print(e)
        # exec('{} = {}'.format(valNotExistEach,None))
    for keysExistEach in keysExist:
        valExistEach = table2Dict.get(keysExistEach)
        # paramExist.append(valExistEach)
        try:
            globals()[valExistEach] = doc[keysExistEach]
            # exec('{} = {}'.format(valExistEach,doc[keysExistEach]))
        except Exception as e:
            print(keysExistEach)
            print(e)

