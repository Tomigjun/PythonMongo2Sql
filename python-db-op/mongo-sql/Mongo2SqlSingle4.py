#coding=utf-8

from pymongo import MongoClient
import pymysql

from UtilTool import str2PureNum

subfix = "_"
tableName = 'public_energy_heating'
db_name = 'ggj-working'
collection_sum_name = 'ggj-working.stats_working_integrate_task'
collection_base_name = 'ggj-working.stats_working_gathered_respondent_task'
collectionReportName = 'ggj-working.stats_working_data'
collectionBaseName = 'ggj-working.stats_working_data101'
client4 = MongoClient(host='192.168.6.204', port=27017)
database = client4[db_name]
# database.authenticate(name = 'admin', password= '123456')
collectionTaskSum = database[collection_sum_name]
collectionTaskBase = database[collection_base_name]
collectionData = database[collectionReportName]
collectionData101 = database[collectionBaseName]
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql4 = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')
cursor4 = clientSql4.cursor()
sql = 'INSERT INTO {} (id,record_id,p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,org_id,period_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

id_index = 1
# id =  100001
id_length = 300000;
id = (id_index -1)*id_length


table4Dict= {
    '301':'p0',
    '3011':'p1',
    '3012':'p2',
    '3013':'p3',
    '302':'p4',
    'p5':'p5',
    '303':'p6',
    '304':'p7',
    '310':'p8',
    '311':'p9',
    '320':'p10',
    '321':'p11',
    '330':'p12',
    '331':'p13',
    '340':'p14',
    '341':'p15',
    '350':'p16',
    '351':'p17',
    '380':'p18',
    '360':'p19',
    '361':'p20',
    '370':'p21',
    '371':'p22',
}
table2KeyList = table4Dict.keys()

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
        valNotExistEach  = table4Dict.get(keysNotExistEach)
        # paramNotExist.append(valNotExistEach)
        try:
            globals()[valNotExistEach] = None
        except Exception as e:
            print(valNotExistEach)
            print(e)
        # exec('{} = {}'.format(valNotExistEach,None))
    for keysExistEach in keysExist:
        valExistEach = table4Dict.get(keysExistEach)
        # paramExist.append(valExistEach)
        try:
            globals()[valExistEach] = doc[keysExistEach]
            # exec('{} = {}'.format(valExistEach,doc[keysExistEach]))
        except Exception as e:
            print(keysExistEach)
            print(e)



cursor4 = clientSql4.cursor()
query = {'reportCode': 'NY203_2020'}
collectionData = database[collectionReportName]
# count = collectionData.count(query)#.sort("info.usage_count_180_day", -1).limit(number)
# if(count == 0):
#     return
res_cussor4 = collectionData.find(query).sort([('_id',-1)]).limit(id_length).skip((id_index-1)*id_length).batch_size(10000)
# res_data4 = list(res_cussor4[:])
for i in res_cussor4:
    id = id +1
    sta_year = i["periodId"][:4]
    hisTableName = tableName+subfix+sta_year
    keyNotExistValueSetParam(i['rowData'])
    recordId = None
    org_code = i["respondentId"]
    org_id = org_code
    org_id = str2PureNum(org_id)
    period_id = i['periodId']
    par = (id,recordId,p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,org_id,period_id)
    try:
        cursor4.execute(sql.format(hisTableName),par)
    except Exception as e:
        print(e)
    try:
        clientSql4.commit()
    except Exception as e:
        clientSql4.rollback()
        # cursor4.close()
        # client4.close()
        print(e)
cursor4.close()
# client4.close()

