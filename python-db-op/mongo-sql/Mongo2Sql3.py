#宗表  5 表
import re

from pymongo import MongoClient
import pymysql

subfix = "_"
tableName = 'public_energy_computer'
db_name = 'ggj-working'
collection_sum_name = 'ggj-working.stats_working_integrate_task'
collection_base_name = 'ggj-working.stats_working_gathered_respondent_task'
collectionReportName = 'ggj-working.stats_working_data'
collectionBaseName = 'ggj-working.stats_working_data101'
client3 = MongoClient(host='192.168.6.204', port=27017)
database = client3[db_name]
# database.authenticate(name = 'admin', password= '123456')
collectionTaskSum = database[collection_sum_name]
collectionTaskBase = database[collection_base_name]
# collectionData = database[collectionReportName]
collectionData101 = database[collectionBaseName]
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql3 = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')
sql = 'INSERT INTO {} (record_id,p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

table3Dict= {
    '201':'p0',
    '210':'p1',
    '220':'p2',
    '2201':'p3',
    '2202':'p4',
    '2203':'p5',
    '230':'p6',
    '240':'p7',
    '2401':'p8',
    '2402':'p9',
    '2403':'p10',
}
table3KeyList = table3Dict.keys()


#通过任务表中的周期和组织获取monggo宗表中的数据
def queryDocByPeriodAndOrg3(periodId,orgId,recordId,periodYear):
    res = []
    hisTableName = tableName+subfix+periodYear
    cursor3 = clientSql3.cursor()
    query = {"periodId": periodId, "respondentId": orgId,'reportCode': 'NY202_2020'}
    collectionData = database[collectionReportName]
    # count = collectionData.count(query)#({"locale": "zh", numericOrdering:true}).sort("_id", -1).limit(number)
    # if(count == 0):
    #     return
    res_cussor3 = collectionData.find(query)
    # res_data3 = list(res_cussor3[:])
    for i in res_cussor3:
        keyNotExistValueSetParam(i['rowData'])
        par = (recordId,p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10)
        try:
            cursor3.execute(sql.format(hisTableName),par)
        except Exception as e:
            print(e)
    try:
        clientSql3.commit()
    except Exception as e:
        clientSql3.rollback()
        # cursor3.close()
        # client3.close()
        print(e)
    cursor3.close()
    # client3.close()


#获取当前doc中的所有key
def keysList(doc):
    keys = []
    for key in doc:
        keys.append(key)
    return keys
#设置插入数据库对应字段的tuple,如果keylist中没有就赋值为Null
def keyNotExistValueSetParam(doc):
    keysExist = keysList(doc)
    keysNotExist = list(set(table3KeyList).difference(set(keysExist)))
    #如果key不存在那么对应的mysql中的字段就为空
    for keysNotExistEach in keysNotExist:
        valNotExistEach  = table3Dict.get(keysNotExistEach)
        # paramNotExist.append(valNotExistEach)
        try:
            globals()[valNotExistEach] = None
        except Exception as e:
            print(valNotExistEach)
            print(e)
        # exec('{} = {}'.format(valNotExistEach,None))
    for keysExistEach in keysExist:
        valExistEach = table3Dict.get(keysExistEach)
        # paramExist.append(valExistEach)
        try:
            globals()[valExistEach] = doc[keysExistEach]
            # exec('{} = {}'.format(valExistEach,doc[keysExistEach]))
        except Exception as e:
            print(keysExistEach)
            print(e)

