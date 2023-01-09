#coding=utf-8
import re

from pymongo import MongoClient
import pymysql

from UtilTool import str2PureNum

subfix = "_"
tableName = 'uap_data.public_energy_base'
db_name = 'ggj-working'
collection_sum_name = 'ggj-working.stats_working_integrate_task'
collection_base_name = 'ggj-working.stats_working_gathered_respondent_task'
collectionReportName = 'ggj-working.stats_working_data'
collectionBaseName = 'ggj-working.stats_working_data101'
client1 = MongoClient(host='192.168.6.204', port=27017)
database = client1[db_name]
# database.authenticate(name = 'admin', password= '123456')
collectionTaskSum = database[collection_sum_name]
collectionTaskBase = database[collection_base_name]
collectionData = database[collectionReportName]
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql1 = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')
sql = 'INSERT INTO {} (id,report_id,org_name,org_id,credit_code,org_type,industry_code,addr,area_code,mobile_phone,comments,period_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

id_index = 1
# id =  100001
id_length = 300000;
id = (id_index -1)*id_length

cursor1 = clientSql1.cursor()
collectionData101 = database[collectionBaseName]
query = {'reportCode': re.compile('NY101_2020')}
res_cussor1 = collectionData101.find(query).sort([('_id',-1)]).limit(id_length).skip((id_index-1)*id_length).batch_size(10000)
# res_data1 = list(res_cussor1[:])
# count = collectionData101.count(query)#.sort("info.usage_count_180_day", -1).limit(number)
# if(count ==0 ):
#     return

#操作NY101即1表
def sum1thTableByDoc(i):
    #record_id 设置为periodid
    org_code = i["respondentId"]
    org_id = org_code
    org_id = str2PureNum(org_id)
    globals()['record_id'] = None
    #todo mongo无数据 需要从sys_org表里拉取填充
    globals()['org_name'] = None
    globals()['org_id']  = org_id
    #todo 目前mongo无此数据
    globals()['credit_code'] = None
    #todo mongo无数据 需要从sys_org表里拉取填充
    globals()['org_type'] = None
    #todo mongo无数据 需要从sys_org表里拉取填充
    globals()['industry_code'] = None
    #todo mongo无数据 需要从sys_org表里拉取填充
    globals()['addr'] = None
    globals()['area_code'] = None
    if('areaId' in i):
        globals()['area_code'] = i['areaId'][:6]
    #todo mongo无数据 需要从sys_org表里拉取填充
    globals()['mobile_phone'] = None
    globals()['comments'] = None
    globals()['period_id']= i['periodId']
    sta_year = i["periodId"][:4]
    globals()['hisTableName'] = tableName+subfix+sta_year

for i in res_cussor1:
    id = id +1
    sum1thTableByDoc(i)
    par = (id,record_id,org_name,org_id,credit_code,org_type,industry_code,addr,area_code,mobile_phone,comments,period_id)
    try:
        cursor1.execute(sql.format(hisTableName),par)
    except Exception as e:
        print(e)
    try:
        clientSql1.commit()
    except Exception as e:
        clientSql1.rollback()
        # cursor1.close()
        # client1.close()
        print(e)
cursor1.close()
# client1.close()



