#宗表  5 表
import re

from pymongo import MongoClient
import pymysql

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
sql = 'INSERT INTO uap_data.public_energy_base (report_id,org_name,org_id,credit_code,org_type,industry_code,addr,area_code,mobile_phone,comments) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
#通过任务表中的周期和组织获取monggo宗表中的数据
def queryDocByPeriodAndOrg1(periodId,orgId,recordId,periodYear):
    res = []
    cursor1 = clientSql1.cursor()
    collectionData101 = database[collectionBaseName]
    query = {"periodId": periodId, "respondentId": orgId,'reportCode': re.compile('NY101_2020')}
    res_cussor1 = collectionData101.find(query);
    # res_data1 = list(res_cussor1[:])
    # count = collectionData101.count(query)#.sort("info.usage_count_180_day", -1).limit(number)
    # if(count ==0 ):
    #     return
    for i in res_cussor1:
        sum1thTableByDoc(i,recordId,orgId)
        par = (record_id,org_name,orgId,credit_code,org_type,industry_code,addr,area_code,mobile_phone,comments)
        try:
            cursor1.execute(sql,par)
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


#操作NY101即1表
def sum1thTableByDoc(i,recordId,orgId):
        #需要获取和record_id直接的关系
        globals()['record_id'] = recordId
        #todo mongo无数据 需要从sys_org表里拉取填充
        globals()['org_name'] = None
        org_id = orgId
        #todo 目前mongo无此数据
        globals()['credit_code'] = None
        #todo mongo无数据 需要从sys_org表里拉取填充
        globals()['org_type'] = None
        #todo mongo无数据 需要从sys_org表里拉取填充
        globals()['industry_code'] = None
        #todo mongo无数据 需要从sys_org表里拉取填充
        globals()['addr'] = None
        globals()['area_code'] = i['areaId'][:6]
        #todo mongo无数据 需要从sys_org表里拉取填充
        globals()['mobile_phone'] = None
        globals()['comments'] = None
