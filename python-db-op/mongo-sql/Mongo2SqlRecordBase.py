#任务表 基础任务表
import time
from pymongo import MongoClient
import pymysql
import operator
from UtilTool import str2PureNum


from Mongo2Sql1 import queryDocByPeriodAndOrg1
from Mongo2Sql2 import queryDocByPeriodAndOrg2
from Mongo2Sql3 import queryDocByPeriodAndOrg3
from Mongo2Sql4 import queryDocByPeriodAndOrg4

db_name = 'ggj-working'
collection_sum_name = 'stats_working_integrate_task'
collection_base_name = 'ggj-working.stats_working_gathered_respondent_task'
collectionReportName = 'stats_working_data'
collectionBaseName = 'ggj-working.stats_working_data101'
client = MongoClient(host='192.168.6.204', port=27017)
database = client[db_name]
# database.authenticate(name = 'admin', password= '123456')
collectionTaskSum = database[collection_sum_name]
collectionTaskBase = database[collection_base_name]
collectionData = database[collectionReportName]
collectionData101 = database[collectionBaseName]
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')
#定义mysql数据库的游标
cursor = clientSql.cursor()
#定义recordid不同的程序定义不同的recordid
id_index = 1
# id =  100001
id_length = 5000;
id = (id_index -1)*id_length + 1

#report_record表同步
sql = "INSERT INTO report_record (id,form_id,form_name,`type`,org_id,org_name,informant_id,informant,sta_principal_id,sta_principal,org_principal_id,org_principal,status,state,is_del,sta_year,sta_month,sta_season,report_date,created_time,updated_time,warnning_code,warning_remark,audit_remarks,mobile) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

#5表查询出来子机构要生成多条reord
# def recordOp5Doc(doc,id,form_id,form_name,type,org_id,org_name,informant_id,informant,sta_principal_id,sta_principal,org_principal_id,org_principal,status,state,is_del,sta_year,sta_month,sta_season,report_date,created_time,updated_time,warnning_code,warning_remark,audit_remarks,mobile):
#     #mongo的组织机构代码对应mysql:org_id
#     org_code  = doc["catalogItemId"]
#     periodId = doc["periodId"]
#     org_id = org_code
#     if(operator.contains(org_id,'NODE')):
#         org_id = org_id.replace("NODE","1001")
#     if(operator.contains(org_id,'SUB')):
#         org_id= org_id.replace("SUB","1002")
#     if(operator.contains(org_id,'X')):
#         org_id = org_id.replace("X",10)
#     #每一条mongo的综合任务都生成4条mysql的综合任务
#     par = (id,form_id,form_name,type,org_id,org_name,informant_id,informant,sta_principal_id,sta_principal,org_principal_id,org_principal,status,state,is_del,sta_year,sta_month,sta_season,report_date,created_time,updated_time,warnning_code,warning_remark,audit_remarks,mobile)
#     try:
#         cursor.execute(sql, par)
#         clientSql.commit()
#         #5表todo 5表有特殊逻辑要根据底下的所有的
#     except Exception as e:
#         clientSql.rollback()
#         print(e)
#     #4条任务中每条任务都对应综

res_cussor = collectionTaskBase.find().sort([('_id',-1)]).limit(100000)
# res_cussor = collectionTaskBase.find().limit(1000000).skip(1000000)
# res_data = list(res_cussor[:])
for i in res_cussor:
        # collectionTaskBase.find(no_cursor_timeout=True): #or collectionBase.find().batch_size(10000):
    #mongo的组织机构代码对应mysql:org_id
    org_code = i["respondentId"]
    periodId = i["periodId"]

    org_id = org_code
    org_id  = str2PureNum(org_id)
    # if(operator.contains(org_id,'NODE')):
    #     org_id = org_id.replace("NODE","1001")
    # if(operator.contains(org_id,'SUB')):
    #     org_id= org_id.replace("SUB","1002")
    # if(operator.contains(org_id,'X')):
    #     org_id = org_id.replace("X",'10')
    #mongo的组织名称对应mysql:org_name
    org_name = i["respondentCaption"]
    #periodId取年、月、日和季度
    sta_year = i["periodId"][:4]
    # sta_year = sta_year -1
    sta_month = i["periodId"][5:7]
    #根据period xxxxy00y  判断年报还是月报 00为年报 其余为对应月份的月报
    if(sta_month == '00'):
        type = 0
    else:
        type =1
    sta_season = int(sta_month)/3
    if(int(sta_month)%3 >0): sta_season = int(sta_season) +1
    #mongo的gatherdtime对应mysql的created_time
    timestamp = time.localtime(i['gatheredTime']/1000)
    createdTime = time.strftime("%Y-%m-%d %H:%M:%S", timestamp)
    #mongo的taskCaption对应formName
    form_name = i["taskCaption"]
    #mongo的respondentTaskStatus任务状态对应status
    #mong 9：未录入；8：录入中；-9：已退回；1：已上报；7：已审核；
    #mysql 1 未录入 2 已录入 3 已上报 4 被驳回 5已审核
    status = i["respondentTaskStatus"]
    if(status == '9') : statusRes = '0'
    if(status == '8') : statusRes = '1'
    if(status == '-9'): statusRes = '2'
    if(status =='1'): statusRes= '3'
    #state是根据status判断的如果status 7已审核那么state为1已审核,其余为0未审核
    state = '0'
    if(status == '7'):
        statusRes = '4'
        state = '1'
    #默认数据行是逻辑未删除
    isDel = '0'
    #mongo的gatherdtime对应mysql的reprotDate
    report_date = createdTime
    #mongo的gatherdtime对应mysql的updated_time
    created_time = createdTime
    #综表的只有年报
    type = '0'
    informant_id = None
    informant = i["queryData"]["unitPersonName"]
    sta_principal_id = None
    sta_principal = informant
    org_principal_id = None
    org_principal = informant
    warnning_code = None
    warning_remark =None
    mobile = i["queryData"]["unitPersonMobile"]
    is_del = 0
    updated_time = created_time
    audit_remarks = None

    #每一条mongo的综合任务都生成4条mysql的综合任务
    for j in range(4):
        id = id +j +1
        form_id =10002 +j
        record_id = id
        par = (id,form_id,form_name,type,org_id,org_name,informant_id,informant,sta_principal_id,sta_principal,org_principal_id,org_principal,statusRes,state,is_del,sta_year,sta_month,sta_season,report_date,created_time,updated_time,warnning_code,warning_remark,audit_remarks,mobile)
        try:

            #1表操作
            if(form_id == 10002):
                queryDocByPeriodAndOrg1(periodId,org_id,record_id,sta_year)
            #2表操作
            if(form_id == 10003):
                queryDocByPeriodAndOrg2(periodId,org_id,record_id,sta_year)
            #3表操作
            if(form_id == 10004):
                queryDocByPeriodAndOrg3(periodId,org_id,record_id,sta_year)
            #4表操作
            if(form_id == 10005):
                queryDocByPeriodAndOrg4(periodId,org_id,record_id,sta_year)
            cursor.execute(sql, par)
            clientSql.commit()
        except Exception as e:
            clientSql.rollback()
            cursor.close()
            client.close()
            print(e)
        #4条任务中每条任务都对应综
cursor.close()
client.close()

