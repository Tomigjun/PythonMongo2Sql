#宗表  5 表
from pymongo import MongoClient
import pymysql
import operator

from UtilTool import str2PureNum

subfix = "_"
tableName = 'public_energy_grade'
# tableName = 'public_energy_grade'
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql5 =pymysql.connect(host='192.168.127.70',user='root',password='root',database='uap_data',charset='utf8mb4')
#定义mysql数据库的游标
cursor5 = clientSql5.cursor()
#连接Momgo数据库
dbName = 'ggj-working'
collectionReportName = 'stats_working_data'
collectionBaseInfoName = 'stats_working_data101'
client5 = MongoClient(host='192.168.2.197', port=27017)
database5 = client5[dbName]
# database.authenticate('admin', '123456')
collectionReport5 = database5[collectionReportName]
collectionBaseInfo5 = database5[collectionBaseInfoName]
sql = "INSERT INTO {} (record_id,idx_id,p_sum,p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
#report_record表同步
sqlRecord = "INSERT INTO report_record (id,form_id,form_name,`type`,org_id,org_name,informant_id,informant,sta_principal_id,sta_principal,org_principal_id,org_principal,status,state,is_del,sta_year,sta_month,sta_season,report_date,created_time,updated_time,warnning_code,warning_remark,audit_remarks,mobile) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
table5tuple =(
    '101:p0',
    '102:p1',
    '103:p2',
    '1031:p3',
    '104:p4',
    '1041:p5',
    '1042:p6',
    '1043:p7',
    '110:p8',
    '111:p9',
    '120:p10',
    '121:p11',
    '130:p12',
    '131:p13',
    '140:p14',
    '141:p15',
    '150:p16',
    '151:p17',
    '1501:p18',
    '1511:p19',
    '1502:p20',
    '1512:p21',
    '160:p22',
    '161:p23',
    '1601:p24',
    '1611:p25',
    '1602:p26',
    '1612:p27',
    '105:p28',
    '1051:p29',
    '170:p30',
    '171:p31',
    '180:p32',
    '181:p33',
    '106:p34',
    # 'None:p35',
    '190:p36',
    '192:p35',
    '190:p36',
    '191:p37',
    '192:p38',
    # 'None:p39',
)
table5Dict= {
    '101':'p0',
    '102':'p1',
    '103':'p2',
    '1031':'p3',
    '104':'p4',
    '1041':'p5',
    '1042':'p6',
    '1043':'p7',
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
    '105':'p28',
    '1051':'p29',
    '170':'p30',
    '171':'p31',
    '180':'p32',
    '181':'p33',
    '106':'p34',
    # 'None':'p35',
    '190':'p36',
    '192':'p35',
    '190':'p36',
    '191':'p37',
    '192':'p38',
    # 'None':'p39',
}
#p35和p39没有对应字段直接为空
p35 = None
p39 = None
table5KeyList = []
table5ParamList = []

for table5tupleEach in table5tuple:
    key = table5tupleEach.split(':')[0].strip()
    sqlParam = table5tupleEach.split(':')[1].strip()
    table5KeyList.append(key)
    table5ParamList.append(sqlParam)

#获取当前doc中的所有key
def keysList(doc):
    keys = []
    for key in doc:
        keys.append(key)
    return keys
#设置插入数据库对应字段的tuple,如果keylist中没有就赋值为Null
def keyNotExistValueSetParam(doc):
    keysExist = keysList(doc)
    keysNotExist = list(set(table5KeyList).difference(set(keysExist)))
    paramNotExist = []
    paramExist = []
    #如果key不存在那么对应的mysql中的字段就为空
    for keysNotExistEach in keysNotExist:
        valNotExistEach  = table5Dict.get(keysNotExistEach)
        # paramNotExist.append(valNotExistEach)
        try:
            globals()[valNotExistEach] = None
        except Exception as e:
            print(valNotExistEach)
            print(e)
        # exec('{} = {}'.format(valNotExistEach,None))
    for keysExistEach in keysExist:
        valExistEach = table5Dict.get(keysExistEach)
        # paramExist.append(valExistEach)
        try:
            globals()[valExistEach] = doc[keysExistEach]
            # exec('{} = {}'.format(valExistEach,doc[keysExistEach]))
        except Exception as e:
            print(keysExistEach)
            print(e)


#5表查询出来子机构要共用reord
def recordOp5Doc(id,form_id,form_name,type,org_id,org_name,informant_id,informant,sta_principal_id,sta_principal,org_principal_id,org_principal,status,state,is_del,sta_year,sta_month,sta_season,report_date,created_time,updated_time,warnning_code,warning_remark,audit_remarks,mobile):
    #mongo的组织机构代码对应mysql:org_id
    org_id = str2PureNum(org_id)
    par = (id,form_id,form_name,type,org_id,org_name,informant_id,informant,sta_principal_id,sta_principal,org_principal_id,org_principal,status,state,is_del,sta_year,sta_month,sta_season,report_date,created_time,updated_time,warnning_code,warning_remark,audit_remarks,mobile)
    try:
        cursor5.execute(sqlRecord, par)
        clientSql5.commit()
        # cursor5.close()
        #5表todo 5表有特殊逻辑要根据底下的所有的
    except Exception as e:
        clientSql5.rollback()
        print(e)
    #todo4条任务中每条任务都对应综



#通过report_code和orgid获取当前市的所有区的id,并生成多条record,然后插入
def getAllRegionAndCreateRecord(id,form_id,form_name,type,org_id,org_name,informant_id,informant,sta_principal_id,sta_principal,org_principal_id,org_principal,status,state,is_del,sta_year,sta_month,sta_season,report_date,created_time,updated_time,warnning_code,warning_remark,audit_remarks,mobile):
    reportCode = 'NY304_' + sta_year
    hisTableName = tableName+subfix+sta_year
    query = {"reportCode": reportCode, "respondentId": org_id}
    for i in collectionReport5.find(query):
        if(operator.contains(i['catalogItemId'],org_id)):
            recordId = id
            #批量生成全局变量p0-p39
            keyNotExistValueSetParam(i["rowData"])
            par = (recordId,i["catalogInfos"][0],i["rowData"]['enterpriseCount'],p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27,p28,p29,p30,p31,p32,p33,p34,p35,p36,p37,p38,p39)
            try:
                cursor5.execute(sql.format(hisTableName), par)
                clientSql5.commit()
            except Exception as e:
                clientSql5.rollback()
                print(e)
# clientSql.close()
# client.close()
