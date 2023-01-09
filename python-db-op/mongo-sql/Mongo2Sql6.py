#宗表  6 表
import re

from pymongo import MongoClient
import pymysql
import operator


subfix = "_"
tableName = "public_energy_collect"
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql6 = pymysql.connect(host='192.168.127.70',port=3308,user='root',password='root',database='uap_data',charset='utf8mb4')
#连接Momgo数据库
dbName = 'ggj-working'
collectionReportName = 'stats_working_data'
client6 = MongoClient(host='192.168.2.197', port=27017)
database6 = client6[dbName]
sql = ""
table6tuple = (
    #mongo的key值:form_idx的id
    # 太阳能光热利用系统集热器面积
    '190 : 10100',
    # 新能源汽车数量
    '1043 : 10031',
    #太阳能光电利用系统装机容量
    '191 :10101',
    #柴油车数量
    '1042 : 10030',
    #热力消费量
    '170 : 10094',
    #地热能利用系统装机容量
    '192 : 10102',
    #汽油车数量
    '1041 : 10069',
    #热力消费量/费用
    '171 : 10095',
    #汽油消费量
    '150 : 10080',
    #汽油消费量/费用
    '151 : 10081',
    #煤炭消费量
    '130 : 10076',
    #煤炭消费量/费用
    '131 : 10077',
    #110 电消费量
    '110 : 10072',
    #111 电消费量/费用
    '111 : 10073',
    #1612 柴油消费量/其他用油量/费用
    '1612 : 10091',
    #1512 汽油消费量/其他用油量/费用
    '1512 : 10085',
    #1611 柴油消费量/车辆用油量/费用
    '1611 : 10089',
    #1511 汽油消费量/车辆用油量/费用
    '1511 : 10083',
    #180 其他能源消费量
    '180 : 10096',
    #1031 编制人数
    '1031 : 10027',
    #181 其他能源消费量/费用
    '181 : 10097',
    #160 柴油消费量
    '160 : 10086',
    #161 柴油消费量/费用
    '161 : 10087',
    #1051 液化石油气消费量/费用
    '1051 : 10093',
    #140 天然气消费量
    '140 : 10078',
    #141 天然气消费量/费用
    '141 : 10079',
    #120 水消费量
    '120 : 10074',
    #121 水消费量/费用
    '121 : 10075',
    #1602 柴油消费量/其他用油量
    '1602 : 10090',
    #100 公共机构数量
    '100 : 10023',
    #1502 汽油消费量/其他用油量
    '1502 :10084',
    #1601 柴油消费量/车辆用油量
    '1601 :10088',
    #101 用地面积
    '101 :10064',
    #1501 汽油消费量/车辆用油量
    '1501 : 10082',
    #102 建筑面积
    '102 : 10065',
    #103 用能人数
    '103 : 10066',
    #104 车辆总量
    '104 : 10068',
    #105 液化石油气消费量
    '105 : 10092',
    #106 充电桩数量
    '106 : 10058',
)
#通过任务表中的周期和组织获取monggo宗表中的数据
def queryDocByPeriodAndOrg6(periodId,orgId,recordId,periodYear):
    res = []
    collectionReport6 = database6[collectionReportName]
    #定义mysql数据库的游标
    cursor6 = clientSql6.cursor()
    query = {"periodId": periodId, "respondentId": orgId, "reportCode": "NY301_2020"}
    count = collectionReport6.count(query)
    if(count == 0):
        return
    for i in collectionReport6.find(query):
        sum6thTableByDoc(i)
    #遍历完所有的 301表之后就生成了一个含有39行的大表:
    sql = "INSERT INTO {} ( `record_id`, `idx_id`, `total`, `government`, `subtotal`, `education`, `technology`, `civilization`, `health`, `physical`, `other`, `organization`) " \
          "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    # 插入的39条数据中的一条例下:
    #par =(recordId,10100,total_10100,government_10010,subtotal_10010,education_10010,technology_10010,civilization_10010,health_10010,physical_10010,other_10010,organization_10010);
    for item in table6tuple:
        oldKey = item.split(':')[0].strip()
        newKey = item.split(':')[1].strip()
        par =(recordId,newKey,globals()['total_'+ str(newKey)],globals()['government_'+str(newKey)],globals()['subtotal_'+str(newKey)],globals()['education_'+ str(newKey)],globals()['technology_'+str(newKey)],
               globals()['civilization_'+str(newKey)],globals()['health_'+str(newKey)],globals()['physical_'+str(newKey)],globals()['other_'+str(newKey)],globals()['organization_'+str(newKey)])
        hisTableName = tableName+subfix+periodYear
        try:
            cursor6.execute(sql.format(hisTableName), par)
            clientSql6.commit()
        except Exception as e:
            print(e)
            clientSql6.rollback()
    # cursor.close()
    # client.close()

#批量定义变量名:从元祖里拿出来变量进行赋值
def tupSetVarAndVal (tup,prefix,rowData):
    for item in tup:
        oldKey = item.split(':')[0].strip()
        newKey = item.split(':')[1].strip()
        globals()[prefix+'_'+newKey]= rowData[oldKey]





#操作NY301即6表
def sum6thTableByDoc(i):
        #需要获取和record_id直接的关系
        record_id = ""
        #周期和表名确定一条对应的mongo任务信息
        periodId = i["periodId"]
        reportCode = i["reportCode"]
        table_name = "public_energy_collect"

        rowData = i["rowData"]

        #纵列的类型
        catalogItemId = i["catalogItemId"]

        #合计
        if(catalogItemId == 'S001'):
            #批量定义变量 total_10100 total_10098 分别取得mongo对应的数据
            #生成了39个变量
            tupSetVarAndVal(table6tuple,'total',rowData)
        #国家机关
        if(catalogItemId == 'S002'):
            #批量定义变量 government_10100 government_10098 分别取得mongo对应的数据
            #生成了39个变量
            tupSetVarAndVal(table6tuple,'government',rowData)
        #小计
        if(catalogItemId == 'S003'):
            tupSetVarAndVal(table6tuple,'subtotal',rowData)
        #教育
        if(catalogItemId == 'S004'):
            tupSetVarAndVal(table6tuple,'education',rowData)
        #科技
        if(catalogItemId == 'S005'):
            tupSetVarAndVal(table6tuple,'technology',rowData)
        #文化
        if(catalogItemId == "S006"):
            tupSetVarAndVal(table6tuple,'civilization',rowData)
        #卫生
        if(catalogItemId == "S007" ):
            tupSetVarAndVal(table6tuple,'health',rowData)
        #体育
        if(catalogItemId == "S008" ):
            tupSetVarAndVal(table6tuple,'physical',rowData)
        #其他
        if(catalogItemId == "S009"):
            tupSetVarAndVal(table6tuple,'other',rowData)
        #团体组织
        if(catalogItemId == "S010"):
            tupSetVarAndVal(table6tuple,'organization',rowData)
