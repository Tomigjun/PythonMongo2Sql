#宗表  8 表
from pymongo import MongoClient
import pymysql
import operator


subfix = "_"
tableName = "public_heating_collect"
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql8 = pymysql.connect(host='192.168.127.70',port=3308,user='root',password='root',database='uap_data',charset='utf8mb4')
#连接Momgo数据库
dbName = 'ggj-working'
collectionReportName = 'stats_working_data'
collectionBaseInfoName = 'stats_working_data101'
client8 = MongoClient(host='192.168.2.197',port=27017)
database8 = client8[dbName]
collectionReport8 = database8[collectionReportName]
sql = ""
table8tuple = (
    # 集中供暖面积（按热量收费）
    '3013 : 10140',
    # 集中供暖面积（按面积收费）
    '3012 : 10139',
    # 其中：独立供暖面积
    '3011 : 10138',
    # 集中供暖费用（按面积收费）
    '380 : 10155',
    # 其他能源消费量（）
    '370 : 10158',
    # 热力消费量
    '360 : 10156',
    # 其他能源消费量（）/费用
    '371 : 10159',
    # 柴油消费量
    '350 : 10153',
    # 热力消费量/费用
    '361 : 10156',
    # 柴油消费量
    '350 : 10153',
    # 柴油消费量/费用
    '351 : 10154',
    # 煤炭消费量
    '330 : 10149',
    # 天然气消费量/费用
    '341 : 10152',
    # 电消费量
    '320 : 10147',
    # 煤炭消费量/费用
    '331 : 10150',
    # 水消费量
    '310 : 10145',
    # 电消费量/费用
    '321 : 10148',
    # 水消费量/费用
    '311 : 10146',
    # 采暖面积
    '301 : 10137',
    # 采暖天数
    '302 : 10141',
    # 热水锅炉热功率
    '303 : 10143',
    # 蒸汽锅炉蒸发量
    '304 : 10144',
)
#通过任务表中的周期和组织获取monggo宗表中的数据
def queryDocByPeriodAndOrg8(periodId,orgId,recordId,periodYear):
    res = []
    query = {"periodId": periodId,"respondentId": orgId, "reportCode": "NY303_2020"}
    collectionReport8 = database8[collectionReportName]
    count = collectionReport8.count(query)#.sort("info.usage_count_180_day", -1).limit(number)
    if(count == 0):
        return
    #定义mysql数据库的游标
    cursor8 = clientSql8.cursor()
    for i in collectionReport8.find(query):
        sum8thTableByDoc(i)
    #遍历完所有的 301表之后就生成了一个含有39行的大表:
    sql = "INSERT INTO {} ( `record_id`, `idx_id`, `total`, `government`, `subtotal`, `education`, `technology`, `civilization`, `health`, `physical`, `other`, `organization`) " \
          "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    # 插入的39条数据中的一条例下:
    #par =(recordId,10100,total_10100,government_10010,subtotal_10010,education_10010,technology_10010,civilization_10010,health_10010,physical_10010,other_10010,organization_10010);
    for item in table8tuple:
        oldKey = item.split(':')[0].strip()
        newKey = item.split(':')[1].strip()
        par =(recordId,newKey,globals()['total_'+ str(newKey)],globals()['government_'+str(newKey)],globals()['subtotal_'+str(newKey)],globals()['education_'+ str(newKey)],globals()['technology_'+str(newKey)],
              globals()['civilization_'+str(newKey)],globals()['health_'+str(newKey)],globals()['physical_'+str(newKey)],globals()['other_'+str(newKey)],globals()['organization_'+str(newKey)])
        hisTableName = tableName+subfix+periodYear
        try:
            cursor8.execute(sql.format(hisTableName), par)
            clientSql8.commit()
        except Exception as e:
            print(e)
            clientSql8.rollback()
    # try:
    #
    # except Exception as e:
    #     clientSql.rollback()
    #     cursor.close()
    #     client.close()
    #     print(e)
    # cursor.close()
    # client.close()
#批量定义变量名:从元祖里拿出来变量进行赋值
def tupSetVarAndVal (tup,prefix,rowData):
    for item in tup:
        oldKey = item.split(':')[0].strip()
        newKey = item.split(':')[1].strip()
        globals()[prefix+'_'+newKey] = rowData[oldKey]


#操作NY302基表
def sum8thTableByDoc(i):
    #需要获取和record_id直接的关系
    record_id = ""
    #周期和表名确定一条对应的mongo任务信息
    periodId = i["periodId"]
    reportCode = i["reportCode"]
    table_name = "public_computer_collect"

    rowData = i["rowData"]

    #纵列的类型
    catalogItemId = i["catalogItemId"]
    #合计
    if(catalogItemId == 'S001'):
        #批量定义变量 total_10100 total_10098 分别取得mongo对应的数据
        #生成了39个变量
        tupSetVarAndVal(table8tuple,'total',rowData)
    #国家机关
    if(catalogItemId == 'S002'):
        #批量定义变量 government_10100 government_10098 分别取得mongo对应的数据
        #生成了39个变量
        tupSetVarAndVal(table8tuple,'government',rowData)
    #小计
    if(catalogItemId == 'S003'):
        tupSetVarAndVal(table8tuple,'subtotal',rowData)
    #教育
    if(catalogItemId == 'S004'):
        tupSetVarAndVal(table8tuple,'education',rowData)
    #科技
    if(catalogItemId == 'S005'):
        tupSetVarAndVal(table8tuple,'technology',rowData)
    #文化
    if(catalogItemId == "S006"):
        tupSetVarAndVal(table8tuple,'civilization',rowData)
    #卫生
    if(catalogItemId == "S007" ):
        tupSetVarAndVal(table8tuple,'health',rowData)
    #体育
    if(catalogItemId == "S008" ):
        tupSetVarAndVal(table8tuple,'physical',rowData)
    #其他
    if(catalogItemId == "S009"):
        tupSetVarAndVal(table8tuple,'other',rowData)
    #团体组织
    if(catalogItemId == "S010"):
        tupSetVarAndVal(table8tuple,'organization',rowData)

