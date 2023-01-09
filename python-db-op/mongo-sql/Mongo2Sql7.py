#宗表  7 表
from pymongo import MongoClient
import pymysql
import operator


subfix = "_"
tableName = "public_computer_collect"
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
clientSql7 = pymysql.connect(host='192.168.127.70',port=3308,user='root',password='root',database='uap_data',charset='utf8mb4')
#连接Momgo数据库
dbName = 'ggj-working'
collectionReportName = 'stats_working_data'
collectionBaseInfoName = 'stats_working_data101'
client7 = MongoClient(host='192.168.2.197',port=27017)
database7 = client7[dbName]
collectionBaseInfo7 = database7[collectionBaseInfoName]
sql = ""
table7tuple = (
    #250 其他能源消费量
    '250 : 10096',
    #240 总用电量
    '240 : 10122',
    #230 UPS装机容量
    '230 : 10121',
    #220 设备总功率
    '220 : 10117',
    #210 机柜总数量
    '210 : 10116',
    #200 机房数量
    '200 : 10114',
    #2403 配电及附属设备用电量
    '2403 : 10125',
    #201 机房建筑面积
    '201 : 10115',
    #2402 空气调节设备用电量
    '2402 : 10124',
    #2203 配电及附属设备功率
    '2203 : 10120',
    #2401 IT设备用电量
    '2401 : 10123',
    #2202 空气调节设备功率
    '2202 : 10119',
    #2201 IT设备功率
    '2201 : 10118',
)
#通过任务表中的周期和组织获取monggo宗表中的数据
def queryDocByPeriodAndOrg7(periodId,orgId,recordId,periodYear):
    res = []
    query = {"periodId": periodId,"respondentId": orgId, "reportCode": "NY302_2020"}
    collectionReport7 = database7[collectionReportName]
    count = collectionReport7.count(query)#.sort("info.usage_count_180_day", -1).limit(number)
    if(count == 0):
        return
    #定义mysql数据库的游标
    cursor7 = clientSql7.cursor()
    for i in collectionReport7.find(query):
            sum7thTableByDoc(i)
    #遍历完所有的 301表之后就生成了一个含有39行的大表:
    sql = "INSERT INTO {} ( `record_id`, `idx_id`, `total`, `government`, `subtotal`, `education`, `technology`, `civilization`, `health`, `physical`, `other`, `organization`) " \
          "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    # 插入的39条数据中的一条例下:
    #par =(recordId,10100,total_10100,government_10010,subtotal_10010,education_10010,technology_10010,civilization_10010,health_10010,physical_10010,other_10010,organization_10010);
    for item in table7tuple:
        oldKey = item.split(':')[0].strip()
        newKey = item.split(':')[1].strip()
        par =(recordId,newKey,globals()['total_'+ str(newKey)],globals()['government_'+str(newKey)],globals()['subtotal_'+str(newKey)],globals()['education_'+ str(newKey)],globals()['technology_'+str(newKey)],
              globals()['civilization_'+str(newKey)],globals()['health_'+str(newKey)],globals()['physical_'+str(newKey)],globals()['other_'+str(newKey)],globals()['organization_'+str(newKey)])
        hisTableName = tableName+subfix+periodYear
        try:
            cursor7.execute(sql.format(hisTableName), par)
            clientSql7.commit()
        except Exception as e:
            print(e)
            clientSql7.rollback()
    # cursor.close()
    # client.close()
#批量定义变量名:从元祖里拿出来变量进行赋值
def tupSetVarAndVal (tup,prefix,rowData):
    for item in tup:
        oldKey = item.split(':')[0].strip()
        newKey = item.split(':')[1].strip()
        globals()[prefix+'_'+newKey] = rowData[oldKey]


#操作NY302基表
def sum7thTableByDoc(i):
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
            tupSetVarAndVal(table7tuple,'total',rowData)
        #国家机关
        if(catalogItemId == 'S002'):
            #批量定义变量 government_10100 government_10098 分别取得mongo对应的数据
            #生成了39个变量
            tupSetVarAndVal(table7tuple,'government',rowData)
        #小计
        if(catalogItemId == 'S003'):
            tupSetVarAndVal(table7tuple,'subtotal',rowData)
        #教育
        if(catalogItemId == 'S004'):
            tupSetVarAndVal(table7tuple,'education',rowData)
        #科技
        if(catalogItemId == 'S005'):
            tupSetVarAndVal(table7tuple,'technology',rowData)
        #文化
        if(catalogItemId == "S006"):
            tupSetVarAndVal(table7tuple,'civilization',rowData)
        #卫生
        if(catalogItemId == "S007" ):
            tupSetVarAndVal(table7tuple,'health',rowData)
        #体育
        if(catalogItemId == "S008" ):
            tupSetVarAndVal(table7tuple,'physical',rowData)
        #其他
        if(catalogItemId == "S009"):
            tupSetVarAndVal(table7tuple,'other',rowData)
        #团体组织
        if(catalogItemId == "S010"):
            tupSetVarAndVal(table7tuple,'organization',rowData)
