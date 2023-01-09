#engine_room表
from pymongo import MongoClient
import pymysql
import operator
import time

# 连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
from UtilTool import str2PureNum

clientSql = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')
# 定义mysql数据库的游标
cursor = clientSql.cursor()
client = MongoClient(host='192.168.6.204', port=27017)
dbName = 'ggj-working'
collectionName = 'ggj-metadata.stats_responden_computerroom'
database = client[dbName]
collection = database[collectionName]

subfix = "_"
sql = "INSERT INTO uap_data.engine_room (org_id,code,name,square,cabinets,power,it_power,ac_power,distribut_power,ups,status,created_time,updated_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
respondentCodeList = []
# 遍历mongo数据库，加入batch_size(10000)方法解决解决 MongoDB 的 cursor id is not valid at server 问题
for i in collection.find().sort('_id',-1):
    _id = i['_id']
# 处理mongo中respondentCode转换为mysql中的Org_id字段 机房编号
# 处理mongo中respondentCode把字母都变成数字维护到mysql中
    org_id = None
    if("respondentCode" in i):
        respondentCode = i["respondentCode"]
        org_id = str2PureNum(respondentCode)
#     #循环遍历取出所有的respondentCode
#     respondentCodeList.append(respondentCode)
#todo 补充havingroom和roomnum
# for respondentCode in respondentCodeList:
#     query = {"respondentCode": respondentCode}
#     for j in collection.find(query):
#         org_id = respondentCode
#         if(operator.contains(org_id,'NODE')):
#             org_id = org_id.replace("NODE","1001")
#         if(operator.contains(org_id,'SUB')):
#             org_id= org_id.replace("SUB","1002")
#         if(operator.contains(org_id,'X')):
#             org_id = org_id.replace("X","10")
#         if(operator.contains(org_id,'MB1C')):
#             org_id = org_id.replace("MB1C","1010")
#
#     #如果机构机房数量多余1个
#     if(count >= 1):
#         #则向sys_org表中相应机构添加having_room添加数据"1"代表有机房,room_num添加机房数量count
#         sql1 = "INSERT INTO sys_org (having_room,room_num) VALUES ('1',%s) WHERE org_id = %s"%(count,org_id)
#         try:
#             cursor.execute(sql1)
#             clientSql.commit()
#         except:
#             clientSql.rollback()
#     else:
#         #否则向sys_org表中相应机构添加having_room添加数据"0"代表无机房,room_num添加机房数量为0
#         sql0 = "INSERT INTO sys_org (having_room,room_num) VALUES ('0','0') WHERE org_id = %s"
#         try:
#             cursor.execute(sql0,org_id)
#             clientSql.commit()
#         except:
#             clientSql.rollback()

# 处理mongo中computerRoomCaption转换为mysql中的name字段 机房名称
    name = None
    if("computerRoomCaption" in i):
        name = i["computerRoomCaption"]

# 处理mongo中isDeleted转换为mysql中的status字段 状态（1.启用2.禁用）
    status = None
    if("isDeleted" in i):
        status = i["isDeleted"]

# 处理mongo中computerRoomTime转换为mysql中的create_time字段 创建时间
    created_time = None
    if("computerRoomTime" in i):
        computerRoomTime = i["computerRoomTime"]
        created_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(computerRoomTime/1000))

# 处理mongo中roomArea转换为mysql中的square字段 机房建筑面积
    square = None
    if("roomArea" in i):
        square = i["roomArea"]

# 处理mongo中cabinetsNum转换为mysql中的cabinets字段 机柜总数
    cabinets = None
    if("cabinetsNum" in i):
        cabinets = i["cabinetsNum"]

# 处理mongo中equipmentPower转换为mysql中的power字段 总功率
    power = None
    if("equipmentPower" in i):
        power = i["equipmentPower"]

# 处理mongo中itPower转换为mysql中的it_power字段 it设备功率
    it_power = None
    if("itPower" in i):
        it_power = i["itPower"]

# 处理mongo中airPower转换为mysql中的ac_prower字段 空气调节设备功率
    ac_power = None
    if("airPower" in i):
        ac_power = i["airPower"]

# 处理mongo中distributionPower转换为mysql中的distribut_power字段 配电及附属设备功率
    distribut_power = None
    if("distributionPower" in i):
        distribut_power = i["distributionPower"]

# 处理mongo中upsPower转换为mysql中的ups字段 UPS装机容量
    ups = None
    if("upsPower" in i):
     ups = i["upsPower"]

    # 文档中不包含的字段
    # 机房编码
    code = ""
    if("computerRoomCode" in i ):
        code = i["computerRoomCode"]
    # 更新时间
    updated_time = created_time

    par = (
        org_id,code,name,square,cabinets,power,it_power,ac_power,distribut_power,ups,status,created_time,updated_time)
    try:
        cursor.execute(sql, par)
        clientSql.commit()
    except Exception as e:
        clientSql.rollback()
        print('初始失败的_id:',_id)
        print(e)

cursor.close()
clientSql.close()
