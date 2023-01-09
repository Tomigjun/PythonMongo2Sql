# #engine_room表
# from pymongo import MongoClient
# import pymysql
# import operator
# import time
#
# # 连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
# clientSql = pymysql.connect(host='localhost',user='root',password='123456',database='uap_data',charset='utf8')
# # 定义mysql数据库的游标
# cursor = clientSql.cursor()
# # 连接Momgo数据库
# # clientMongo = MongoClient('localhost', 27017)
# #clientMongo = MongoClient('mongodb://admin:123456@192.168.2.197:27017/db')
# client = MongoClient(host='192.168.2.197',port=27017)
# dbName = 'ggj-working'
# collectionName = 'stats_respondent_computerroom'
# database = client[dbName]
# collection = database[collectionName]
#
# subfix = "_"
# sql = "INSERT INTO uap_data.engine_room (org_id,code,name,square,cabinets,power,it_power,ac_power,distribut_power,ups,status,created_time,updated_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
#
# # 遍历mongo数据库，加入batch_size(10000)方法解决解决 MongoDB 的 cursor id is not valid at server 问题
# for i in collection.find().batch_size(10000):
# # 处理mongo中_id转换为mysql中的id字段
#     _id = i["_id"]
#     id = _id
#     if(operator.contains(id,'NODE')):
#         id = id.replace("NODE","1001")
#     if(operator.contains(id,'SUB')):
#         id = id.replace("SUB","1002")
#     if(operator.contains(id,'X')):
#         id = id.replace("X","10")
#
# # 处理mongo中respondentCode转换为mysql中的Org_id字段 机房编号
# # 处理mongo中respondentCode把字母都变成数字维护到mysql中
#     respondentCode = i["respondentCode"]
#     org_id = respondentCode
#     if(operator.contains(org_id,'NODE')):
#         org_id = org_id.replace("NODE","1001")
#     if(operator.contains(org_id,'SUB')):
#         org_id= org_id.replace("SUB","1002")
#     if(operator.contains(org_id,'X')):
#         org_id = org_id.replace("X","10")
#     if(operator.contains(org_id,'MB1C')):
#         org_id = org_id.replace("MB1C","1010")
#
# # 处理mongo中computerRoomCaption转换为mysql中的name字段 机房名称
#     name = i["computerRoomCaption"]
#
# # 处理mongo中isDeleted转换为mysql中的status字段 状态（1.启用2.禁用）
#     status = i["isDeleted"]
#
# # 处理mongo中computerRoomTime转换为mysql中的create_time字段 创建时间
#     computerRoomTime = i["computerRoomTime"]
#     created_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(computerRoomTime/1000))
#
# # 处理mongo中roomArea转换为mysql中的square字段 机房建筑面积
#     square = i["roomArea"]
#
# # 处理mongo中cabinetsNum转换为mysql中的cabinets字段 机柜总数
#     cabinets = i["cabinetsNum"]
#
# # 处理mongo中equipmentPower转换为mysql中的power字段 总功率
#     power = i["equipmentPower"]
#
# # 处理mongo中itPower转换为mysql中的it_power字段 it设备功率
#     it_power = i["itPower"]
#
# # 处理mongo中airPower转换为mysql中的ac_prower字段 空气调节设备功率
#     ac_power = i["airPower"]
#
# # 处理mongo中distributionPower转换为mysql中的distribut_power字段 配电及附属设备功率
#     distribut_power = i["distributionPower"]
#
# # 处理mongo中upsPower转换为mysql中的ups字段 UPS装机容量
#     ups = i["upsPower"]
#
#     # 文档中不包含的字段
#     # 机房编码
#     code = None
#     # 更新时间
#     updated_time = created_time
#
#     par = (
#         org_id,code,name,square,cabinets,power,it_power,ac_power,distribut_power,ups,status,created_time,updated_time)
#     try:
#         cursor.execute(sql, par)
#         clientSql.commit()
#     except Exception as e:
#         clientSql.rollback()
#         print(e)
#
# respondentCodes = []
# #查询机构，拼成一个集合
# for n in collection.find().batch_size(10000):
#     respondentCodes.append(n["respondentCode"])
#
# #筛选机构
# respondentCodes = list(set(respondentCodes))
#
# #遍历机构
# for k in respondentCodes:
#     org_id = k
#     if(operator.contains(org_id,'NODE')):
#         org_id = org_id.replace("NODE","1001")
#     if(operator.contains(org_id,'SUB')):
#         org_id= org_id.replace("SUB","1002")
#     if(operator.contains(org_id,'X')):
#         org_id = org_id.replace("X","10")
#     if(operator.contains(org_id,'MB1C')):
#         org_id = org_id.replace("MB1C","1010")
#     print(org_id)
#
#     #设置计数器统计数量
#     count = 0
#
#     #遍历表
#     for j in collection.find().batch_size(10000):
#     #如果当前机房所属机构号与其他机房所属机构号有重复
#         if(k == j["respondentCode"]):
#         #该机构机房数量增加1
#             count += 1
#     print(count)
#
#     #如果机构机房数量多于1个
#     if(count >= 1):
#         #则向sys_org表中相应机构添加having_room添加数据"1"代表有机房,room_num添加机房数量count
#         sql1 = "UPDATE uap_data.sys_org SET have_room = 1,room_num = %s WHERE org_id = %s"
#         par1 = (count,org_id)
#         try:
#             cursor.execute(sql1,par1)
#             clientSql.commit()
#         except Exception as e:
#             clientSql.rollback()
#             print(e)
#     else:
#         #否则向sys_org表中相应机构添加having_room添加数据"0"代表无机房,room_num添加机房数量为0
#         sql0 = "UPDATE uap_data.sys_org SET have_room = 0,room_num = 0 WHERE org_id = %s"
#         par0 = (org_id)
#         try:
#             cursor.execute(sql0,par0)
#             clientSql.commit()
#         except:
#             clientSql.rollback()
#
# cursor.close()
# clientSql.close()
