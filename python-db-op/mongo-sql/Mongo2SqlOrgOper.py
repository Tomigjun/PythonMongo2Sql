#org_oper表
from pymongo import MongoClient
import pymysql
import operator
from UtilTool import str2PureNum


# 连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
# clientSql = pymysql.connect(host="192.168.127.70", user="root", password="root", database="uap_data", charset='utf8' )
clientSql = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')

# 定义mysql数据库的游标
cursor = clientSql.cursor()
# 连接Momgo数据库
# clientMongo = MongoClient('localhost', 27017)
# clientMongo = MongoClient('mongodb://username:password@localhost:27017/ggj-metadata')
# client = MongoClient(host='192.168.2.197',port=27017)
client = MongoClient(host='192.168.6.204', port=27017)

dbName = 'ggj-working'
collectionName = 'ggj-metadata.stats_respondent_organization_person'
database = client[dbName]
collection = database[collectionName]

#  联系人信息  org_oper表
sql = "INSERT INTO org_oper (oper_name,org_id,`type`,dept,tel_phone,mobile_phone,fax_no,email,area_code,status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
# 遍历mongo数据库，加入batch_size(10000)方法解决解决 MongoDB 的 cursor id is not valid at server 问题
for i in collection.find().sort('_id',-1):
    _id = i['_id']
    # 处理mongo中respondentCode把字母都变成数字维护到mysql中
    org_id = None
    if("respondentCode" in i):
        org_code = i["respondentCode"]
        org_id = str2PureNum(org_code)
    dept = None
    if("respondentCaption" in i):
        dept = i["respondentCaption"]
    oper_name = None
    if("unitPersonName" in i):
        oper_name = i["unitPersonName"]
    tel_phone = None
    if("unitPersonMobile" in i):
        tel_phone = i["unitPersonMobile"]
    mobile_phone = None
    if("unitPersonMobile" in i):
        mobile_phone = i["unitPersonMobile"]


    # 以下是 mongo表没有的  我们表有的
    fax_no = ""
    type = 0
    email = ""
    area_code = ""
    status = 0


    par = (
        oper_name,org_id,type,dept,tel_phone,mobile_phone,fax_no,email,area_code,status)
    try:
        cursor.execute(sql, par)
        clientSql.commit()
    except Exception as e:
        clientSql.rollback()
        print('初始失败的_id:',_id)
        print(e)

cursor.close()
clientSql.close()
