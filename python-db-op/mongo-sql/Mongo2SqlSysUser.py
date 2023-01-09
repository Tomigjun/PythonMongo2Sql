#sys_user表
import time

from pymongo import MongoClient
import pymysql
import operator

# 连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
from UtilTool import str2PureNum

# clientSql = pymysql.connect(host="192.168.127.70", user="root", password="root", database="uap_data", charset='utf8' )
clientSql = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')

# 定义mysql数据库的游标
cursor = clientSql.cursor()
# 连接Momgo数据库
# clientMongo = MongoClient('localhost', 27017)
# clientMongo = MongoClient('mongodb://username:password@localhost:27017/ggj-metadata')
client = MongoClient(host='192.168.6.204', port=27017)
dbName = 'ggj-working'
collectionName = 'ggj-common.stats_respondent_user'
database = client[dbName]
collection = database[collectionName]
#先获取北京的用户
# mongoQuery = {'principalConfig.areaId': '110000000000'}
#  账号信息  sys_user表
sql = "INSERT INTO uap_data.sys_user (org_id,user_name,nick_name,user_type,email,phonenumber,sex,avatar,password,status,del_flag,login_ip,create_by,create_time,update_by,update_time,remark,id_card,`type`,is_initial_pwd,ip_begin,ip_end,login_time_begin,login_time_end) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
# 遍历mongo数据库，加入batch_size(10000)方法解决解决 MongoDB 的 cursor id is not valid at server 问题
for i in collection.find().sort('_id',-1):
    _id = i['_id']
    # 处理mongo中respondentId把字母都变成数字维护到mysql中
    org_code = i["respondentId"]
    org_id = str2PureNum(org_code)
    user_name = i["_id"]
    phonenumber = None
    if("legalMobile" in i):
        phonenumber = i["legalMobile"]
    nick_name = None
    if("userCaption" in i):
        nick_name = i["userCaption"]
    user_type = None
    if("user_type" in i):
        user_type = i["userType"]
    password = '$2a$10$urbgoR3NzEbG7QDNwEOPNurlcOWMhfWFTPd/kzFzG2qWKz0QylkGy'
    #密码重置为1qaz!QAZ
    create_time = None
    if("createTime" in i):
        createTime = i["createTime"]
        create_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(createTime/1000))
    update_time = None
    if("updateTime" in i):
        updateTime = i["updateTime"]
        update_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(updateTime/1000))


    # 以下是 mongo表没有的  我们表有的
    email = ""
    sex = ""
    avatar = ""
    status = "0"
    del_flag = "0"
    login_ip = ""
    create_by = ""
    update_by = ""
    remark = ""
    id_card = ""
    type = ""
    is_initial_pwd = ""
    ip_begin = ""
    ip_end = ""
    login_time_begin = ""
    login_time_end = ""


    par = (
        org_id,user_name,nick_name,user_type,email,phonenumber,sex,avatar,password,status,del_flag,login_ip,create_by,create_time,update_by,update_time,remark,id_card,type,is_initial_pwd,ip_begin,ip_end,login_time_begin,login_time_end)
    try:
        cursor.execute(sql, par)
        clientSql.commit()
    except Exception as e:
        clientSql.rollback()
        print('初始失败的_id:',_id)
        print(e)

cursor.close()
clientSql.close()
