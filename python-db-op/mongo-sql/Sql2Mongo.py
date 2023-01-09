# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 14:00:55 2018

@author: admin
"""
from pymongo import MongoClient
import pymysql

#--------------------------mysql数据库操作------------------------------
config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'passwd': 'root',
    'db':'uap_data',
    'charset':'utf8mb4',
    'cursorclass':pymysql.cursors.DictCursor
}

#创建连接
conn = pymysql.connect(**config)
# 执行sql语句

def query_mysql():
    try:
        with conn.cursor() as cursor:
            sql='select * from report_record limit 100'
            cursor.execute(sql)
            result= cursor.fetchall()
            #            print(result)
            return result
    finally:
        conn.close();
#--------------------mongodb数据库信息-----------------------------
settings = {
    "ip":'localhost',   #ip
    "port":27017,           #端口
    "db_name" : "myDB",    #数据库名字
    "set_name" : "myDB"   #集合名字
}
if __name__ == "__main__":
    mongo = MongoClient('localhost',27017)
    db=mongo[settings["db_name"]]
    mySet=db[settings["set_name"]]
    myConn_list = query_mysql()
    if  mySet.find().count!=0:
        mySet.remove()
        for item in myConn_list:
            mySet.insert(item);
    else:
        for item in myConn_list:
            mySet.insert(item);
