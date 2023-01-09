#sys_org表
import time
from os.path import join

from pymongo import MongoClient
import pymysql
import operator
#todo 备份reposentCode
#连接mysql数据库  后面加上 charset="utf8mb4" 这样方便传输汉字，解决字符集不匹配的问题
# clientSql = pymysql.connect(host='192.168.127.70',user='root',password='root',database='uap_data',charset='utf8mb4')
clientSql = pymysql.connect(host='192.168.6.203',port=13306,user='root',password='22cc@GWLN',database='uap_data',charset='utf8mb4')
# 定义mysql数据库的游标
cursor = clientSql.cursor()
# 连接Momgo数据库
# clientMongo = MongoClient('localhost', 27017)
clientMongo = MongoClient(host='192.168.6.204', port=27017)
mongoDb = clientMongo['ggj-working']
collection = mongoDb['ggj-metadata.stats_profession_agency_info']
#先导出北京市的
# mongoQuery = {'areaCode': '110000000000'}
# public_energy_base表即：基表1
subfix = "_"
tableName = "sys_org"
sql = "INSERT INTO uap_data.sys_org (org_id,parent_id,ancestors,org_name,order_num,leader,phone,email,status,del_flag,create_by,create_time,update_by,update_time,full_name,org_code,`type`,credit_code,`level`,admin_level,funds,industry_code,org_type,province,city,area,area_code,addr,post_code,sort,contact,mobile_phone,tel_phone,fax,office_mode,office_area,have_room,room_num,is_heating,heating_type,students,lodge_ratio,univer_id_no,univer_type,hospital_level,direct_id,bind_num,respondent_code) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
# 遍历mongo数据库，加入batch_size(10000)方法解决解决 MongoDB 的 cursor id is not valid at server 问题
for i in collection.find().sort('_id',-1):
        #.batch_size(10000):
    _id = i['_id']
    # 处理mongo中respondentCode把字母都变成数字维护到mysql中
    org_code = i["respondentCode"]
    org_id = org_code
    # if(operator.contains(org_id,'NODE')):
    #     org_id = org_id.replace("NODE","1001")
    # if(operator.contains(org_id,'SUB')):
    #     org_id= org_id.replace("SUB","1002")
    # if(operator.contains(org_id,'X')):
    #     org_id = org_id.replace("X",'10')
    orgIdCharStr = ''
    for char in org_id:
        if(char.isalpha()):
            orgIdCharStr = orgIdCharStr + (str(ord(char)-65))
        else:
            orgIdCharStr = orgIdCharStr + char
    org_id = orgIdCharStr
    org_name = None
    full_name = None
    if("respondentShortCaption" in i):
        org_name = i["respondentShortCaption"]
    if("respondentCaption" in i):
        full_name = i["respondentCaption"]
    parentAgencyId = None
    if("parentAgencyId" in i):
        parentAgencyId = i["parentAgencyId"]
    parent_id= parentAgencyId
    parentCharStr = ''
    if(parent_id != None):
        for char in parent_id:
            if(char.isalpha()):
                parentCharStr = parentCharStr + (str(ord(char)-65))
            else:
                parentCharStr = parentCharStr + char
    parent_id = parentCharStr
    if(parent_id == ''):
        parent_id = 0

    # if(operator.contains(parent_id,'NODE')):
    #     parent_id = parent_id.replace("NODE","1001")
    # if(operator.contains(parent_id,'SUB')):
    #     parent_id= parent_id.replace("SUB","1002")
    # if(operator.contains(parent_id,'X')):
    #     parent_id = parent_id.replace("X",'10')
    #节点信息即祖级信息
    agencyNodeInfo = i["agencyNodeInfo"]
    ancestors = agencyNodeInfo
    ancestorsCharStr = ''
    for char in ancestors:
        if(char.isalpha()):
            ancestorsCharStr = ancestorsCharStr + (str(ord(char)-65))
        else:
            ancestorsCharStr = ancestorsCharStr + char
    ancestors = ancestorsCharStr

    # if(operator.contains(ancestors,'NODE')):
    #     ancestors = ancestors.replace("NODE","1001")
    # if(operator.contains(ancestors,'SUB')):
    #     ancestors= ancestors.replace("SUB","1002")
    # if(operator.contains(ancestors,'X')):
    #     ancestors = ancestors.replace("X",'10')
    areaCode = None
    province = None
    area_code = None
    city = None
    area = None
    if("areaCode" in i):
        areaCode = i["areaCode"]
        area_code = areaCode[:6]
        #todo 需确认，暂时使用areacode前6位
        province = areaCode[:6]
        city = area_code[6:12]
        area = city = area_code[6:12]
    elif  ("agencyAreaId" in i):
        areaCode = i["agencyAreaId"]
        area_code = areaCode[:6]
        #todo 需确认，暂时使用areacode前6位
        province = areaCode[:6]
        city = area_code[6:12]
        area = city = area_code[6:12]


    type = None
    #mysql的是1 管理机构 2 主管机构 3 公共机构
    agencyType = i["agencyType"]
    if agencyType == "0":
        type = 1
    if agencyType == "2":
        type = 2
    if agencyType == "3":
        type = 3
    if agencyType == "9":
        type = 9
    #mysql 1 国家机关 2 事业单位 3 教育事业 4 科技事业 5 文化事业 6 卫生事业 7 体育事业 8 其他 9团体组织
    organizationType = None
    if("organizationType" in i):
        organizationType = i["organizationType"]
    org_type = organizationType
    if organizationType == "01":
        org_type = 1
    if organizationType == None:
        org_type = 2
    if organizationType == "03":
        org_type = 9
    if organizationType == "025":
        org_type = 7
    if organizationType == "022":
        org_type = 4
    if organizationType == "021":
        org_type = 3
    if organizationType == '023':
        org_type = 5
    if organizationType == '024':
        org_type = 6
    if organizationType == '026':
        org_type = 8
    # org_type = None
    level = None
    if("level" in i ):
        level = i["level"]
    administrativeLevel = None
    admin_level = None
    if("administrativeLevel" in i):
        administrativeLevel = i["administrativeLevel"]
    if(administrativeLevel == '001'):
        admin_level =1
    if(administrativeLevel == '002'):
        admin_level =2
    if(administrativeLevel == '003'):
        admin_level =3
    if(administrativeLevel == '004'):
        admin_level =4
    if(administrativeLevel == '005'):
        admin_level =5
    if(administrativeLevel == '006'):
        admin_level =6
    if(administrativeLevel == '007'):
        admin_level =7
    if(administrativeLevel == '008'):
        admin_level =8
    if(administrativeLevel == '009'):
        admin_level =9
    industryClassification = None
    if("industryClassification" in i):
        industryClassification = i["industryClassification"]
    industry_code = industryClassification

    #mysql 1 全额财政 2 部分财政
    financialFunds = None
    if("financialFunds" in i):
        financialFunds = i["financialFunds"]
    funds = financialFunds
    if(funds == '001'):
        funds = 1
    if(funds == '002'):
        funds = 2
    if(funds == '003'):
        funds = 3
    addr = None
    if("address" in i):
        addr = i["address"]
    post_code = None
    if("postCode" in i):
        post_code = i["postCode"]
    createTime = i["createTime"]
    timestamp = time.localtime(createTime/1000)
    createdTime = time.strftime("%Y-%m-%d %H:%M:%S", timestamp)
    contact = None
    if("unitPersonName" in i):
        contact = i["unitPersonName"]
    mobile_phone = None
    if("unitPersonMobile" in i):
        mobile_phone = i["unitPersonMobile"]
    sort = i["order"]

     #todo 是否第一次登录 需要在表中新增字段 需确定是user表还是org表

    #todo以下是凭感觉赋值
    order_num =sort
    leader = contact
    phone = mobile_phone
    email = ""
    status = 0
    del_flag = 0
    create_by = contact
    update_by = contact
    update_time = createdTime
    credit_code = ""
    tel_phone= mobile_phone
    fax =""
    office_mode = ""
    office_area = areaCode
    have_room =None
    room_num =None
    is_heating =None
    heating_type =None
    students =None
    lodge_ratio = None
    univer_id_no = None
    univer_type = None
    hospital_level = None
    #目前未用到，如果需要实现则需要取所有agencyNodeInfo从后往前取然后查询出机构类型为管理机构的数据的id
    direct_id = None
    bind_num =None

    par = (org_id,parent_id,ancestors,org_name,order_num,leader,phone,email,status,del_flag,create_by,createdTime,update_by,update_time,full_name,org_code,type,credit_code,level,admin_level,funds,industry_code,org_type,province,city,area,area_code,addr,post_code,sort,contact,mobile_phone,tel_phone,fax,office_mode,office_area,have_room,room_num,is_heating,heating_type,students,lodge_ratio,univer_id_no,univer_type,hospital_level,direct_id,bind_num,org_code)
    try:
        cursor.execute(sql, par)
        clientSql.commit()
    except Exception as e:
        clientSql.rollback()
        print('初始失败的_id:',_id)
        print(e)

cursor.close()
clientSql.close()
