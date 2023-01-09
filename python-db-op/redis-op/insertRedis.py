import time
import redis
import operator
# redis_pool = redis.ConnectionPool(host='127.0.0.1', port= 6379, password= 'your pw', db= 0)
# redis_conn = redis.Redis(connection_pool= redis_pool)
redis_conn = redis.Redis(host='192.168.2.197', port=6379, password='naricev5500', db= 0, decode_responses=True)
keyList = redis_conn.keys('R004003*')
valList = []
i =0;

for key in keyList:
    print ("Type Of",type(key),"key = ",key,end=" , ")
    try:
        # redis_conn.hset(key,"A005000001", 1)
        val= redis_conn.hexists(key)
        if(val == False):
           print('false')
        print ("value = ",val)
        valList.append(val)
    except Exception as e:
        print ("== redis.get(%s) 发生异常："%(key),str(e))
print(len(valList))
print(len(keyList))

# # 通过 json.loads() 将json格式字符串数据转换成字典数据类型（json反序列化操作）
# dirctObj = json.loads(localRedis.hget("testObject","001"))

# print(keyList)
# keyListRes = []
# for i in keyList:
#     val = redis_conn.get(i)
#     print(val)
#     keyListRes.append(str(i, 'UTF-8'))
#
# for j in keyListRes:
#     val = redis_conn.get(j)
#     print(val)
# # name_dict = {
# #     'name_4': 'Zarten_4',
# #     'name_5': 'Zarten_5'
# # }
# # ctx.mset(name_dict)
# # res = ctx.execute()
# print("result: ", keyListRes)
