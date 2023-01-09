import redis
localRedis = redis.Redis(host='192.168.2.197', port=6379,  password='naricev5500',  db= 0, decode_responses=True)

keyList = localRedis.keys()

for key in keyList:
    print ("Type Of",type(key),"key = ",key,end=" , ")
    try:
        print ("value = ",localRedis.get(key))
    except Exception as e:
        print ()
        print ("== redis.get(%s) error："%(key),str(e))



