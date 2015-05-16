import sys
import meta_api

d = { "one":"1", "two":2, "three":3, "four":4 ,"five":5 }
s = str(d)

rc = meta_api.init_redis_handle("localhost")
print "rc:", rc
rc = meta_api.add_new_record(s)
print "rc:", rc
