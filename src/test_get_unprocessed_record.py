import sys
import cad_config
import meta_api

rc = meta_api.init_redis_handle(cad_config.scheduler['redis_hostname'])
if rc != 0:
    print "init_redis_handle failed, rc=" + str(rc)
    sys.exit("abort")

dict ={}
(rc, dict) = meta_api.get_unprocessed_record('file_name', '*.vcf', 'privacy_rule_status')
print "rc = " + str(rc)
print "dict: " + str(dict)
pass

#rc = meta_api.add_new_record()