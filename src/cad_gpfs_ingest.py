#!/usr/bin/python
import sys
import subprocess
import re
import json
import os.path
import tempfile
import hashlib
import meta_api
import cad_analyze_vcf

# Filename : cad_gpfs_ingest.py
# Purpose  : Independent program to search gpfs file system and ingest metadata
#            into the Meta Engine

# Change Log:
# 20150505 - initial release
#
#
# class - gpfs_class
#
#
class gpfs_class:
    
    def __init__(self):
        try:
            # abort if I am not running on a gpfs client node
            subprocess.check_call(["which", "mmgetstate"])
        
            # abort if mmgetstate is not in active state
            cmd = "/usr/lpp/mmfs/bin/mmgetstate | grep active | wc -l"
            output = subprocess.check_output(cmd, shell=True)
            if int(output) != 1:
                sys.exit("FATAL - GPFS not in active state")
            return
            
        except subprocess.CalledProcessError:
            sys.exit("FATAL - GPFS client not installed")
        return

    def apply_query_policy(self, days):

        # this is the base gpfs policy used in mmapplypolicy
        base_policy = """
define(LAST_MODIFIED,(DAYS(CURRENT_TIMESTAMP)-DAYS(MODIFICATION_TIME)))
RULE EXTERNAL LIST 'AllFiles' EXEC 'python cad_gpfs_exec.py' ESCAPE '%/, '
RULE 'ListAllFiles' LIST 'AllFiles' DIRECTORIES_PLUS
SHOW( VARCHAR( FILESET_NAME )   || ' ' ||
      VARCHAR( POOL_NAME )      || ' ' ||
      VARCHAR( FILE_SIZE )      || ' ' ||
      VARCHAR( KB_ALLOCATED )   || ' ' ||
      VARCHAR( USER_ID )        || ' ' ||
      VARCHAR( GROUP_ID )       || ' ' ||
      VARCHAR( DAYS( CREATION_TIME ))     || ' ' ||
      VARCHAR( DAYS( MODIFICATION_TIME )) || ' ' ||
      VARCHAR( DAYS( CHANGE_TIME ))       || ' ' ||
      VARCHAR( DAYS( ACCESS_TIME ))       || ' ' ||
      CASE WHEN XATTR( 'dmapi.IBMObj' ) IS NOT NULL THEN 'M'
           WHEN XATTR( 'dmapi.IBMPMig') IS NOT NULL THEN 'P'
           ELSE 'R'
      END ) \n"""

        # parse json file - caddy configuration data
        f = os.path.basename(sys.argv[0])
        s = re.match("(\w+).(\w+)", f)
        jfile = s.group(1) + ".json"
        try:
            jdata = json.loads(open(jfile).read())
        except:
            sys.exit("FATAL - json file format error")

        # extract configuration data
        try:
            gwd = jdata['gwd']
            lwd = jdata['lwd']
            maxfiles = jdata['maxfiles']
            threadlevel = jdata['threadlevel']
            nodelist = jdata['nodelist']
            debuglevel = jdata['debuglevel']
            redis_hostname = jdata['redis_hostname']
            mtime_rule = "(LAST_MODIFIED <= "+str(days)+")"

        except KeyError:
            sys.exit("FATAL - json file config key missing")

        # loop through search list
        for key in jdata['search_list']:
            try:
                fsname = key['fsname']
                fileset_rule = "FOR FILESET ('" +key['fileset']+ "')\n"
                if days == 0:
                    select_rule = "WHERE "+key['select']+"\n"
                else:
                    select_rule = "WHERE "+mtime_rule+" AND "+key['select']+"\n"
                
                # write customize policy conditions - fileset, mtime, sql filter
                f = tempfile.NamedTemporaryFile(delete=False)
                f.write(base_policy)
                f.write(fileset_rule)
                f.write(select_rule)
                policy_file = f.name
                f.close()
                print policy_file

            except KeyError:
                sys.exit("FATAL - json file search list key missing")

            # run mmapplypolicy to query file metadata (use "-I defer" for testing)
            cmd = ('/usr/lpp/mmfs/bin/mmapplypolicy '+fsname+
            ' -P '+policy_file+' -B '+maxfiles+' -m '+threadlevel+
            ' -g '+gwd+' -s '+lwd+' -N '+nodelist+' -L '+debuglevel)
            try:
                subprocess.check_call(cmd, shell=True)
                os.remove(policy_file)
            except subprocess.CalledProcessError:
                sys.exit("FATAL - mmapplypolicy command failed")                
        return    

    def process_filelist(self, filelist):
    
        # process filelist generated by the gpfs policy engine
        # Format: InodeNumber GenNumber SnapId [OptionalShowArgs] -- FullPathToFile
        #   [ShowArgs]:
        #   fileset_name       :str 4
        #   pool_name          :str 5
        #   file_size          :int 6
        #   kb_allocated       :int 7
        #   user_id            :str 8
        #   group_id           :str 9
        #   creation_timed     :int 10
        #   modification_timed :int 11
        #   change_timed       :int 12
        #   access_timed       :int 13
        #   dmapi_state        :'R','P',"M' 14
    
        vcfc = cad_analyze_vcf.vcf_class()
        with open(filelist, 'r') as fh:
            for line in fh:
                try:
                    #              in1   gen2  snp3    fs4   po5   sz6   kb7
                    #              uid8  gid9  rt10  mt11  ct12  at13  dm14 
                    #              file15
                    p = re.match("(\d+) (\d+) (\d+)\s+(\S+) (\S+) (\d+) (\d+) "
                                 "(\S+) (\S+) (\d+) (\d+) (\d+) (\d+) (\S+) --"
                                 "(.*)", line)
                except:
                    print "ERROR - regex match error:", line
                    continue
                
                # *** SCHEMA DEFINITION ***
                r = {'filename_hash'      : hashlib.sha224(p.group(15)).hexdigest(),
                     'fileset_name'       : p.group(4),
                     'pool_name'          : p.group(5),
                     'file_size'          : p.group(6),
                     'kb_allocated'       : p.group(7),
                     'user_id'            : p.group(8),
                     'group_id'           : p.group(9),
                     'creation_timed'     : p.group(10),
                     'modification_timed' : p.group(11),
                     'change_timed'       : p.group(12),
                     'access_timed'       : p.group(13),
                     'dmapi_state'        : p.group(14),
                     'file_name'          : p.group(15).lstrip()}

                print r
                rc = meta_api.meta_add_new_rec(r)
                print "rc:", rc
                vcfc.process_vcf_file(p.group(15))
        fh.close()
        return
#
#
# main program
# Params   :
#   $1 - days, ingest file with mtime < days, zero=ingest all (default)
#
if __name__ == "__main__":
    # process input argv
    try:
        days = sys.argv[1]
        
    except IndexError:
        days = 0

    # run GPFS mmapplypolicy
    gpfs = gpfs_class()
    gpfs.apply_query_policy(days)

    print "I am done"
    