import sys
import subprocess
import logging
import re
import os.path
import datetime
import tempfile
import hashlib
import cad_config
import meta_api


# Filename : cad_gpfs_ingest.py
# Purpose  : Independent program to search gpfs file system and ingest metadata
#            into the Meta Engine

# Change Log:
# 20150505 - initial release
# 20150610 - converted json config file to cad_config module parameters
# 20150610 - converted all runtime messages to python loggong
#
#
# functions
#
#
def exec_shell(cmd):
    logging.debug("exec_shell: " + cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    out = []

    while True:
        line = p.stdout.readline()
        if line == '' and p.poll() != None:
            break
        else:
            line = line.rstrip()
            logging.debug("+++ " + line)
            out.append(line)

    logging.debug("exec_shell: rc=" + str(p.returncode))
    return p.returncode, out
#
#
# class - gpfs_class
#
#
class gpfs_class:

    def __init__(self):
        try:
            # abort if mmgetstate is not in active state
            cmd = "/usr/lpp/mmfs/bin/mmgetstate | grep active | wc -l"
            out = subprocess.check_output(cmd, shell=True)
            if int(out) != 1:
                logging.critical("__init__: GPFS not in active state")
                sys.exit("cad_gpfs_ingest.py - abort, GPFS not active")

        except subprocess.CalledProcessError:
            logging.critical("__init__: GPFS client not installed")
            sys.exit("cad_gpfs_ingest.py - abort, GPFS not installed")

        logging.debug("__init__: GPFS ready")
        return


    def apply_query_policy(self, days):

        # this is the base gpfs policy used in mmapplypolicy
        base_policy = """
define(LAST_MODIFIED,(DAYS(CURRENT_TIMESTAMP)-DAYS(MODIFICATION_TIME)))
RULE EXTERNAL LIST 'AllFiles' EXEC '/usr/local/bin/python cad_gpfs_exec.py' ESCAPE '%/, '
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


        # abort if search_pattern or directories list is empty
        if len(cad_config.search_pattern) == 0:
            logging.error("cad_config.conf: search_pattern list is empty")
            return 1
        
        if len(cad_config.directories) == 0:
            logging.error("cad_config.conf: [directories] list is empty")
            return 2
        

        # format gpfs policy select rule
        if days == 0:
            r = "WHERE "
        else:
            r = "WHERE (LAST_MODIFIED <= " + str(days) + ") AND "
        x = "("
        for p in cad_config.search_pattern:
            pattern = p.replace('*', '%')
            r = r + x + "lower(NAME) like \'" + pattern + "'"
            x = " or "
        select_rule = r + ")\n"
        logging.debug("apply_query_policy: select_rule: " + select_rule)


        # write gpfs policy file
        f = tempfile.NamedTemporaryFile(delete=False)
        f.write(base_policy)
        f.write(select_rule)
        policy_file = f.name
        logging.debug("apply_query_policy: policy_file: " + policy_file)
        f.close()


        # loop through directories we want to search for new files      
        returncode = 0
        for key in cad_config.directories:
            dir = cad_config.directories[key]
            
            # abort if gpfs mount point not found
            cmd ="df -h | grep " + cad_config.gpfs['gpfs_dev'] + " | awk '{print $6}'"
            (rc, out) = exec_shell(cmd)
            if rc != 0:
                logging.error("apply_query_policy: check GPFS mount failed: " + cmd)
                for m in out:
                    logging.error("+++ " + m)
                return 3
            
            if out == "":
                logging.error("apply_query_policy: GPFS file system not mounted: "
                + cad_config.gpfs['gpfs_dev'])
                return 4

            # create gwd and lwd if not exist
            gpfs_mp = out[0]
            gwd = gpfs_mp + "/" + cad_config.gpfs['gwd']
            if not os.path.exists(gwd):
                os.makedirs(gwd)
                logging.info("apply_query_policy: created directory: " + gwd)
            
            lwd = gpfs_mp + "/" + cad_config.gpfs['lwd']
            if not os.path.exists(lwd):
                os.makedirs(lwd)
                logging.info("apply_query_policy: created directory: " + lwd)
    
            # skip this directory if not under gpfs_mp
            if dir[:len(gpfs_mp)] != gpfs_mp:
                logging.error("directory is not in GPFS file system: " + dir)
                continue
            
            debuglevel = '0'
            if cad_config.scheduler['log_level'].upper() == 'DEBUG':
                debuglevel = '1'
    
            # run mmapplypolicy to query file metadata (use "-I defer" for testing)
            cmd = ('/usr/lpp/mmfs/bin/mmapplypolicy '+ dir
                + ' -P ' + policy_file 
                + ' -B ' + cad_config.gpfs['maxfiles']
                + ' -m ' + cad_config.gpfs['threadlevel']
                + ' -g ' + gwd
                + ' -s ' + lwd
                + ' -N ' + cad_config.gpfs['nodelist']
            #   + ' -I prepare '
                + ' -L ' + debuglevel
                + ' --scope fileset')
            (rc, out) = exec_shell(cmd)
            
            if rc == 0:
                logging.info("apply_query_policy: directory scan successful: " + dir)
            else:
                returncode = 5
                logging.error("apply_query_policy: directory scan failed: " + dir)
                for m in out:
                    logging.error("+++ " + m)

        os.remove(policy_file)        
        return returncode


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

        count = 0
        logging.debug("process_filelist: started: " + filelist)
        
        debug_mode = 0;
        if cad_config.scheduler['log_level'].upper() == 'DEBUG':
            debug_mode = 1;
            
        rc = meta_api.init_redis_handle(cad_config.scheduler['redis_hostname'], debug_mode)
        if rc != 0:
            logging_error("process_filelist: abort - failed to get redis db handle, rc=" + str(rc))
            return 1

        with open(filelist, 'r') as fh:
            for row in fh:
                if row == "\n":    # skip blank line
                    continue
                
                try:
                    #              in1   gen2  snp3    fs4   po5   sz6   kb7
                    #              uid8  gid9  rt10  mt11  ct12  at13  dm14 
                    #              file15
                    p = re.match("(\d+) (\d+) (\d+)\s+(\S+) (\S+) (\d+) (\d+) "
                                 "(\S+) (\S+) (\d+) (\d+) (\d+) (\d+) (\S+) --"
                                 "(.*)", row)
                except:
                    logging.error("process_filelist: regex error: " + str(row))
                    continue
                
                # *** SCHEMA DEFINITION ***
                d = {}
                f = p.group(15).lstrip()
                d['timestamp']          = str(datetime.datetime.now())
                d['file_name']          = f
                d['filename_hash']      = hashlib.sha224(f).hexdigest()
                d['fileset_name']       = p.group(4)
                d['pool_name']          = p.group(5)
                d['file_size']          = p.group(6)
                d['kb_allocated']       = p.group(7)
                d['user_id']            = p.group(8)
                d['group_id']           = p.group(9)
                d['creation_timed']     = p.group(10)
                d['modification_timed'] = p.group(11)
                d['change_timed']       = p.group(12)
                d['access_timed']       = p.group(13)
                d['dmapi_state']        = p.group(14)

                rc = meta_api.add_new_record(d)
                if rc == 0:
                    count += 1
                else:
                    logging_error("process_filelist: abort - meta_api add_new_record failed, rc=" + str(rc))
                    return 2
                
        fh.close()
        logging.debug("process_filelist: completed: " + filelist + " - rows=" + str(count))
        return 0
#
#
# main program (unit testing)
# Params   :
#   $1 - days, ingest file with mtime < days, zero=ingest all (default)
#
if __name__ == "__main__":
    # process input argv
    try:
        days = sys.argv[1]

    except IndexError:
        days = 0

    # initiate logging
    log_level = cad_config.scheduler['log_level'].upper()
    log_file = cad_config.scheduler['log_file']
    logging.basicConfig(level=log_level, filename=log_file, filemode='a',
    format='%(asctime)s, %(process)d %(module)s %(lineno)d - %(levelname)s %(message)s')
    logging.debug("main: started, argv= " + str(sys.argv))

    # run GPFS mmapplypolicy
    gpfs = gpfs_class()
    gpfs.apply_query_policy(days)

    print "cad_gpfs_ingest.py - done"

