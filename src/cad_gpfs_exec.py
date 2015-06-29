import sys
import os.path
import logging
import cad_config
import cad_gpfs_ingest


# Filename : cad_gpfs_exec.py
# Purpose  : Called by mmapplypolicy to process file ingest
# Params   :
#   $1 - command - "LIST", "TEST"
#   $2 - filelist

# Change Log:
# 20150505 - initial release
# 20150605 - added logging
#
#
# main program
#
#
# initiate logging
log_level = cad_config.scheduler['log_level'].upper()
log_file = cad_config.scheduler['log_file']
logging.basicConfig(level=log_level, filename=log_file, filemode='a',
format='%(asctime)s, %(process)d %(module)s %(lineno)d - %(levelname)s %(message)s')
logging.debug("main: started, argv= " + str(sys.argv))

if sys.argv[1] == 'LIST':
    # process filelist
    gpfs = cad_gpfs_ingest.gpfs_class()
    gpfs.process_filelist(sys.argv[2])
    
elif sys.argv[1] == 'TEST':
    if os.path.isdir(sys.argv[2]):
        exit(0)
    else:
        logging.error("main: directory does not exist:" + str(sys.argv[2]))
        exit(1)


