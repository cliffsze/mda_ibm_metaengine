import sys
import os.path
import cad_gpfs_ingest

# Filename : cad_gpfs_exec.py
# Purpose  : Called by mmapplypolicy to process file ingest
# Params   :
#   $1 - command - "LIST", "TEST"
#   $2 - filelist

# Change Log:
# 20150505 - initial release
#
#
# main program
#
#  
if sys.argv[1] == 'LIST':
    # process filelist
    gpfs = cad_gpfs_ingest.gpfs_class()
    gpfs.process_filelist(sys.argv[2])
    
elif sys.argv[1] == 'TEST':
    if os.path.isdir(sys.argv[2]):
        exit(0)
    else:
        exit(1)


