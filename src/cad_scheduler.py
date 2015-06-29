import sys
import os
import logging
import cad_config
import cad_gpfs_ingest
#import cad_meta_ingest
import cad_analyze_vcf
import cad_analyze_dicom


# Filename : cad_scheduler.py
# Purpose  : cron job to call the python caddies - metadata ingest, vcf analysis and dicom analysis
#
# Inputs - cad_config.conf

# Change Log:
# 20150613 - initial release
#
#
# Main Program
#
#
try:
    # initiate logging
    log_level = cad_config.scheduler['log_level'].upper()
    log_file = cad_config.scheduler['log_file']
    logging.basicConfig(level=log_level, filename=log_file, filemode='a',
    format='%(asctime)s, %(process)d %(module)s %(lineno)d - %(levelname)s %(message)s')
    logging.info("main: cad_scheduler started")
    
    
    # run metadata scan to collect new files
    if cad_config.gpfs['use_gpfs_scan'].capitalize():
        logging.info("main: start file system scan using GPFS policy engine")
        gpfs = cad_gpfs_ingest.gpfs_class()
        days = int(cad_config.scheduler['job_delta_days'])
        rc = gpfs.apply_query_policy(days)
        logging.info("main: gpfs metadata scan, rc=" + str(rc))
    
    else:
        logging.info("main: start file system scan using os.walk")
    
    
    # retrieve and process new vcf files
    logging.info("main: start vcf file processing")
    vc = cad_analyze_vcf.vcf_class()
    rc = vc.process_medb_vcf()
    logging.info("main: vcf analysis, rc=" + str(rc))
    
    
    # retrieve and process new dicom files
    logging.info("main: start dicom file processing")
    dc = cad_analyze_dicom.dicom_class()
    rc = dc.process_medb_dicom()
    logging.info("main: dicom analysis, rc=" + str(rc))
    

except KeyError:
    sys.exit("abort")

sys.exit("cad_scheduler.py - done")