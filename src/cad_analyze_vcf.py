import sys
import logging
import datetime
import cad_config
import meta_api
import vcf


# Filename : cad_analyze_vcf.py
# Purpose  : process vcf formatted file, determine PHI state
#            (using pyVCF release 0.6.7 - VCFv4.0 and 4.1 parser)
# Params   :
#   $1 - format is filelist if filename=*.list, otherwise process as a single vcf file
#        filelist - contains 1 vcf file name per line (must be FQDN), space allowed, quoted string is optional

# Change Log:
# 20150505 - initial release
# 20150625 - converted all runtime messages to python loggong
# 20150625 - main program used for unit testing
# 20150625 - use cad_config.conf parameters
# 20150625 - process one or more
# 20150625 - read from metaengine and save status to metaengine
# 20150811 - process_vcf_file - reason message updated
#
#
# class - vcf_class
#
#
class vcf_class:

    def __init__(self):
        return


    def append_medb_record(self, recid, status, reason):

        # *** SCHEMA DEFINITION ***
        d = {}
        d['file_format']         = 'vcf'  
        d['privacy_timestamp']   = str(datetime.datetime.now())
        d['privacy_rule_status'] = status    # is_pii, not_pii, indeterminate, file_not_found, duplicate
        d['privacy_rule_reason'] = reason
        return meta_api.append_to_record(recid, d)

    
    def process_medb_vcf(self):
    
        # acquire metaengine db handle
        debug_mode = 0;
        if cad_config.scheduler['log_level'].upper() == 'DEBUG':
            debug_mode = 1;
            
        rc = meta_api.init_redis_handle(cad_config.scheduler['redis_hostname'], debug_mode)        
        if rc != 0:
            logging.critical("query_medb_vcf: init_redis_handle failed, rc=" + str(rc))
            return 1
        
        # get unprocessed vcf files from metaengine db
        dict ={}
        (rc, dict) = meta_api.get_unprocessed_record('file_name', '*.vcf', 'privacy_rule_status')
        if rc != 0:
            logging.critical("query_medb_vcf: query db failed, rc=" + str(rc))
            return 2
        
        # loop through dict items - determine vcf file pii status
        logging.info("query_medb_vcf: unprocessed item count: " + str(len(dict)))
        succ = 0
        fail = 0
        dupl = 0
        processed_items = []
        
        for recid in dict:
            file = dict[recid]
            
            # analyze new vcf file
            if not file in processed_items:
                (rc, status, reason) = self.process_vcf_file(file)
                if rc == 0:
                    succ += 1
                else:
                    fail += 1
                
                rc = self.append_medb_record(recid, status, reason)
                if rc != 0:
                    logging.error("query_medb_vcf: append record failed, recid=" + recid)
                
                # append successful, add to processed_items list to detect duplicates
                if rc == 0:
                    processed_items.append(file)
  
            # this is a duplicate file name
            else:
                dupl += 1
                rc = self.append_medb_record(recid, 'duplicate', 'none')
                if rc != 0:
                    logging.error("query_medb_vcf: append record failed, recid=" + recid)
                
        logging.info("query_medb_vcf: completed, succ="
        + str(succ) + ", fail=" + str(fail) + ", dupl=" + str(dupl))
                  
        return 0

    
    # (rc, status, reason) = process_vcf_file( filename )
    # status = is_pii | not_pii
    def process_vcf_file(self, vcf_file):
        
        # process vcf_file with PyVCF
        try:
            vcf_file = vcf_file.lstrip()
            vcf_file = vcf_file.rstrip()
            logging.info("process_vcf_file: " + vcf_file)
            vcf_reader = vcf.Reader(open(vcf_file, 'r'))
            
        except:
            logging.error("process_vcf_file: file open or read error: " + vcf_file)
            return 1, 'indeterminate', 'none'
        
        # determine if vcf is PII
        pii_thres = float(cad_config.vcf['pii_germlinesomatic_pct'])/100
        logging.debug("+++ pii_germlinesomatic threshold: " + str(pii_thres))
        
        try:
            fmt = vcf_reader.formats['SS']
            total_samples = 0
            total_germlinesomatic = 0
            for record in vcf_reader:
                for key in record._sample_indexes:
                    idx = record._sample_indexes[key]
                    total_samples += 1
                    if record.samples[idx]['SS'] == 1:
                        total_germlinesomatic += 1
            
            if total_samples == 0:
                total_samples = 1
            germlinesomatic_ratio = float(total_germlinesomatic)/float(total_samples)
            logging.debug("+++ germlinesomatic_ratio=" + str(germlinesomatic_ratio))
            if ( germlinesomatic_ratio > pii_thres):
                status = "is_pii"
                reason = ("total germlinesomatic/total samples > " + str(pii_thres))
            else:
                status = "not_pii"
                reason = ("total germlinesomatic/total samples < " + str(pii_thres))
            rc = 0

        except ValueError:
            status = "indeterminate"
            reason = "ValueError while parsing"
            rc = 2

        except KeyError:
            status = "not_pii"
            reason = "FORMAT ID=SS definition not found"
            rc = 0
            
        logging.info("process_vcf_file: status: " + status + ", reason: " + reason)
        return rc, status, reason
#
#
# main program (for unit testing)
# python cad_analyze_vcf [vcf_file]
#
#
if __name__ == "__main__":

    try:
        # process input argv - vcf file
        vcf_file = str(sys.argv[1])
        print "main: argv[1]: " + vcf_file

        # initiate logging to stdout
        log_level = cad_config.scheduler['log_level'].upper()
        root = logging.getLogger()
        root.setLevel(log_level)
        
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s, %(process)d %(module)s %(lineno)d - %(levelname)s %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)        
        
        # instantiate vcf_class and run process_vcf_file
        vc = vcf_class()
        (rc, status, reason) = vc.process_vcf_file(vcf_file)
        
        print "main: rc=" + str(rc) 
        print "main: status: " + str(status)
        print "main: reason: " + reason

    except IndexError:
        print "USAGE - python cad_analyze_vcf [vcf_file]"
        
    sys.exit("cad_analyze_vcf.py - done")
