import sys
import os
import ConfigParser
import logging
import datetime
import cad_config
import meta_api
import dicom
import dicom_phi_rules


# Filename : cad_analyze_dicom.py
# Purpose  : process dicom formatted file, determine PHI state (using pydicom release 0.9.8)
#            rules processing programmed in pyke 
# Params   :
#   $1 - format is filelist if filename=*.list, otherwise process as a single dicom file
#        filelist - contains 1 dicom file per line (must be FQDN), space allowed, quoted string is optional
#
# Inputs - cad_config.conf parameters:
#   [dicom]
#   log_file
#   rules_file

# Change Log:
# 20150613 - initial release
#
#
# functions
#
#
def lrstrip(item):
    out = item
    out = out.lstrip()
    out = out.rstrip()
    return out
#
#
# class - dicom_class
#
#
class dicom_class:

    dicom_phi_dict = {}


    def __init__(self):

        # initialize dicom_phi_dict
        global dicom_phi_dict
        dicom_phi_dict = dicom_phi_rules.init_phi_dict()


    def append_medb_record(self, recid, status, phi_rule, undef_rule):

        # *** SCHEMA DEFINITION ***
        d = {}
        d['file_format']              = 'dicom'
        d['privacy_timestamp']        = str(datetime.datetime.now())
        d['privacy_rule_status']      = status    # is_phi, not_phi, indeterminate, file_not_found, duplicate
        d['privacy_dicom_phi_rule']   = phi_rule
        d['privacy_dicom_unref_rule'] = undef_rule

        return meta_api.append_to_record(recid, d)
    #
    #
    # rc = process_medb_dicom()
    #
    #
    def process_medb_dicom(self):

        # acquire metaengine db handle
        debug_mode = 0;
        if cad_config.scheduler['log_level'].upper() == 'DEBUG':
            debug_mode = 1;

        rc = meta_api.init_redis_handle(cad_config.scheduler['redis_hostname'], debug_mode)
        if rc != 0:
            logging.critical("query_medb_dicom: init_redis_handle failed, rc=" + str(rc))
            return 1

        # get unprocessed dicom files from metaengine db
        dict ={}
        (rc, dict) = meta_api.get_unprocessed_record('file_name', '*.dcm', 'privacy_rule_status')
        if rc != 0:
            logging.critical("query_medb_dicom: query db failed, rc=" + str(rc))
            return 2

        # loop through dict items - determine dicom file phi status
        logging.info("query_medb_dicom: unprocessed item count: " + str(len(dict)))
        succ = 0
        fail = 0
        dupl = 0
        processed_items = []

        for recid in dict:
            file = dict[recid]

            # analyze new vcf file
            if not file in processed_items:
                (rc, status, phi_rule, undef_rule) = self.process_dicom_file(file)

                # dicom analysis successful
                if rc == 0:
                    succ += 1
                    rc = self.append_medb_record(recid, status, phi_rule, undef_rule)
                    if rc != 0:
                        logging.error("query_medb_dicom: append record failed, recid=" + recid)

                    # append successful, add to processed_items list to detect duplicates
                    if rc == 0:
                        processed_items.append(file)

                # dicom file not found
                else:
                    fail += 1
                    rc = self.append_medb_record(recid, 'file_not_found', 'none', 'none')

            # this is a duplicate file name
            else:
                dupl += 1
                rc = self.append_medb_record(recid, 'duplicate', 'none', 'none')
                if rc != 0:
                    logging.error("query_medb_dicom: append record failed, recid=" + recid)

        logging.info("query_medb_dicom: completed, succ="
                     + str(succ) + ", fail=" + str(fail) + ", dupl=" + str(dupl))

        return 0
        #
        #
        # (rc, status, reason) = process_dicom_file(dicom_file)
        #
        #
    def process_dicom_file(self, dicom_file):

        global dicom_phi_dict
        phi_rule = list()
        undef_rule = list()

        # extract dicom dataset
        try:
            logging.info("process_dicom_file: " + dicom_file)
            dataset = dicom.read_file(dicom_file)

        except:
            logging.error("process_dicom_file: file open error: " + dicom_file)
            return 1, 'file_not_found', 'none', 'none'

        tags_total = len(dataset)
        tags_blank = 0
        tags_nokey = 0
        tags_isphi = 0
        tags_nophi = 0

        # parse all the data_elements for PHI data
        # Format: [tag] [Name] [VR] [value]
        for data_element in dataset:

            # extract tag and value
            tag = str(data_element.tag)
            tag = tag.replace(", ", ",")            
            val = lrstrip(str(data_element.value))    

            # tag does not exist in phi rule list (no DE-identification rule)
            if not tag in dicom_phi_dict:
                undef_rule.append(tag)
                tags_nokey += 1
                continue

            # extract is_phi and anon_rule
            # Format: [Name] [VR] [VM] [version] [is_phi] [anonymization_rule]
            rule = dicom_phi_dict[tag]
            is_phi = rule[0][4]
            anon_rule = lrstrip(rule[0][5])
            anon_rule = anon_rule.lower()

            # extract year if anon_rule is "incrementdate"
            # Format: yyyymmdd
            if "incrementdate" in anon_rule:
                try:
                    yyyy = int(val[:4])
                except:
                    logging.error("process_dicom_file: incorrect date: " + tag + "=" + str(val))
                    yyyy = 1851

            # tag is not phi
            if not is_phi:
                tags_nophi += 1
                continue

            # tag is phi and
            # anon_rule = "" or
            # anon_rule = "remove" or
            # (val = ""  and anon_rule = "empty") or
            # (anon_rule = "incrementdate" and year of val > 1850)
            elif (len(anon_rule) == 0 
                  or "remove" in anon_rule
                  or (len(val) > 0 and "empty" in anon_rule)
                  or ("incrementdate" in anon_rule and yyyy > 1850)):
                phi_rule.append(tag)
                tags_isphi += 1

            # tag matched=is_phi, undefined rule
            else:
                phi_rule.append(tag)
                tags_isphi += 1

        # error if tag counts don't match
        if tags_total != (tags_blank + tags_nokey + tags_isphi + tags_nophi):
            logging.error("determine_phi_status: tag count mismatch")      
            return 2, 'indeterminate', 'none', 'none'

        # construct return parameters
        if tags_isphi == 0:
            status = "not_phi"
        else:
            status = "is_phi"

        # we are done, print status and reason
        logging.info("process_dicom_file: status: " + status)
        logging.info("process_dicom_file: reason: tag counts - total:" + str(tags_total) + " blank:" + str(tags_blank)
                     + " nokey:" + str(tags_nokey) + " isphi:" + str(tags_isphi) + " nophi:" + str(tags_nophi))

        return 0, status, str(phi_rule), str(undef_rule)
        #
        #
        # anonymize_dicom_file(dicom_file):
        #
        #	
        def anonymize_dicom_file(self, dicom_file):
            logging.debug("anonymize_dicom_file: method not implemented")
            pass
#
#
# main program (for unit testing)
#
#
if __name__ == "__main__":

    # process one input argument - dicom file
    #try:
    dicom_file = str(sys.argv[1])

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

    # instantiate dicom_class and run process_dicom_file(dicom_file)
    dc = dicom_class()
    dc.process_dicom_file(dicom_file)

#    except IndexError:
#        print "USAGE: python cad_analyze_dicom.py [dicom file]")

    sys.exit("cad_analyze_dicom.py - done")
