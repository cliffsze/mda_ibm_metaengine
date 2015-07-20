import sys
import os
import logging
import cad_config
import dicom
from pyke import knowledge_engine, krb_traceback, goal


# Filename : cad_pyke_dicom.py
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
# 20150715 - initial release


#
#
# (rc, status, reason) = process_dicom_file(dicom_file)
#
#
def process_dicom_file(dicom_file):
    
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
    
    engine = knowledge_engine.engine(__file__)
    engine.reset()
    goal_isphi = goal.compile('dicom_file.isphi_tag($tag)')
    goal_nokey = goal.compile('dicom_file.nokey_tag($tag)')
    goal_blank = goal.compile('dicom_file.blank_tag($tag)')
    goal_nophi = goal.compile('dicom_file.nophi_tag($tag)')

    # extract tag and value from dicom file and add as pyke specific fact
    for data_element in dataset:
        tag = str(data_element.tag).lower()
        tag = tag.replace(", ", ",")            
        val = str(data_element.value)
        val = val.rstrip().lstrip()
        engine.add_case_specific_fact('dicom_file', 'attribute_is', (tag, val))

    engine.activate('cad_pyke_dicom')
    # engine.get_kb('dicom_file').dump_specific_facts()
    
    with goal_isphi.prove(engine) as gen:
        for vars, plan in gen:
            tags_isphi += 1
            phi_rule.append(vars['tag'])

    with goal_nokey.prove(engine) as gen:
        for vars, plan in gen:
            tags_nokey += 1
            undef_rule.append(vars['tag'])

    with goal_blank.prove(engine) as gen:
        for vars, plan in gen:
            tags_blank += 1

    with goal_nophi.prove(engine) as gen:
        for vars, plan in gen:
            tags_nophi += 1
            
    # error if tag counts don't match
    if tags_total != (tags_blank + tags_nokey + tags_isphi + tags_nophi):
        logging.error("process_dicom_file: tag count mismatch,"
                      + " total:" + str(tags_total) 
                      + " blank:" + str(tags_blank) 
                      + " nokey:" + str(tags_nokey) 
                      + " isphi:" + str(tags_isphi)
                      + " nophi:" + str(tags_nophi))
        return 2, 'indeterminate', 'none', 'none'

    # construct return parameters
    if tags_isphi == 0 and tags_nokey == 0:
        status = "not_phi"
    else:
        status = "is_phi"

    # we are done, print status and reason
    logging.info("process_dicom_file: status: " + status)
    logging.info("process_dicom_file: reason: tag counts -"
                 + " total:" + str(tags_total) 
                 + " blank:" + str(tags_blank) 
                 + " nokey:" + str(tags_nokey) 
                 + " isphi:" + str(tags_isphi) 
                 + " nophi:" + str(tags_nophi))
    return 0, status, str(phi_rule), str(undef_rule) 
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
    process_dicom_file(dicom_file)

#    except IndexError:
#        print "USAGE: python cad_analyze_dicom.py [dicom file]")

    sys.exit("cad_analyze_dicom.py - done")

