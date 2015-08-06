import sys
import logging
import csv
import meta_api
import cad_config


# Filename : cad_reports.py
# Purpose  : extract all records from metaengine database and generate status report
# Params   :
#   $1 - file_format attribute - "dcm", "vcf"

# Change Log:
# 20150730 - initial release
#

def generate_report(search, list_history):
	debug_mode = 0;
	if cad_config.scheduler['log_level'].upper() == 'DEBUG':
		debug_mode = 1;
		
	rc = meta_api.init_redis_handle(cad_config.scheduler['redis_hostname'], debug_mode)
	if rc != 0:
		logging.critical("generate_reports: init_redis_handle failed, rc=" + str(rc))
		return 1

	# retrieve metadata records from metaengine db
	logging.info("generate_report: retrieve 'file_format'=" + search)
	
	rdict = {}
	rc,rdict = meta_api.get_records('file_format', search.lower())
	if rc != 0:
		logging.critical("generate_reports: query db failed, rc=" + str(rc))
		return 2
	
	# format data into csv file
	adict = {}
	report_name = "cad_report_" + search + ".csv"
	fh = open(report_name, 'wb')
	writer = csv.writer(fh)
	
	# write header
	if search == "vcf":
		writer.writerow(["file_format", "file_name", "privacy_timestamp", "privacy_rule_status", 
		                 "privacy_rule_reason"])
	else:    # dicom
		writer.writerow(["file_format", "file_name", "privacy_timestamp", "privacy_rule_status", 
		                 "privacy_dicom_phi_rule", "privacy_dicom_unref_rule"])

	# filter out old entries, save list of latest entries in new_dict
	new_dict ={}
	for key in rdict:
		adict = rdict[key]
		file_name = adict['file_name']
		if file_name in new_dict:
			if adict['privacy_timestamp'] > new_dict[file_name]:
				new_dict[file_name] = adict['privacy_timestamp']
		else:
			new_dict[file_name] = adict['privacy_timestamp']

	# write entries
	for key in rdict:
		adict = rdict[key]
		
		# skip over old entries if list_history=False
		if not list_history:
			if adict['privacy_timestamp'] != new_dict[adict['file_name']]:
				continue
			
		try:
			writer.writerow([adict['file_format'], 
				             adict['file_name'], 
				             adict['privacy_timestamp'],
				             adict['privacy_rule_status'],
				             adict['privacy_rule_reason']])
		except KeyError:    # dicom
			writer.writerow([adict['file_format'], 
		                     adict['file_name'], 
		                     adict['privacy_timestamp'],
		                     adict['privacy_rule_status'],
		                     adict['privacy_dicom_phi_rule'],
		                     adict['privacy_dicom_unref_rule']])
	fh.close()
	return 0
#
#
# main program (for unit testing)
#
#
if __name__ == "__main__":

	# process one input argument - wildcard search
	pattern = sys.argv[1].lower()

	# initiate logging to stdout
	logging.basicConfig(filemode='a', level=logging.DEBUG, handlers=[logging.StreamHandler()],
	    format='%(asctime)s, %(process)d %(module)s %(lineno)d - %(levelname)s %(message)s')
	logger = logging.getLogger()
	print "cad_report.py - unit test"
	print "search_pattern: " + pattern
	
	generate_report(search=pattern, list_history=False)

	sys.exit("cad_report.py - done.")