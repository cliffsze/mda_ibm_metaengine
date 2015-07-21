import sys
import os
import logging
import fnmatch
import stat
import datetime
import time
import cad_config
import meta_api


# Filename : cad_meta_ingest.py
# Purpose  : Independent program to search generic file system and ingest metadata
#            into the Meta Engine

# Change Log:
# 20150629 - initial release


class meta_class:

	def __init__(self):
		# abort if search_pattern or directories list is empty
		if len(cad_config.search_pattern) == 0:
			logging.error("cad_config.conf: search_pattern list is empty")
			sys.exit("cad_meta_ingest.py - abort")
	
		if len(cad_config.directories) == 0:
			logging.error("cad_config.conf: [directories] list is empty")
			sys.exit("cad_meta_ingest.py - abort")
		
		debug_mode = 0;
		if cad_config.scheduler['log_level'].upper() == 'DEBUG':
			debug_mode = 1;		

		rc = meta_api.init_redis_handle(cad_config.scheduler['redis_hostname'], debug_mode)
		if rc != 0:
			logging_error("meta_class: abort - failed to get redis db handle, rc=" + str(rc))
			sys.exit("cad_meta_ingest.py - abort")

		return
	


	def ingest_metadata(self, days):
		
		# loop through directories we want to search for new files
		for key in cad_config.directories:
			root_dir = cad_config.directories[key]

			# abort if the directory does not exist
			if not os.path.isdir(root_dir):
				logging.error("ingest_metadata: directory not found: " + root_dir)
				continue
			
			# perform os.walk and match filename pattern
			logging.info("ingest_metadata: processing directory: " + root_dir)
			count = 0
			for root, directories, files in os.walk(root_dir):
				for file in files:
					for pattern in cad_config.search_pattern:
						
						# skip this file if pattern not match
						if not fnmatch.fnmatch(file, pattern):
							continue
						
						# skip this file if days > 0 and mtime > days
						filepath = os.path.join(root, file)
						if days > 0:								
							mode = os.stat(filepath)
							delta_mtime_days = (time.time() - mode.st_mtime) / 86400.0																
							if delta_mtime_days > float(days):
								logging.debug("ingest_metadata: skipping: " + filepath + ", delta_mtime_days=" + str(delta_mtime_days))
								continue
						
						# add this file to metaengine database
						#   timestamp
						#   fileset_name
						#   file_size 
						#   user_id
						#   group_id
						#   modification_timed
						#   change_timed
						#   access_timed

						logging.debug("ingest_metadata: " + filepath)
						mode = os.stat(filepath)
						
						# *** SCHEMA DEFINITION ***
						d = {}							
						d['timestamp']          = str(datetime.datetime.now())
						d['file_name']          = filepath
						d['file_size']          = str(mode.st_size)
						d['user_id']            = str(mode.st_uid)
						d['group_id']           = str(mode.st_gid)
						d['modification_timed'] = str(int(mode.st_mtime / 86400.0))
						d['change_timed']       = str(int(mode.st_ctime / 86400.0))
						d['access_timed']       = str(int(mode.st_atime / 86400.0))
						logging.debug("ingest_metadata: metadata: " + str(d))
						
						rc = meta_api.add_new_record(d)
						if rc == 0:
							count += 1
						else:
							logging_error("ingest_metadata: abort - meta_api add_new_record failed, rc=" + str(rc))
							return 1
			logging.info("ingest_metadata: files ingested=" + str(count))
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

	# initiate logging to stdout
	log_level = cad_config.scheduler['log_level'].upper()
	logging.basicConfig(filemode='a', level=log_level, handlers=[logging.StreamHandler()],
                        format='%(asctime)s, %(process)d %(module)s %(lineno)d - %(levelname)s %(message)s')
	logger = logging.getLogger()
	logging.debug("main: started, argv= " + str(sys.argv))
	
	# run ingest_metadata
	mc = meta_class()	
	rc = mc.ingest_metadata(days)
	logging.debug("main: ingest_metadata, rc=" + str(rc))
	
	sys.exit("cad_meta_ingest.py - done")