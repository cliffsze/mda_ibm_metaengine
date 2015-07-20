import sys
import os
import cad_analyze_dicom
import cad_pyke_dicom
#from os import walk

myroot = "/gpfs-fs1/tmp/monitor2"
file_paths = []  # List which will store all of the full filepaths.
dc = cad_analyze_dicom.dicom_class()

# Walk the tree.
for root, directories, files in os.walk(myroot):
	for filename in files:
		# Join the two strings in order to form the full filepath.
		filepath = os.path.join(root, filename)
		if filepath.endswith(".dcm"):
			file_paths.append(filepath)  # Add it to the list.
			print "FILE: ", filepath
			(rc, status, phi_rule, undef_rule) = cad_pyke_dicom.process_dicom_file(filepath)
			print rc, status, str(phi_rule), str(undef_rule)
			
			(rc, status, phi_rule, undef_rule) = dc.process_dicom_file(filepath)
			print rc, status, str(phi_rule), str(undef_rule)			


