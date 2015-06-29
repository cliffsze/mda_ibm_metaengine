import sys
import os
import ConfigParser
import collections


# Filename : cad_config.py
# Purpose  : reads cad_config.conf and setup shared variables
#
# Inputs - cad_config.conf
# Outputs - dict data type
#    scheduler      (dict)
#    directories    (dict)
#    gpfs_setting   (dict)
#    vcf            (dict)
#    dicom          (dict)
#    caddies        (list)
#    search_pattern (list)

# Change Log:
# 20150613 - initial release
#
#
# functions
#
#
# create named-based key-value pairs
def populate_dict(section):
    dict = collections.defaultdict(list)
    for item in c.items(section):
        dict[item[0]] = item[1]
    return dict
#
#
# main program
#
#
# read configuration parameters
home_dir = os.path.dirname(os.path.realpath(__file__))
config_file = home_dir + '/cad_config.conf'
c = ConfigParser.SafeConfigParser()
c.read(config_file)

# extract data from each section
scheduler = populate_dict('scheduler')
directories = populate_dict('directories')
gpfs = populate_dict('gpfs')
vcf = populate_dict('vcf')
dicom = populate_dict('dicom')

# build caddy name list
caddies = list()
caddies.append('vcf')
caddies.append('dicom')

# build caddy search pattern list
search_pattern = list()
for caddy in caddies:
    e = c.get(caddy, 'search_pattern')
    plist = e.split(',')
    for item in plist:
        search_pattern.append(item)
pass

