import sys
import vcf


# Filename : cad_analyze_vcf.py
# Purpose  : process vcf formatted file, determine PHI state
#            (using pyVCF release 0.6.7 - VCFv4.0 and 4.1 parser)
# Params   :
#   $1 - format is filelist if filename=*.list, otherwise process as a single vcf file
#        filelist - contains 1 vcf file name per line (must be FQDN), space allowed, quoted string is optional

# Change Log:
# 20150505 - initial release
#
#
# class - vcf_class
#
#
class vcf_class:

    def __init__(self):
        return
    
    def process_file_type(self, filename):
    
        # determine if file ends with .list or not
        f = filename.lower()
        s = f.rfind('.list',0,len(f))
        
        # process as file list if file ends with .list
        if len(f)-s == 5:
            with open(filename, 'r') as fh:
                for line in fh:
                    self.process_vcf_file(line)
            fh.close
        
        # otherwise, just process as a single vcf file
        else:
            self.process_vcf_file(filename)
        return
    
    def process_vcf_file(self, vcf_file):
        
        # process vcffile with PyVCF
        try:
            vcf_file = vcf_file.lstrip()
            vcf_file = vcf_file.rstrip()
            vcf_reader = vcf.Reader(open(vcf_file, 'r'))
        except IOError:
            print "ERROR - file not found, skipping:", vcf_file
            return

        # determine if vcf is PHI
        try:
            print "filename:", vcf_reader.filename
            fmt = vcf_reader.formats['SS']
            total_samples = 0
            total_germlinesomatic = 0
            for record in vcf_reader:
                total_samples += 1
                if record.samples[0]['SS'] == 1:
                    total_germlinesomatic += 1
            is_phi = float(total_germlinesomatic)/float(total_samples) > 0.5
            rule = "total germlinesomatic/total samples > 50%"

        except KeyError:
            is_phi = False
            rule = "FORMAT ID=SS definition not found"

        print "is_phi:",is_phi,"\trule:",rule
        return
#
#
# main program
#
#
if __name__ == "__main__":
    # process input argv
    try:
        filename = str(sys.argv[1])
        vcfc = vcf_class()
        vcfc.process_file_type(filename)

    except IndexError:
        print "FATAL - please enter file name as argv[1]"
        
