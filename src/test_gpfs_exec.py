import sys
import os
import tempfile
import meta_api
import cad_gpfs_ingest


test_data = """
8192 686492707 0  tmp system 3975 32 0 0 735728 735728 735728 735753 R -- /gpfs-fs1/tmp/monitor2/testPIIPos2.vcf
8194 1164847654 0  tmp system 3975 32 0 0 735728 735728 735728 735753 R -- /gpfs-fs1/tmp/monitor2/testPIINeg2.vcf
8195 1597726698 0  tmp system 3975 32 0 0 735728 735728 735728 735753 R -- /gpfs-fs1/tmp/monitor2/test2.vcf
8196 311302341 0  tmp system 3975 32 0 0 735728 735728 735728 735753 R -- /gpfs-fs1/tmp/monitor2/test.vcf
8198 30717223 0  tmp system 3975 32 0 0 735728 735728 735728 735753 R -- /gpfs-fs1/tmp/monitor2/testPIINeg1.vcf
8201 376009672 0  tmp system 3975 32 0 0 735728 735728 735728 735753 R -- /gpfs-fs1/tmp/monitor2/testPIIPos1.vcf
8203 1690751695 0  tmp system 13 0 0 0 735728 735728 735728 735753 R -- /gpfs-fs1/tmp/monitor2/dummy.vcf
101376 1341111765 0  tmp system 24803558 24224 0 0 735713 735713 735713 735753 R -- /gpfs-fs1/tmp/monitor2/IACS-OXPHOS-MP-OX10-01-01R_140517_SN1222_0251_AC3LTPACXX_s_7_TGACCA.star.rg.recalibed1.haplotyper.filter.vcf
101377 1895511347 0  tmp satapool 3089887 3040 0 0 735713 735713 735713 735753 R -- /gpfs-fs1/tmp/monitor2/IACS-TRIM-JA-MK26-60-01R_140731_SN1120_0316_BC55EYACXX_s_8_TGACCA.star_recalibed.haplotyper.vcf
1070087 497957604 0  tmp system 12804841 12512 0 0 735724 735724 735724 735753 R -- /gpfs-fs1/tmp/monitor2/testPIINeg3.vcf
"""

f = tempfile.NamedTemporaryFile(delete=False)
f.write(test_data)
test_file = f.name
f.close()

gpfs = cad_gpfs_ingest.gpfs_class()
gpfs.process_filelist(test_file)
os.remove(test_file)


sys.exit("done")