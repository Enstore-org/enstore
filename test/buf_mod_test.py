import scsi_mode_select
import ftt_driver
import ftt
import sys

driver=ftt_driver.FTTDriver()
try:
    driver.open(sys.argv[1], mode=1, retry_count=3)
except Exception, detail:
    print "Exception", detail

stats = driver.get_stats()
print "Drive Type", stats[ftt.PRODUCT_ID]
res = scsi_mode_select.ftt_mode_sense_6_bytes(driver, allocation_length=6)
for i in res:
    print "0x%0.2X"%(i,)
#db=[0x00, 0x00, 0x10,0x00]
db=[0x00, 0x00, 0x00,0x00]
res = scsi_mode_select.ftt_mode_select_6_bytes(driver, data_block_length = 4, data_block = db)
print "res ", res
res = scsi_mode_select.ftt_mode_sense_6_bytes(driver, allocation_length=6)
for i in res:
    print "0x%0.2X"%(i,)
