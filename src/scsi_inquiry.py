#!/usr/bin/env python
"""
Low level SCSI commands - helpers for T10000 drives
"""

import sys
import time

import ftt
import ftt_driver
import Trace
import e_errors

MS_HEADER_DATA_LENGTH = 4 # Mode Sense / Select Data Header Length
P_25H_LEN = 0x1e+2 # length of Page 0x25
P_0FH_LEN = 0x0e + 2 # length of Page 0x0f

def ftt_inquiry(driver,            # ftt driver
               page_code = 0x00,
               allocation_length_msb = 0x00,
               allocation_length_lsb = 0x00,
               control_byte = 0x00
               ):
  """
  6 - bytes Inquiry command according to SCSI reference

  :type driver: :obj:`ftt_driver.FTTDriver`
  :type page_code: :obj:`int`
  :arg page_code: page code
  :type allocation_length: :obj:`int`
  :arg allocation_length: data allocation lenght for retuned data
  :type control_byte: :obj:`int`
  :arg control_byte: control byte
  :rtype: :obj:`list` - mode sense data
  """

  cdb_inquiry = [0x12, # inquiry command
                 0x01,
                 page_code,
                 allocation_length_msb,
                 allocation_length_lsb,
                 control_byte
                 ]
  print "CDB_INQ"
  print_list(cdb_inquiry)
  res = driver.ftt.do_read_scsi_command("Inquiry Page %s"%(page_code&0x27,),
                                        cdb_inquiry,
                                        6,
                                        allocation_length_lsb,
                                        10) # timeout
  return res;

def print_list(l):
  for i in l:
    print "%02x"%(i&0xff,),
  print ""

def sprint_list(l):
  s = ""
  for i in l:
    s = "%s%02x "%(s,i&0xff,)
  return s

def enable_trace_at_start(levels):
    # levels - list of levels to enable
    for level in levels:
        Trace.print_levels[level]=1

if __name__ == '__main__':

  if len(sys.argv) != 2:
    usage()
    sys.exit(-1)
  enable_trace_at_start([32])
  driver = ftt_driver.FTTDriver()
  print "Openinig device. This may take a minute if tape is not mounted"
  try:
    driver.open(sys.argv[1], mode=1, retry_count=3)
  except Exception, detail:
    print "Exception", detail

  stats = driver.get_stats()
  print "Drive Type", stats[ftt.PRODUCT_ID]


  res = ftt_inquiry(driver,            # ftt driver
                    page_code = 0x83,
                    allocation_length_msb = 0x00,
                    allocation_length_lsb = 74,
                    #allocation_length_lsb = 200,
                    control_byte = 0x00
                    )
  print_list(res)



