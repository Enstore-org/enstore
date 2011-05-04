#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
import sys

import ftt
import ftt_driver
import Trace
import e_errors

MS_HEADER_DATA_LENGTH = 4 # Mode Sense / Select Data Header Length 
P_25H_LEN = 0x1e+2 # length of Page 0x25 
P_0FH_LEN = 0x0e + 2 # length of Page 0x0f

# 6 - bytes mode sense command according to SCSI reference
def ftt_mode_sense_6_bytes(driver,            # ftt driver
                           dbd = 0x08,               # do not return block descriptor
                           page_code = 0x00,
                           subpage_code = 0x00,
                           allocation_length = 0x00,
                           control_byte = 0x00
                           ):
  cdb_mode_sense = [0x1a, # mode sense command
                    dbd,
                    page_code,
                    subpage_code,
                    allocation_length,
                    control_byte
                    ]
  res = driver.ftt.do_read_scsi_command("mode sense Page %s"%(page_code&0x27,),
                                        cdb_mode_sense,
                                        6,
                                        allocation_length,
                                        10) # timeout
  return res;

# 6 - bytes mode select command according to SCSI reference
# !!! use this command at your own risk.
# The best practice is to fist issue a correspondind mode sense command
# and then modify received data and send mode select command 
def ftt_mode_select_6_bytes(driver,          # ftt driver
                            pf = 0x10,              # page formatted data
                            reserved_msb = 0x00,
                            reserved_lsb = 0x00,
                            data_block_length = 0x00,
                            control_byte = 0x00,
                            data_block = None
                           ):
  cdb_mode_select = [0x15,             # mode select command
                     pf,
                     reserved_msb,
                     reserved_lsb,
                     data_block_length,
                     control_byte
                    ]

  Trace.trace(32, "ftt_mode_select_6_bytes: Sending Mode Select: %s"%(sprint_list(cdb_mode_select),))
  Trace.trace(32, "ftt_mode_select_6_bytes: Mode select data: %s"%(sprint_list(data_block),))

  res = driver.ftt.do_write_scsi_command("mode select",
                                   cdb_mode_select,
                                   6,
                                   data_block,
                                   data_block_length,
                                   10) # timeout
  return res;


# set "Allow Maximum Capacity" bit for T10000C drive 
def t10000c_amc(driver,     # ftt driver
                amc = 0x00        # may have 2 values 1 - enable, 0 - disable
                ):        
  if not amc in (0,1):
    return False
  
  OFFSET_TO_AMC = 5 
  OFFSET_TO_DBD = 8
  
  # read what are the settings first (Mode select page 0x25)
  
  data = ftt_mode_sense_6_bytes(driver,
                                page_code = 0x25,
                                allocation_length = P_25H_LEN + MS_HEADER_DATA_LENGTH,
                                control_byte = 0x00
                                )
  Trace.trace(32, "t10000c_amc: Data: %s"%(sprint_list(data),)) 

  # modify data buffer to set AMC bit
  data[0] = 0x00
  data[1] = 0x00
  data[2] = 0x10
  data[3] = 0x00
  data[MS_HEADER_DATA_LENGTH + OFFSET_TO_AMC] = data[MS_HEADER_DATA_LENGTH + OFFSET_TO_AMC] & 0xfe | amc
  
  res = ftt_mode_select_6_bytes(driver,                 # ftt driver
                                data_block_length = P_25H_LEN + MS_HEADER_DATA_LENGTH,
                                data_block = data
                                )

  sense_data = ftt_mode_sense_6_bytes(driver,
                                      page_code = 0x25,
                                      allocation_length = P_25H_LEN + MS_HEADER_DATA_LENGTH,
                                      control_byte = 0x00
                                )
  Trace.trace(32, "t10000c_amc: Sense Data: %s"%(sprint_list(sense_data),)) 

  if sense_data[MS_HEADER_DATA_LENGTH + OFFSET_TO_AMC] != data[MS_HEADER_DATA_LENGTH + OFFSET_TO_AMC]:
    Trace.log(e_errors.ERROR, "AMC setting failed: out %s in %s"%
              (data[MS_HEADER_DATA_LENGTH + OFFSET_TO_AMC],
               sense_data[MS_HEADER_DATA_LENGTH + OFFSET_TO_AMC]))
    return False
  else:
    return True

# set "Data Compression Enabled" bit for T10000C drive 
def t10000c_dce(driver,     # ftt driver
                dce = 0x00        # may have 2 values 0x80 - enable, 0 - disable
                ):        
  if not dce in (0,0x80):
    return False

  OFFSET_TO_DCE = 2  
  # read what are the settings first (Mode select page 0x0f
  
  data = ftt_mode_sense_6_bytes(driver,
                                page_code = 0x0f,
                                allocation_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                                control_byte = 0x00
                                )
  Trace.trace(32, "t10000c_dce: Data: %s"%(sprint_list(data),)) 

  # modify data buffer to set dce bit
  data[0] = 0x00
  data[1] = 0x00
  data[2] = 0x10
  data[3] = 0x00
  data[MS_HEADER_DATA_LENGTH + OFFSET_TO_DCE] = data[MS_HEADER_DATA_LENGTH + OFFSET_TO_DCE] & 0x40 | dce

  res = ftt_mode_select_6_bytes(driver,                 # ftt driver
                                data_block_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                                data_block = data
                                )

  sense_data = ftt_mode_sense_6_bytes(driver,
                                      page_code = 0x0f,
                                      allocation_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                                      control_byte = 0x00
                                )
  Trace.trace(32, "t10000c_dce: Sense Data: %s"%(sprint_list(sense_data),)) 

  if sense_data[MS_HEADER_DATA_LENGTH + OFFSET_TO_DCE]&0xff != data[MS_HEADER_DATA_LENGTH + OFFSET_TO_DCE]:
    Trace.log(e_errors.ERROR, "AMC setting failed: out %0x in %0x"%
              (data[MS_HEADER_DATA_LENGTH + OFFSET_TO_DCE],
               sense_data[MS_HEADER_DATA_LENGTH + OFFSET_TO_DCE]&0xff))
    return False
  else:
    return True

# set "Select Data Compression Algorithm" bit for T10000C drive 
def t10000c_sdca(driver,     # ftt driver
                 sdca = 0x00        # may have 2 values 1 - LZ1 comprssion on write, 0 - no compression
                 ):        
  if not sdca in (0,1):
    return False
  OFFSET_TO_SDCA = 14  
  # read what are the settings first (Mode select page 0x0f
  
  data = ftt_mode_sense_6_bytes(driver,
                                page_code = 0x10,
                                allocation_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                                control_byte = 0x00
                                )
  Trace.trace(32, "t10000c_sdca: Data: %s"%(data,)) 

  # modify data buffer to set dce bit
  data[0] = 0x00
  data[1] = 0x00
  data[2] = 0x10
  data[3] = 0x00
  data[MS_HEADER_DATA_LENGTH + OFFSET_TO_SDCA] = sdca

  res = ftt_mode_select_6_bytes(driver,                 # ftt driver
                                data_block_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                                data_block = data
                                )

  sense_data = ftt_mode_sense_6_bytes(driver,
                                      page_code = 0x10,
                                      allocation_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                                      control_byte = 0x00
                                )
  Trace.trace(32, "t10000c_sdca: Sense Data: %s"%(sense_data,)) 

  if sense_data[MS_HEADER_DATA_LENGTH + OFFSET_TO_SDCA] != data[MS_HEADER_DATA_LENGTH + OFFSET_TO_SDCA]:
    Trace.log(e_errors.ERROR, "AMC setting failed: out %0x in %0x"%
              (data[MS_HEADER_DATA_LENGTH + OFFSET_TO_SDCA],
               sense_data[MS_HEADER_DATA_LENGTH + OFFSET_TO_SDCA]))
    return False
  else:
    return True


def  t10000_set_compression(driver, # ftt driver
                            compression = False # False - compression OFF, True - comression ON
                            ):
  dce = 0
  sdca = 0
  if compression:
    dce = 0x80
    sdca = 1
  if t10000c_dce(driver,dce):
    return (t10000c_sdca(driver,sdca))
  else:
    return False

def print_list(l):
  for i in l:
    print "%02x"%(i&0xff,),
  print ""

def sprint_list(l):
  s = ""
  for i in l:
    s = "%s%02x "%(s,i&0xff,)
  return s

def page_25_test(driver):
  stats = driver.get_stats()
  remaining = stats[ftt.REMAIN_TAPE]
  if remaining:
    remaining = long(remaining)* 1024L 
    print "Remaining Bytes", remaining

  # we are reading sense page 0x25
  res = ftt_mode_sense_6_bytes(driver,
                               page_code = 0x25,
                               allocation_length = P_25H_LEN + MS_HEADER_DATA_LENGTH,
                               control_byte = 0x00
                             )
  print "Current Settings"
  print_list(res)

  res = ftt_mode_sense_6_bytes(driver,
                               page_code = 0x25+0x40,
                               allocation_length = P_25H_LEN + MS_HEADER_DATA_LENGTH,
                               control_byte = 0x00
                             )
  print "Changeable Settings"
  print_list(res)

  amc = raw_input("enter 0 or 1 to set AMC or just press Enter: ")
  if amc:
    amc = int(amc)
    if amc > 1 or amc < 0:
      print "only 0 or 1 is allowed"
      sys.exit(1)
  
    print("Set AMC to %s"%(amc,))
    if t10000c_amc(driver,  # ftt driver
                   amc = amc # may have 2 values 1 - enable, 0 - disable
                   ):
      print "AMC was set successfully"
      stats = driver.get_stats()
      remaining = stats[ftt.REMAIN_TAPE]
      if remaining:
        remaining = long(remaining)* 1024L
        print "Remaining Bytes", remaining 

    else:
      print "AMC setting failed"
  
def page_0f_test(driver):
  stats = driver.get_stats()
  remaining = stats[ftt.REMAIN_TAPE]
  if remaining:
    remaining = long(remaining)* 1024L 
    print "Remaining Bytes", remaining

  # we are reading sense page 0x25
  res = ftt_mode_sense_6_bytes(driver,
                               page_code = 0x0f,
                               allocation_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                               control_byte = 0x00
                             )
  Trace.trace(32, "page_0f_test. Current Settings")
  Trace.trace(32, "page_0f_test. %s"%(sprint_list(res)))

  res = ftt_mode_sense_6_bytes(driver,
                               page_code = 0x0f+0x40,
                               allocation_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                               control_byte = 0x00
                             )
  Trace.trace(32, "page_0f_test. Changeable Settings")
  Trace.trace(32, "page_0f_test. %s"%(sprint_list(res)))


def page_10_test(driver):
  stats = driver.get_stats()
  remaining = stats[ftt.REMAIN_TAPE]
  if remaining:
    remaining = long(remaining)* 1024L 
    print "Remaining Bytes", remaining

  # we are reading sense page 0x25
  res = ftt_mode_sense_6_bytes(driver,
                               page_code = 0x10,
                               allocation_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                               control_byte = 0x00
                             )
  Trace.trace(32, "page_10_test. Current Settings")
  Trace.trace(32, "page_10_test. %s"%(sprint_list(res)))

  res = ftt_mode_sense_6_bytes(driver,
                               page_code = 0x10+0x40,
                               allocation_length = P_0FH_LEN + MS_HEADER_DATA_LENGTH,
                               control_byte = 0x00
                             )
  Trace.trace(32, "page_0f_test. Changeable Settings")
  Trace.trace(32, "page_0f_test. %s"%(sprint_list(res)))

def usage(cmd_name):
  print "usage: %s <device name>"%(cmd_name)

def prompt():
  print "0 - exit"
  print "1 - Data Compression Page (0x0f)"
  print "2 - Device Configuration Page (0x10)"
  print "3 - Read/Write Control Page (0x25)"
  print "4 - set DCE in Data Compression Page (0x0f)"
  print "5 - set SDCA in Device Configuration Page (0x0f)"
  print "6 - set compression"
  return raw_input("Make your selection: ")

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
  driver.open(sys.argv[1], mode=1, retry_count=3)
  stats = driver.get_stats()
  print "Drive Type", stats[ftt.PRODUCT_ID]
  while True:
    test_number = prompt()
    if test_number:
      test_number = int(test_number)
      if test_number == 1:
        page_0f_test(driver)
      elif test_number == 2:
        page_10_test(driver)
      elif test_number == 3:
        page_25_test(driver)
      elif test_number == 4:
        dce = raw_input("set - 1, clear - 0: ")
        if dce:
          dce = int(dce)
          if dce == 1:
            dce = 0x80
          else:
            dce = 0
          t10000c_dce(driver, dce = dce)
      elif test_number == 5:
        sdca = raw_input("set - 1, clear - 0: ")
        if sdca:
          sdca = int(sdca)
          t10000c_sdca(driver, sdca = sdca)
      elif test_number == 6:
        c = raw_input("set - 1, clear - 0: ")
        print t10000_set_compression(driver, c)
      else:
        break
    else:
      break
  
  
    
