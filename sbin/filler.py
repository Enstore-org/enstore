#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# this file has to run in cron job. It fills averages for encp_xfer data and
# puts them into encp_xfer_average_by_storage_group
#
# Requires no input arguments
#
###############################################################################

import sys
import time
import popen2
import os
import getopt, string
import math
import accounting_query
import enstore_constants
import enstore_functions
import configuration_client

MB=1024*1024.


def showError(msg):
    sys.stderr.write("Error: " + msg)

def usage():
    print ""
def main():
    intf = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
    csc.csc = csc
    acc = csc.get(enstore_constants.ACCOUNTING_SERVER, {})

    accounting_db_server_name = acc.get('dbhost')
    accounting_db_name        = acc.get('dbname')

    login_string = "psql  %s -h %s -t -q -c "%(accounting_db_name,accounting_db_server_name,)
    cmd = "%s \" select max(unix_time) from encp_xfer_average_by_storage_group;\""%(login_string,)
    inp,out = os.popen2 (cmd, 'r')
    inp.write (cmd)
    inp.close ()

    zero_time  = 1045689052
    for line in out.readlines() :
        if line.isspace():
            continue
        zero_time=int(line.strip(' '))
    out.close()
    
    delta_time = 60 * 20
    zero_time  = int(zero_time+0.5*delta_time)

#    print 'Max time',zero_time

    now_time   =  int(time.time())

    while zero_time < now_time:
      stop_time       = zero_time + delta_time;
      middle_time     = int(zero_time + 0.5*delta_time)
      str_middle_time   = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(middle_time))
      str_from_time   = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(zero_time))
      str_to_time     = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(stop_time))
      select_stmt = "%s \""%(login_string,)
      select_stmt = select_stmt +  "insert into encp_xfer_average_by_storage_group "
      select_stmt = select_stmt +  " ( select "
      select_stmt = select_stmt + str(middle_time)
      select_stmt = select_stmt + ",'"
      select_stmt = select_stmt + str_middle_time
      select_stmt = select_stmt + "','"
      select_stmt = select_stmt + str_from_time
      select_stmt = select_stmt + "','"
      select_stmt = select_stmt + str_to_time
      select_stmt = select_stmt + "',storage_group, rw,"
      select_stmt = select_stmt + "avg(overall_rate)/1024./1024,"
      select_stmt = select_stmt + "avg(network_rate)/1024./1024,"
      select_stmt = select_stmt + "avg(disk_rate)/1024./1024,"
      select_stmt = select_stmt + "avg(transfer_rate)/1024./1024,"
      select_stmt = select_stmt + "avg(drive_rate)/1024./1024,"
      select_stmt = select_stmt + "avg(size)/1024./1024,"
      select_stmt = select_stmt + "stddev(overall_rate)/1024./1024,"
      select_stmt = select_stmt + "stddev(network_rate)/1024./1024,"
      select_stmt = select_stmt + "stddev(disk_rate)/1024./1024,"
      select_stmt = select_stmt + "stddev(transfer_rate)/1024./1024,"
      select_stmt = select_stmt + "stddev(drive_rate)/1024./1024,"
      select_stmt = select_stmt + "stddev(size)/1024./1024, count(*) from"
      select_stmt  = select_stmt + " encp_xfer where date between '"
      select_stmt  = select_stmt + str_from_time
      select_stmt  = select_stmt + "' and '"
      select_stmt  = select_stmt + str_to_time
      select_stmt  = select_stmt + "' group by storage_group, rw)"
      select_stmt  = select_stmt + ";\" enstore"

#      print 'Executing:',select_stmt
      sys.exit(0)
      os.system(select_stmt)
      zero_time = zero_time + delta_time

    sys.exit(0)

if __name__ == "__main__":
    main()
