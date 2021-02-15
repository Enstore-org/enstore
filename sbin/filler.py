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

from __future__ import print_function
import sys
import time
import pg
import enstore_constants
import configuration_client
import types
import traceback

PB = 1024. * 1024. * 1024. * 1024. * 1024.
TB = 1024. * 1024. * 1024. * 1024.
GB = 1024. * 1024. * 1024.
MB = 1024. * 1024.
KB = 1024.

SELECT_LAST_TIME = "select max(unix_time) from encp_xfer_average_by_storage_group"


def showError(msg):
    sys.stderr.write("Error: " + msg)


def usage():
    print("")


def main():
    intf = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc = configuration_client.ConfigurationClient(
        (intf.config_host, intf.config_port))
    csc.csc = csc
    acc = csc.get(enstore_constants.ACCOUNTING_SERVER, {})

    db = pg.DB(host=acc.get('dbhost', "localhost"),
               dbname=acc.get('dbname', "accounting"),
               port=acc.get('dbport', 5432),
               user=acc.get('dbuser', "enstore"))

    zero_time = 1045689052
    res = db.query(SELECT_LAST_TIME)
    for row in res.getresult():
        if not row:
            continue
        zero_time = row[0]

    if isinstance(zero_time, type(None)):
        zero_time = 1045689052

    delta_time = 60 * 20
    zero_time = int(zero_time + 0.5 * delta_time)
    now_time = int(time.time())

    while zero_time < now_time:
        stop_time = zero_time + delta_time
        middle_time = int(zero_time + 0.5 * delta_time)
        str_middle_time = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(middle_time))
        str_from_time = time.strftime(
            '%Y-%m-%d %H:%M:%S',
            time.localtime(zero_time))
        str_to_time = time.strftime(
            '%Y-%m-%d %H:%M:%S',
            time.localtime(stop_time))
        select_stmt = "insert into encp_xfer_average_by_storage_group "
        select_stmt = select_stmt + " ( select "
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
        select_stmt = select_stmt + " encp_xfer where date between '"
        select_stmt = select_stmt + str_from_time
        select_stmt = select_stmt + "' and '"
        select_stmt = select_stmt + str_to_time
        select_stmt = select_stmt + "' group by storage_group, rw)"
        db.query(select_stmt)
        zero_time = zero_time + delta_time
    db.close()


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        print("Failed")
        exc, msg, tb = sys.exc_info()
        for l in traceback.format_exception(exc, msg, tb):
            print(l)
        sys.exit(1)
    sys.exit(0)
