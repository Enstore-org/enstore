#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# This script produces histograms showing tape usage per storage
# group
#
# Requires no input arguments
#
###############################################################################

from __future__ import print_function
import sys
import time
import popen2
import os
import string
import enstore_constants
import enstore_functions
import configuration_client
import pg
import accounting_query
import histogram

MB = 1024 * 1024.

# SELECT_STMT= "select last_access,system_inhibit_1,(1.-remaining_bytes/1024./1024./1024. / (capacity_bytes/1024./1024./1024))*100 as percentage from volume where file_family!='none' and label not like '_______deleted' and  capacity_bytes>0 and storage_group='%s' and (1.-remaining_bytes/1024./1024./1024. / (capacity_bytes/1024./1024./1024))>0 and last_access>'1970-12-31 17:59:59'"
SELECT_STMT = "select last_access,system_inhibit_1,(1.-remaining_bytes/1024./1024./1024. / (capacity_bytes/1024./1024./1024))*100 as percentage from volume where file_family!='none' and label not like '%deleted' and  capacity_bytes>0 and (1.-remaining_bytes/1024./1024./1024. / (capacity_bytes/1024./1024./1024))>0 and last_access>'1970-12-31 17:59:59' and storage_group='"


def showError(msg):
    sys.stderr.write("Error: " + msg)


def usage():
    print("Usage: %s  <file_family> " % (sys.argv[0],))


def main():

    intf = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc = configuration_client.ConfigurationClient(
        (intf.config_host, intf.config_port))

    acc = csc.get("database", {})

    inq = csc.get(enstore_constants.INQUISITOR, {})
    web_dir = os.path.join(inq.get('html_file', ''), "file_family_usage")

    storage_groups = []

    db = pg.DB(host=acc.get('db_host', "localhost"),
               dbname=acc.get('dbname', "enstoredb"),
               port=acc.get('db_port', 5432),
               user=acc.get('dbuser', "enstore"))

    res = db.query("select distinct(storage_group) from volume")

    for row in res.getresult():
        if not row:
            continue
        storage_groups.append(row[0])

    histograms = []

    now_time = time.time()
    start_time = now_time - 365 * 3600 * 24

    str_now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now_time))
    str_start_time = time.strftime(
        '%Y-%m-%d %H:%M:%S',
        time.localtime(start_time))

    if not os.path.exists(web_dir):
        os.makedirs(web_dir)
        os.system("cp ${ENSTORE_DIR}/etc/*.gif %s" % (web_dir))
    os.chdir(web_dir)

    for sg in storage_groups:
        h1 = histogram.Histogram1D(
            sg, "%s tape occupancy" %
            (sg), 1000, 0, 100)
        h1.set_logy(True)
        h1.set_ylabel("Number of Volumes / %s" % (h1.get_bin_width(0)))
        h1.set_xlabel("Fill Fraction")
        h1.set_marker_type("impulses")
        h1.set_opt_stat(True)

        h2 = histogram.Histogram1D(
            "%s_active" %
            sg,
            "%s active volumes vs last access time" %
            (sg),
            120,
            float(start_time),
            float(now_time))
        h2.set_ylabel("Number of active volumes ")
        h2.set_xlabel("Date")
        h2.set_time_axis(True)
        h2.set_marker_type("points")

        h3 = histogram.Histogram1D(
            "%s_time" %
            sg,
            "%s tape occupancy  vs last access time" %
            (sg),
            120,
            float(start_time),
            float(now_time))
        h3.set_ylabel("Fill Fraction")
        h3.set_xlabel("Date")
        h3.set_time_axis(True)
        h3.set_profile(True)
        h3.set_marker_type("points")

#        select_stmt=SELECT_STMT%(sg,)
        select_stmt = SELECT_STMT + sg + "'"

        res = db.query(select_stmt)
        for row in res.getresult():
            if not row:
                continue
            h1.fill(row[2])
            if (row[1] == 'none'):
                h2.fill(
                    time.mktime(
                        time.strptime(
                            row[0],
                            '%Y-%m-%d %H:%M:%S')))
                h3.fill(
                    time.mktime(
                        time.strptime(
                            row[0],
                            '%Y-%m-%d %H:%M:%S')),
                    row[2])
        histograms.append(h1)
        histograms.append(h2)
        histograms.append(h3)
    db.close()

    for hist in histograms:
        if (hist.n_entries() > 0):
            hist.plot()
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        print("Failed to execute ")
        sys.exit(0)
    sys.exit(0)
