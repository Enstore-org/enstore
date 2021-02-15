#!/usr/bin/env python

###############################################################################
#
# $Id$ 0000 0000 0000 0000 00010000
#
###############################################################################

###############################################################################
#
# This script monitors files in dcache
#
###############################################################################


from __future__ import print_function
import sys
import string
import pg
import time
import os
import re
import pnfsidparser
import getopt
import histogram
import configuration_client
import enstore_constants
import socket
import enstore_files


def usage(cmd):
    print("Usage: %s -s [--sleep=] " % (cmd,))
    print("\t --sleep : sampling interval in seconds")


def do_work(db_name, username):
    #
    # extract entries from volatile files
    #
    db = pg.DB(db_name, user=username)
    sql_txt = "select xact_commit from pg_stat_database"
    res = db.query(sql_txt)

    sn = 0
    inter = interval

    for row in res.getresult():
        if not row:
            continue
        sn = sn + row[0]
    db.close()
    return sn


if __name__ == '__main__':

    interval = 10
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:ss:", ["help", "sleep="])
    except getopt.GetoptError:
        print("Failed to process arguments")
        usage(sys.argv[0])
        sys.exit(2)
    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(1)
        if o in ("-s", "--sleep"):
            interval = a

    #
    # get info from config server
    #
    intf = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc = configuration_client.ConfigurationClient(
        (intf.config_host, intf.config_port))
    retry = 0
    timeout = 1
    system_name = csc.get_enstore_system(1, retry)
    config_dict = {}
    if system_name:
        config_dict = csc.dump(timeout, retry)
        config_dict = config_dict['dump']
    else:
        configfile = os.environ.get('ENSTORE_CONFIG_FILE')
        f = open(configfile, 'r')
        code = string.join(f.readlines(), '')
        configdict = {}
        exec(code)
        config_dict = configdict
        ret = configdict['known_config_servers']
        def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
                    int(os.environ['ENSTORE_CONFIG_PORT']))
        for item in ret.items():
            if socket.getfqdn(item[1][0]) == socket.getfqdn(def_addr[0]):
                system_name = item[0]

    inq_d = config_dict.get(enstore_constants.INQUISITOR, {})

    html_dir = None
    if "html_file" in inq_d:
        html_dir = inq_d["html_file"]
    else:
        html_dir = enstore_files.default_dir

    html_host = None
    if "host" in inq_d:
        html_host = inq_d["host"]
    else:
        html_host = enstore_files.default_dir

    ntuple = histogram.Ntuple(
        "transactions_on_%s" %
        system_name,
        "transactions per second on %s" %
        system_name)
    ntuple.set_time_axis()
    ntuple.set_line_color(1)
    ntuple.set_line_width(2)
    ntuple.set_marker_type("lines")
    ntuple.set_time_axis_format("%H:%M")
    ntuple.set_ylabel("Transactions per second")
    ntuple.set_xlabel("(hour:minute)")
    so = 0
    sn = 0
    n = 0
    try:
        while True:
            so = sn
            sn = do_work("template1", "enstore")
            sd = (sn - so + interval / 2) / interval
            if so != 0:
                #print time.ctime(), sd," trans/sec"
                ntuple.get_data_file().write("%s %f\n" %
                                             (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), sd))
            n = n + 1
            if n % 10 == 0:
                ntuple.get_data_file().close()
                ntuple.plot("1:3")
                ntuple.data_file = open(ntuple.get_data_file_name(), "a")
                cmd = "$ENSTORE_DIR/sbin/enrcp transactions_on_*  %s.fnal.gov:%s" % (
                    html_host, html_dir)
                if os.system(cmd):
                    print("failed ", cmd)
                    sys.exit(1)
            if n % 8640 == 0:
                os.system("rm -f %s" % (ntuple.get_data_file_name()))
                ntuple.data_file = open(ntuple.get_data_file_name(), "w")
            time.sleep(interval)
    except (KeyboardInterrupt, SystemExit):
        ntuple.get_data_file().close()
        ntuple.set_line_color(1)
        ntuple.set_line_width(1)
        ntuple.set_marker_type("lines")
        ntuple.plot("1:3")
        sys.exit(0)
