#!/usr/bin/env python

###############################################################################
#
# $Id$
# Take a snapshot of requests for all library managers
###############################################################################
from __future__ import print_function
import time
import os
import sys
import getopt

import enstore_functions2
import configuration_client


def print_help():
    print("""get snapshots of library manager(s) queue(s) and active volumes
    OPTIONS:
    -l, --library library_manager - library manager to monitor, if not specified, get all from configuration
    -o, --out-dir output_directory - root of the history, if not specified, get all from configuration
    -t, --time interval - monitoring inteval in seconds, if not specified, make one snapshot
    -h, --help - print help
    """)


if __name__ == "__main__":

    # parse command line arguments
    library_managers = []
    out_dir = None
    interval = 0
    opts, args = getopt.getopt(sys.argv[1:], "l:o:t:h", [
                               "library", "out-dir", "time", "help"])
    for o, a in opts:
        if o in ["-l", "--library"]:
            library = library_managers = [a]
        elif o in ["-o", "--out-dir"]:
            out_dir = a
        elif o in ["-t", "--time"]:
            interval = int(a)
        elif o in ["-h", "--help"]:
            print_help()
            sys.exit(0)
    if not (out_dir and library_managers):
        # need to use enstore configuration
        default_config_host = enstore_functions2.default_host()
        default_config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((default_config_host,
                                                        default_config_port))

        rc = csc.dump_and_save()
    if not out_dir:
        # will use web_dir as a parent directory
        inquisitor = csc.saved_dict.get("inquisitor")
        out_dir = inquisitor.get("html_file", None)
        if not out_dir:
            print("Destination directory is not defined")
            sys.exit(1)

    if not library_managers:
        # all library managers will be taken from configuration
        for key in csc.saved_dict.keys():
            if key.find(".library_manager") > 0:
                library_managers.append(key)
    while True:
        tm = time.localtime()
        dname = "%04d-%02d-%02d" % (tm[0], tm[1], tm[2])
        history_dir = os.path.join(out_dir, "request_history", dname)
        lm_dirs = []
        for key in library_managers:
            lm_dirs.append(os.path.join(history_dir, key))

        for lm_path in lm_dirs:
            if not os.path.exists(lm_path):
                try:
                    os.makedirs(lm_path)
                except os.error as msg:
                    print("Can not create %s. %s" % (lm_path, msg))
                    sys.exit(1)

            tm = time.localtime()
            suffix = "%02d-%02d-%02d" % (tm[3], tm[4], tm[5])
            dst = "%s/%s-%s.%s" % (lm_path,
                                   os.path.basename(lm_path),
                                   suffix,
                                   "txt")
            #print "DST", dst
            if not os.path.exists(dst):
                # we do all enstore commands via shell,
                # because they all (except --queue-length)
                # have formatted output
                cmd = "/usr/local/etc/setups.sh;enstore lib --queue-length %s" % (
                    os.path.basename(lm_path),)
                ret = enstore_functions2.shell_command(cmd)
                if ret:
                    queue_length = int(ret)
                #print "QL", queue_length
                if queue_length > 0:
                    #print "writing to", dst
                    out_f = open(dst, "w")
                    cmd = "/usr/local/etc/setups.sh;enstore lib --get-queue '' %s" % (
                        os.path.basename(lm_path),)
                    ret = enstore_functions2.shell_command(cmd)
                    if ret:
                        out_f.write(ret)
                        cmd = "/usr/local/etc/setups.sh;enstore lib --vols %s" % (
                            os.path.basename(lm_path),)
                        ret = enstore_functions2.shell_command(cmd)
                        if ret:
                            out_f.write(ret)
                    out_f.close()
        if interval:
            time.sleep(interval)
        else:
            break
