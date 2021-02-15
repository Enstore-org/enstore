#!/usr/bin/env python
##############################################################################
#
# $Id$
#
# script generates http://www-stken.fnal.gov/enstore/pnfsExports.html
#
##############################################################################
import os
import sys
import re
import time
import enstore_constants
import configuration_client
import enstore_functions2

FILE_NAME = "/tmp/pnfsExports.html"
NCOLUMNS = 3

if __name__ == "__main__":
    export_file = open("/etc/exports", "r")
    reverse_content = {}
    for l in export_file:
        if not l:
            continue
        parts = l.strip().split()
        for h in parts[1:]:
            host, option = re.sub(r"[\(\)]", " ", h).strip().split()
            if host not in reverse_content:
                reverse_content[host] = []
            reverse_content[host].append("%-30s %-17s" % (parts[0], option))
    export_file.close()

    f = open(FILE_NAME, "w")
    for line in ("<html> <head> <title>PNFS Exports Page</title> </head> <body>\n",
                 "<body bgcolor=\"#ffffff\" text=#a0a0ff\">\n",
                 "<h1><center>Chimera Exports Page </center><h1><hr>\n",
                 "<h1><center>Chimera Information: %s </center><h1>\n" % (
                     time.ctime()),
                 "<h1><center>Chimera ExportList Fetch Begin: %s </center><h1><hr>\n" % (
                     time.ctime()),
                 "<table bgcolor=\"#dfdff0\" nosave>\n"):
        f.write(line)

    f.write("<tr>\n")

    columns = 0
    keys = sorted(reverse_content.keys())

    for host in keys:
        mpoints = sorted(reverse_content[host])
        if columns and not columns % NCOLUMNS:
            f.write("</tr><tr>\n")
        f.write("<td style=\"vertical-align:top\"> <pre>\n")
        f.write("{0:-^60}".format(host))
        f.write("\n")
        for mp in mpoints:
            f.write("%s\n" % mp)
        f.write("</pre> </td>\n")
        columns += 1

#    for host, mpoints in reverse_content.iteritems():
#        columns+=1
#        if not columns % NCOLUMNS :
#            f.write("</tr><tr>\n")
#        f.write("<td style=\"vertical-align:top\"> <pre>\n")
#        f.write("{0:-^60}".format(host))
#        f.write("\n")
#        for mp in mpoints:
#            f.write("%s\n"%mp)
#        f.write("</pre> </td>\n")

    if columns % NCOLUMNS:
        f.write("</tr>\n")
    for line in ("</table>\n",
                 "<h1><center>Chimera ExportList Fetch Done:  %s </center><h1><hr>\n" % (
                     time.ctime()),
                 "</html>\n"):
        f.write(line)
    f.close()

    csc = configuration_client.get_config_dict()
    web_server_dict = csc.get("web_server")
    web_server_name = web_server_dict.get(
        "ServerName", "localhost").split(".")[0]
    output_file = "/tmp/%s_pnfs_monitor" % (web_server_name,)
    html_dir = csc.get("crons").get("html_dir", None)
    inq_d = csc.get(enstore_constants.INQUISITOR, {})
    html_host = inq_d.get("host", "localhost")
    if not html_dir:
        sys.stderr.write(
            time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(
                    time.time())) +
            " : html_dir is not found \n")
        sys.stderr.flush()
        sys.exit(1)
    cmd = "$ENSTORE_DIR/sbin/enrcp %s %s:%s" % (f.name, html_host, html_dir)
    rc = os.system(cmd)
    if rc:
        txt = "Failed to execute command %s\n.\n" % (cmd,)
        sys.stderr.write(
            time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(
                    time.time())) +
            " : " +
            txt +
            "\n")
        sys.stderr.flush()
        sys.exit(1)
    sys.exit(0)
