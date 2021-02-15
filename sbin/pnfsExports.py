#!/usr/bin/env python
from __future__ import print_function
import sys
import string
import os
import getopt
import time
import traceback


def tod(T=None):
    if T is None:
        T = time.time()
    return time.strftime("%c", time.localtime(T))


def do_pnfs(command):
    result_lf = os.popen(command, 'r').readlines()
    result = []
    for line in result_lf:
        result.append(string.replace(line, '\012', ''))
    return result


if __name__ == "__main__":

    all = 0
    hostname = 'd0ensrv1'
    ncolumns = 3

    longopts = ["hostname=", "all"]
    arglist = sys.argv[1:]
    opts, args = getopt.getopt(arglist, "", longopts)
    for opt, val in opts:
        # be very friendly about '_' and '-'
        opt = string.replace(opt, '_', '')
        opt = string.replace(opt, '-', '')
        if opt == "hostname":
            hostname = val
        elif opt == "all":
            all = 1

    if len(args) != 0:
        print("Usage:\n", sys.argv[0], end=' ')
        for optname in longopts:
            if optname[-1] == '=':
                print("--" + optname + "value", end=' ')
            else:
                print("--" + optname, end=' ')
        sys.exit(-1)

    if not all:
        hostlist = [hostname, ]
    else:
        hostlist = do_pnfs("pmount show hosts")

    print('<html> <head> <title>PNFS Exports Page</title> </head> <body>')
    print('<body bgcolor="#ffffff" text=#a0a0ff">')
    print('<h1><center>PNFS Exports Page </center><h1><hr>')

    print('<h1><center>PNFS Information: %s</center><h1>' % (tod(),))
    print('<pre>')
    showall = do_pnfs("mdb showall")
    for line in showall:
        print(line)
    print('</pre><hr>')

    print('<h1><center>PNFS ExportList Fetch Begin: %s</center><h1><hr>' % tod())
    print('<table bgcolor="#dfdff0" nosave >')
    print('<tr>\n')

    col = 0

    for host in hostlist:
        if col == ncolumns:
            print('</tr>\n <tr>')
            col = 0
        col = col + 1
        print('<td> <pre>')
        try:
            allowed = do_pnfs("pmount show host " + host)
            for line in range(0, len(allowed)):
                print(allowed[line])
        except BaseException:
            print('host problems', host)
            traceback.print_tb()

        print('</pre> </td>')

    print('</table>')
    print('<h1><center>PNFS ExportList Fetch Done: %s</center><h1><hr>' % tod())
    print('</html>')
