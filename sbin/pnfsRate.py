#!/usr/bin/env python

# this can only be run on the node where pnfs is running AND only if the
# uid has acces to /pnfs/fs (typically root)

from __future__ import print_function
import sys
import time
import string
import os
import getopt
import traceback


def clearScreen():
    sys.stdout.write(chr(27) + "[H" + chr(27) + "[J")
    sys.stdout.flush()


def tod(T=None):
    if T is None:
        T = time.time()
    return time.strftime("%c", time.localtime(T))


def pnfsRate(mountpoint, db, sleeptime, once):
    counter = 0
    absolute = {}
    delta = {}
    try:
        f = open(mountpoint + '/.(get)(database)(' + db + ')', 'r')
        line = f.read()
        eol = string.find(line, '\012')
        if eol != -1:
            database = line[0:eol]
        else:
            database = line
        f.close()
    except IOError as emess:
        message = "mountpoint=%s db=%s  sleeptime=%d once=%d" % (
            mountpoint, db, sleeptime, once)
        message = message + "ERROR: Does the mountpoint exist?\n %s", emess
        return message
    except TypeError as message:
        message = "mountpoint=%s db=%s  sleeptime=%d once=%d" % (
            mountpoint, db, sleeptime, once)
        message = message + "ERROR: Does the database exist?\n %s", emess
        return message

    if string.find(line[0], "enabled") != 0:
        f = open(mountpoint + '/.(get)(counters)(' + db + ')', 'r')
        while True:
            counter = counter + 1
            message = ""
            f.seek(0)
            line = f.read()
            for line in string.split(line):
                if string.find(line, '\0') != 0:
                    item, svalue = string.split(line, "=")
                    value = string.atol(svalue)
                    old = absolute.get(item, 0)
                    delta[item] = value - old
                    absolute[item] = value
            time_delta = delta['time']
            if time_delta != 0 and counter != 1:
                message = message + \
                    "%s\n %s\n" % (tod(absolute['time']), database)
                items = sorted(delta.keys())
                message = message + \
                    "%15s %10s %6s  %s\n" % (
                        "Attribute", "Absolute", "Change", "Rate")
                message = message + \
                    "%15s %10s %6s  %s\n" % (
                        "=========", "========", "======", "====")
                for k in items:
                    rate = delta[k] / 1. / time_delta
                    message = message + \
                        "%15s %10d %6d  %.3g/s\n" % (k,
                                                     absolute[k], delta[k], rate)
                if once:
                    return message
                else:
                    clearScreen()
                    print(message)
            time.sleep(sleeptime)


if __name__ == "__main__":

    mountpoint = '/pnfs/sam/mammoth'
    database = '7'
    sleeptime = 10
    once = 0
    all = 0
    genhtml = 0
    ncolumns = 3
    lssizecmd = None

    longopts = [
        "mountpoint=",
        "database=",
        "sleeptime=",
        "once",
        "all",
        "genhtml",
        "lssizecmd="]
    arglist = sys.argv[1:]
    opts, args = getopt.getopt(arglist, "", longopts)
    for opt, val in opts:
        # be very friendly about '_' and '-'
        opt = string.replace(opt, '_', '')
        opt = string.replace(opt, '-', '')
        if opt == "mountpoint":
            mountpoint = val
        elif opt == "database":
            database = val
        elif opt == "sleeptime":
            sleeptime = string.atoi(val)
        elif opt == "once":
            once = 1
        elif opt == "all":
            all = 1
        elif opt == "genhtml":
            genhtml = 1
        elif opt == "lssizecmd":
            lssizecmd = val

    if len(args) != 0:
        print("Usage:\n", sys.argv[0], end=' ')
        for optname in longopts:
            if optname[-1] == '=':
                print("--" + optname + "value", end=' ')
            else:
                print("--" + optname, end=' ')
        sys.exit(-1)

    maxdb = string.atoi(database)

    if all:
        once = 1
        mindb = 0
    else:
        if genhtml and not once:
            genhtml = 0
        mindb = maxdb

    maxdb = maxdb + 1

    if genhtml:
        col = 0
        print('<html> <head> <title>PNFS Counter Status Page</title> </head> <body>')
        print('<meta http-equiv="Refresh" content="1800">')
        print('<body bgcolor="#ffffff" text=#a0a0ff">')
        print('<h1><center>PNFS Counter Status Page </center><h1><hr>')

        if lssizecmd:
            try:
                parts = string.split(lssizecmd)
                node = parts[0]
                location = parts[1]
                command = "enrsh " + node + " ' ls -alsFh " + location + "'"
                #print 'command=',command
                sizes = os.popen(command, 'r').readlines()
                print(
                    '<h1><center>PNFS Database Sizes: %s %s</center><h1>' %
                    (parts, tod()))
                print('<pre>')
                for line in range(0, len(sizes)):
                    print(sizes[line], end=' ')
                print('</pre><hr>')
            except BaseException:
                # forget it for now
                print("ERROR on ", lssizecmd)
                pass

        print(
            '<h1><center>PNFS Counter Fetch Begin: %s</center><h1><hr>' %
            tod())
        print('<table bgcolor="#dfdff0" nosave >')
        print('<tr>\n')

    if database > 0:
        for db in range(mindb, maxdb):
            #print 'enrsh '+node+' "/usr/local/bin/isPnfsDbEnabled %s"'%db
            try:
                enabled = os.popen(
                    'enrsh ' +
                    node +
                    ' "/usr/local/bin/isPnfsDbEnabled %s" 2>/dev/null ' %
                    db,
                    'r').readlines()
            except BaseException:
                enabled = ['0\n']
            #print enabled
            if len(enabled) > 0 and string.find(enabled[0], "1\n") == 0:
                rates = pnfsRate(mountpoint, "%d" % db, sleeptime, once)
                if genhtml:
                    if col == ncolumns:
                        print('</tr>\n <tr>')
                        col = 0
                    col = col + 1
                    print('<td> <pre>')
                print(rates)
                if genhtml:
                    print('</pre> </td>')
    if genhtml:
        print('</table>')
        print(
            '<h1><center>PNFS Counter Fetch Done: %s</center><h1><hr>' %
            tod())
        print('</html>')
