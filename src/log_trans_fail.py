#!/usr/bin/env python

import os
import pprint
import string
import sys
import time

def cmd(command):
    print command
    p = os.popen(command,'r')
    text = p.read()
    s = p.close()
    lines = []
    for line in string.split(text,'\n'):
        line = string.strip(line)
        if line:
            lines.append(line)
    return lines


def get_failures(log,grepv='GONE|NUL|DSKMV|disk',grep=""):
    thisnode = os.uname()[1]
    if len(thisnode) > 2:
        gang = thisnode[0:3]
    else:
        gang = ' '
    if gang == 'd0e':
        grepv_ = " DI|"+" DC|"+grepv
    elif gang == 'stk':
        grepv_ = "JDE|"+grepv
    else:
        grepv_ = grepv
    
    # just force the directory.
    failed = cmd('cd /diska/enstore-log; egrep "transfer.failed|SYSLOG.Entry" %s /dev/null|grep -v exception |egrep -v "%s" | egrep "%s"' %(log,grepv_,grep))
    return failed

def parse_failures(failed):
    Vol = {}
    Drv = {}
    for l in failed:
        token = string.split(l,' ')
        size = len(token)
        thetime = token[0]
        thetime = string.replace(thetime,'LOG-','')
        node = token[1]
        drive = token[5]
        location = token[size-1]
        volume = token[size-2]
        volume = string.replace(volume,'volume=','')
        reason = string.join(token[6:size-2])
        error = [thetime, node, drive, location, volume, reason]
        if Vol.has_key(volume):
            Vol[volume].append(error)
        else:
            Vol[volume] = [error,]
        if Drv.has_key(drive):
            Drv[drive].append(error)
        else:
            Drv[drive] = [error,]
    return (Vol,Drv)

def print_vols(Vol):
    keys = Vol.keys()
    keys.sort()
    for v in  keys:
        print v
        info = Vol[v]
        for err in range(0,len(info)):
            error = info[err]
            print "   %-13s %-10s %20s %s" % (error[3],error[2],error[0],error[5])

def print_drv(Drv):
    keys = Drv.keys()
    keys.sort()
    for d in keys:
        print d
        info = Drv[d]
        for err in range(0,len(info)):
            error = info[err]
            print "   %-13s %-10s %20s %s" % (error[3],error[4],error[0],error[5])

def logname(t):
    t_tup=time.localtime(t)
    return "LOG-%4.4i-%2.2i-%2.2i" %(t_tup[0],t_tup[1],t_tup[2])



if __name__ == "__main__":
    now = time.time()
    today =  time.asctime(time.localtime(now))[0:10]
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = "today"

    if choice == "today":
        logfile = logname(now)
    elif choice == "week":
        logfile = ""
        for day in range(6,-1,-1):
            logfile=logfile+" "+logname(now-day*86400)
    elif choice == "month":
        logfile = ""
        for day in range(30,-1,-1):
            logfile=logfile+" "+logname(now-day*86400)
    else:
        logfile = choice


    print time.ctime(now)

    f=get_failures(logfile)
    Vol,Drv = parse_failures(f)

    print
    print '--------------------------------------------------------------------------------------------------'
    print
    print_vols(Vol)

    print
    print '--------------------------------------------------------------------------------------------------'
    print
    print_drv(Drv)
