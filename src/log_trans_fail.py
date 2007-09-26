#!/usr/bin/env python
######################################################################
#  $Id$
#
#  Make the log "FAILED transfers" page.
#
######################################################################

# system imports
import os
import pprint
import string
import sys
import time

# enstore imports
import configuration_client

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

def get_log_dir():
    # get log dir
    config_host = os.getenv('ENSTORE_CONFIG_HOST')
    config_port = os.getenv('ENSTORE_CONFIG_PORT')
    log_dir = None
    if config_host and config_port:
        csc  = configuration_client.ConfigurationClient((config_host,
                                                         int(config_port)))
        log_server = csc.get('log_server')
        if log_server:
            log_dir = log_server.get('log_file_path', None)
    return log_dir

def verify_log_dir(log_dir):
    return (not os.path.exists(log_dir))

#def get_failures(log, log_dir, grepv='GONE|NUL|DSKMV|disk', grep=""):
def get_failures(log, log_dir, grepv="", grep=""):
    #thisnode = os.uname()[1]
    #if len(thisnode) > 2:
    #    gang = thisnode[0:3]
    #else:
    #    gang = ' '
    #if gang == 'd0e':
    #    grepv_ = " DI|"+" DC|"+grepv
    #elif gang == 'stk':
    #    grepv_ = "JDE|"+grepv
    #else:
    #    grepv_ = grepv
    grepv_ = grepv

    # just force the directory.
    failed = cmd('cd %s; egrep "transfer.failed|SYSLOG.Entry" %s /dev/null|grep -v exception |egrep -v "%s" | egrep "%s"' % (log_dir, log, grepv_, grep))
    return failed

def parse_failures(failed):
    Vol = {}
    Drv = {}
    for l in failed:
        syslog_entry = 0
        token = string.split(l,' ')
        if l.find("SYSLOG.Entry") != -1:
            syslog_entry = 1
        token = string.split(l,' ')
        size = len(token)
        thetime = token[0]
        thetime = string.replace(thetime,'LOG-','')
        node = token[1]
        drive = token[5]
        location = ''
        if not syslog_entry:
            location = token[size-1]
            volume = token[size-2]
        else:
            volume = token[size-1] 
        volume = string.replace(volume,'volume=','')
        if syslog_entry:
            reason = l
        else:
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

def print_vols(Vol, fp):
    keys = Vol.keys()
    keys.sort()
    for v in  keys:
        fp.write("%s\n" % (str(v),))  #print v
        info = Vol[v]
        for err in range(0,len(info)):
            error = info[err]
            #print "   %-13s %-10s %20s %s" % (error[3],error[2],error[0],error[5])
            fp.write("   %-13s %-10s %20s %s\n" % \
                     (error[3],error[2],error[0],error[5]))

def print_drv(Drv, fp):
    keys = Drv.keys()
    keys.sort()
    for d in keys:
        fp.write("%s\n" % (str(d),))  #print d
        info = Drv[d]
        for err in range(0,len(info)):
            error = info[err]
            #print "   %-13s %-10s %20s %s" % (error[3],error[4],error[0],error[5])
            fp.write("   %-13s %-10s %20s %s\n" % \
                     (error[3],error[4],error[0],error[5]))

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

    log_dir = get_log_dir()
    if log_dir == None:
        sys.stderr.write("Unable to obtain log directory.\n")
        sys.exit(1)
    if verify_log_dir(log_dir):
        sys.stderr.write("Unable to find log directory.\n")
        sys.exit(1)
        
    failures = get_failures(logfile, log_dir)
    
    Vol,Drv = parse_failures(failures)

    #Obtain the output filename.   Use a temporary file to hold the
    # output.  Then swap it in for the real file at the end.
    failed_filename = os.path.join(log_dir, "transfer_failed.txt")
    temp_filename = "%s.temp" % (failed_filename)
    temp_fp = open(temp_filename, "w")

    #Output the header.
    temp_fp.write("%s: Failed Transfers Report\n" % (time.ctime(now),))
    temp_fp.write("Brought to You by: %s\n" % (os.path.basename(sys.argv[0]),))
    temp_fp.write("\n" + "-" * 80 + "\n\n")

    #Output the volume failures.
    print_vols(Vol, temp_fp)

    #Output a seperator.
    temp_fp.write("\n" + "-" * 80 + "\n\n")

    #Output the drive failures.
    print_drv(Drv, temp_fp)
