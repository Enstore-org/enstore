#!/usr/bin/env python
######################################################################
#  $Id$
#
#  Make the log "FAILED transfers" page.
#
######################################################################

# system imports
from __future__ import print_function
import os
#import pprint
import string
import sys
import time
import socket

# enstore imports
import configuration_client


def cmd(command):
    print(command)
    p = os.popen(command, 'r')
    text = p.read()
    p.close()
    lines = []
    for line in string.split(text, '\n'):
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
        csc = configuration_client.ConfigurationClient((config_host,
                                                        int(config_port)))
        log_server = csc.get('log_server')
        if log_server:
            log_dir = log_server.get('log_file_path', None)
    return log_dir


def get_html_dir():
    # get html dir
    config_host = os.getenv('ENSTORE_CONFIG_HOST')
    config_port = os.getenv('ENSTORE_CONFIG_PORT')
    html_dir = None
    if config_host and config_port:
        csc = configuration_client.ConfigurationClient((config_host,
                                                        int(config_port)))
        crons = csc.get('crons')
        if crons:
            html_dir = crons.get('html_dir', None)
    return html_dir


def get_web_server():
    # get web server hostname
    config_host = os.getenv('ENSTORE_CONFIG_HOST')
    config_port = os.getenv('ENSTORE_CONFIG_PORT')
    web_server_host = None
    if config_host and config_port:
        csc = configuration_client.ConfigurationClient((config_host,
                                                        int(config_port)))
        web_server = csc.get('web_server')
        if web_server:
            web_server_host = web_server.get('ServerHost', None)
    return web_server_host


def verify_log_dir(log_dir):
    return (not os.path.exists(log_dir))


def copy_it(src, dst):
    fp_r = open(src, "r")
    fp_w = open(dst, "w")

    data = fp_r.readlines()
    fp_w.writelines(data)

    fp_r.close()
    fp_w.close()

# def get_failures(log, log_dir, grepv='GONE|NUL|DSKMV|disk', grep=""):


def get_failures(log, log_dir, grepv="", grep=""):
    #thisnode = os.uname()[1]
    # if len(thisnode) > 2:
    #    gang = thisnode[0:3]
    # else:
    #    gang = ' '
    # if gang == 'd0e':
    #    grepv_ = " DI|"+" DC|"+grepv
    # elif gang == 'stk':
    #    grepv_ = "JDE|"+grepv
    # else:
    #    grepv_ = grepv
    grepv_ = grepv

    # just force the directory.
    ###failed = cmd('cd %s; egrep "transfer.failed|SYSLOG.Entry" %s /dev/null|grep -v exception |egrep -v "%s" | egrep "%s"' % (log_dir, log, grepv_, grep))
    failed = cmd(
        'cd %s; egrep "transfer.failed|SYSLOG.Entry" %s /dev/null|grep -v exception' %
        (log_dir, log))
    return failed


def parse_failures(failed):
    Vol = {}
    Drv = {}
    for l in failed:
        syslog_entry = 0
        token = string.split(l, ' ')
        if l.find("SYSLOG.Entry") != -1:
            syslog_entry = 1
        token = string.split(l, ' ')
        size = len(token)
        thetime = token[0]
        thetime = string.replace(thetime, 'LOG-', '')
        node = token[1]
        drive = token[5]
        location = ''
        if not syslog_entry:
            location = token[size - 1]
            volume = token[size - 2]
        else:
            volume = token[size - 1]
        volume = string.replace(volume, 'volume=', '')
        if syslog_entry:
            reason = l
        else:
            reason = string.join(token[6:size - 2])
        error = [thetime, node, drive, location, volume, reason]
        if volume in Vol:
            Vol[volume].append(error)
        else:
            Vol[volume] = [error, ]
        if drive in Drv:
            Drv[drive].append(error)
        else:
            Drv[drive] = [error, ]
    return (Vol, Drv)


def print_vols(Vol, fp):
    keys = sorted(Vol.keys())
    for v in keys:
        fp.write("%s\n" % (str(v),))  # print v
        info = Vol[v]
        for err in range(0, len(info)):
            error = info[err]
            #print "   %-13s %-10s %20s %s" % (error[3],error[2],error[0],error[5])
            fp.write("   %-13s %-10s %20s %s\n" %
                     (error[3], error[2], error[0], error[5]))


def print_drv(Drv, fp):
    keys = sorted(Drv.keys())
    for d in keys:
        fp.write("%s\n" % (str(d),))  # print d
        info = Drv[d]
        for err in range(0, len(info)):
            error = info[err]
            #print "   %-13s %-10s %20s %s" % (error[3],error[4],error[0],error[5])
            fp.write("   %-13s %-10s %20s %s\n" %
                     (error[3], error[4], error[0], error[5]))


def make_failed_page(Vol, Drv, out_file_fp):

    # Output the header.
    out_file_fp.write("%s: Failed Transfers Report\n" % (time.ctime(now),))
    out_file_fp.write("Brought to You by: %s\n" %
                      (os.path.basename(sys.argv[0]),))
    out_file_fp.write("\n" + "-" * 80 + "\n\n")

    # Output the volume failures.
    print_vols(Vol, out_file_fp)

    # Output a seperator.
    out_file_fp.write("\n" + "-" * 80 + "\n\n")

    # Output the drive failures.
    print_drv(Drv, out_file_fp)


def logname(t):
    t_tup = time.localtime(t)
    return "LOG-%4.4i-%2.2i-%2.2i" % (t_tup[0], t_tup[1], t_tup[2])


if __name__ == "__main__":
    now = time.time()
    today = time.asctime(time.localtime(now))[0:10]
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = "today"

    if choice == "today":
        logfile = logname(now)
    elif choice == "week":
        logfile = ""
        for day in range(6, -1, -1):
            logfile = logfile + " " + logname(now - day * 86400)
    elif choice == "month":
        logfile = ""
        for day in range(30, -1, -1):
            logfile = logfile + " " + logname(now - day * 86400)
    else:
        logfile = choice

    log_dir = get_log_dir()
    if log_dir is None:
        sys.stderr.write("Unable to obtain log directory.\n")
        sys.exit(1)
    if verify_log_dir(log_dir):
        sys.stderr.write("Unable to find log directory.\n")
        sys.exit(1)

    html_dir = get_html_dir()
    if not html_dir:
        sys.stderr.write("Unable to obtain html directory.\n")
        sys.exit(1)

    web_server = get_web_server()
    if not html_dir:
        sys.stderr.write("Unable to obtain web server hostname.\n")
        sys.exit(1)

    failures = get_failures(logfile, log_dir)

    Vol, Drv = parse_failures(failures)

    # Obtain the output filename.   Use a temporary file to hold the
    # output.  Then swap it in for the real file at the end.
    fname = "transfer_failed.txt"
    failed_filename = os.path.join(html_dir, fname)
    #temp_filename = "%s.temp" % (failed_filename)
    temp_filename = os.path.join("/tmp/", fname)
    temp_fp = open(temp_filename, "w")

    make_failed_page(Vol, Drv, temp_fp)

    temp_fp.close()

    if socket.gethostname() == web_server:
        # Do this if the web server and log server are on the same
        # machine.
        try:
            copy_it(temp_filename, failed_filename)
        except (OSError, IOError) as msg:
            sys.stderr.write("Unable to copy file from %s to %s: %s\n" %
                             (temp_filename, failed_filename, str(msg)))
            sys.exit(1)
    else:
        # Other wise copy it remotely.
        ##
        cmd("enrcp %s %s" % (temp_filename, failed_filename))
        print(cmd)
