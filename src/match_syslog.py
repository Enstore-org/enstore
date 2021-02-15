#!/usr/bin/env python
from __future__ import print_function
import Trace
import sys
import popen2
import time
import os

syslog = "/var/log/messages"


def match_syslog(match):
    # if self.watch_syslog:
    now = time.time()
    try:
        stats = os.stat(syslog)
    except BaseException:
        # file does not exist
        return

    syslog_updated = stats[9]  # time when syslog has been updated
    if now - syslog_updated > 300.:
        return
    year = time.localtime(now)[0]
    cmd = 'tail %s | egrep "%s"' % (syslog, match)
    #sys.stderr.write("command %s"%(cmd,))
    pipeObj = popen2.Popen3(cmd, 0, 0)
    if pipeObj is None:
        return
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()  # result has returned string
    for line in result:
        outline = line[:-1]
        # extract time from syslog
        s_tuple = outline.split()
        # convert time from syslog to seconds
        human_time = "%s %s %s %s" % (year, s_tuple[0], s_tuple[1], s_tuple[2])
        t_tuple = time.strptime(human_time, "%Y %b %d %H:%M:%S")
        sys_log_time = time.mktime(t_tuple)
        if now - sys_log_time <= 6000.:
            # this line perhaps is related to the event
            print(outline)


# usage for this command is match_syslog.py <string_to_match>
match_syslog(sys.argv[1])
