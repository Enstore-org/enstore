#!/usr/bin/env python
#
# $Id$
#

import sys
import os
import string
import time
import pprint

mail_victims = "bakken@fnal.gov,cgw@fnal.gov,moibenko@fnal.gov"

config = eval(os.popen("enstore config --show",'r').read())

def hms(s):
    s = int(s)
    m = s/60
    s = s-m*60
    h = m/60
    m = m-60*h
    if h:
        return "%2d:%02d:%02d" % (h,m,s)
    elif m:
        return "%2d:%02d" % (m,s)
    else:
        return "%2d" % (s,)
    
def endswith(a,b):
    return a[-len(b):]==b

def startswith(a,b):
    return a[:len(b)]==b

def ssh(host, cmd):
    ssh_cmd = "ssh -n %s '%s'" % (host, cmd)
    print ssh_cmd
    p = os.popen(ssh_cmd, 'r')
    r = p.read()
    return r

reset_times = {}

def getps(mover):
    conf = config[mover]
    host = conf['host']
    text= ssh(host,"ps axw|grep python|grep -v grep|grep %s"%(mover,))
    ret = []
    for line in string.split(text,'\n'):
        line = string.strip(line)
        if line:
            ret.append(line)
    return ret

def reset(mover, reason=None):
    now = time.time()
    last_reset = reset_times.get(mover,0)
    if now-last_reset < 600:
        print "Not resetting", mover, "more than once per 10 minutes"
        return
    if reason:
        sendmail("resetting mover %s"%mover, reason=reason)
    reset_times[mover]=now
    err = stop(mover)
    if err:
        reboot(mover)
    else:
        start(mover)

def reboot(mover):
    conf = config[mover]
    host = conf['host']
    movers = get_movers()
    need_drain = []
    for other in movers:
        if other==mover:
            continue
        otherhost = config[other]['host']
        if otherhost == host:
            need_drain.append(other)
    draining = []
    for other in need_drain:
        print "Must drain", other
        d = get_status(other)
        state = d.get('state')
        print other,'\t', state
        if state in ('ERROR', 'OFFLINE'):
            continue
        else:
            print ssh(host,'. /usr/local/etc/setups.sh; setup enstore; enstore mov --start-draining=1 %s' % (other,))
            print ssh(host, "rm /tmp/enstore/root/mover_lock")
            draining.append(other)

    time.sleep(1)
    retry = 120
    while draining and retry>0:
        time.sleep(15)
        still_draining = []
        for other in draining:
            d = get_status(other)
            state = d.get('state')
            print other, state
            if state == 'DRAINING':
                still_draining.append(other)
        if still_draining:
            print "still draining", still_draining
        draining = still_draining
        retry = retry-1
    print ssh(host, "rm -f /tmp/enstore/root/mover_lock")
    print ssh(host, "/sbin/shutdown -r now")
    time.sleep(30)

def sendmail(subject, reason):
    mail_cmd = '/bin/mail -s "%s" %s'%(subject,mail_victims)
    p=os.popen(mail_cmd, 'w')
    p.write('reason: %s\n' % (reason,))
    p.write('\n.\n')
    p.close()
    
def start(mover, reason=None):
    conf = config[mover]
    host = conf['host']
    print ssh(host, "rm -f /tmp/enstore/root/mover_lock")
    print ssh(host, '. /usr/local/etc/setups.sh; setup enstore; enstore Estart %s "--just %s"' % (host, mover))
    if reason:
        sendmail("mover %s has been started"%mover, reason=reason)
        
def stop(mover):
    conf = config[mover]
    host = conf['host']
    
    for retry in 0,1:
        lines=getps(mover)
        if not lines:
            print mover, "is not running"
            return 0
        for line in lines:
            words = string.split(line)
            if words and words[0]:
                pid = words[0]
                state = '?'
                if len(words)>2:
                    state = words[2]
                print pid, '\t', state
                if retry==0:
                    print ssh(host, "kill %s" % (pid,))
                else:
                    print ssh(host, "kill -9 %s" % (pid,))
        time.sleep(10)
        
    lines = getps(mover)
    if not lines:
        print mover, "killed"
        return 0
    for line in lines:
        words = string.split(line)
        if words and words[0]:
            pid = words[0]
            state = '?'
            if len(words)>2:
                state = words[2]
            print pid, '\t', state
    return -1

def check(mover):
    print "%-30s"%(mover,),
    sys.stdout.flush()
    d = get_status(mover)
    state = d.get('state')
    time_in_state = d.get('time_in_state')
    status = d.get('status')
    print "\t%-30s\t%-20s"%(status, state), 
    if time_in_state:
        print '\t%10s' % hms(time_in_state)
##      if state in ['SEEK', 'MOUNT_WAIT', 'DISMOUNT_WAIT'] and int(time_in_state)>1200:
        if state in ['SEEK'] and int(time_in_state)>1200:
            return -1, "Mover in state %s for %s" % (state, hms(time_in_state))
    else:
        print


    if state=='ERROR':
        print mover,'\t', status
        return -1,  "Mover in ERROR state.\n\nFull status: %s" % pprint.pformat(d)
    elif status[0]=='TIMEDOUT':
        return -2, "Status request timed out"
    elif status  != ("ok", None):
        return -3, "Status request returned %s.\n\nFull status: %s" % status, pprint.pformat(d)
    else:
        return 0, None
    
def get_status(mover):    
    p = os.popen("enstore mov --status --retries=1 %s" % mover)
    r = p.read()
    s = p.close()
    l = string.split(r,'\n')
    e, l = l[0], l[1:]
    while l and startswith(l[0],' '):
        e=e+l[0]
        l=l[1:]
    d=eval(e)
    return d

    
def get_movers():
    movers = filter(lambda s: endswith(s, '.mover'), config.keys())
    movers.sort()
    return movers

def main(reset_on_error=0):
    movers = get_movers()
    while 1:
        print time.ctime(time.time())
        all_ok = 1
        for mover in movers:
            noreset = 1
            err, reason = check(mover)
            if err:
                all_ok=0
            if err == -1: #ERROR state
                if reset_on_error:
                    noreset = 0
                    reset(mover, reason)
            elif err < -1: #Some other error - timeout?
                #Check to see if there's a single process in D state
                lines = getps(mover)
                if lines:
                    for line in lines:
                        print line
                if len(lines)==1:
                    words = string.split(lines[0])
                    if len(words)>2 and words[2]=='D':
                        print mover, lines[0]
                        if reset_on_error:
                            noreset = 0
                            reset(mover,reason="Uninterruptible I/O wait.\nProcess status: %s" % lines[0])
                if len(lines)==0:
                    if reset_on_error:
                        noreset = 0
                        start(mover,reason="No mover process was running")
            if err and noreset:
                print "error on", mover, "not resetting"
        if all_ok:
            print "All Movers OK"
        print "Sleeping"
        time.sleep(60)
    
if __name__=='__main__':
    main(reset_on_error=1)
    

