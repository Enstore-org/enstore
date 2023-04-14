#!/usr/bin/env python
#
# $Id$
#

import sys
import os
import string
import time
import pprint
from en_eval import en_eval
import getopt

mail_victims = os.environ.get("ENSTORE_MAIL", "enstore-auto@fnal.gov")
config = en_eval(os.popen("enstore config --show",'r').read())

prog = sys.argv[0]
thishost = os.uname()[1]

def tod():
    return time.ctime(time.time())

def sleep(sleep_time):
    print "Sleeping for",sleep_time,"seconds",tod()
    time.sleep(sleep_time)
    return 

class Logfile:
    def __init__(self, filename):
        self.outfile = open(filename, 'a+')
        now = tod()
        self.outfile.write("mover-nanny starting at %s\n" % (now,))
        self.outfile.flush()
    def close(self):
        self.outfile.close()
    def write(self, stuff):
        self.outfile.write(stuff)
        self.outfile.flush()
    def flush(self):
        self.outfile.flush()
        
_stdout = sys.stdout
_stderr = sys.stderr

sys.stdout = Logfile("/home/enstore/mover-nanny.log")
sys.stderr = sys.stdout

def sendmail(subject, reason):
    # I know the hardware doesn't work.  Disable all mail till it does.
    # disable all mail 11/16/00 J Bakken
    print tod(), subject, reason
    return
    mail_cmd = '/bin/mail -s "%s" %s'%(subject,mail_victims)
    p=os.popen(mail_cmd, 'w')
    p.write('reason: %s\n' % (reason,))
    p.write('\n\n')
    p.write("This message sent at %s by %s running on %s\n\n" %
            (tod(), prog, thishost))
    p.close()

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

def is_mover(s):
    return endswith(s, '.mover')

def get_movers(null_mv=0):
    print 'Getting configuration'
    config = en_eval(os.popen("enstore config --show",'r').read())
    mv = filter(is_mover, config.keys())
    if null_mv != 0:
        movers = mv
    else:
        movers = []
        for m in mv:
            if m.find("null") == -1:
                movers.append(m)
    movers.sort()
    print 'Found movers:',pprint.pprint(movers)
    return movers

def ssh(host, cmd):
    ssh_cmd = "enrsh -n %s '%s'" % (host, cmd)
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
    pprint.pprint(ret)
    return ret

def get_sched():
    print 'Getting schedule',tod()
    sched_dict = {}
    key = None
    p = os.popen('enstore sched --show','r')
    lines = p.read()
    s = p.close()
    lines = string.split(lines,'\n')
    for line in lines:
        words = string.split(line)
        if not words:
            continue
        if words[0]=='Enstore':
            if len(words)<3:
                continue
            key = string.lower(words[2])
        elif words[0][0]=='-':
            continue
        else:
            if key:
                sched_dict[key] = sched_dict.get(key,[]) + [words[0]]
    pprint.pprint(sched_dict)
    return sched_dict

def get_status(mover):    
    p = os.popen("enstore mov --status --timeout=120 --retries=1 %s" % mover,'r')
    r = p.read()
    s = p.close()
    l = string.split(r,'\n')
    e, l = l[0], l[1:]
    while l and startswith(l[0],' '):
        e=e+l[0]
        l=l[1:]
    d={}
    try:
        d=en_eval(e)
    except:
        pass
    return d

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
            p = os.popen('enstore mov --start-draining=1 %s' % (other,),'r')
            print p.read()
            s = p.close()
            print ssh(host, "rm /tmp/enstore/root/mover_lock%s"%(other,))
            draining.append(other)

    time.sleep(1)
    retry = 120
    while draining and retry>0:
        sleep(15)
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
    sleep(30)

    
def start(mover, reason=None):
    conf = config[mover]
    host = conf['host']
    print ssh(host, "rm -f /tmp/enstore/root/mover_lock")
    p = os.popen('enstore Estart %s "--just %s"' % (host, mover),'r')
    print p.read()
    s = p.close()
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
        sleep(10)
        
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
        if int(time_in_state)>1200:
            if state not in ['IDLE', 'ACTIVE', 'OFFLINE','HAVE_BOUND']:
                return -1, "Mover in state %s for %s" % (state, hms(time_in_state))
        if int(time_in_state)>7200 and state == 'ACTIVE':
            return -1, "Mover in state %s for %s" % (state, hms(time_in_state))
    else:
        print

    if state=='ERROR':
        print mover,'\t', status
        return -1,  "Mover in ERROR state.\n\nFull status: %s" % pprint.pformat(d)
    elif status and status[0]=='TIMEDOUT':
        return -2, "Status request timed out"
    elif status  != ("ok", None):
        return -3, "Status request returned %s.\n\nFull status: %s" % (status, pprint.pformat(d))
    else:
        return 0, None
    

def main(reset_on_error=0, check_null=0):
    strikes = {}
    while 1:
        movers = get_movers(check_null)
        print tod()
        scheduled = get_sched()
        known_down = scheduled.get('known',[])
        all_ok = 1
        for mover in movers:
            if mover in known_down:
                print "%-30s"%(mover,),
                print  "\tknown down"
                continue
            noreset = 1
            err, reason = check(mover)
            if err == 0:
                strikes[mover]=0
            if err:
                all_ok=0
            if err == -1: #ERROR state
                if reset_on_error:
                    noreset = 0
                    strikes[mover]=0
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
                            strikes[mover]=0
                            reset(mover,reason="Uninterruptible I/O wait.\nProcess status: %s" % lines[0])
                if len(lines)==0:
                    if reset_on_error:
                        noreset = 0
                        strikes[mover]=0
                        start(mover,reason="No mover process was running")
            
            if err and noreset:
                n_strikes = strikes.get(mover,0) + 1
                strikes[mover] = n_strikes
                if n_strikes > 3:
                    strikes[mover] = 0
                    reset(mover,reason=
                          "%d sequential errors getting mover status"
                          %n_strikes)
                else:
                    print "error on", mover, "not resetting (%d)"%n_strikes
        sleep(60)
    
def usage():
    print "Usage %s [-h] [--check-null] [--reset-on-error]"%(sys.argv[0],)
    
if __name__=="__main__":
    reset_on_error = 0
    check_null = 0
    opts, args = getopt.getopt(sys.argv[1:], "h", ["check-null","reset-on-error"])
    for o,a in opts:
        if o == "-h":
            usage()
        if o == "--check-null":
            check_null = 1
        if o == "--reset-on-error":
           reset_on_error = 1
        
    
    main(reset_on_error, check_null)
