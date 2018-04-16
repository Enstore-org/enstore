#!/usr/bin/env python

import socket
import select
import errno
import os
import sys
import time
import getopt
import string
import shutil
import configuration_client
import log_client
import udp_client
import library_manager_client
import e_errors
import enstore_functions2
import Trace
import enstore_mail

interval = 30
event_dict = {}
time_for_record = time.time()
MY_NAME = "LM_NANNY"
RETRY_ATTEMPTS = 5
RETRY_TO = 20
DEBUG_LOG = 11

def print_help():
    print """
    attempt to restart library_managers if they do not respond on their ports,
    but appear to be running
    library_manager_nanny.py [OPTIONS] [library_manager1[ library_manager2]....
    OPTIONS:
    -t, --time interval - monitoring inteval in seconds (default: 30s)
    -h, --help - print help
    -m, --mail - mail recipient
    -r, --restart - restart library manager unconditionally.
                    Without this option library manager restart only during off hours.
    -d, --debug <level1,[level2,....]> - turn on debug levels
    if library managers are not specified they will be taken from the configuration
    """


def record_event(library_manager, event):
    # library_manager - library manager for which to record event
    # event - NOT_RUNNING, RESTARTED, None
    global event_dict
    global time_for_record

    if not event_dict.has_key(library_manager):
        event_dict[library_manager] = {'NOT_RUNNING': 0,
                                       'RESTARTED':   0,
                                       }
    # update event count
    event_dict[library_manager][event] = event_dict[library_manager][event]+1

    # log acquired statistics
    now = time.time()
    if (now - time_for_record) >= 600: # log statistics every 10 min
        time_for_record = now
        Trace.log(e_errors.INFO, "Monitoring Statistics: %s"%(event_dict,))

def port_netstat(port):
    queue = -1
    try:
        cmd = 'netstat -npl 2>/dev/null | grep %s'%(port,)
        l = enstore_functions2.shell_command(cmd)[0]
        tl = ' '.join(l.translate(None, string.whitespace[:5]).split())
        tl.strip()
        if "udp" in tl:
            a=tl.split()
            queue = a[1]
    except:
        pass
    return queue


def get_netstat(lm_port, encp_port, mover_port):
    r_queue = port_netstat(lm_port) if lm_port else 0
    e_queue = port_netstat(encp_port) if encp_port else 0
    m_queue = port_netstat(mover_port) if mover_port else 0
    cmd = 'netstat -s 2>/dev/null | grep "packet receive errors"'
    result = enstore_functions2.shell_command(cmd)[0]
    r = " ".join(result.translate(None, string.whitespace[:5]).split())
    r_err = long(r.split()[0]) if "errors" in result else 0
    return r_queue, e_queue, m_queue, r_err

class LMC(library_manager_client.LibraryManagerClient):
    def __init__(self, csc, library_manager_name):
        library_manager_client.LibraryManagerClient.__init__(self, csc, library_manager_name)
        self.lm_configuration = csc.get(library_manager_name)
        self.host = self.lm_configuration.get("host")
        self.control_port = self.lm_configuration.get("port")
        self.encp_port = self.lm_configuration.get("encp_port")
        self.mover_port = self.lm_configuration.get("mover_port")
        self.udpc = udp_client.UDPClient()

    def is_monitored(self):
        return not ("noupdown" in self.lm_configuration)

    # check on alive status
    def alive(self, address, rcv_timeout=RETRY_ATTEMPTS, tries=RETRY_TO):
        #Send and recieve the alive message.
        try:
            x = self.udpc.send({'work':'alive'}, address,
                               rcv_timeout, tries)
        except (socket.error, select.error, e_errors.EnstoreError), msg:
            if hasattr(msg, "errno") and msg.errno == errno.ETIMEDOUT:
                x = {'status' : (e_errors.TIMEDOUT, None)}
            else:
                x = {'status' : (e_errors.BROKEN, str(msg))}
        except errno.errorcode[errno.ETIMEDOUT]:
            x = {'status' : (e_errors.TIMEDOUT, None)}
        return x

    def ping_lm_port(self, port):
        """
        Ping library manager port
        """
        rv = 0
        if port:
            rc = self.alive((self.host, port))
            if rc['status'][0] == e_errors.TIMEDOUT:
                # retry to diminish false alarms
                time.sleep(10)
                rc = self.alive((self.host, port))
                if rc['status'][0] == e_errors.TIMEDOUT:
                    rv = 1
        return rv

    def is_lm_running(self):
        """
        Check if library manager programm is running.
        Note that library manager is multiprocessed, multithreaded
        """
        result = None
        try:
            grep_cmd = "enstore EPS %s | fgrep %s | fgrep -v fgrep | fgrep -v %s"%(self.host.split('.')[0], self.server_name, __file__.split('/')[1])
            res = enstore_functions2.shell_command(grep_cmd)
            if res:
                procs = []
                for line in res[0].split("\n"):
                    if line:
                        # line looks as:
                        # enstore  32266  0.0  0.3 171172 32481    2 pts/0    Sl   14:25 00:00:00 python /opt/enstore/sbin/library_manager LTO4GST.library_manager
                        tline = ' '.join(line.translate(None, string.whitespace[:5]).split()) # remove extra whitespaces
                        proc = tline.split()[1]
                        if not proc in procs:
                            procs.append(proc)
                if procs:
                    result = len(procs), procs
        except:
            Trace.handle_error()
            pass
        return result

    def kill_lm(self, procs_to_kill):
        killed = True
        procs = procs_to_kill
        proc_cnt = len(procs)
        while proc_cnt:
            Trace.log(e_errors.INFO, "Killing %s processes. Processes to kill %s"%(self.server_name, len(procs)))
            killcmd = "rgang %s 'kill %s'"%(self.host.split('.')[0], " ".join(procs))
            res = enstore_functions2.shell_command(killcmd)
            time.sleep(10)
            res = self.is_lm_running()
            if res:
                new_cnt = res[0]
                procs = res[1]
                if new_cnt >= proc_cnt:
                    killed = False
                    break
                else:
                   proc_cnt = new_cnt
            else:
                break
        return killed


    def restart(self, debug_levels=None):
        Trace.log(e_errors.INFO, "Will try to restart %s library manager"%(self.server_name, ))

        command = 'enstore Estop %s "--just %s"'%(self.host.split('.')[0], self.server_name)
        res = enstore_functions2.shell_command(command)
        # check that lm stopped
        time.sleep(10)
        # check that LM is actually stopped
        rc = self.is_lm_running()
        if rc:
            # LM processes are still running, try to kill them.
            Trace.log(e_errors.INFO, "%s did not stop. Will try to kill"%(self.server_name, ))
            if not self.kill_lm(rc[1]):
                Trace.alarm(e_errors.ERROR, "Failed to kill %s library manager"%(self.server_name, ))
                return
        # no lm processes are running
        command = 'enstore Estart %s "--just %s"'%(self.host.split('.')[0], self.server_name)
        res = enstore_functions2.shell_command(command)
        time.sleep(10)
        if self.is_lm_running():
            Trace.alarm(e_errors.INFO, "Successfully restarted %s library manager"%(self.server_name, ))
            if debug_levels:
                command = 'enstore lib --do-print %s %s'%(debug_levels,self.server_name)
                res = enstore_functions2.shell_command(command)

            src = '/var/log/enstore/tmp/enstore/%s.out.sav'%(self.server_name,)
            dst = '%s.%s'%(src, time.strftime('%Y-%m_%d_%H:%M:%S', time.localtime()))
            record_event(lmc.server_name, "RESTARTED")
            shutil.copy(src, dst)
        else:
            Trace.alarm(e_errors.ERROR, "Failed to restart %s library manager"%(self.server_name, ))




mail_recipient = os.environ.get("ENSTORE_MAIL", None)
prog_name = sys.argv[0].split('/')[-1]
restart = False
levels = None
opts, args = getopt.getopt(sys.argv[1:], "d:t:h:r", ["debug", "timeout", "help", "restart"])
for o, a in opts:
    if o in ["-t", "--time"]:
        interval = int(a)
    if o in ["-m", "--mail"]:
       mail_recipient = a
    if o in ["-d", "--debug"]:
        levels = a
    if o in ["-h", "--help"]:
        print_help()
        sys.exit(0)
    if o in ["-r", "--restart"]:
        restart = True

if not mail_recipient:
    print "Please specify mail recipient"
    sys.exit(1)

csc = configuration_client.ConfigurationClient((os.environ['ENSTORE_CONFIG_HOST'],
                                                int(os.environ['ENSTORE_CONFIG_PORT'])))
logc = log_client.LoggerClient(csc, MY_NAME)
Trace.init(MY_NAME)

lm_list_0 = csc.get_library_managers()

# get library managers from stdin
lm_list = []
for k in lm_list_0:
    l = lm_list_0[k]
    # find list entry corresponding to library manager
    # from the argument list
    for lm in args:
        if l['name'] == lm:
            lm_list.append(lm)
            # found lm, can break here
            break
# create library manager clients
lmc_list = []
for lm in lm_list:
    lmc_list.append(LMC(csc, lm))

try:
    while True:
        for lmc in lmc_list:
            if not lmc.is_monitored():
                # we do not care about library managers that are not
                # watched
                continue

            # Check whether library manager is running.
            if not lmc.is_lm_running():
                continue  # library manager is not running, we do not care: why

            # get current queue length
            ql = lmc.get_pending_queue_length(timeout=10)
            Trace.log(DEBUG_LOG, "LM %s pending_queue_length returned %s"%(lmc.server_name, ql,))

            # show netstats
            control_buf, encp_buf, mover_buf, udp_errors =  get_netstat(lmc.control_port,
                                                                        lmc.encp_port,
                                                                        lmc.mover_port)

            Trace.log(DEBUG_LOG, "net stats: CB %s ENCPB %s MOVB %s ERR %s"%(control_buf,
                                                                             encp_buf,
                                                                             mover_buf,
                                                                             udp_errors))

            # Number of LM ports
            # can be 1 or 3.
            # If it is 1 the library manager does not have
            # separate threads to serve requests.
            # If it is 3 the library manager has
            # separate threads to serve requests coming
            # from encp, movers, and other client.
            # We need to know how many ports the
            # LM has and how many of these ports do not respond
            not_responding_ports = 0

            rc = lmc.ping_lm_port(lmc.control_port)
            if rc:
                Trace.log(e_errors.ERROR, "Library manager %s is not responding on %s %s"%
                          (lmc.server_name, lmc.host, lmc.control_port))
            not_responding_ports = not_responding_ports + rc

            rc = lmc.ping_lm_port(lmc.mover_port)
            if rc:
                Trace.log(e_errors.ERROR, "Library manager %s is not responding on %s mover port %s"%
                          (lmc.server_name, lmc.host, lmc.mover_port))
            not_responding_ports = not_responding_ports + rc

            rc = lmc.ping_lm_port(lmc.encp_port)
            if rc:
                Trace.log(e_errors.ERROR, "Library manager %s is not responding on %s encp port %s"%
                          (lmc.server_name, lmc.host, lmc.encp_port))
            not_responding_ports = not_responding_ports + rc

            # library manager is running and hanging
            if not_responding_ports > 0:
                record_event(lmc.server_name, "NOT_RUNNING")

                # Restart library manager on weekdays (Mon - Fri) after work hours
                # and on weekend.
                # Otherwise send e-mail to developer
                t = time.localtime()
                if (t.tm_wday in (5,6) or # weekend
                    (t.tm_hour not in xrange(8, 17)) or # weekday before 8:00am or after 5:00pm
                    (restart)): # restart unconditionally
                    # restart LM
                    lmc.restart(levels)
                else: # weekdays between 8:00 and 17:00
                    Trace.alarm(e_errors.INFO, "Library manager %s does not get restarted during work hours"%(lmc.server_name, ))
                    enstore_mail.send_mail(MY_NAME, "Library manager %s is not responding."%(lmc.server_name,),
                                           "Library manager %s is not responding. Check log file"%(lmc.server_name,), mail_recipient)



        time.sleep(interval)
except KeyboardInterrupt:
    Trace.log(e_errors.INFO, "Monitoring Statistics: %s"%(event_dict,))
except:
    Trace.handle_error()

