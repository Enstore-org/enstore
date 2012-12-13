#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
import socket
import select
import errno
import os
import sys
import time
import getopt

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

def print_help():
    print """
    attempt to restart library_managers if they do not respond on their ports,
    but appear to be running
    library_manager_nanny.py [OPTIONS] [library_manager1[ library_manager2]....
    OPTIONS:
    -t, --time interval - monitoring inteval in seconds (default: 30s) 
    -h, --help - print help
    -m, --mail - mail recepient
    if library managers are not specified they will be taken from the cofiguration
    """


# check on alive status
def alive(address, rcv_timeout=0, tries=0):
    u = udp_client.UDPClient()
    #Send and recieve the alive message.
    try:
        x = u.send({'work':'alive'}, address,
                        rcv_timeout, tries)
    except (socket.error, select.error, e_errors.EnstoreError), msg:
        if hasattr(msg, "errno") and msg.errno == errno.ETIMEDOUT:
            x = {'status' : (e_errors.TIMEDOUT, None)}
        else:
            x = {'status' : (e_errors.BROKEN, str(msg))}
    except errno.errorcode[errno.ETIMEDOUT]:
        Trace.trace(14,"alive - ERROR, alive timed out")
        x = {'status' : (e_errors.TIMEDOUT, None)}
    return x

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
    
    

mail_recepient = os.environ.get("ENSTORE_MAIL", None)
prog_name = sys.argv[0].split('/')[-1]
opts, args = getopt.getopt(sys.argv[1:], "t:h", ["timeout", "help"])
for o, a in opts:
    if o in ["-t", "--time"]:
        interval = int(a)
    elif o in ["-m", "--mail"]:
       mail_recepient = a 
    elif o in ["-h", "--help"]:
        print_help()
        sys.exit(0)

if not mail_recepient:
    print "Please specify mail recepient"
    sys.exit(1)
    
csc = configuration_client.ConfigurationClient((os.environ['ENSTORE_CONFIG_HOST'],
                                                int(os.environ['ENSTORE_CONFIG_PORT'])))
logc = log_client.LoggerClient(csc, MY_NAME)
Trace.init(MY_NAME)

lm_list_0 = csc.get_library_managers2()

if len(args) > 1:
    # get library managers from stdin
    lm_list = []
    for l in lm_list_0:
        # find list entry corresponding to library manager
        # from the argument list
        for lm in args:
            if l['name'] == lm:
                lm_list.append(l)
                # found lm, can break here
                break
else:
   lm_list = lm_list_0
try:
    while True:
        for lm in lm_list:
            if lm.has_key("noupdown"):
                # we do not care about library managers that are not
                # watched
                continue
            host = lm.get("host", None)
            if not host:
                continue
            # Check whether library manager is running.
            result = None
            grep_cmd = "enstore EPS %s | fgrep %s | fgrep -v fgrep | fgrep -v %s | wc -l "%(host.split('.')[0], lm['name'], prog_name)
            res = enstore_functions2.shell_command(grep_cmd)
            if res:
                result = int(res[0]) # stdout
            if not result:
                continue  # library manager is not running, we do not care: why 
            # Number of LM ports
            # can be 1 or 3.
            # If it is 1 the library manager does not have
            # separate threads to serve requests.
            # If it is 3 the library manager has
            # separate threads to serve requests coming
            # from encp, movers, and other client.
            # We need need to know how many ports the
            # LM has and how many of these ports do not respond
            not_responding_ports = 0

            port = lm.get("port", None)
            if port:
                port = int(port)
                rc = alive((host, port), 10, 5)
                if rc['status'][0] == e_errors.TIMEDOUT:
                    # retry to diminish false alarms
                    time.sleep(10)
                    rc = alive((host, port), 10, 5)
                    if rc['status'][0] == e_errors.TIMEDOUT:
                        Trace.log(e_errors.ERROR, "Library manager %s is not responding on %s %s"%(lm['name'], host, port))
                        not_responding_ports = not_responding_ports + 1

            port = lm.get("mover_port", None)
            if port:
                port = int(port)
                rc = alive((host, port), 10, 5)
                if rc['status'][0] == e_errors.TIMEDOUT:
                    # retry to diminish false alarms
                    time.sleep(10)
                    rc = alive((host, port), 10, 5)
                    if rc['status'][0] == e_errors.TIMEDOUT:
                        Trace.log(e_errors.ERROR, "Library manager %s is not responding on %s mover port %s"%(lm['name'], host, port))
                        not_responding_ports = not_responding_ports + 1

            port = lm.get("encp_port", None)
            if port:
                port = int(port)
                rc = alive((host, port), 10, 5)
                if rc['status'][0] == e_errors.TIMEDOUT:
                    # retry to diminish false alarms
                    time.sleep(10)
                    rc = alive((host, port), 10, 5)
                    if rc['status'][0] == e_errors.TIMEDOUT:
                        Trace.log(e_errors.ERROR, "Library manager %s is not responding on %s encp port %s"%(lm['name'], host, port))
                        not_responding_ports = not_responding_ports + 1

            # library manager is running and hanging
            if not_responding_ports > 0:
                record_event(lm['name'], "NOT_RUNNING")
                # get current queue length
                lmc = library_manager_client.LibraryManagerClient(csc, lm['name'])
                ql = lmc.get_pending_queue_length(timeout=5)
                Trace.log(e_errors.INFO, "pending_queue_length returned %s"%(ql,))
                
                # Restart library manager on weekdays (Mon - Fry) after work hours
                # and on weekend.
                # Otherwise send e-mail to developer
                t = time.localtime()
                if (t[6] in (5,6) or # weekend
                    (t[3] < 8 or t[3] > 17)): # weekday before 8:00am or after 5:00pm
                    # restart LM
                    Trace.log(e_errors.INFO, "Will try to restart %s library manager"%(lm['name'], ))
                    command = 'enstore Estop %s "--just %s"'%(host.split('.')[0], lm['name'])
                    res = enstore_functions2.shell_command(command)
                    # check that lm stopped
                    time.sleep(10)
                    res = enstore_functions2.shell_command(grep_cmd)
                    result = None
                    if res:
                        result = int(res[0])
                        if result == 0:
                            # no lm processes are running
                            command = 'enstore Estart %s "--just %s"'%(host.split('.')[0], lm['name'])
                            res = enstore_functions2.shell_command(command)
                            time.sleep(10)
                            result = None
                            command = "enstore EPS %s | fgrep %s | fgrep -v fgrep | wc -l "%(host.split('.')[0], lm['name'])
                            res = enstore_functions2.shell_command(grep_cmd)
                            if res:
                                result = int(res[0])
                                if result != 0:
                                    Trace.alarm(e_errors.INFO, "Successfully restarted %s Library manager"%(lm['name'], ))
                                    record_event(lm['name'], "RESTARTED")

                        else:
                            command = "enstore EPS %s | fgrep %s | fgrep -v fgrep"%(host.split('.')[0], lm['name'])
                            res = enstore_functions2.shell_command(command)

                    if not result:
                        Trace.alarm(e_errors.ERROR, "Failed to restart of %s Library manager"%(lm['name'], ))
                else: # weekdays between 8:00 and 17:00
                    Trace.alarm(e_errors.INFO, "Library manager %s does not get restarted during work hours"%(lm['name'], ))
                    enstore_mail.send_mail(MY_NAME, "Library manager %s is not responding on %s %s"%(lm['name'], host, port),
                                           "Library manager %s is not responding"%(lm['name'],), mail_recepient)
                    


        time.sleep(interval)
except KeyboardInterrupt:
    Trace.log(e_errors.INFO, "Monitoring Statistics: %s"%(event_dict,))
except:
    Trace.handle_error()

