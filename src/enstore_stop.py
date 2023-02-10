#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# stop enstore
#
#
#

#Notes: Various functions return 0 for the server is not running an 1 if it is.

# system imports
import sys
import os
import string
import errno
import socket
import signal
import pwd
import time
import select

# enstore imports
import setpath
import e_errors
import enstore_constants
import enstore_functions
import enstore_functions2
import udp_client
import Trace
import generic_client
import option

import configuration_client
import log_client
import event_relay_client
import enstore_start

#Less hidden side effects to call this?  Also, pychecker perfers it.
### What does this give us?
setpath.set_enstore_paths()

MY_NAME = "ENSTORE_STOP"
SEND_TO = 3
SEND_TM = 1

def get_csc():
    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))

    rtn = csc.alive(configuration_client.MY_SERVER, SEND_TO, SEND_TM)

    if e_errors.is_ok(rtn):
        return csc
    
    return None
'''
def this_host():
    rtn = socket.gethostbyname_ex(socket.getfqdn())

    return [rtn[0]] + rtn[1] + rtn[2]

def is_on_host(host):

    if host in this_host():
        return 1

    return 0
'''
def get_temp_file(server_name):
    #Get the pid file information.
    try:
        user_name = pwd.getpwuid(os.geteuid())[0]
    except KeyError:
        user_name = os.geteuid()
    pid_dir = os.path.join(enstore_functions.get_enstore_tmp_dir(),
                           user_name)
    pid_file = os.path.join(pid_dir, server_name + ".pid")

    return pid_file

############################################################################

#Remove the pid file for the server servername.
def remove_pid_file(servername):
    try:
        os.unlink(get_temp_file(servername))
    except:
        exc, msg, tb = sys.exc_info()
        if hasattr(msg, "errno") and msg.errno == errno.ENOENT:
            pass   #file already deleted
        else:
            print "Unlink:", str(msg)

# type should be one of "mover", "library_manager", "media_changer".
def find_servers_by_type(csc, type):

    #Get the configuration dictionary.

    #First try the configuration server.  If that failes and it is the
    # ENSTORE_CONFIG_HOST node, get the configuration dictionary directly
    # from file.
    try:
        if csc:
            config_dict = csc.dump(timeout=SEND_TO, retry=SEND_TM)
            if e_errors.is_ok(config_dict) and config_dict.has_key("dump"):
                #The configuration dictionary is originaly in a sub-ticket.
                # Change this, but remember the status.
                status =  config_dict['status']
                config_dict = config_dict['dump']
                config_dict['status'] = status
        else:
            config_dict = {'status':(e_errors.TIMEDOUT,
                                     enstore_constants.CONFIGURATION_SERVER)}
    except (socket.error, select.error, e_errors.EnstoreError), msg:
        if msg.errno == errno.ETIMEDOUT:
            config_dict = {'status':(e_errors.TIMEDOUT,
                                     enstore_constants.CONFIGURATION_SERVER)}
        else:
            config_dict = {'status':(e_errors.NET_ERROR, str(msg))}
    except errno.errorcode[errno.ETIMEDOUT]:
        config_dict = {'status':(e_errors.TIMEDOUT, None)}
    if not e_errors.is_ok(config_dict) and \
       enstore_start.is_on_host(os.environ.get('ENSTORE_CONFIG_HOST', None)):
        config_dict = enstore_functions.get_config_dict()
        if config_dict:
            config_dict = config_dict.configdict

    #If we do not have a valid dictionary, skip it.
    if config_dict.get('status', None) and not e_errors.is_ok(config_dict):
        return []

    #Pullout the requested info.
    the_list = []
    for item in config_dict.keys():
        if item[-(len(type)):] == type:
            the_list.append(item)

    return the_list

#Detects if a process is still running with a given id.  Returns 1 if the
# process exists, zero if gone.
def detect_process(pid):
    try:
        os.kill(pid, 0)
        return 1
    except OSError, msg:
        ##print msg
        if hasattr(msg, "errno") and msg.errno == errno.EPERM:
            print msg
            return 1
        else:
            return 0

def kill_root_process(pid):
    # check for sudo.
    if os.system("sudo -V > /dev/null 2> /dev/null"): #if true sudo not found.
        sudo = str("")
    else:
        sudo = str("sudo")  #found
    if sudo:
        return (os.system("%s %s %s"%(sudo, "/bin/kill", pid)) >> 8)
    return 1
    
def kill_process(pid):

    if detect_process(pid):
        #If the process doesn't go away terminate it.
        try:
            os.kill(pid, signal.SIGTERM)

            time.sleep(1) #If the SIGTERM succeded, give the kernel a moment...
        except OSError, msg:
            if hasattr(msg, "errno") and msg.errno != errno.EPERM:
                return 1

            #Lastly, try a SIGKILL.
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError, msg:
                return detect_process(pid)

        if detect_process(pid):
            #If we got here, the enstore process caught the SIGTERM.
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError, msg:
                return detect_process(pid)

    #The process is already gone.
    return 0

def quit_process(gc):
    if not enstore_start.is_on_host(gc.server_address[0]):
        return None

    #Send the quit message.
    try:
        #rtn = u.send({'work':"quit"}, gc.server_address, SEND_TO, SEND_TM)
        rtn = gc.quit(SEND_TO, SEND_TM)
    except (socket.error, select.error, e_errors.EnstoreError), msg:
        if msg.errno == errno.ETIMEDOUT:
            rtn = {'status':(e_errors.TIMEDOUT, gc.server_name)}
        else:
            rtn = {'status':(e_errors.NET_ERROR, str(msg))}
    except errno.errorcode[errno.ETIMEDOUT]:
        rtn = {'status':(e_errors.TIMEDOUT, None)}
    if e_errors.is_ok(rtn):
        time.sleep(1)
        return detect_process(rtn['pid'])
    else:
        return 1

#Nicely, try to stop the server.  If that fails, kill it.
def stop_server(gc, servername):
    #Stop the server.
    print "Stopping %s: %s:%s" % (servername, gc.server_address[0],
                                  gc.server_address[1])

    #Get this information for the pid.
    rtn = gc.alive(servername, SEND_TO, SEND_TM)

    if not quit_process(gc):
        #Success, the process is dead.
        remove_pid_file(servername)
        print "Stopped %s." % (servername,)
        return 0
    if servername.find("mover"):
        u = udp_client.UDPClient()
	try:
            rtn1 = u.send({'work':"status"}, gc.server_address, SEND_TO, SEND_TM)
        except (socket.error, select.error, e_errors.EnstoreError), msg:
            if msg.errno == errno.ETIMEDOUT:
                rtn1 = {'status':(e_errors.TIMEDOUT, servername)}
            else:
                rtn1 = {'status':(e_errors.NET_ERROR, str(msg))}
            if not e_errors.is_ok(rtn1):
                return 1
        except errno.errorcode[errno.ETIMEDOUT]:
            rtn1 = {'status':(e_errors.TIMEDOUT, None)}
            if not e_errors.is_ok(rtn1):
                return 1
        
        try:
            if rtn1['state'] == 'DRAINING':
                print "%s will stop when transfer is finished"%(servername,)
                return 0
        except KeyError:
            rtn1['state'] = "Unknown"
        
    if not e_errors.is_ok(rtn):
        return 1

    #If the process still remains, kill it.
    #Note: there is a possible race condition here.  If the gc.quit()
    # succeds, but another process with the same pid is started
    # before kill_process, the new process will wrongfully be killed.
    if servername.find("mover"):
        print "killing %s in state %s"%(servername, rtn1['state'])
        rtn2 = kill_root_process(rtn['pid'])
    else:
        rtn2 = kill_process(rtn['pid'])
    if rtn2:
        print "Enstore server, %s, remains." % (servername,)
        return 1
    else:
        remove_pid_file(servername)
        print "Stopped %s." % (servername,)
        return 0

def stop_server_from_pid_file(servername):
    #If there is no response from the server, determine if it is hung.
    try:
        fp = open(get_temp_file(servername), "r")
        data = fp.readlines()
        fp.close()
    except (OSError, IOError), msg:
        print "Unable to read pid file for %s: %s" % (servername, msg)
        return

    #Make sure there is something in the file.
    if not data:
        remove_pid_file(servername)
        return 0
    
    #Determine if there is a process with the id.
    for item in data:
        #split pid and time
        pid, date, time = item.split(" ")
        pid = int(pid.strip())
        if detect_process(pid):
            if os.uname()[0] == "Linux":
                #If we get here it is becuase the process is still there and
                # we are on a linux node.  Proceed with checking the /proc
                # filesystem for confirmation.
                fp = open("/proc/%s/cmdline" % pid, "r")
                data = fp.readline()
                fp.close()
                if(data.find(servername) > 0):
                    #If we get here, then we know that the process is the
                    # enstore server in question.
                    print "The %s process is running with pid %s.  Killing." \
                          % (servername, pid)
                    if servername.find("mover"):
                        rtn2 = kill_root_process(int(pid))
                    else:
                        rtn2 = kill_process(int(pid))
                    if rtn2:
                        print "Enstore server, %s, remains." % (servername,)
                        return 1
                    else:
                        remove_pid_file(servername)
                        print "Stopped %s." % (servername,)
                        return 0
                else:
                    print "A process is running with the last know pid for " \
                          "%s." % (servername,)
                    return 1
            else:
                print "A process is running with the last know pid for %s." % \
                      (servername,)
                return 1
        else:
            remove_pid_file(servername)
            print "No %s running." % (servername,)
            return 0

    #Cleanup.  Should never get here.
    remove_pid_file(servername)
    return 0

############################################################################

#def check_db(csc, name):
#
#    use_name = "volume_clerk" #Use this servers name for host/ip finding.
#
#    # Get the address and port of the server.
#    if csc != None:
#        info = csc.get(use_name, SEND_TO, SEND_TM)
#    if csc == None or not e_errors.is_ok(info):
#        info = enstore_functions.get_dict_from_config_file(use_name,None)
#
#    #If we still do not have the necessary info, skip it.
#    if info == None or not e_errors.is_ok(info):
#        return
#    # If the process is running on this host continue, if not running on
#    # this host return.
#    if not is_on_host(info.get('host', None)) and \
#       not is_on_host(info.get('hostip', None)):
#        return
#
#    print "Checking %s." % name
#    
#    rtn = os.popen("ps -elf | grep %s | grep -v grep" % (name,)).readlines()
#
#    if rtn:
#        pid = int(re.sub("\s+", " ", rtn[0]).split(" ")[3])
#        print "Stopping %s: %d" % (name, pid)
#        if(kill_process(pid)):
#            #If we get here the process remains.
#            print "Database server %s remains." % name

#If the event relay responded to alive messages, this would not be necessary.
def check_event_relay(csc):

    name = "event_relay"

    # Get the address and port of the server.
    if csc != None:
        info = csc.get(name, SEND_TO, SEND_TM)
    if csc == None or not e_errors.is_ok(info):
        info = enstore_functions.get_dict_from_config_file(name,None)

    #If we still do not have the necessary info, skip it.
    if info == None or not e_errors.is_ok(info):
        return
    # If the process is running on this host continue, if not running on
    # this host return.
    if not enstore_start.is_on_host(info.get('host', None)) and \
       not enstore_start.is_on_host(info.get('hostip', None)):
        return

    erc = event_relay_client.EventRelayClient(event_relay_host=info['hostip'],
                                              event_relay_port=info['port'])

    print "Checking %s." % name

    rtn = erc.alive() #rtn = 0 implies alive, rtn = 1 implies dead.

    if not rtn:
        print "Stopping %s." % name

        #Tell the event relay to quit.
        erc.quit()

        #Make sure that event_relay is not still alive.  It sould not be
        # this complicated, but there were situations were TIMEDOUT still
        # occured when there was no good reason for it to occur.  Putting
        # a loop here fixed it.
        for unused in (0, 1, 2):
            try:
                #rtn = 0 implies alive, rtn = 1 implies dead.
                rtn = erc.alive()
            except (socket.error, select.error, e_errors.EnstoreError), msg:
                if msg.errno == errno.ETIMEDOUT:
                    rtn = {'status':(e_errors.TIMEDOUT,
                                     enstore_constants.EVENT_RELAY)}
                else:
                    rtn = {'status':(e_errors.NET_ERROR, str(msg))}
            except errno.errorcode[errno.ETIMEDOUT]:
                rtn = {'status':(e_errors.TIMEDOUT,
                                 errno.errorcode[errno.ETIMEDOUT])}
            if rtn == 1:
                break
        
        if not rtn:
            stop_server_from_pid_file(name)
        else:
            #If erc is successfully halted, remove the pid file for it.
            try:
                os.unlink(get_temp_file(name))
            except OSError:
                pass
    else:
        stop_server_from_pid_file(name)

def check_server(csc, name):

    # Get the address and port of the server.
    if csc != None:
        info = csc.get(name, SEND_TO, SEND_TM)
    if csc == None or not e_errors.is_ok(info):
        info = enstore_functions.get_dict_from_config_file(name,None)

    ##HACK:
    #Do a hack for the monitor server.  Since, it runs on all enstore
    # machines we need to add this information before continuing.
    if e_errors.is_ok(info) and name == enstore_constants.MONITOR_SERVER:
        info['host'] = socket.gethostname()
        info['hostip'] = socket.gethostbyname(info['host'])
        info['port'] = enstore_constants.MONITOR_PORT
    ##END HACK.

    #If we still do not have the necessary info, skip it.
    if info == None or not e_errors.is_ok(info):
        return
    # If the process is running on this host continue, if not running on
    # this host return.
    if not enstore_start.is_on_host(info.get('host', None)) and \
       not enstore_start.is_on_host(info.get('hostip', None)):
        return

    gc = generic_client.GenericClient(csc, name,
                                      flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM,
                                      rcv_timeout=SEND_TO, rcv_tries=SEND_TM, 
                                      server_address=(info['hostip'],
                                                      info['port']))

    print "Checking %s." % name

    #Stop the server.
    if(stop_server(gc, name)):  #If returned true, there was an error.
        stop_server_from_pid_file(name)  #If it still lives, kill it.

class EnstoreStopInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "STOP"
        self.just = None
        self.all = 0 #False

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.start_options)
    
    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

        self.completed_server_name()

    def completed_server_name(self):
        if self.just:
            count = 0
            for name in self.complete_names:
                if self.just == name[:len(self.just)]:
                    count = count + 1
                    hold = name
            if count == 1:
                self.just = hold

    def should_stop(self, server_name):

        if self.all:
            return 1
        if self.just == server_name:
            return 1
        if self.just == None and server_name not in self.non_default_names:
            return 1

        return 0

    non_default_names = ["monitor_server","pnfs_agent"]

    complete_names = [
        "accounting_server",
        "drivestat_server",
        "configuration_server",
        "event_relay",
        "log_server",
        "alarm_server",
        "volume_clerk",
        "file_clerk",
        "info_server",
#        "db_checkpoint",
#        "db_deadlock",
        "inquisitor",
        "ratekeeper",
        "library",
        "media",
        "mover",
        "monitor_server",
        "pnfs_agent",
        ]

    start_options = {
        option.JUST:{option.HELP_STRING:"specify single server",
                     option.VALUE_NAME:"just",
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_LABEL:"server name",                     
		     option.USER_LEVEL:option.ADMIN,
                     },
        option.ALL:{option.HELP_STRING:"specify all servers",
                     option.VALUE_USAGE:option.IGNORED,
                     option.DEFAULT_NAME:"all",
                     option.DEFAULT_TYPE:option.INTEGER,
		     option.USER_LEVEL:option.ADMIN,
                     },
        }

def is_there(name):
    rtn = os.popen("ps -elf | grep %s | grep -v grep" % (name,)).readlines()
    if rtn:
        return 1
    else:
        return 0

def do_work(intf):
    Trace.init(MY_NAME)
    csc = get_csc()
    #If the log server is still running, send log messages there.
    if csc and e_errors.is_ok(csc.alive(enstore_constants.LOG_SERVER, 2, 2)):
        logc = log_client.LoggerClient(csc, MY_NAME,
                                       enstore_constants.LOG_SERVER)
        Trace.set_log_func(logc.log_func)
    #Begin stopping enstore.

    #Movers.
    movers = find_servers_by_type(csc, enstore_constants.MOVER)
    for mover in movers:
        if intf.should_stop(enstore_constants.MOVER) or \
           intf.should_stop(mover):
            check_server(csc, mover)

    #Migrators.
    migrators = find_servers_by_type(csc, enstore_constants.MIGRATOR)
    for migrator in migrators:
        if intf.should_stop(enstore_constants.MIGRATOR) or \
           intf.should_stop(migrator):
            check_server(csc, migrator)

    #Media changers.
    media_changers = find_servers_by_type(csc, enstore_constants.MEDIA_CHANGER)
    for media_changer in media_changers:
        if intf.should_stop(enstore_constants.MEDIA_CHANGER) or \
           intf.should_stop(media_changer):
            check_server(csc, media_changer)

    #Libraries.
    libraries = find_servers_by_type(csc, enstore_constants.LIBRARY_MANAGER)
    for library_manager in libraries:
        if intf.should_stop(enstore_constants.LIBRARY_MANAGER) or \
           intf.should_stop(library_manager):
            check_server(csc, library_manager)

    # udp to amq proxy servers
    proxy_servers = find_servers_by_type(csc, enstore_constants.UDP_PROXY_SERVER)
    for proxy_server in proxy_servers:
        if intf.should_stop(enstore_constants.UDP_PROXY_SERVER) or \
           intf.should_stop(proxy_server):
            check_server(csc, proxy_server)

    #Added by Dmitry, stopping pnfs_agent
    agents = find_servers_by_type(csc, enstore_constants.PNFS_AGENT)
    for agent in agents:
        if intf.should_stop(enstore_constants.PNFS_AGENT) or \
           intf.should_stop(agent):
            check_server(csc, agent)

    # db_checkpoint and db_deadlock should be stopped if and only if
    # both file_clerk and volume_clerk are gone.
    # However, the complication is: the clerks are terminated by
    # themselves, after receiving a quit request from clients. That is,
    # there is a delay between sending the quit request and the server
    # actually finishes itself. Therefore, simply looking at the pid
    # would not tell the whole story.
    #
    # The heuristic used here is as follows:
    # [1] if intf.should_stop(server) is true, the server is considered
    #     gone.
    # [2] if intf.should_stop(server) is false, look for pid to
    #     determine if it is there.
    # [3] if both file_clerk and volume_clerk are gone, stop those two
    #     database deamons

    #Stop the servers.
    
    for server in [ enstore_constants.ACCOUNTING_SERVER,
                    enstore_constants.DRIVESTAT_SERVER,
                    enstore_constants.ALARM_SERVER,
                    enstore_constants.LM_DIRECTOR,
                    enstore_constants.DISPATCHER,
                    enstore_constants.INFO_SERVER,
                    enstore_constants.FILE_CLERK,
                    enstore_constants.VOLUME_CLERK,
                    enstore_constants.INQUISITOR,
                    enstore_constants.RATEKEEPER,
                    enstore_constants.MONITOR_SERVER,
                    enstore_constants.LOG_SERVER]:

        if intf.should_stop(server):
            check_server(csc, server)

    #Stop the Berkley DB dameons.
    # if (intf.should_stop(enstore_constants.FILE_CLERK) and \
    #     intf.should_stop(enstore_constants.VOLUME_CLERK)) or \
    #    (intf.should_stop(enstore_constants.FILE_CLERK) and \
    #     not is_there(enstore_constants.VOLUME_CLERK)) or \
    #    (intf.should_stop(enstore_constants.VOLUME_CLERK) and \
    #     not is_there(enstore_constants.FILE_CLERK)):
    #     check_db(csc, "db_checkpoint")
    #     check_db(csc, "db_deadlock")

    #Stop the event relay.
    if intf.should_stop(enstore_constants.EVENT_RELAY):
        check_event_relay(csc)
    #Stop the configuration server.
    if intf.should_stop(enstore_constants.CONFIGURATION_SERVER):
        check_server(csc, enstore_constants.CONFIGURATION_SERVER)

if __name__ == "__main__":   # pragma: no cover

    intf = EnstoreStopInterface(user_mode=0)

    do_work(intf)
