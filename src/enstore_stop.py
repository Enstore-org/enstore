#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# stop enstore
#
#
#


# system imports
#
import sys
import re
import os
import string
import errno
import socket
import signal
import generic_client
import option

# enstore imports
import setpath
import e_errors
import enstore_functions2
import udp_client

import alarm_client
import configuration_client
import configuration_server
import file_clerk_client
import inquisitor_client
import library_manager_client
import log_client
import media_changer_client
import mover_client
import monitor_client
import volume_clerk_client
import ratekeeper_client
import event_relay_client


def get_csc():
    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))

    rtn = csc.alive(configuration_client.MY_SERVER, 5, 3)

    if e_errors.is_ok(rtn):
        return csc
        
    return None

def this_host():
    rtn = socket.gethostbyaddr(socket.getfqdn())

    return [rtn[0]] + rtn[1] + rtn[2]

def is_on_host(host):

    if host in this_host():
        return 1

    return 0

def check_csc(csc):

    rtn = csc.alive(configuration_client.MY_SERVER, 5, 3)

    if e_errors.is_ok(rtn):
        if csc.server_address[0] in this_host():
            #Stop the configuration server.
            print "Stopping configuration server: %s:%s" % csc.server_address
            os.kill(rtn['pid'], signal.SIGTERM)
    else:
        print "Configuration server already stopped: %s:%s" % \
              csc.server_address

def check_db(csc, name):

    info = csc.get("volume_clerk", 5, 3)
    if not info.get('host', None) in this_host() and \
           not info.get('hostip', None) in this_host():
        return

    print "Checking %s." % name
    
    rtn = os.popen("ps -elf | grep %s | grep -v grep" % (name,)).readlines()

    if rtn:
        pid = int(re.sub("\s+", " ", rtn[0]).split(" ")[3])
        print "Stopping %s: %d" % (name, pid)
        os.kill(pid, signal.SIGTERM)

#If the event relay responded to alive messages, this would not be necessary.
def check_event_relay(csc):

    info = csc.get("event_relay", 5, 3)
    if not info.get('host', None) in this_host() and \
           not info.get('hostip', None) in this_host():
        return

    erc = event_relay_client.EventRelayClient(event_relay_host=info['hostip'],
                                              event_relay_port=info['port'])

    print "Checking event_relay."

    rtn = erc.alive()
    
    if not rtn:
        print "Stopping %s." % ("event_relay",)
        erc.quit()

def check_server(csc, name):


    # Get the address and port of the server.
    info = csc.get(name, 5, 3)
    # If the process is running on this host continue, if not running on
    # this host return.
    if not info.get('host', None) in this_host() and \
           not info.get('hostip', None) in this_host():
        return
    
    gc = generic_client.GenericClient(csc, name,
                                      server_address=(info['hostip'],
                                                      info['port']))

    print "Checking %s." % name

    try:
        # Determine if the host is alive.
        rtn = gc.alive(name, 3, 3)
    except errno.errorcode[errno.ETIMEDOUT]:
        rtn = {'status':(e_errors.TIMEDOUT, errno.errorcode[errno.ETIMEDOUT])}
        
    # If the host is alive, tell it to to quit.
    if e_errors.is_ok(rtn):
        #Stop the server.
        print "Stopping %s: %s:%s" % (name, info['hostip'], info['port'])
        gc.quit(3, 3)
    else:
        print "Already stopped %s: %s:%s" % (name, info.get('hostip', None),
                                             info.get('port', None))
    

class EnstoreStopInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "STOP"
        self.just = None

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

    def should_start(self, server_name):

        if self.just == server_name or self.just == None:
            return 1

        return 0

    complete_names = [
        "configuration_server",
        "event_relay",
        "log_server",
        "alarm_server",
        "volume_clerk",
        "file_clerk",
        "db_checkpoint",
        "db_deadlock",
        "inquisitor",
        "ratekeeper",
        "library",
        "media",
        "mover",
        ]
        

    start_options = {
        option.JUST:{option.HELP_STRING:"specify single server",
                     option.VALUE_NAME:"just",
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_LABEL:"server name",                     
		     option.USER_LEVEL:option.ADMIN,
                     }
        }


def do_work(intf):

    csc = get_csc()

    if csc == None:
        print "No configuration server running."
        sys.exit(1)
    
    #Get the library names.
    libraries = csc.get_library_managers({}).keys()
    libraries = map((lambda l: l + ".library_manager"), libraries)

    #Begin stopping enstore.

    if intf.should_start("mover"):
        for library in libraries:
            for mover in csc.get_movers(library):
                mover = mover['mover']
                check_server(csc, mover)
    elif intf.just[-6:] == ".mover":
        check_server(csc, intf.just)

    if intf.should_start("media"):
        for library in libraries:
            media = csc.get_media_changer(library)
            check_server(csc, media)
    elif intf.just[-14:] == ".media_changer":
        check_server(csc, intf.just)

    if intf.should_start("library"):
        for library in libraries:
            check_server(csc, library)
    elif intf.just[-16:] == ".library_manager":
        check_server(csc, intf.just)

    if intf.should_start("ratekeeper"):
        check_server(csc, "ratekeeper")
    if intf.should_start("inquisitor"):
        check_server(csc, "inquisitor")
    if intf.should_start("file_clerk"):
        check_server(csc, "file_clerk")
    if intf.should_start("volume_clerk"):
        check_server(csc, "volume_clerk")
    if intf.should_start("db_checkpoint"):
        check_db(csc, "db_checkpoint")
    if intf.should_start("db_deadlock"):
        check_db(csc, "db_deadlock")
    if intf.should_start("alarm_server"):
        check_server(csc, "alarm_server")
    if intf.should_start("log_server"):
        check_server(csc, "log_server")
    if intf.should_start("event_relay"):
        check_event_relay(csc)
    if intf.should_start("configuration_server"):
        check_csc(csc)

if __name__ == '__main__':

    intf = EnstoreStopInterface(user_mode=0)

    do_work(intf)
