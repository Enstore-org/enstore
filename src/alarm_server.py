#
# system import
import sys
import time
import copy
import errno
import string
import regsub
import types
import os

# enstore imports
import timeofday
import traceback
import callback
import log_client
import configuration_client
import volume_clerk_client
import file_clerk_client
import admin_clerk_client
import library_manager_client
import media_changer_client
import mover_client
import dispatching_worker
import interface
import generic_server
import generic_cs
import udp_client
import Trace
import e_errors
import enstore_status

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

TRUE = 1
FALSE = 0

class AlarmServerMethods(dispatching_worker.DispatchingWorker):

    # dump everything we have
    def dump(self, ticket):
	Trace.trace(10,"{dump "+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
	self.send_reply(ticket)
	Trace.trace(10,"}dump")

class AlarmServer(AlarmServerMethods, generic_server.GenericServer):

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
	Trace.trace(10, '{__init__')
	self.print_id = "ALRMS"
	self.verbose = verbose

	# get the config server
	configuration_client.set_csc(self, csc, host, port, verbose)
	keys = self.csc.get("alarm_server")
	self.hostip = keys['hostip']
        Trace.init(keys["logname"])
	try:
	    self.print_id = keys['logname']
	except:
	    pass
	dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))
        # get a logger
        self.logc = log_client.LoggerClient(self.csc, keys["logname"], \
                                            'logserver', 0)
	Trace.trace(10, '}__init__')

class AlarmServerInterface(interface.Interface):

    def __init__(self):
	Trace.trace(10,'{alrmi.__init__')
	# fill in the defaults for possible options
	self.verbose = 0
	interface.Interface.__init__(self)

	# now parse the options
	self.parse_options()
	Trace.trace(10,'}iqsi.__init__')

    # define the command line options that are valid
    def options(self):
	Trace.trace(16, "{}options")
	return self.config_options()+\
	       ["verbose="] +\
	       self.help_options()

if __name__ == "__main__":
    Trace.init("alarm_server")
    Trace.trace(1,"alarm server called with args "+repr(sys.argv))

    # get interface
    intf = AlarmServerInterface()

    # get the alarm server
    als = AlarmServer(0, intf.verbose, intf.config_host, intf.config_port)

    while 1:
        try:
            Trace.trace(1,'Alarm Server (re)starting')
            als.logc.send(log_client.INFO, 1, "Alarm Server (re)starting")
            als.serve_forever()
        except:
	    als.serve_forever_error("alarm_server", als.logc)
            continue
    Trace.trace(1,"Alarm Server finished (impossible)")
