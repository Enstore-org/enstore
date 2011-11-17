#!/usr/bin/env python

###############################################################################
#
# $Id$
# Policy Engine Server and Migration Dispatcher Client
#
###############################################################################

# system imports
import sys

# enstore imports
import enstore_constants
import generic_client
import option
import Trace
import e_errors

MY_NAME = enstore_constants.DISPATCHER_CLIENT
MY_SERVER = enstore_constants.DISPATCHER
RCV_TIMEOUT = 10
RCV_TRIES = 5

class DispatcherClient(generic_client.GenericClient): 

    def __init__(self, csc, name=MY_SERVER,
                 flags=0, logc=None, alarmc=None,
                 rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES,
                 server_address = None):
        self.name = name
        generic_client.GenericClient.__init__(self,csc, MY_NAME, server_address,
                                              server_name=name,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc)
        self.timeout = rcv_timeout
        self.tries = rcv_tries

class DispatcherClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.show = 0
        self.load = 0
        self.alive_rcv_timeout = generic_client.DEFAULT_TIMEOUT
        self.alive_retries = generic_client.DEFAULT_TRIES
        self.summary = 0
        self.timestamp = 0
        self.threaded_impl = None
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.alive_options, self.trace_options)

def do_work(intf):
    dispatcher_client = DispatcherClient((intf.config_host, intf.config_port))
    Trace.init(dispatcher_client.get_name(MY_NAME))
    reply = dispatcher_client.handle_generic_commands(MY_SERVER, intf)
    #for level in range(5,100):
    #    Trace.print_levels[level]=1

    if intf.alive:
        if reply['status'] == (e_errors.OK, None):
            print "Policy Engine Server and Migration dispatcher found at %s." % (reply['address'],)
    if reply:
        pass

    else:
	intf.print_help()
    



if __name__ == "__main__" :
    # fill in interface
    intf = DispatcherClientInterface(user_mode=0)

    do_work(intf)
