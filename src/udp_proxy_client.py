#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

#system imports
import sys

#enstore imports
import Trace
import generic_client
import configuration_client
#import udp_client
import e_errors
import enstore_constants

MY_NAME = enstore_constants.UDP_PROXY_CLIENT  
RCV_TIMEOUT = 20
RCV_TRIES = 3

class ProxyClient(generic_client.GenericClient):

    def __init__(self, csc, name="",
                 flags=0, logc=None, alarmc=None,
                 rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES,
                 server_address = None):
        self.name = name
        generic_client.GenericClient.__init__(self,csc,MY_NAME, server_address,
                                              server_name=name,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc)
        self.timeout = rcv_timeout
        self.tries = rcv_tries

class ProxyClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "udp_proxy_client"
        self.alive_rcv_timeout = 2  #Required here
        self.alive_retries = 2      #Required here
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)
    parameters = ["udp_proxy_server_name"]    

    def valid_dictionaries(self):
        return (self.help_options, self.alive_options, self.trace_options)

    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)
        if len(self.args) < 1: #if only "enstore udp proxy server" is specified.
            self.print_help()
            sys.exit(0)

        self.name = self.args[0]
        self.name = self.complete_server_name(self.name, "udp_proxy_server")

# we need this in order to be called by the enstore.py code
def do_work(intf):
    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))

    if intf.help:
        intf.print_help()
        return
    rc = ProxyClient(csc, rcv_timeout = intf.alive_rcv_timeout,
                     rcv_tries=intf.alive_retries)

    reply = rc.handle_generic_commands(intf.name, intf)

    #The user simply typed "enstore udp_proxy" and nothing else.
    if reply == None:
        intf.print_help()
    #The user performed an action.
    else:
        if intf.alive:
            if reply['status'] == (e_errors.OK, None):
                print "UPD Proxy Server %s found at %s." % (intf.name, reply['address'],)
        rc.check_ticket(reply)
        

if __name__ == "__main__":   # pragma: no cover

    intf = ProxyClientInterface(user_mode=0)
    
    Trace.init(MY_NAME)
    Trace.print_levels=range(1,10)
    Trace.trace( 6, 'udp_proxy called with args: %s'%(sys.argv,) )

    do_work(intf)
