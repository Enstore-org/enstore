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

MY_NAME = enstore_constants.RATEKEEPER_CLIENT   #"RATEKEEPER_CLIENT"
MY_SERVER = enstore_constants.RATEKEEPER        #"ratekeeper"

class RatekeeperClient(generic_client.GenericClient):

    def __init__(self, csc, server_address=None, rcv_timeout=0, rcv_tries=0,
                 flags=0, logc=None, alarmc=None):
        generic_client.GenericClient.__init__(self,csc,MY_NAME, server_address,
                                              server_name=MY_SERVER,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc)
        self.timeout = rcv_timeout
        self.tries = rcv_tries
        #self.ratekeeper_addr = server_address

    # send Active Monitor probe request
    #def send_ticket (self, ticket):
    #    x = self.u.send(ticket, self.server_address, self.timeout, self.tries)
    #    return x

class RatekeeperClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "ratekeeper"
        self.alive_rcv_timeout = 2  #Required here
        self.alive_retries = 2      #Required here
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)
        
    def valid_dictionaries(self):
        return (self.help_options, self.alive_options)

    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

# we need this in order to be called by the enstore.py code
def do_work(intf):
    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))

    rc = RatekeeperClient(csc, rcv_timeout = intf.alive_rcv_timeout,
                          rcv_tries=intf.alive_retries)

    reply = rc.handle_generic_commands(intf.name, intf)

    #The user simply typed "enstore ratekeeper" and nothing else.
    if reply == None:
        intf.print_help()
    #The user performed an action.
    else:
        rc.check_ticket(reply)
        

if __name__ == "__main__":   # pragma: no cover

    intf = RatekeeperClientInterface(user_mode=0)
    
    Trace.init(MY_NAME)
    Trace.trace( 6, 'msc called with args: %s'%(sys.argv,) )

    do_work(intf)
