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
import e_errors
import enstore_constants
import option

MY_NAME = enstore_constants.MIGRATOR_CLIENT  
RCV_TIMEOUT = 20
RCV_TRIES = 3

class MigratorClient(generic_client.GenericClient):

    def __init__(self, csc, name="",rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES,
                 flags=0, logc=None, alarmc=None):

        self.log_name = "C_"+name.upper()
        generic_client.GenericClient.__init__(self,csc, self.log_name,
                                              flags = flags, logc = logc,
                                              alarmc = alarmc,
                                              rcv_timeout = rcv_timeout,
                                              rcv_tries = rcv_tries,
                                              server_name = name)

        self.timeout = rcv_timeout
        self.tries = rcv_tries

    def status(self, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        r = self.send({"work": "get_status"})
        return r

    def quit_and_exit(self, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        r = self.send({"work": "quit_and_exit"})
        return r

class MigratorClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "migrator_client"
        self.alive_rcv_timeout = 2  #Required here
        self.alive_retries = 2      #Required here
        self.status = 0
        self.start_draining = 0
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    parameters = ["migrator_name"]    

    def valid_dictionaries(self):
        return (self.help_options, self.alive_options, self.trace_options, self.migrator_options)
    
    migrator_options = {
        option.STATUS:{option.HELP_STRING:"print migrator status",
                                      option.DEFAULT_VALUE:option.DEFAULT,
                                      option.DEFAULT_TYPE:option.INTEGER,
                                      option.VALUE_USAGE:option.IGNORED,
                                      option.USER_LEVEL:option.ADMIN},
        option.OFFLINE:{option.HELP_STRING:"offline migrator",
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.DEFAULT_NAME: "start_draining",
                        option.VALUE_USAGE:option.IGNORED,
                        option.USER_LEVEL:option.ADMIN},
        }
    


    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)
        if len(self.args) < 1: #if only "enstore migrator" is specified.
            self.print_help()
            sys.exit(0)

        self.name = self.args[0]
        self.name = self.complete_server_name(self.name, "migrator")

# we need this in order to be called by the enstore.py code
def do_work(intf):
    if intf.help:
	intf.print_help()
        return
        
    mc = MigratorClient((intf.config_host, intf.config_port), intf.name)
    Trace.init(mc.get_name(MY_NAME))
    reply = mc.handle_generic_commands(intf.name, intf)
    if intf.alive:
        if reply['status'] == (e_errors.OK, None):
            print "Migrator %s found at %s." % (intf.name, reply['address'],)
    if reply:
        pass

    elif intf.status:
        ticket = mc.status()
        import pprint
        pprint.pprint(ticket)
    elif intf.start_draining:
        ticket = mc.quit_and_exit()
        print ticket
    else:
	intf.print_help()
        

if __name__ == "__main__":

    intf = MigratorClientInterface(user_mode=0)
    
    Trace.init(MY_NAME)
    Trace.print_levels=range(1,10)
    Trace.trace( 6, 'migrator called with args: %s'%(sys.argv,) )

    do_work(intf)
