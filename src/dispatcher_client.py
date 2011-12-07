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

   # reload policy when this method is called
   # by the request from the client
    def reload_policy(self):
        r = self.send({'work': 'reload_policy'})
        return r

   # get current policy
    def show_policy(self):
        r = self.send({'work': 'show_policy'})
        return r

   # get content of pools
    def show_queue(self):
        r = self.send({'work': 'show_queue'})
        return r



class DispatcherClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.show = 0
        self.load = 0
        self.get_queue = 0
        self.alive_rcv_timeout = generic_client.DEFAULT_TIMEOUT
        self.alive_retries = generic_client.DEFAULT_TRIES
        self.summary = 0
        self.timestamp = 0
        self.threaded_impl = None
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.alive_options, self.trace_options,
                self.policy_options)

    policy_options = {
        option.LOAD:{option.HELP_STRING:"load a new policy file",
                     option.DEFAULT_TYPE:option.INTEGER,
		     option.USER_LEVEL:option.ADMIN
                     },
        option.GET_QUEUE:{option.HELP_STRING:"print content of pools",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.USER_LEVEL:option.ADMIN
                          },
        option.SHOW:{option.HELP_STRING:"print the current policy in python format",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.USER_LEVEL:option.ADMIN,
                     }
        }



def do_work(intf):
    dispatcher_client = DispatcherClient((intf.config_host, intf.config_port))
    Trace.init(dispatcher_client.get_name(MY_NAME))
    reply = dispatcher_client.handle_generic_commands(MY_SERVER, intf)

    if intf.alive:
        if reply['status'] == (e_errors.OK, None):
            print "Policy Engine Server and Migration dispatcher found at %s." % (reply['address'],)
    if reply:
        pass

    elif intf.load:
        reply = dispatcher_client.reload_policy()
        if reply.has_key('status'):
            if reply['status'][0] == e_errors.OK:
                print "Policy reloaded"
            else:
                print "Error reloading policy: %s"%(reply['status'],)
        else:
            print "Error reloading policy: %s"%(reply,)
    elif intf.show:
        import pprint
        reply = dispatcher_client.show_policy()
        if reply.has_key('status') and reply['status'][0] == e_errors.OK:
            # correct reply must contain 'dump' key by design
            pprint.pprint(reply['dump'])
        else:
            pprint.pprint(reply)
    elif intf.get_queue:
        import pprint
        reply = dispatcher_client.show_queue()
        if reply.has_key('status') and reply['status'][0] == e_errors.OK:
            # correct reply must contain 'dump' key by design
            pprint.pprint(reply['pools'])
        

    else:
	intf.print_help()
    



if __name__ == "__main__" :
    # fill in interface
    intf = DispatcherClientInterface(user_mode=0)

    do_work(intf)
