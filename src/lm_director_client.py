#!/usr/bin/env python

###############################################################################
#
# $Id$
# Library Manager Director Client
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

MY_NAME = enstore_constants.LM_DIRECTOR_CLIENT
MY_SERVER = enstore_constants.LM_DIRECTOR
RCV_TIMEOUT = 10
RCV_TRIES = 5

class LMDClient(generic_client.GenericClient): 

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

    def reload_policy(self):
        r = self.send({'work': 'reload_policy'})
        return r

    def show_policy(self):
        r = self.send({'work': 'show_policy'})
        return r
        
class LMDClientInterface(generic_client.GenericClientInterface):
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
        return (self.help_options, self.alive_options, self.trace_options,
                self.policy_options)



    policy_options = {
        option.LOAD:{option.HELP_STRING:"load a new policy file",
                     option.DEFAULT_TYPE:option.INTEGER,
		     option.USER_LEVEL:option.ADMIN
                     },
        option.SHOW:{option.HELP_STRING:"print the current policy in python format",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.USER_LEVEL:option.ADMIN,
                     }
        }

def do_work(intf):
    lmd_client = LMDClient((intf.config_host, intf.config_port))
    Trace.init(lmd_client.get_name(MY_NAME))
    reply = lmd_client.handle_generic_commands(MY_SERVER, intf)

    if intf.alive:
        if reply['status'] == (e_errors.OK, None):
            print "Library Manager Director found at %s." % (reply['address'],)

    if reply:
        pass
    elif intf.load:
        reply = lmd_client.reload_policy()
        if reply.has_key('status'):
            if reply['status'][0] != e_errors.OK:
                print "Error reloading policy: %s"%(reply,)
                return
            # This is a special case when policy needs to get loaded to
            # dispatcher as well
            import dispatcher_client
            disp_client = dispatcher_client.DispatcherClient((intf.config_host, intf.config_port))
            reply = disp_client.reload_policy()
            if reply.has_key('status'):
                if reply['status'][0] == e_errors.OK:
                    print "Policy reloaded"
                else:
                    print "Error reloading policy: %s"%(reply,)
            else:
                print "Error reloading policy: %s"%(reply,)
                return
        else:
            print "Error reloading policy: %s"%(reply,)
        
    elif intf.show:
        import pprint
        reply = lmd_client.show_policy()
        if reply.has_key('status') and reply['status'][0] == e_errors.OK:
            # correct reply must contain 'dump' key by design
            pprint.pprint(reply['dump'])
        else:
            pprint.pprint(reply)
    else:
	intf.print_help()
    



if __name__ == "__main__":
    # fill in interface
    intf = LMDClientInterface(user_mode=0)

    do_work(intf)
