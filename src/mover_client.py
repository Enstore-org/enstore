#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
#
#########################################################################
#                                                                       #
# Mover client.                                                         #
# Mover access methods                                                  #
#                                                                       #
#########################################################################

# system imports
import sys
import string
import pprint

#enstore imports
#import udp_client
import option
import generic_client
import Trace

R_TO = 30
R_T = 3

class MoverClient(generic_client.GenericClient):
    def __init__(self, csc, name="", rcv_timeout = R_TO, rcv_tries = R_T,
                 flags=0, logc=None, alarmc=None):
        self.mover=name
        self.log_name = "C_"+string.upper(name)
        generic_client.GenericClient.__init__(self, csc, self.log_name,
                                              flags = flags, logc = logc,
                                              alarmc = alarmc,
                                              rcv_timeout = rcv_timeout,
                                              rcv_tries = rcv_tries,
                                              server_name = name)
        #self.server_address = self.get_server_address(self.mover, rcv_timeout, rcv_tries)

    def status(self, rcv_timeout=R_TO, tries=R_T):
        return self.send({"work" : "status"}, rcv_timeout, tries)

    def clean_drive(self, rcv_timeout=R_TO, tries=R_T):
        return self.send({"work":"clean_drive"}, rcv_timeout, tries)

    def start_draining(self, rcv_timeout=R_TO, tries=R_T):
        return self.send({"work" : "start_draining"}, rcv_timeout, tries)

    def stop_draining(self, rcv_timeout=R_TO, tries=R_T):
        return self.send({"work" : "stop_draining"}, rcv_timeout, tries)

    def warm_restart(self, rcv_timeout=R_TO, tries=R_T):
        return self.send({"work" : "warm_restart"}, rcv_timeout, tries)

    def device_dump(self, sendto=[], notify=[], rcv_timeout=R_TO, tries=R_T):
        # print "device_dump(self, sendto="+`sendto`+', notify='+`notify`+', rcv_timeout='+`rcv_timeout`+', tries='+`tries`+')'
        return self.send({"work" : "device_dump_S",
                          "sendto" : sendto,
                          "notify" : notify}, rcv_timeout, tries)

class MoverClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.mover = ""
        self.local_mover = 0
        self.clean_drive = 0
        self.enable = 0
        self.status = 0
        self.start_draining = 0
        self.stop_draining = 0
        self.notify = []
        self.sendto = []
        self.dump = 0
        self.warm_restart = 0
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.mover_options)

    mover_options = {
        option.CLEAN_DRIVE:{option.HELP_STRING:"clean tape drive",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.ADMIN},
        option.DOWN:{option.HELP_STRING:"set mover to offline state",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_NAME: "start_draining",
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.DUMP:{option.HELP_STRING:
                     "get the tape drive dump (used only with M2 movers)",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.NOTIFY:{option.HELP_STRING:
                       "send e-mail.  Used with --dump option only",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_LABEL:"e_mail_address",
                       option.USER_LEVEL:option.ADMIN},
        option.OFFLINE:{option.HELP_STRING:"set to offline state",
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.DEFAULT_NAME: "start_draining",
                        option.VALUE_USAGE:option.IGNORED,
                        option.USER_LEVEL:option.ADMIN},
        option.ONLINE:{option.HELP_STRING:"set to online state",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.DEFAULT_NAME: "stop_draining",
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        option.SENDTO:{option.HELP_STRING:
                       "send e-mail.  Used with --dump option only",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_LABEL:"e_mail_address",
                       option.USER_LEVEL:option.ADMIN},
        option.STATUS:{option.HELP_STRING:"print mover status",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        option.UP:{option.HELP_STRING:"set mover to online state",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.DEFAULT_NAME: "stop_draining",
                   option.VALUE_USAGE:option.IGNORED,
                   option.USER_LEVEL:option.ADMIN},
        option.WARM_RESTART:{option.HELP_STRING:"gracefully restart the mover",
                             option.DEFAULT_VALUE:option.DEFAULT,
                             option.DEFAULT_TYPE:option.INTEGER,
                             option.VALUE_USAGE:option.IGNORED,
                             option.USER_LEVEL:option.ADMIN},
        }
    """
    alive_options = option.Interface.alive_options.copy()
    alive_options[option.ALIVE] = {
        option.DEFAULT_VALUE:1,
        option.DEFAULT_TYPE:option.INTEGER,
        option.DEFAULT_NAME:"alive",
        option.VALUE_NAME:"mover",
        option.VALUE_USAGE:option.REQUIRED,
        option.FORCE_SET_DEFAULT:option.FORCE,
        option.HELP_STRING:"prints message if the server is up or down.",
        option.SHORT_OPTION:"a"
        }
    """

    parameters = ["mover_name"]
        
    def parse_options(self):
        generic_client.GenericClientInterface.parse_options(self)

        if (getattr(self, "help", 0) or getattr(self, "usage", 0)):
            pass
        elif len(self.argv) <= 1: #if only "enstore mover" is specified.
            self.print_help()
        elif len(self.args) < 1: #if a valid switch doesn't have the mover.
            self.print_usage("expected mover parameter")
        else:
            try:
                self.mover = self.args[0]
                del self.args[0]
            except KeyError:
                self.mover = ""

        self.mover = self.complete_server_name(self.mover, "mover")

def do_work(intf):
    # get a mover client
    movc = MoverClient((intf.config_host, intf.config_port), intf.mover)
    Trace.init(movc.get_name(movc.log_name))

    ticket = {}
    msg_id = None

    ticket = movc.handle_generic_commands(intf.mover, intf)
    if ticket:
        pass

    elif intf.status:
        ticket = movc.status(intf.alive_rcv_timeout,intf.alive_retries)
        pprint.pprint(ticket)
    elif intf.local_mover:
        ticket = movc.local_mover(intf.enable, intf.alive_rcv_timeout,
                                  intf.alive_retries)
    elif intf.clean_drive:
        ticket = movc.clean_drive(intf.alive_rcv_timeout, intf.alive_retries)
        print ticket
    elif intf.start_draining:
        ticket = movc.start_draining(intf.alive_rcv_timeout, intf.alive_retries)
    elif intf.stop_draining:
        ticket = movc.stop_draining(intf.alive_rcv_timeout, intf.alive_retries)
    elif intf.warm_restart:
        ticket = movc.warm_restart(intf.alive_rcv_timeout, intf.alive_retries)
    elif intf.dump:
        ticket = movc.device_dump(intf.sendto, intf.notify, intf.alive_rcv_timeout, intf.alive_retries)
    else:
        intf.print_help()
        sys.exit(0)

    movc.check_ticket(ticket)

if __name__ == "__main__" :
    Trace.init("MOVER_CLI")
    Trace.trace(6,"movc called with args "+repr(sys.argv))

    # fill in the interface
    intf = MoverClientInterface(user_mode=0)

    do_work(intf)
