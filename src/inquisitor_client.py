#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
import types
import time

# enstore imports
import generic_client
#import udp_client
import enstore_constants
import e_errors
import option
import Trace

MY_NAME = enstore_constants.INQUISITOR_CLIENT    #"INQ_CLIENT"
MY_SERVER = enstore_constants.INQUISITOR         #"inquisitor"

RCV_TIMEOUT = 0
RCV_TRIES = 0

class Inquisitor(generic_client.GenericClient):

    def __init__(self, csc, server_address=None,
		 flags=0, logc=None, alarmc=None,
                 rcv_timeout=RCV_TIMEOUT,
                 rcv_tries=RCV_TRIES):
        generic_client.GenericClient.__init__(self, csc, MY_NAME,
                                              server_address,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              server_name = MY_SERVER)
	#self.server_name = MY_SERVER
        #self.server_address = self.get_server_address(self.server_name)

    def update (self):
	t = {"work"       : "update" }
	# tell the inquisitor to update the enstore system status info
	return self.send(t)

    def update_and_exit (self):
	t = {"work"     : "update_and_exit"}
	# tell the inquisitor to get out of town
	return self.send(t)

    def set_update_interval (self, tout):
	t = {"work"         : "set_update_interval" ,
             "update_interval"  : tout }
	# tell the inquisitor to set the select timeout
	return self.send(t)

    def get_update_interval (self):
	t = {"work"     : "get_update_interval" }
	# tell the inquisitor to get the select wake up timeout
	return self.send(t)

    def get_last_alive (self, server_list):
	t = {"work"    : "get_last_alive",
             "servers" : server_list}
	# tell the inquisitor to get the last time the server[s] were alive
	return self.send(t)

    def max_encp_lines (self, value):
	# tell the inquisitor to set the value for the max num of displayed
	# encp lines
	return self.send({"work"       : "set_max_encp_lines" ,
                          "max_encp_lines"  : value })

    def get_max_encp_lines (self):
	# tell the inquisitor to return the maximum displayed encp lines
	return self.send({"work"       : "get_max_encp_lines" } )

    def refresh (self, value):
	# tell the inquisitor to set the value for the html file refresh
	return self.send({"work"     : "set_refresh" ,
                          "refresh"  : value })

    def get_refresh (self):
	# tell the inquisitor to return the current html file refresh value
	return self.send({"work"       : "get_refresh" } )

    def subscribe (self):
	# tell the inquisitor to subscribe to the event relay
	return self.send({"work"     : "subscribe" })

    def down (self, server_list, time, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers as down
	return self.send({"work"    : "down",
			  "servers" : server_list,
			  "time"    : time}, rcv_timeout, tries)

    def up (self, server_list, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers as up
	return self.send({"work"    : "up",
			  "servers" : server_list }, rcv_timeout, tries)

    def nooutage (self, server_list, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers as no longer scheduled 
	# for an outage
	return self.send({"work"    : "nooutage",
			  "servers" : server_list }, rcv_timeout, tries)

    def outage (self, server_list, time, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers as scheduled for an outage
	return self.send({"work"    : "outage",
			  "servers" : server_list,
			  "time"    : time}, rcv_timeout, tries)

    def nooverride (self, server_list, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers' status as no longer set
	# to the user specified value
	return self.send({"work"    : "nooverride",
			  "servers" : server_list}, rcv_timeout, tries)

    def override (self, server_list, status, reason="<no given reason>", rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers' status as to be set to
	# the user specified value independent of what it really is
	return self.send({"work"       : "override",
			  "servers"    : server_list,
			  "saagStatus" : status,
                          "reason"     : reason}, rcv_timeout, tries)

    def show (self, rcv_timeout=0, tries=0):
	# tell the inquisitor to return the outage/status of the servers in the 
	# schedule file
	return self.send({"work"    : "show" }, rcv_timeout, tries)

    def print_show(self, ticket):
	print "\n Enstore Items Scheduled To Be Down"
	print   " ----------------------------------"
	outage_d = ticket["outage"]
	keys = outage_d.keys()
	keys.sort()
	for key in keys:
	    print "   %s : %s"%(key, outage_d[key])
	else:
	    print ""
	# now output the servers that are known down but not scheduled down
	offline_d = ticket["offline"]
	if offline_d:
	    keys = offline_d.keys()
	    keys.sort()
	    print "\n Enstore Items Known Down"
	    print   " ------------------------"
	    for key in keys:
		print "   %s : %s"%(key, offline_d[key])
	    else:
		print ""
	# output the servers that we have seen down and been monitoring
	seen_down_d = ticket["seen_down"]
	if seen_down_d:
	    keys = seen_down_d.keys()
	    keys.sort()
	    print "\n Enstore Items Down and the Number of Times Seen Down"
	    print   " ----------------------------------------------------"
	    for key in keys:
		print "   %s : %s"%(key, seen_down_d[key])
	    else:
		print ""

	# output information on servers whose status is being overridden
	override_d = ticket["override"]
	if override_d:
	    keys = override_d.keys()
	    now = time.time()
	    keys.sort()
	    print "\n Enstore Items Being Overridden"
	    print   " ------------------------------"
	    for key in keys:
		if type(override_d[key]) == types.ListType:
		    secs_down = now - override_d[key][1]
		    print "   %s : %s for %.0d seconds"%(key, override_d[key][0], secs_down)
		else:
		    print "   %s : %s"%(key, override_d[key])
	    else:
		print ""


class InquisitorClientInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.do_parse = flag
        #self.restricted_opts = opts
	self.update = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.refresh = 0
	self.get_refresh = 0
	self.max_encp_lines = 0
	self.get_max_encp_lines = 0
	self.logfile_dir = ""
	self.start_time = ""
	self.stop_time = ""
        self.media_changer = []
        self.keep = 0
        self.keep_dir = ""
        self.output_dir = ""
        self.update_interval = -1
        self.get_update_interval = 0
        self.get_last_alive = ""
	self.subscribe = None
	self.show = 0
	self.up = ""
	self.down = ""
	self.time = ""
        self.reason = ""
	self.outage = ""
	self.nooutage = ""
	self.override = ""
	self.nooverride = ""
	self.saagstatus = ""
	self.update_and_exit = 0
        self.is_up = ""
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.inquisitor_options)

    inquisitor_options = {
        option.DOWN:{option.HELP_STRING:"servers to mark down",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"server[,server]",
                     option.USER_LEVEL:option.ADMIN,
                     },
        option.DUMP:{option.HELP_STRING:
                     "print (stdout) state of servers in memory",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN,
                     },
        option.GET_MAX_ENCP_LINES:{option.HELP_STRING:
                                   "return number of displayed lines on the "
                                   "encp history web page",
                                   option.DEFAULT_TYPE:option.INTEGER,
                                   option.DEFAULT_VALUE:option.DEFAULT,
                                   option.VALUE_USAGE:option.IGNORED,
                                   option.USER_LEVEL:option.ADMIN,
                                   },
        option.GET_REFRESH:{option.HELP_STRING:
                            "return the refresh interval for inquisitor "
                            "created web pages",
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.ADMIN,
                            },
        option.GET_LAST_ALIVE:{option.HELP_STRING:
                               "return the last time a heartbeat was received "
                               "by the inquisitor for the listed servers",
                               option.VALUE_TYPE:option.STRING,
                               option.VALUE_USAGE:option.REQUIRED,
                               option.VALUE_LABEL:"server[,server]",
                               option.USER_LEVEL:option.ADMIN,
                               },
        option.GET_UPDATE_INTERVAL:{option.HELP_STRING:
                                    "return the interval between updates of "
                                    "the server system status web pages",
                                    option.DEFAULT_TYPE:option.INTEGER,
                                    option.DEFAULT_VALUE:option.DEFAULT,
                                    option.VALUE_USAGE:option.IGNORED,
                                    option.USER_LEVEL:option.ADMIN,
                                    },
        option.IS_UP:{option.HELP_STRING:
                        "check if <server> is up",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"server",
                        option.USER_LEVEL:option.ADMIN,
                        },
        option.MAX_ENCP_LINES:{option.HELP_STRING:"set the number of displayed"
                               " lines on the encp history web page",
                               option.VALUE_TYPE:option.INTEGER,
                               option.VALUE_USAGE:option.REQUIRED,
                               option.VALUE_LABEL:"num_lines",
                               option.USER_LEVEL:option.ADMIN,
                               },
        option.NOOUTAGE:{option.HELP_STRING:"remove the outage check from the "
                         "SAAG page for the specified servers",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"server[,server]",
                         option.USER_LEVEL:option.ADMIN,
                         },
        option.NOOVERRIDE:{option.HELP_STRING:"do not override the status of "
                           "the specified servers",
                           option.VALUE_TYPE:option.STRING,
                           option.VALUE_USAGE:option.REQUIRED,
                           option.VALUE_LABEL:"server[,server]",
                           option.USER_LEVEL:option.ADMIN,
                           },
        option.OUTAGE:{option.HELP_STRING:"set the outage check on the SAAG "
                       "page for the specified servers",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_LABEL:"server[,server]",
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.OVERRIDE:{option.HELP_STRING:"override the status of the "
                       "specified servers with the saagstatus option",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"server[,server]",
                         option.USER_LEVEL:option.ADMIN,
                         },
        option.REASON:{option.HELP_STRING:"information associated with a "
                     "server marked down or with an outage",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"string",
                     option.USER_LEVEL:option.ADMIN,
                     },
        option.REFRESH:{option.HELP_STRING:"set the refresh interval for "
                        "inquisitor created web pages",
                        option.VALUE_TYPE:option.INTEGER,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"seconds",
                        option.USER_LEVEL:option.ADMIN,
                        },
        option.SAAG_STATUS:{option.HELP_STRING:"status to use for override",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"status",
                            option.USER_LEVEL:option.ADMIN,
                            },
        option.SHOW:{option.HELP_STRING:"print (stdout) the servers "
                     "scheduled down, known down, seen down and overridden",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN,
                     },
        option.SUBSCRIBE:{option.HELP_STRING:"subscribe the inquisitor to "
                          "the event relay",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.DEFAULT_VALUE:option.DEFAULT,
                          option.VALUE_USAGE:option.IGNORED,
                          option.USER_LEVEL:option.ADMIN,
                          },
        option.TIME:{option.HELP_STRING:"information associated with a "
                     "server marked down or with an outage",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"string",
                     option.USER_LEVEL:option.ADMIN,
                     },
        option.UP:{option.HELP_STRING:
                   "servers to mark up",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"server[,server]",
                   option.USER_LEVEL:option.ADMIN,
                   },
        option.UPDATE:{option.HELP_STRING:
                       "update the server system status web page",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.UPDATE_AND_EXIT:{option.HELP_STRING:
                                "update the server system status web page and "
                                "exit the inquisitor",
                                option.DEFAULT_TYPE:option.INTEGER,
                                option.DEFAULT_VALUE:option.DEFAULT,
                                option.VALUE_USAGE:option.IGNORED,
                                option.USER_LEVEL:option.ADMIN,
                                },
        option.UPDATE_INTERVAL:{option.HELP_STRING:
                                "set the interval between updates of the "
                                "system servers status web page",
                                option.VALUE_TYPE:option.INTEGER,
                                option.VALUE_USAGE:option.REQUIRED,
                                option.VALUE_LABEL:"seconds",
                                option.USER_LEVEL:option.ADMIN,
                                },
        }

def concat_time_reason(time, reason):
    if time:
        return "%s - %s"%(time, reason)
    else:
        return reason

# this is where the work is actually done
def do_work(intf):
    # now get an inquisitor client
    iqc = Inquisitor((intf.config_host, intf.config_port))
    Trace.init(iqc.get_name(MY_NAME))

    ticket = iqc.handle_generic_commands(MY_SERVER, intf)
    if ticket:
        pass

    elif intf.dump:
        ticket = iqc.dump(intf.alive_rcv_timeout, intf.alive_retries)

    elif intf.update:
        ticket = iqc.update()

    elif intf.update_and_exit:
        ticket = iqc.update_and_exit()

    elif not intf.update_interval == -1:
        ticket = iqc.set_update_interval(intf.update_interval)

    elif intf.get_last_alive:
        ticket = iqc.get_last_alive(intf.get_last_alive)
        servers_l = ticket['servers'].keys()
        servers_l.sort()
        for server in servers_l:
            print "%s   %s"%(server, time.ctime(ticket['servers'][server]))

    elif intf.get_update_interval:
        ticket = iqc.get_update_interval()
	print repr(ticket['update_interval'])

    elif intf.is_up:
	ticket = iqc.show(rcv_timeout=30, tries=1)
	if e_errors.is_ok(ticket):
            outage = ticket['outage']
            offline = ticket['offline']
            server = intf.is_up.upper()
            for i in outage.keys()+offline.keys():
                if server == i.upper():
                    print "no"
                    sys.exit(1)

            print "yes"
            sys.exit(0)
        else:
            print "unknown"
            sys.exit(2)

    elif intf.get_refresh:
        ticket = iqc.get_refresh()
	print repr(ticket['refresh'])

    elif intf.refresh:
        ticket = iqc.refresh(intf.refresh)

    elif intf.max_encp_lines:
        ticket = iqc.max_encp_lines(intf.max_encp_lines)

    elif intf.get_max_encp_lines:
        ticket = iqc.get_max_encp_lines()
	print repr(ticket['max_encp_lines'])

    elif intf.subscribe:
	ticket = iqc.subscribe()

    elif intf.up:
	ticket = iqc.up(intf.up)

    elif intf.down:
        intf.time = concat_time_reason(intf.time, intf.reason)
	ticket = iqc.down(intf.down, intf.time)

    elif intf.outage:
        intf.time = concat_time_reason(intf.time, intf.reason)
	ticket = iqc.outage(intf.outage, intf.time)

    elif intf.nooutage:
	ticket = iqc.nooutage(intf.nooutage)

    elif intf.override and intf.saagstatus:
	if intf.saagstatus in enstore_constants.SAAG_STATUS:
            if intf.reason:
	        ticket = iqc.override(intf.override, intf.saagstatus, reason = intf.reason)
            else:
	        ticket = iqc.override(intf.override, intf.saagstatus)
	else:
	    # we did not get legal status values
            try:
                sys.stderr.write("ERROR: Invalid saagstatus value.")
                sys.stderr.write("    Legal values are: red, yellow, green, question\n")
                sys.stderr.flush()
            except IOError:
                pass
	    intf.print_help()
	    sys.exit(1)

    elif intf.nooverride:
	ticket = iqc.nooverride(intf.nooverride)

    elif intf.show:
	ticket = iqc.show()
	if e_errors.is_ok(ticket):
	    iqc.print_show(ticket)

    else:
	intf.print_help()
        sys.exit(0)

    del iqc.csc.u
    del iqc.u     # del now, otherwise get name exception (just for python v1.5???)

    iqc.check_ticket(ticket)


if __name__ == "__main__":   # pragma: no cover
    Trace.init(MY_NAME)
    Trace.trace(6,"iqc called with args "+repr(sys.argv))

    # fill in interface
    intf = InquisitorClientInterface(user_mode=0)

    do_work(intf)
