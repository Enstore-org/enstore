###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# Log client.                                                           #
# This is a simple log client. It sends log messages to the log server  #
# via port specified in the Log Server dictionary entry in the enstore  #
# configuration file ( can be specified separately)                     #
#########################################################################

# system imports
import sys
import os
import pwd
import time
import select
import exceptions
import errno
import socket

#enstore imports
import generic_client
import generic_cs
import configuration_client
import interface
import udp_client
import Trace
import e_errors

# Severity codes
ERROR=0
USER_ERROR=1
WARNING=2
INFO=3
MISC=4

# severity translator
sevdict = { ERROR      : 'E', \
            USER_ERROR : 'U', \
            WARNING    : 'W', \
            INFO       : 'I', \
            MISC       : 'M'
            }

# send a message to the logger
def logit(message="HELLO", logname="LOGIT",config_host="", config_port=7510):

    try:
        if config_host == "" :
            (config_host,ca,ci) = socket.gethostbyaddr("pcfarm4.fnal.gov")

        csc = configuration_client.ConfigurationClient(config_host,config_port)
	csc.connect()

        # get a logger
        logc = LoggerClient(logname,  'logserver', 0)

        # send the message
        return logc.send(INFO, 1, message)

    except:
        return  str(sys.exc_info()[0])+" "+str(sys.exc_info()[1])



class LoggerClient(generic_client.GenericClient):

    def __init__(self,
                 csc = 0,                   # get our own configuration client
                 i_am_a = "LOGCLNT",        # Abbreviated client instance name
                                            # try to make it of capital letters
                                            # not more than 8 characters long
                 servername = "logserver",  # log server name
                 verbose=0,
                 host=interface.default_host(),
                 port=interface.default_port()):

	self.print_id = "LOGC"
        self.i_am = i_am_a
        self.pid = os.getpid()
        self.uid = os.getuid()
        pwdb_entry = pwd.getpwuid(self.uid)
        self.uname = pwdb_entry[0]
        self.logger = servername
        self.log_priority = 7
        self.debug = verbose
	self.verbose = verbose
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()

    def send (self, severity, priority, format, *args) :
        if  ((1<<priority) & self.log_priority) == 0 :
           return

        if severity in range(ERROR, MISC) :
            msg = '%.6d %.8s' % (self.uid, self.uname)
            msg = msg + ' ' + sevdict[severity] + ' ' + self.i_am + ' '
	    if args != ():
                str = format % args
	    else:
	        str = format
	    Trace.trace( severity, str )
            msg = msg + ' ' + str
	    if self.verbose & generic_cs.DEBUG:
	        try:
	            print add_id("", msg)
	        except:
	            pass
            ticket = {'work' : 'log_message',
                      'message' : msg }
            lticket = self.csc.get(self.logger)
            self.u.send_no_wait(ticket, (lticket['hostip'], lticket['port']))
            return {"status" : (e_errors.OK, None)}
        else :
            return {"status" : (e_errors.WRONGPARAMETER, \
				"wrong_severity_level")}
#
# priorty allows turning logging on and off in a server.
#  Coventions - setting log_priority to 0 should turn off all logging.
#             - default priority on send is 1 so the default is to log a message
#             - the default log_priority to test against is 10 so a priority
#                     send with priorty < 10 will normally be logged
#             - a brief trace message (1 per file per server should be priority 10
#             - file/server trace messages should 10> <20
#             - debugging should be > 20
    def set_logpriority(priority):
        log_priority = priority

    def get_logpriority():
        return log_priority


    # check on alive status
    def alive(self, rcv_timeout=0, tries=0):
        lticket = self.csc.get("logserver")
        return  self.u.send({'work':'alive'},
                            (lticket['hostip'], lticket['port']), rcv_timeout, tries)

    # get the current log file name
    def get_logfile_name(self, rcv_timeout=0, tries=0):
        Trace.trace(10,'{get_logfile_name')
        l = self.csc.get("logserver")
        x = self.u.send({'work':'get_logfile_name'}, \
	                (l['hostip'], l['port']), rcv_timeout, tries)
        Trace.trace(10,'}get_logfile_name'+repr(x))
        return x

    # get the last log file name
    def get_last_logfile_name(self, rcv_timeout=0, tries=0):
        Trace.trace(10,'{get_last_logfile_name')
        l = self.csc.get("logserver")
        x = self.u.send({'work':'get_last_logfile_name'}, \
	                (l['hostip'], l['port']), rcv_timeout, tries)
        Trace.trace(10,'}get_last_logfile_name'+repr(x))
        return x

    # reset the servers verbose flag
    def set_verbose(self, verbosity, rcv_timeout=0, tries=0):
        Trace.trace(10,'{set_verbose (client)')
        l = self.csc.get("logserver")
        x = self.u.send({'work':'set_verbose', 'verbose': verbosity}, \
	                (l['hostip'], l['port']), rcv_timeout, tries)
        Trace.trace(10,'}set_verbose (client) '+repr(x))
        return x

class LoggerClientInterface(interface.Interface):

    def __init__(self):
        self.config_file = ""
        self.test = 0
        self.logit1 = 0
        self.alive = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.get_logfile_name = 0
	self.get_last_logfile_name = 0
	self.got_server_verbose = 0
        interface.Interface.__init__(self)

        # fill in the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options() + self.verbose_options()+\
               ["config_file=", "test", "logit="] +\
	       ["get_logfile_name", "get_last_logfile_name"] +\
               self.alive_options()+self.help_options()


    """ 
    This function takes arbitrary number of arguments. The mandatory arguments
    are:
       severity - see severity codes above
       priority - an integer which is compared to a bit mask(log_priority)
		  1 implies always log, 2 imply normally go to logger,
                  4 more complete file tracing, 8,16,... detailed debugging.
                  the log_priority is set on a per server basis.
                  note, a log_prioirty = 0 should turn off logging
       format - any string which can contain formatters
    Example:
        send (ticket = logc.send (ERROR, 1, 'Error: errno=%d, and its \
        interpretation is: %s', err, os.strerror(err))
    """


if __name__ == "__main__" :
    import sys
    Trace.init("log client")
    Trace.trace(1,"logc called with args "+repr(sys.argv))

    # fill in interface
    intf = LoggerClientInterface()

    # get a log client
    logc = LoggerClient(0, "LOGCLNT", "logserver", intf.verbose, \
                        intf.config_host, intf.config_port)

    if intf.alive:
        ticket = logc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE

    elif intf.get_last_logfile_name:
        ticket = logc.get_last_logfile_name(intf.alive_rcv_timeout,\
	                                    intf.alive_retries)
	generic_cs.enprint(ticket['last_logfile_name'])
	msg_id = generic_cs.CLIENT

    elif intf.get_logfile_name:
        ticket = logc.get_logfile_name(intf.alive_rcv_timeout,\
	                               intf.alive_retries)
	generic_cs.enprint(ticket['logfile_name'])
	msg_id = generic_cs.CLIENT

    elif intf.got_server_verbose:
        ticket = logc.set_verbose(intf.server_verbose,intf.alive_rcv_timeout,\
	                          intf.alive_retries)
	msg_id = generic_cs.CLIENT

    elif intf.test:
        ticket = logc.send(ERROR, 1, "This is a test message %s %d", 'TEST', 3456)
	msg_id = generic_cs.CLIENT
        #ticket = logc.send(INFO, 21, "this is an INFO message")

    elif intf.logit:
        ticket = logit(intf.logmsg)
	msg_id = generic_cs.CLIENT

    del logc.csc.u
    del logc.u		# del now, otherwise get name exception (just for python v1.5???)

    logc.check_ticket(ticket, msg_id)
