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
import generic_client_server
import generic_client
import configuration_client
import udp_client
import pprint
import Trace

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

        csc = configuration_client.configuration_client(config_host,config_port)
	csc.connect()

        # get a logger
        logc = LoggerClient(logname,  'logserver', 0)

        # send the message
        return logc.send(INFO, 1, message)

    except:
        return  str(sys.exc_info()[0])+" "+str(sys.exc_info()[1])



class LoggerClient(generic_client_server.GenericClientServer, generic_client.GenericClient):

    def __init__(self,
                 csc = [],                  # get our own configuration client
                 i_am_a = "LOGCLNT",        # Abbreviated client instance name
                                            # try to make it of capital letters
                                            # not more than 8 characters long
                 servername = "logserver",  # log server name
                 debug=0,                   # debug output
                 host=generic_client_server.default_host(),
                 port=generic_client_server.default_port()):
        self.i_am = i_am_a
        self.pid = os.getpid()
        self.uid = os.getuid()
        pwdb_entry = pwd.getpwuid(self.uid)
        self.uname = pwdb_entry[0]
        self.logger = servername
        self.debug = debug
        self.log_priority = 7
        self.config_list = 0
        self.list = 0
        self.doalive = 0
        self.test = 0
        self.logit1 = 0
        configuration_client.set_csc(self, csc, host, port)
        self.u = udp_client.UDPClient()

    # define the command line options that are valid
    def options(self):
        return generic_client_server.GenericClientServer.config_options(self)+\
               generic_client_server.GenericClientServer.list_options(self) +\
               ["config_list", "config_file=", "test", "logit=", "alive"] +\
               generic_client_server.GenericClientServer.options(self)

    # parse our own options
    def parse_options(self):
        generic_client_server.GenericClientServer.parse_options(self)
        self.debug = self.dolist

    # our help stuff 
    def help_line(self):
        return generic_client_server.GenericClientServer.help_line(self)+" media_changer volume drive"

    """ send the request to the Media Loader server and then send answer
    to user.
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
    def send (self, severity, priority, format, *args) :
        if  (priority & self.log_priority) == 0 :
           return

        if severity in range(ERROR, MISC) :
            msg = '%.6d %.8s' % (self.uid, self.uname)
            msg = msg + ' ' + sevdict[severity] + ' ' + self.i_am + ' '
            str = format % args
            msg = msg + ' ' + str
            if self.debug:
                print msg
            ticket = {'work' : 'log_message',
                      'message' : msg }
            lticket = self.csc.get(self.logger)
            self.u.send_no_wait(ticket, (lticket['host'], lticket['port']))
            return {"status" : "ok"}
        else :
            return {"status" : "wrong_severity_level"}
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
    def alive(self):
        lticket = self.csc.get("logserver")
        return  self.u.send({'work':'alive'},
                            (lticket['host'], lticket['port']))


if __name__ == "__main__" :
    Trace.init("log client")
    import getopt
    import socket
    import string

    # fill in defaults
    logc = LoggerClient()

    # see what the user has specified. bomb out if wrong options specified
    logc.parse_options()
    logc.csc.connect()

    if logc.doalive:
        ticket = logc.alive()

    elif logc.test:
        ticket = logc.send(ERROR, 1, "This is a test message %s %d", 'TEST', 3456)
        #ticket = logc.send(INFO, 21, "this is an INFO message")

    elif logc.logit:
        ticket = logit(logc.logmsg)

    if ticket['status'] != 'ok' :
        print "Bad status:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)
    elif list:
        pprint.pprint(ticket)
        sys.exit(0)
