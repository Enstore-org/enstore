#########################################################################
#                                                                       #
# Log client.                                                           #
# This is a simple log client. It sends log messages to the log server  #
# via port specified in the Log Server dictionary entry in the enstore  #
# configuration file ( can be specified separately)                     #
#########################################################################
#  $Id$

import sys
import os
import pwd
import time
import select
import exceptions
import errno
from configuration_client import configuration_client
from udp_client import UDPClient
import pprint

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

class LoggerClient:

    def __init__(self,
                 configuration_client,
                 i_am_a,             # Abbreviated client instance name
                                     # try to make it of capital letters
                                     # not more than 8 characters long
                 servername,         # log server name
                 debug) :            # debug output
        self.csc = configuration_client
        self.u = UDPClient()
        self.i_am = i_am_a
        self.pid = os.getpid()
        self.uid = os.getuid()
	pwdb_entry = pwd.getpwuid(self.uid)
	self.uname = pwdb_entry[0]
        self.logger = servername
        self.debug = debug

    """ send the request to the Media Loader server and then send answer
    to user.
    This function takes arbitrary number of arguments. The mandatory arguments
    are:
       severity - see severity codes above
       format - any string which can contain formatters
    Example:
        send (ticket = logc.send (ERROR, 'Error: errno=%d, and its \
        interpretation is: %s', err, os.strerror(err))
    """
    def send (self, severity, format, *args) :
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

    # check on alive status
    def alive(self):
        lticket = self.csc.get("logserver")
        return  self.u.send({'work':'alive'},
                            (lticket['host'], lticket['port']))


if __name__ == "__main__" :
    import getopt
    import socket
    import string

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_file = ""
    config_list = 0
    list = 0
    alive = 0
    test = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_file=","config_list",
               "test","list","verbose","alive","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--test" :
            test = 1
        elif opt == "--alive" :
            alive = 1
        elif opt == "--list" or opt == "--verbose":
            list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options, "media_changer volume drive"
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if we number of arguments is wron
#    if len(args) < 3 :
#       print "python ",sys.argv[0], options, "media_changer volume drive"
#       print "   do not forget the '--' in front of each option"
#       sys.exit(1)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    logc = LoggerClient(csc, 'LOGCLNT', 'logserver', list)

    if alive:
        ticket = logc.alive()

    elif test:
        ticket = logc.send(ERROR, "This is a test message %s %d", 'TEST', 3456)
        #ticket = logc.send(INFO, "this is an INFO message")

    if ticket['status'] != 'ok' :
        print "Bad status:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)
    elif list:
        pprint.pprint(ticket)
        sys.exit(0)
