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
import errno
import socket

#enstore imports
import generic_client
import configuration_client
import udp_client
import Trace
import e_errors

MY_NAME = "LOG_CLIENT"
MY_SERVER = "log_server"

# send a message to the logger
def logit(logc, message="HELLO", logname="LOGIT"):

    # reset our log name
    logc.log_name = logname
        
    # send the message
    Trace.log(e_errors.INFO, message)

    return {"status" : (e_errors.OK, None)}

class LoggerLock:
    def __init__(self):
	self.locked = 0
    def unlock(self):
	self.locked = 0
    def test_and_set(self):
	s = self.locked
	self.locked=1
	return s

class LoggerClient(generic_client.GenericClient):

    def __init__(self,
                 csc = 0,                    # get our own configuration client
                 i_am_a = MY_NAME,           # Abbreviated client instance name
                                             # try to make it capital letters
                                             # not more than 8 characters long
                 servername = MY_SERVER):    # log server name
        # need the following definition so the generic client init does not
        # get another logger client
        self.is_logger = 1
        generic_client.GenericClient.__init__(self, csc, i_am_a)
        self.log_name = i_am_a
        self.uname = pwd.getpwuid(os.getuid())[0]
        self.log_priority = 7
	lticket = self.csc.get( servername )
	self.logger_address = (lticket['hostip'], lticket['port'])
        self.log_dir = lticket.get("log_file_path", "")
        self.u = udp_client.UDPClient()
	Trace.set_log_func( self.log_func )
	self.lock = LoggerLock() 

    def log_func( self, time, pid, name, args ):
	#prevent log func from calling itself recursively
	if self.lock.test_and_set():    return

	severity = args[0]
	msg      = args[1]
        if self.log_name:
            ln = self.log_name
        else:
            ln = name
	if severity > e_errors.MISC: severity = e_errors.MISC
	msg = '%.6d %.8s %s %s  %s' % (os.getpid(),self.uname,
				       e_errors.sevdict[severity],ln,msg)
	ticket = {'work':'log_message', 'message':msg}
	self.u.send_no_wait( ticket, self.logger_address )
	return 	self.lock.unlock()

    def send( self, severity, priority, format, *args ):
	if args != (): format = format%args
	Trace.log( severity, format )
	return {"status" : (e_errors.OK, None)}

#
# priorty allows turning logging on and off in a server.
#  Coventions - setting log_priority to 0 should turn off all logging.
#             - default priority on send is 1 so the default is to log a message
#             - the default log_priority to test against is 10 so a priority
#                     send with priorty < 10 will normally be logged
#             - a brief trace message (1 per file per server should be priority 10
#             - file/server trace messages should 10> <20
#             - debugging should be > 20
    def set_logpriority(self, priority):
        self.log_priority = priority

    def get_logpriority(self):
        return self.log_priority

    # get the current log file name
    def get_logfile_name(self, rcv_timeout=0, tries=0):
        x = self.u.send( {'work':'get_logfile_name'}, self.logger_address,
			 rcv_timeout, tries )
        return x

    # get the last log file name
    def get_last_logfile_name(self, rcv_timeout=0, tries=0):
        x = self.u.send( {'work':'get_last_logfile_name'}, self.logger_address,
	                 rcv_timeout, tries )
        return x

class LoggerClientInterface(generic_client.GenericClientInterface):

    def __init__(self):
        self.config_file = ""
        self.test = 0
        self.logit = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.get_logfile_name = 0
	self.get_last_logfile_name = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return self.client_options()+\
               ["config_file=", "test", "logit="] +\
	       ["get_logfile_name", "get_last_logfile_name"]


    """ 
    This function takes two arguments:
       severity - see severity codes above
       msg      - any string
    Example:
        Trace.log( ERROR, 'Error: errno=%d, and its interpretation is: %s'%\
	(err,os.strerror(err)) )
    """


if __name__ == "__main__" :
    import sys
    Trace.init(MY_NAME)
    Trace.trace(6,"logc called with args "+repr(sys.argv))

    # fill in interface
    intf = LoggerClientInterface()

    # get a log client
    logc = LoggerClient((intf.config_host, intf.config_port), MY_NAME,
                        MY_SERVER)

    if intf.alive:
        ticket = logc.alive(MY_SERVER, intf.alive_rcv_timeout,
                            intf.alive_retries)

    elif intf.get_last_logfile_name:
        ticket = logc.get_last_logfile_name(intf.alive_rcv_timeout,\
	                                    intf.alive_retries)
	print(ticket['last_logfile_name'])

    elif intf.get_logfile_name:
        ticket = logc.get_logfile_name(intf.alive_rcv_timeout,\
	                               intf.alive_retries)
	print(ticket['logfile_name'])

    elif intf.test:
        Trace.log( e_errors.ERROR,
		   "This is a test message %s %d"%('TEST',3456) )
        ticket = {}

    elif intf.logit:
        ticket = logit(logc, intf.logit)

    else:
	intf.print_help()
        sys.exit(0)

    del logc.csc.u
    del logc.u		# del now, otherwise get name exception (just for python v1.5???)

    logc.check_ticket(ticket)
