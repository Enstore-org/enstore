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

# send a message to the logger
def logit(message="HELLO", logname="LOGIT",config_host="", config_port=7510):

    try:
        if config_host == "" :
            (config_host,ca,ci) = socket.gethostbyaddr("pcfarm4.fnal.gov")

        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
	csc.connect()

        # get a logger
        logc = LoggerClient((config_host, config_port), logname,  'log_server')

        # send the message
        return Trace.log(e_errors.INFO, message)

    except:
        return  str(sys.exc_info()[0])+" "+str(sys.exc_info()[1])

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
                 servername = "log_server"): # log server name
        self.csc = csc
        # need the following definition so the generic client init does not
        # get another logger client
        self.is_logger = 1
        generic_client.GenericClient.__init__(self, csc, i_am_a)
        self.i_am = i_am_a
        self.uname = pwd.getpwuid(os.getuid())[0]
        self.log_priority = 7
	lticket = self.csc.get( servername )
	self.logger_address = (lticket['hostip'], lticket['port'])
        self.u = udp_client.UDPClient()
	Trace.set_log_func( self.log_func )
	self.lock = LoggerLock() 

    def log_func( self, time, pid, name, args ):
	#prevent log func from calling itself recursively
	if self.lock.test_and_set():    return

	severity = args[0]
	msg      = args[1]
	if severity > e_errors.MISC: severity = e_errors.MISC
	msg = '%.6d %.8s %s %s  %s' % (os.getpid(),self.uname,
				       e_errors.sevdict[severity],name,msg)
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


    # check on alive status
    def alive(self, rcv_timeout=0, tries=0):
        try:
            x = self.u.send( {'work':'alive'}, self.logger_address,
                             rcv_timeout, tries )
        except errno.errorcode[errno.ETIMEDOUT]:
            Trace.trace(14,"alive - ERROR, alive timed out")
            x = {'status' : (e_errors.TIMEDOUT, None)}
        return x

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
        self.logit = 0
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
                        "log_server")

    if intf.alive:
        ticket = logc.alive(intf.alive_rcv_timeout,intf.alive_retries)

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
        #ticket = logc.send(e_errors.INFO, 21, "this is an INFO message")

    elif intf.logit:
        ticket = logit(intf.logmsg)

    else:
	intf.print_help()
        sys.exit(0)

    del logc.csc.u
    del logc.u		# del now, otherwise get name exception (just for python v1.5???)

    logc.check_ticket(ticket)
