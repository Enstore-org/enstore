###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
# Log Server.                                                           #
# Receives log messages form the client process and logs them into      #
# the log file.                                                         #
# Log file is being open for append in the directory specified in the   #
# corresponding entry in the configuration dictionary (sever does not   #
# take any arguments so far).                                           #
# The log file has a name LOG-YYYY-MM-DD                                #
# where:                                                                #
#     YYYY - is four digit year number                                  #
#     MM - month of the year (1 - 12)                                   #
#     DD - day of the month                                             #
# at midnight currently open log file gets closed and another one is    #
# open.                                                                 #
# Format of the message in the dictionary is as follows:                #
# HH:MM:SS HOST PID UID SL CLNTABBREV MESSAGE                           #
# where:                                                                #
#    HH:MM:SS - Log Server time when the message has been received      #
#    HOST - name of the host where client sending the message runs      #
#    PID - Process ID of the client which sent a message                #
#    UID - User ID of the client                                        #
#    SL - severity level abbreviation (see client code)                 #
#    MESSAGE - arbitrary message received from the client               #
#########################################################################

# system imports
import sys
import os
import string
import time
import traceback

#enstore imports
import configuration_client
import dispatching_worker
import generic_server
import generic_cs
import interface
import timeofday
import socket
import Trace

"""Logger Class. Instance of this class is a log server. Multiple instances
   of this class can run using unique port numbers. But it actually is not
   recommended. It is assumed that the only one Log Server will serve the
   whole system.
"""
class Logger(  dispatching_worker.DispatchingWorker
	     , generic_server.GenericServer):

    def __init__(self, csc=0, host=interface.default_host(), \
                 port=interface.default_port(), test=0, verbose=0):
        Trace.trace(10, '{__init__')
	self.print_id = "LOGGERS"
        # get the config server
        configuration_client.set_csc(self, csc, host, port, verbose)
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        keys = self.csc.get("logserver")
	self.enprint(keys, generic_cs.SERVER, verbose)
        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'],
	                                              keys['port']))
        if keys["log_file_path"][0] == '$':
	    tmp = keys["log_file_path"][1:]
	    try:
	        tmp = os.environ[tmp];
	    except:
	        self.enprint("log_file_path '"+keys["log_file_path"]+\
	                     "' configuration ERROR")
	        sys.exit(1)
	    self.logfile_dir_path = tmp
	else:
	    self.logfile_dir_path =  keys["log_file_path"]
	self.test = test
	self.verbose = verbose
        Trace.trace(10, '}__init__')

    def open_logfile(self, logfile_name) :
        # try to open log file for append
	self.enprint("opening "+logfile_name, generic_cs.SERVER, self.verbose)
        try:
            self.logfile = open(logfile_name, 'a')
	    self.enprint("opened for append ", generic_cs.SERVER, self.verbose)
        except :
	    try:
		self.logfile = open(logfile_name, 'w')
	    except:
	        self.enprint("Can not open log "+logfile_name)
		os._exit(1)
	    self.enprint("opened for write", generic_cs.SERVER, self.verbose)


    # log the message recieved from the log client
    def log_message(self, ticket) :
        tm = time.localtime(time.time()) # get the local time
	# take care of case where we can't figure out the host name
	try:
	    host = socket.gethostbyaddr(self.reply_address[0])[0]
	except:
	    host = str(sys.exc_info()[1])
        # format log message
        message = "%.2d:%.2d:%.2d %-8s %s\n" % \
                  (tm[3], tm[4], tm[5],
                   host,
                   ticket['message'])

	self.enprint(message, generic_cs.SERVER, self.verbose)
        res = self.logfile.write(message)    # write log message to the file
        self.logfile.flush()
	self.enprint(res, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	             self.verbose)

    def serve_forever(self):                      # overrides UDPServer method
        tm = time.localtime(time.time())          # get the local time
        day = current_day = tm[2];
        if self.test :
            min = current_min = tm[4]
        # form the log file name
        fn = 'LOG-%04d-%02d-%02d' % (tm[0], tm[1], tm[2])
        if self.test:
            ft = '-%02d-%02d' % (tm[3], tm[4])
            fn = fn + ft

        self.logfile_name = self.logfile_dir_path + "/" + fn
        # open log file
        self.open_logfile(self.logfile_name)
        while 1:
            self.handle_request() # this method will eventually call
                                  # log_message()

            # get local time
            tm = time.localtime(time.time())
            day = tm[2];
            if self.test :
                min = tm[4]
            # if test flag is not set reopen log file at midnight
            if not self.test :
                # check if day has been changed
                if day != current_day :
                    # day changed: close the current log file
                    self.logfile.close()
                    current_day = day;
                    # and open the new one
                    fn = 'LOG-%04d-%02d-%02d' % (tm[0], tm[1], tm[2])
                    self.logfile_name = self.logfile_dir_path + "/" + fn
                    self.open_logfile(self.logfile_name)
            else :
                # if test flag is set reopen log file every minute
                if min != current_min :
                    # minute changed: close the current log file
                    self.logfile.close()
                    current_min = min;
                    # and open the new one
                    fn = 'LOG-%04d-%02d-%02d' % (tm[0], tm[1], tm[2])
                    ft = '-%02d-%02d' % (tm[3], tm[4])
                    fn = fn + ft
                    self.logfile_name = self.logfile_dir_path + "/" + fn
                    self.open_logfile(self.logfile_name)


class LoggerInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{logi.__init__')
        # fill in the defaults for possible options
	self.config_file = ""
	self.verbose = 0
	self.test = 0
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()
        Trace.trace(10,'}logi.__init__')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16, "{}options")
        return self.config_options()+\
	       ["config_file=", "verbose=", "test"] +\
               self.help_options()


if __name__ == "__main__" :
    import getopt
    import socket
    Trace.init("log server")
    Trace.trace(1,"log server called with args "+repr(sys.argv))

    # get the interface
    intf = LoggerInterface()

    logserver = Logger(0, intf.config_host, \
	               intf.config_port, intf.test, intf.verbose)

    while 1:
        try:
            Trace.trace(1,'Log Server (re)starting')
            logserver.serve_forever()
        except:
	    logserver.serve_forever_error("log server")
            Trace.trace(0,format)
            continue
    Trace.trace(1,"Log Server finished (impossible)")
