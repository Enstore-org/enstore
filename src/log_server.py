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
import time
import string

#enstore imports
import dispatching_worker
import generic_server
import e_errors
import hostaddr
import Trace

"""Logger Class. Instance of this class is a log server. Multiple instances
   of this class can run using unique port numbers. But it actually is not
   recommended. It is assumed that the only one Log Server will serve the
   whole system.
"""
MY_NAME = "log_server"

class Logger(  dispatching_worker.DispatchingWorker
	     , generic_server.GenericServer):

    def __init__(self, csc, test=0):
        # need the following definition so the generic client init does not
        # get a logger client
        self.is_logger = 1
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        self.repeat_count = 0
        self.last_message = ''
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        keys = self.csc.get(MY_NAME)
        Trace.init(self.log_name)
        Trace.trace(12, repr(keys))
        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'],
	                                              keys['port']))
        if keys["log_file_path"][0] == '$':
	    tmp = keys["log_file_path"][1:]
	    try:
	        tmp = os.environ[tmp];
	    except:
	        Trace.log(12, "log_file_path '%s' configuration ERROR"\
                          %keys["log_file_path"])
	        sys.exit(1)
	    self.logfile_dir_path = tmp
	else:
	    self.logfile_dir_path =  keys["log_file_path"]
	self.test = test

    def open_logfile(self, logfile_name) :
        # try to open log file for append
        try:
            self.logfile = open(logfile_name, 'a')
            Trace.trace(13, "opened for append")
        except :
	    try:
		self.logfile = open(logfile_name, 'w')
	    except:
	        Trace.trace(12, "Can not open log %s"%logfile_name)
		os._exit(1)
            Trace.trace(13, "opened for write")

    # return the current log file name
    def get_logfile_name(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        ticket["logfile_name"] = self.logfile_name
        self.send_reply(ticket)

    # return the last log file name
    def get_last_logfile_name(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        ticket["last_logfile_name"] = self.last_logfile_name
        self.send_reply(ticket)

    # log the message recieved from the log client
    def log_message(self, ticket) :
        if not ticket.has_key('message'):
            return
        host = hostaddr.address_to_name(self.reply_address[0])
                  ## XXX take care of case where we can't figure out the host name
        message = "%-8s %s"%(host,ticket['message'])
        tm = time.localtime(time.time()) # get the local time
        if message == self.last_message:
            self.repeat_count=self.repeat_count+1
        elif self.repeat_count:
            self.logfile.write("%.2d:%.2d:%.2d last message repeated %d times\n"%
                               (tm[3],tm[4],tm[5], self.repeat_count))
            self.logfile.flush()
            self.repeat_count=0
        self.last_message=message



        # format log message
        message = "%.2d:%.2d:%.2d %s\n" %  (tm[3], tm[4], tm[5], message)
        
        Trace.trace(12, message)
        res = self.logfile.write(message)    # write log message to the file
        self.logfile.flush()
        Trace.trace(12, "%s"%res)

    def serve_forever(self):                      # overrides UDPServer method
        self.repeat_count=0
        self.last_message=''
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
	self.last_logfile_name = ""
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
	            self.last_logfile_name = self.logfile_name
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


class LoggerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
	self.config_file = ""
	self.test = 0
        generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)+\
	       ["config_file=", "test"]


if __name__ == "__main__" :
    Trace.init(string.upper(MY_NAME))
    Trace.trace(6,"log server called with args "+repr(sys.argv))

    # get the interface
    intf = LoggerInterface()

    logserver = Logger((intf.config_host, intf.config_port), intf.test)

    while 1:
        try:
            Trace.trace(6,'Log Server (re)starting')
            logserver.serve_forever()
        except:
	    logserver.serve_forever_error(logserver.log_name)
            Trace.trace(6,"log_server main loop exception")
            continue
    Trace.trace(6,"Log Server finished (impossible)")
