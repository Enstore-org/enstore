#!/usr/bin/env python

"""
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
"""

# system imports
import sys
import os
import time
import string
import glob
import pwd
import threading
import socket
import fcntl
import select
import cPickle

#enstore imports
import dispatching_worker
import generic_server
#import event_relay_client
import monitored_server
import enstore_constants
import event_relay_messages
import e_errors
import hostaddr
import Trace
import log_client
import option
import callback

MY_NAME = enstore_constants.LOG_SERVER   #"log_server"
FILE_PREFIX = "LOG-"
NO_MAX_LOG_FILE_SIZE = -1L
MAX_TCP_MESSAGE_SIZE = enstore_constants.MB*10

def format_date(tm=None):
    if not tm:
	tm = time.localtime(time.time())          # get the local time
    return '%04d-%02d-%02d' % (tm[0], tm[1], tm[2])

class Logger(  dispatching_worker.DispatchingWorker
	     , generic_server.GenericServer):

    """
    Instance of this class is a log server. Multiple instances
    of this class can run using unique port numbers. But it actually is not
    recommended. It is assumed that the only one Log Server will serve the
    whole system.
    """
    def __init__(self, csc, test=0):
	flags = enstore_constants.NO_LOG
        generic_server.GenericServer.__init__(self, csc, MY_NAME, flags=flags,
                                              function = self.handle_er_msg)
        self.repeat_count = 0
        self.index = 1
        self.last_message = ''
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        keys = self.csc.get(MY_NAME)
        Trace.init(self.log_name)
        Trace.set_log_func(self.log_func)  #Log function for itself.
        self.max_queue_size = keys.get("max_queue_size") # maximum size of incoming message queue
	self.alive_interval = monitored_server.get_alive_interval(self.csc,
								  MY_NAME,
								  keys)

        self.use_raw_input = keys.get('use_raw_input') # use raw input to buffer incoming messages

        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'],
	                                              keys['port']),
                                                      use_raw=self.use_raw_input)

        if keys["log_file_path"][0] == '$':
	    tmp = keys["log_file_path"][1:]
	    try:
	        tmp = os.environ[tmp];
	    except:
	        Trace.log(12, "log_file_path '%s' configuration ERROR"
                          %(keys["log_file_path"]))
	        sys.exit(1)
	    self.logfile_dir_path = tmp
	else:
	    self.logfile_dir_path =  keys["log_file_path"]
        if not os.path.exists(self.logfile_dir_path):
            try:
                os.makedirs(self.logfile_dir_path)
            except:
                exc, msg, tb = sys.exc_info()
                print "Can not create %s. %s %s"%(self.logfile_dir_path, exc, msg)
                sys.exit(1)

	self.test = test

        # get the value for max size of a log file
        self.max_log_file_size = keys.get(enstore_constants.MAX_LOG_FILE_SIZE,
                                          NO_MAX_LOG_FILE_SIZE)

        # see if no debug log is desired
        self.no_debug = keys.has_key(enstore_constants.NO_DEBUG_LOG)

	# get the dictionary of ancillary log files
	self.msg_type_logs = keys.get('msg_type_logs', {})
	self.msg_type_keys = self.msg_type_logs.keys()
	self.extra_logfiles = {}
        self.lock = threading.Lock()

        # setup the communications with the event relay task
        self.erc.start([event_relay_messages.NEWCONFIGFILE])
	# start our heartbeat to the event relay process
	self.erc.start_heartbeat(enstore_constants.LOG_SERVER,
				 self.alive_interval)

    #This is the function for Trace.log to use from withing the log server.
    def log_func( self, time, pid, name, args ):
        #Even though this implimentation of log_func() does not use the time
        # parameter, others will.
        __pychecker__ = "unusednames=time"

        #Note: The code to create the variable ticket was taken from
        # the log client.

        severity = args[0]
	msg      = args[1]
        if severity > e_errors.MISC:
            msg = '%s %s' % (severity, msg)
            severity = e_errors.MISC

	msg = '%.6d %.8s %s %s  %s' % (pid, pwd.getpwuid(os.getuid())[0],
				       e_errors.sevdict[severity], name, msg)
	ticket = {'work':'log_message', 'message':msg}

        #Use the same function to write to the log file as it uses for
        # everythin else.
        self.log_message(ticket)

    def open_extra_logs(self, mode='a+'):
	for msg_type in self.msg_type_keys:
	    filename = self.msg_type_logs[msg_type]
	    file_path = "%s/%s%s"%(self.logfile_dir_path, filename,
				    format_date())
	    self.extra_logfiles[msg_type] = open(file_path, mode)

    def close_extra_logs(self):
	for msg_type in self.msg_type_keys:
	    self.extra_logfiles[msg_type].close()
	else:
	    self.extra_logfiles = {}

    def open_logfile(self, logfile_name) :
        dirname, filename = os.path.split(logfile_name)
        debug_file = "DEBUG%s" % (filename,)
        debug_file_name = os.path.join(dirname,debug_file)
        # try to open log file for append
        try:
            if not os.path.exists(dirname):
                os.mkdir(dirname)
            with self.lock:
                self.logfile = open(logfile_name, 'a+')
                if not self.no_debug:
                    self.debug_logfile = open(debug_file_name, 'a+')
                self.open_extra_logs('a+')
        except Exception, detail:
            message="cannot open log %s: %s"%(logfile_name, detail)
            try:
                print  message
                sys.stderr.write("%s\n" % message)
                sys.stderr.flush()
                sys.stdout.flush()
            except IOError:
                pass
            os._exit(1)

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

    # return the requested list of logfile names
    def get_logfiles(self, ticket):
	ticket["status"] = (e_errors.OK, None)
	period = ticket.get("period", "today")
	vperiod_keys = log_client.VALID_PERIODS.keys()
	if period in vperiod_keys:
	    num_files_to_get = log_client.VALID_PERIODS[period]
	    files = os.listdir(self.logfile_dir_path)
	    # we want to take the latest files so sort them in reverse order
	    files.sort()
	    files.reverse()
	    num_files = 0
	    lfiles = []
	    for fname in files:
		if fname[0:4] == FILE_PREFIX:
		    lfiles.append("%s/%s"%(self.logfile_dir_path,fname))
		    num_files = num_files +1
		    if num_files >= num_files_to_get and  not period == "all":
			break
	else:
	    # it was not a shortcut keyword so we assume it is a string of the
	    # form LOG*, use globbing to get the list
	    files = "%s/%s"%(self.logfile_dir_path, period)
	    lfiles = glob.glob(files)
	ticket["logfiles"] = lfiles
        self.send_reply(ticket)

    def is_encp_xfer_msg(self, msg):
	if (string.find(msg, Trace.MSG_ENCP_XFER) == -1) and \
	   (string.find(msg, " E ENCP") == -1):
	    # sub-string not found
	    rtn = 0
	else:
	    rtn = 1
	return rtn

    def write_to_extra_logfile(self, message):
        for msg_type in self.msg_type_keys:
            if not string.find(message, msg_type) == -1:
                # this message has a message type of msg_type
                self.extra_logfiles[msg_type].write(message)
                return

    # log the message recieved from the log client
    def log_message(self, ticket) :
        if not ticket.has_key('message'):
            return
        if 'sender' in ticket: # ticket came over tcp
            host = ticket['sender']
        else:
            host = hostaddr.address_to_name(self.reply_address[0])
                  ## XXX take care of case where we can't figure out the host name
        # determine what type of message is it
        message_type = string.split(ticket['message'])[2]
        message = "%-8s %s"%(host,ticket['message'])
        tm = time.localtime(time.time()) # get the local time
        if message == self.last_message:
            self.repeat_count=self.repeat_count+1
            return
        elif self.repeat_count:
            if message_type != e_errors.sevdict[e_errors.MISC]:
                self.logfile.write("%.2d:%.2d:%.2d last message repeated %d times\n"%
                                   (tm[3],tm[4],tm[5], self.repeat_count))
            if not self.no_debug:
                self.debug_logfile.write("%.2d:%.2d:%.2d last message repeated %d times\n"%
                                         (tm[3],tm[4],tm[5], self.repeat_count))
            self.repeat_count=0
        self.last_message=message

	# alert the event relay if we got an encp transfer message.
	if self.is_encp_xfer_msg(message):
	    Trace.notify(event_relay_messages.ENCPXFER)

        # format log message
        message = "%.2d:%.2d:%.2d %s\n" %  (tm[3], tm[4], tm[5], message)

        with self.lock: # to synchronize logging threads
            try:
                if message_type !=  e_errors.sevdict[e_errors.MISC]:
                    res = self.logfile.write(message)    # write log message to the file
                    self.logfile.flush()
                if not self.no_debug:
                    res = self.debug_logfile.write(message)    # write log message to the file
                if message_type !=  e_errors.sevdict[e_errors.MISC]:
                    self.write_to_extra_logfile(message)
            except:
                exc, value, tb = sys.exc_info()
                for l in traceback.format_exception( exc, value, tb ):
                    print l

    def check_for_extended_files(self, filename):
        if not self.max_log_file_size == NO_MAX_LOG_FILE_SIZE:
            file_l = os.listdir(self.logfile_dir_path)
            # pull out all the files that match the current name at a min
            size = len(filename)
            matching_l = []
            for fname in file_l:
                if fname[:size] == filename:
                    matching_l.append(fname)
            else:
                if matching_l:
                    matching_l.sort()
                    # set next file to be 1 greater than latest one
                    # (size+1 so can skip ".")
                    if matching_l[-1] != filename:
                        self.index = int(matching_l[-1][size+1:]) + 1
                    return matching_l[-1]
        return filename

    def serve_forever(self):                      # overrides UDPServer method
        self.repeat_count=0
        self.last_message=''
        tm = time.localtime(time.time())          # get the local time
        day = current_day = tm[2];
        if self.test :
            min1 = current_min = tm[4]
        # form the log file name
        fn = '%s%s' % (FILE_PREFIX, format_date(tm))
        if self.test:
            ft = '-%02d-%02d' % (tm[3], tm[4])
            fn = fn + ft

        # check for any of the extra log files (.1, .2 ...) open the latest
        # one of these if they exist.
        fn2 = self.check_for_extended_files(fn)
        self.logfile_name = self.logfile_dir_path + "/" + fn2
        self.logfile_name_orig = self.logfile_dir_path + "/" + fn
	self.last_logfile_name = ""
        # make sure file is not greater than max
        try:
            size = os.stat(self.logfile_name)[6]
        except OSError:
            # don't worry if file did not exist
            size = 0
        if not self.max_log_file_size == NO_MAX_LOG_FILE_SIZE and \
           size >= self.max_log_file_size:
            self.logfile_name = "%s.%s"%(self.logfile_name_orig, self.index)
            self.index = self.index + 1
        # open log file
        self.open_logfile(self.logfile_name)

        if self.use_raw_input:
        # prepare raw input
            self.set_out_file()
            self.raw_requests.set_caller_name(self.name)
            self.raw_requests.set_use_queue()
            if self.max_queue_size:
                # Reset incoming message queue.
                self.raw_requests.set_max_queue_size(self.max_queue_size)

            # start receiver thread or process
            self.raw_requests.receiver()

        while 1:
            self.do_one_request()
            # get local time
            tm = time.localtime(time.time())
            day = tm[2]
            if self.test :
                min1 = tm[4]
            # if test flag is not set reopen log file at midnight
            if not self.test :
                # check if day has been changed
                if day != current_day :
                    # day changed: close the current log file
                    self.logfile.close()
                    if not self.no_debug:
                        self.debug_logfile.close()
		    self.close_extra_logs()
	            self.last_logfile_name = self.logfile_name
                    current_day = day;
                    self.index = 1
                    # and open the new one
                    fn = '%s%04d-%02d-%02d' % (FILE_PREFIX, tm[0], tm[1], tm[2])
                    self.logfile_name = self.logfile_dir_path + "/" + fn
                    self.logfile_name_orig = self.logfile_name
                    self.open_logfile(self.logfile_name)
                # check if current log file is > config specified value.
                # if no value in config file, do nothing
                elif self.max_log_file_size != NO_MAX_LOG_FILE_SIZE:
                    # get current file size
                    size = os.stat(self.logfile_name)[6]
                    if size >= self.max_log_file_size:
                        self.logfile.close()
                        if not self.no_debug:
                            self.debug_logfile.close()
                        self.close_extra_logs()
                        # and open the new one
                        self.logfile_name = "%s.%s"%(self.logfile_name_orig, self.index)
                        self.index = self.index + 1
                        self.open_logfile(self.logfile_name)
            else :
                # if test flag is set reopen log file every minute
                if min1 != current_min :
                    # minute changed: close the current log file
                    self.logfile.close()
                    if not self.no_debug:
                        self.debug_logfile.close()
                    current_min = min;
                    # and open the new one
                    fn = '%s%04d-%02d-%02d' % (FILE_PREFIX, tm[0], tm[1],
					       tm[2])
                    ft = '-%02d-%02d' % (tm[3], tm[4])
                    fn = fn + ft
                    self.logfile_name = self.logfile_dir_path + "/" + fn
                    self.logfile_name_orig = self.logfile_name
                    self.open_logfile(self.logfile_name)


    def serve_tcp_clients_recv(self):
        while True:
            if hasattr(self, 'rcv_sockets') and len(self.rcv_sockets) != 0:
                r, w, ex = select.select(self.rcv_sockets, [], [], 10)
                if r:
                    while len(r) > 0:
                        s = r.pop(0)
                        try:
                            fds, junk, junk = select.select([s], [], [], 0.1)
                            if s in fds:
                                data = s.recv(MAX_TCP_MESSAGE_SIZE)
                                if data:
                                    ticket = cPickle.loads(data)
                                    self.log_message(ticket)
                                else:
                                    self.rcv_sockets.remove(s)
                                    break
                            else:
                                self.rcv_sockets.remove(s)
                                break

                        except e_errors.EnstoreError, detail:
                            print "EXCEPT", detail
                            self.rcv_sockets.remove(s)
                            break

    def serve_tcp_clients_enstore(self):
        while True:
            if hasattr(self, 'rcv_sockets') and len(self.rcv_sockets) != 0:
                r, w, ex = select.select(self.rcv_sockets, [], [], 10)
                if r:
                    while len(r) > 0:
                        s = r.pop(0)
                        try:
                            ticket = callback.read_tcp_obj_new(s, timeout=.1, exit_on_no_socket=True)
                            if ticket:
                                #print "TICKET", ticket
                                self.log_message(ticket)
                            else:
                                self.rcv_sockets.remove(s)
                                break
                        except e_errors.EnstoreError, detail:
                            print "EXCEPT", detail
                            self.rcv_sockets.remove(s)
                            break

    serve_tcp_clients = serve_tcp_clients_enstore

    def tcp_server(self):
        print "STARTING TCP SERVER"
        address_family = socket.getaddrinfo(self.server_address[0], None)[0][0]
        listen_socket = socket.socket(address_family, socket.SOCK_STREAM)
        listen_socket.bind(self.server_address)
        self.rcv_sockets = []
        listen_socket.listen(4)
        while True:
            s, addr = listen_socket.accept()
            flags = fcntl.fcntl(s.fileno(), fcntl.F_GETFL)
            fcntl.fcntl(s.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)
            if hostaddr.allow(addr):
                self.rcv_sockets.append(s)

class LoggerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
	self.config_file = ""
	self.test = 0
        generic_server.GenericServerInterface.__init__(self)

    def valid_dictionaries(self):
        return generic_server.GenericServerInterface.valid_dictionaries(self) \
               + (self.logger_options,)

    logger_options = {
        option.CONFIG_FILE:{option.HELP_STRING:
                            "specifies the configuration file to use",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
			    option.USER_LEVEL:option.ADMIN
                            }
        }

    """
    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)+\
            ["config-file=", "test"]
    """

def thread_is_running(thread_name):
    """
    check if named thread is running

    :type thread_name: :obj:`str`
    :arg thread_name: thread name
    """

    threads = threading.enumerate()
    for thread in threads:
        if ((thread.getName() == thread_name) and thread.isAlive()):
            return True
    else:
        return False

if __name__ == "__main__" :
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = LoggerInterface()

    logserver = Logger((intf.config_host, intf.config_port), intf.test)
    logserver.handle_generic_commands(intf)
    #logserver._do_print({'levels':range(5, 400)})

    while 1:
        try:
            tn =  "log_server_udp"
            if not thread_is_running(tn):
                print "STARTING", tn
                dispatching_worker.run_in_thread(tn, logserver.serve_forever)
            tn =  "log_server_tcp_connections"
            if not thread_is_running(tn):
                print "STARTING", tn
                dispatching_worker.run_in_thread(tn, logserver.tcp_server)
            tn =  "log_server_tcp"
            if not thread_is_running(tn):
                print "STARTING", tn
                dispatching_worker.run_in_thread(tn,logserver.serve_tcp_clients)
	except SystemExit, exit_code:
	    sys.exit(exit_code)
        except:
	    logserver.serve_forever_error(logserver.log_name)
            continue
        time.sleep(10)
