#########################################################################
#                                                                       #
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
#  $Id$

import sys
import os
from SocketServer import UDPServer, TCPServer
from configuration_client import configuration_client
from callback import send_to_user_callback
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
import string
import time
import pprint
import socket

list = 0
# Log Methods Class
class LogMethods(DispatchingWorker) :

    def open_logfile(self, logfile_name) :
        # try to open log file for append
        if list :
            print "opening " + logfile_name
        try :
            self.logfile = open(logfile_name, 'a')
            if list :
                print "opened for append"
        except :
            self.logfile = open(logfile_name, 'w')
            if list :
                print "opened for write"

    # log the message recieved from the log client
    def log_message(self, ticket) :
        tm = time.localtime(time.time()) # get the local time
        # format log message
        message = "%.2d:%.2d:%.2d %-8s %s\n" % \
                  (tm[3], tm[4], tm[5],
                   socket.gethostbyaddr(self.reply_address[0])[0],
                   ticket['message'])

        if list:
            print message          # for test
        res = self.logfile.write(message)    # write log message to the file
        self.logfile.flush()
        if list :
            pprint.pprint(res)

"""Logger Class. Instance of this class is a log server. Multiple instances
   of this class can run using unique port numbers. But it actually is not
   recommended. It is assumed that the only one Log Server will serve the
   whole system.
"""
class Logger(LogMethods, GenericServer, UDPServer) :

    def serve_forever(self, logfile_dir_path) :   # overrides UDPServer method
        tm = time.localtime(time.time())          # get the local time
        day = current_day = tm[2];
        # form the log file name
        fn = 'LOG-%04d-%02d-%02d' % (tm[0], tm[1], tm[2])
        self.logfile_name = logfile_dir_path + "/" + fn
        # open log file
        self.open_logfile(self.logfile_name)
        while 1:
            self.handle_request() # this method will eventually call
                                  # log_message()

            # get local time
            tm = time.localtime(time.time())
            day = tm[2];
            # check if day has been changed
            if day != current_day :
                # day changed: close the current log file
                self.logfile.close()
                current_day = day;
                # and open the new one
                fn = 'LOG-%04d-%02d-%02d' % (tm[0], tm[1], tm[2])
                self.logfile_name = logfile_dir_path + "/" + fn
                self.open_logfile(self.logfile_name)

if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_file = ""
    config_list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_file="\
               ,"config_list","list","verbose","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--list" or opt == "--verbose":
            list = 1
        elif opt == "--help" :
            print "python ", options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port

    csc = configuration_client(config_host,config_port)

    keys = csc.get("logserver")
    if list :
        pprint.pprint(keys)
        pprint.pprint(args)
    logserver =  Logger((keys['host'], keys['port']), LogMethods)

    logserver.set_csc(csc)

    while 1:
        try:
            logserver.serve_forever(keys["log_file_path"])
        except:
            print time.strftime("%c",time.localtime(time.time())),\
                  sys.argv,sys.exc_info()[0],sys.exc_info()[1],"\ncontinuing"
            continue
