#
# system import
import sys
import time
import pprint
import copy

# enstore imports
import timeofday
import traceback
import callback
import log_client
import configuration_client
import volume_clerk_client
import dispatching_worker
import SocketServer
import generic_server
import udp_client
import Trace
import e_errors

class InquisitorMethods(dispatching_worker.DispatchingWorker):

    # update the enstore status file - to do this we must contact each of
    # the following and get their status.  
    #	file clerk
    #   admin clerk
    #   volume clerk
    #   configuration server
    #   library manager(s)
    #   media changer(s)
    #   mover(s)
    # then we write the status to the specified file

    # set the timeout value
    def set_timeouts(self, timeout):
        self.timeout = timeout

    # set the directory where we will create the files
    def set_file_dir(self, file_dir):
        self.file_dir = file_dir

    # set a new timeout value
    def set_timeout(self,ticket):
        Trace.trace(10,"{set_timeout "+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
        try:
           self.reply_to_caller(ticket)
        # even if there is an error - respond to caller so he can process it
        except:
           ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
           self.reply_to_caller(ticket)
           Trace.trace(0,"}set_timeout "+repr(ticket["status"]))
           return
        self.timeout = ticket["timeout"]
        Trace.trace(10,"}set_timeout")
        return

    # get the current timeout value
    def get_timeout(self,ticket):
        Trace.trace(10,"{get_timeout "+repr(ticket))
	ret_ticket = { 'timeout' : self.timeout,
	               'status'  : (e_errors.OK, None) }
        try:
           self.reply_to_caller(ret_ticket)
        # even if there is an error - respond to caller so he can process it
        except:
           ret_ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
           self.reply_to_caller(ret_ticket)
           Trace.trace(0,"}set_timeout "+repr(ret_ticket["status"]))
           return
        Trace.trace(10,"}get_timeout")
        return


class Inquisitor(InquisitorMethods,
                generic_server.GenericServer,
                SocketServer.UDPServer):
    pass

if __name__ == "__main__":
    import sys
    import getopt
    import string
    # Import SOCKS module if it exists, else standard socket module socket
    # This is a python module that works just like the socket module, but uses
    # the SOCKS protocol to make connections through a firewall machine.
    # See http://www.w3.org/People/Connolly/support/socksForPython.html or
    # goto www.python.org and search for "import SOCKS"
    try:
        import SOCKS; socket = SOCKS
    except ImportError:
        import socket
    Trace.init("inquisitor")
    Trace.trace(1,"inquisitor called with args "+repr(sys.argv))

    # defaults
    (config_hostname,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_host = ci[0]
    config_port = "7500"
    config_list = 0
    file_dir = ""
    timeout = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port="\
               ,"config_list","file_dir=","timeout=","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist:
        if opt == "--config_host":
            config_host = value
        elif opt == "--config_port":
            config_port = value
        elif opt == "--config_list":
            config_list = 1
        elif opt == "--file_dir":
            file_dir = value
        elif opt == "--timeout":
            timeout = value
        elif opt == "--help":
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    csc = configuration_client.ConfigurationClient(config_host,config_port,\
                                                    config_list)

    #   pretend that we are the test system
    #   remember, in a system, there is only one bfs
    #   get our port and host from the name server
    #   exit if the host is not this machine
    keys = csc.get("inquisitor")
    iq = Inquisitor( (keys["hostip"], keys["port"]), InquisitorMethods)
    iq.set_csc(csc)

    # if no timeout was entered on the command line, get it from the 
    # configuration file.
    try:
        timeout = keys["timeout"]
    except:
        timeout = 120
    iq.set_timeouts(timeout)

    # get the directory where the files we create will go.  this should
    # be in the configuration file.
    try:
        file_dir = keys["file_dir"]
    except:
        file_dir = "/tmp"
    iq.set_file_dir(file_dir)

    # get a logger
    logc = log_client.LoggerClient(csc, keys["logname"], 'logserver', 0)
    iq.set_logc(logc)
    indlst=['external_label']
    while 1:
        try:
            Trace.trace(1,'Inquisitor (re)starting')
            logc.send(log_client.INFO, 1, "Inquisitor (re)starting")
            iq.serve_forever()
        except:
            traceback.print_exc()
            format = timeofday.tod()+" "+\
                     str(sys.argv)+" "+\
                     str(sys.exc_info()[0])+" "+\
                     str(sys.exc_info()[1])+" "+\
                     "inquisitor serve_forever continuing"
            print format
            logc.send(log_client.ERROR, 1, format)
            Trace.trace(0,format)
            continue
    Trace.trace(1,"Inquisitor finished (impossible)")




