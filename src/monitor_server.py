#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import types
import os
import traceback
import pprint

# enstore imports
import dispatching_worker
import generic_server
import interface
import Trace
import e_errors
import hostaddr
import callback

MY_NAME = "Monitor_Server"

class MonitorServer(dispatching_worker.DispatchingWorker):

    def __init__(self, csc, configfile=interface.default_file()):
	self.running = 0
	self.print_id = MY_NAME
        print "Monitor Server at %s %s" %(csc[0], csc[1])
        Trace.trace(10,
            "Monitor Server at %s %s" %(csc[0], csc[1]))
        dispatching_worker.DispatchingWorker.__init__(self, csc)

	self.running = 1

        # always nice to let the user see what she has
        Trace.trace(10, repr(self.__dict__))

    def simulate_encp_transfer(self, ticket):
        ticket['status'] = ('ok', None)
        self.reply_to_caller(ticket)
        pid=self.fork()
        if pid: #parent
            return
        
        #simulate mover connecting on callback and a read_from_HSM transfer
        localhost, localport, well_known_sock = callback.get_data_callback(
            use_multiple=0,
            fixed_ip=ticket['remote_interface'],verbose=0)
        ticket['mover']={'callback_addr': (localhost,localport)}
        callback.send_to_user_callback(ticket)
        xfer_sock, address = well_known_sock.accept()
        well_known_sock.close()
        sendstr = "S"*ticket['block_size']
        for x in xrange(ticket['block_count']):
            xfer_sock.send(sendstr)
        xfer_sock.close()
            
class MonitorServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
      
        generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)


if __name__ == "__main__":

    pid=os.fork()
    if pid: #parent
        os.waitpid(pid,0)
        sys.exit(0)
    else: #child
        pid=os.fork()
        if pid:
            sys.exit(0)
        
            
    Trace.init(MY_NAME)
    Trace.trace( 6, "called args="+repr(sys.argv) )
    import sys

    # get the interface
    intf = MonitorServerInterface()
    # get a monitor server:
    intf.config_host = "" #listen on all interfaces on this machine
    intf.config_port = 9999 #HACK -- should ger port from config server
    ms = MonitorServer((intf.config_host, intf.config_port))
    sys.stdin.close()
    sys.stdout.close()
    sys.stderr.close()
    while 1:
        try:
            Trace.trace(6,"Monitor Server (re)starting")
            ms.serve_forever()
	except SystemExit, exit_code:
	    sys.exit(exit_code)
        except:
            exc,msg,tb=sys.exc_info()
            format = "%s %s %s %s %s: serve_forever continuing" % (
                timeofday.tod(),sys.argv,exc,msg,MY_NAME)
            Trace.log(e_errors.ERROR, str(format))

            continue

    Trace.trace(6,"Monitor Server finished (impossible)")






