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
import enstore_html
import enstore_files
import configuration_client


"""
be an active monitor server.

The sever is sensitive to the the global configuration server.  We
plan to run one active monitor server on each node of an enstore
cluster, with the possible exception of node liated in the
configuration item "veto nodes"

The sever has two functions:

1) as a server, perform a transfer to the monitor server client using
the same pattern, and some of the same code mechanisms used by the mover
in a READ_FROM_HSM. The ability of two interfaces to really talk to
each other and the data transfer rate are measured.

2) Collect measurements and flush them to an html file for display on
the web.  The logic of the monitor server client is such the the
measurements against all nodes of an enstore cluster are sent to a
designated monitor server, named in the config file, accessed via an
IP address stored in the configuration item "html_gen_host"

"""

MY_NAME = "Monitor_Server"

class MonitorServer(dispatching_worker.DispatchingWorker):

    def __init__(self, csc, html_dir, html_refresh_time):
	self.running = 0
	self.print_id = MY_NAME
        print "Monitor Server at %s %s" %(csc[0], csc[1])
        Trace.trace(10,
            "Monitor Server at %s %s" %(csc[0], csc[1]))
        dispatching_worker.DispatchingWorker.__init__(self, csc)

        self.html_dir = html_dir
        self.html_refresh_time = html_refresh_time
	self.running = 1

        # always nice to let the user see what she has
        Trace.trace(10, repr(self.__dict__))
        self.page = None

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
        os._exit(0)


    def _become_html_gen_host(self):
        #setup for HTML output if we are so stimulated by a client
        #self.page is None if we have not setup.
        if not self.page:
            self.page = enstore_html.EnActiveMonitorPage(
            ["Time", "user IP", "Enstore IP", "Blocks", "Bytes/Block",
		    "Time", "Rate (MB/S)"], self.html_refresh_time)
        else:
            pass #have already set up
        
    def recieve_measurement(self, ticket):
        self.reply_to_caller({"status" : ('ok', "")})
        self._become_html_gen_host() #setup for making html
        self.page.add_measurement(ticket["measurement"])

    def flush_measurements(self, ticket):
        self.reply_to_caller({"status" : ('ok', "")})
        self._become_html_gen_host()
        file = enstore_files.EnFile(self.html_dir + "/active_monitor.html")
        file.open()
        file.write(str(self.page))
        file.close()

        
class MonitorServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
      
        generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)

config = None

if __name__ == "__main__":

 
    Trace.init(MY_NAME)
    Trace.trace( 6, "called args="+repr(sys.argv) )
    import sys

    intf = MonitorServerInterface()
    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))
    config = csc.get('active_monitor')

    ##temp cmd line processing - should go through "interface"
    if len(sys.argv)>1:
        config['html_file'] = sys.argv[1]

    html_directory = config['html_file'] # this item seems to really be a dir
    ms = MonitorServer(('', config['server_port']),
                       html_directory,
                       config['html_refresh_time']
                       )

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






