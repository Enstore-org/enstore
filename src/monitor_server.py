#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import types
import os
import socket
import traceback
import pprint
import time
import enstore_functions

# enstore imports
import dispatching_worker
import generic_server
import interface
import Trace
import e_errors
import hostaddr
import socket_ext
import callback
import enstore_html
import enstore_files
import configuration_client
import timeofday
import enstore_constants
import udp_client

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

MY_NAME = "MNTR_SRV"

class MonitorServer(dispatching_worker.DispatchingWorker, generic_server.GenericServer):

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
        
        #simulate mover connecting on callback and a read_from_HSM transfer
        localhost, localport, well_known_sock = callback.get_callback(
            verbose=0)
        well_known_sock.listen(4)
        ticket['mover']={'callback_addr': (localhost,localport)}
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(ticket['callback_addr'])
        callback.write_tcp_obj(sock,ticket)
        sock.close()
        xfer_sock, address = well_known_sock.accept()
        xfer_ip = ticket['remote_interface'] ## XXX this is such a stupid name
                                                                  ## because it's not "remote" on this end
                                                                  ## Sigh...

        interface=hostaddr.interface_name(xfer_ip)
        if interface:
            status=socket_ext.bindtodev(xfer_sock.fileno(),interface)
            if status:
                Trace.log(e_errors.ERROR, "bindtodev(%s): %s"%(interface,os.strerror(status)))
        
        well_known_sock.close()

        #Now that all of the socket connections have been opened, let the
        # transfers begin.
        #When sending, the time isn't important.
        if ticket['transfer'] == "send_from_server":
            sendstr = "S"*ticket['block_size']
            for x in xrange(ticket['block_count']):
                xfer_sock.send(sendstr)
            ticket['elapsed'] = -1
        #Since we are recieving the data, recording the time is important.
        elif ticket['transfer'] == "send_to_server":
            data=xfer_sock.recv(1)
            if not data:
                raise "Server closed connection"
            bytes_received=len(data)
            t0=time.time()
            while bytes_received < ticket['block_size']*ticket['block_count']:
                data = xfer_sock.recv(ticket['block_size'])
                if not data: #socket is closed
                    raise "Server closed connection"
                bytes_received=bytes_received+len(data)
            ticket['elapsed']=time.time()-t0

        ticket['status'] = ('ok', None)
        self.reply_to_caller(ticket)
        xfer_sock.close()

    def _become_html_gen_host(self):
        #setup for HTML output if we are so stimulated by a client
        #self.page is None if we have not setup.
        if not self.page:
            self.page = enstore_html.EnActiveMonitorPage(
                ["Time", "user IP", "Enstore IP",
#                 "Blocks", "Bytes/Block", "Read Time", "Write Time",
                 "Read Rate (MB/S)", "Write Rate (MB/S)"],
                self.html_refresh_time)
        else:
            pass #have already set up

    def recieve_measurement(self, ticket):
        self.reply_to_caller({"status" : ('ok', "")})
        self._become_html_gen_host() #setup for making html
        self.page.add_measurement(ticket["measurement"])

    def flush_measurements(self, ticket):
        self.reply_to_caller({"status" : ('ok', "")})
        self._become_html_gen_host()
        file = enstore_files.EnFile("%s/%s"%(self.html_dir, 
					     enstore_constants.NETWORKFILE))
        file.open()
        file.write(str(self.page))
        file.close()

class MonitorServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        self.html_dir = None
        generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)+\
               self.alive_options() + ["html-dir="]

config = None

if __name__ == "__main__":

 
    Trace.init(MY_NAME)
    Trace.trace( 6, "called args="+repr(sys.argv) )
    import sys

    intf = MonitorServerInterface()
    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))
    config = csc.get('monitor')

    #If the command line is specified, then
    if intf.html_dir:
        html_directory = intf.html_dir
    else:
        html_directory = config['html_dir']

    ms = MonitorServer(('', config['port']),
                       html_directory,
                       config['refresh']
                       )
    ms.handle_generic_commands(intf)

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
