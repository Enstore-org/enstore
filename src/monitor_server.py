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
import select
import traceback
import pprint
import time
import fcntl, FCNTL
import errno

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
import enstore_functions
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

SEND_TO_SERVER = "send_to_server"
SEND_FROM_SERVER = "send_from_server"
SERVER_CLOSED_CONNECTION = "Server closed connection"
CLIENT_CLOSED_CONNECTION = "Client closed connection"

class MonitorServer(dispatching_worker.DispatchingWorker, generic_server.GenericServer):

    def __init__(self, csc):
        self.timeout = 10
	self.running = 0
	self.print_id = MY_NAME
        print "Monitor Server at %s %s" %(csc[0], csc[1])
        Trace.trace(10,
            "Monitor Server at %s %s" %(csc[0], csc[1]))
        dispatching_worker.DispatchingWorker.__init__(self, csc)

	self.running = 1

        #If socket.MSG_DONTWAIT isn't there add it, because should be.
        if not hasattr(socket, "MSG_DONTWAIT"): #Python 1.5 test
            socket.MSG_DONTWAIT = None
            host_type = os.uname()[0]
            if host_type == 'Linux':
                socket.MSG_DONTWAIT = 64
            elif host_type[:4]=='IRIX':
                socket.MSG_DONTWAIT = 128

        # always nice to let the user see what she has
        Trace.trace(10, repr(self.__dict__))
        self.page = None

    def simulate_encp_transfer(self, ticket):
        reply = {'status'     : (None, None),
                 'block_size' : ticket['block_size'],
                 'block_count': ticket['block_count']}
        
        #simulate mover connecting on callback and a read_from_HSM transfer
        data_ip = ticket['remote_interface'] ## XXX this is a silly name
                                             ## because it's not "remote"
                                             ## on this end.  Sigh...

        localhost, localport, listen_sock = callback.get_callback(ip=data_ip)
        listen_sock.listen(1)
        ticket['mover']={'callback_addr': (localhost,localport)}

        #Create the TCP socket, remeber the current settings (to reset them
        # back later) and set the new file control flags.
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        flags = fcntl.fcntl(sock.fileno(), FCNTL.F_GETFL)
        fcntl.fcntl(sock.fileno(), FCNTL.F_SETFL,flags|FCNTL.O_NONBLOCK)

        for retry in xrange(self.timeout):
            try:
                sock.connect(ticket['callback_addr'])
                if not sock:
                    print "unable to open connection to client"
                    raise CLIENT_CLOSED_CONNECTION
                break #Success, so get out of the loop.
            except socket.error, detail:
                #We have seen that on IRIX, when the connection succeds, we
                # get an ISCONN error.
                if hasattr(errno, 'ISCONN') and detail[0] == errno.ISCONN:
                    break
                #Rename this error to be handled the same as the others.
                elif detail[0] == errno.ECONNREFUSED:
                    print "connection refused"
                    return
                    raise CLIENT_CLOSED_CONNECTION, detail[0]
                else:
                    pass #somethin...something...something...
                time.sleep(1)
        else: #If we did not break out of the for loop, flag the error.
            print "major problem"
            raise SERVER_CLOSED_CONNECTION

        #Success on the connection!  Restore flag values.
        fcntl.fcntl(sock.fileno(), FCNTL.F_SETFL, flags)

        #All this connect() timeout code, when this is all that it is used for.
        callback.write_tcp_obj(sock,ticket)
        sock.close()

        #wait for a responce
        r,w,ex = select.select([listen_sock], [], [listen_sock],
                               self.timeout)
        if not r :
            print "passive listen did not hear back from monitor client via TCP"
            reply['status'] = ('ETIMEDOUT', "failed to simulate encp")
            self.reply_to_caller(reply)
            listen_sock.close()
            return

        data_sock, address = listen_sock.accept()

        interface=hostaddr.interface_name(data_ip)
        if interface:
            status=socket_ext.bindtodev(data_sock.fileno(),interface)
            if status:
                Trace.log(e_errors.ERROR, "bindtodev(%s): %s"%(interface,os.strerror(status)))
        
        listen_sock.close()

        #Determine the amount of bytes to transfer for the rate test.
        bytes_to_transfer = ticket['block_size']*ticket['block_count']

        #Now that all of the socket connections have been opened, let the
        # transfers begin.
        if ticket['transfer'] == SEND_FROM_SERVER:
            bytes_sent = 0
            sendstr = "S"*ticket['block_size']
            while bytes_sent < bytes_to_transfer:
                r,w,ex = select.select([], [data_sock], [data_sock],
                               self.timeout)
                if w:
                    bytes_left = bytes_to_transfer - bytes_sent
                    if bytes_left < ticket['block_size']:
                        sendstr = "S"*bytes_left
                    try:
                        bytes_sent = bytes_sent + data_sock.send(sendstr,
                                                         socket.MSG_DONTWAIT)
                    except socket.error, detail:
                        reply['status'] = (CLIENT_CLOSED_CONECTION, detail[1])
                        self.reply_to_caller(reply)
                        data_sock.close()
                        print "Timing out after send."
                        return

                
            print "Data sent", bytes_sent
            reply['elapsed'] = -1

        elif ticket['transfer'] == SEND_TO_SERVER:
            bytes_received = 0
            t0=time.time() #Grab the current time.
            print "Blocking on read select for", self.timeout, "seconds"
            while bytes_received < bytes_to_transfer:
                r,w,ex = select.select([data_sock], [], [data_sock],
                               self.timeout)
                if r:
                    try:
                        data = data_socket.recv(ticket['block_size'])
                        bytes_received=bytes_received+len(data)
                    except socket.error, detail:
                        reply['status'] = (CLIENT_CLOSED_CONNECTION, detail[1])
                        self.reply_to_caller(reply)
                        data_sock.close()
                        print "Timming out after recv."
                        return
                
            print "Data recieved", bytes_received
            reply['elapsed']=time.time()-t0

        reply['status'] = ('ok', None)
        self.reply_to_caller(reply)
        data_sock.close()
        
    def _become_html_gen_host(self, ticket):
        #setup for HTML output if we are so stimulated by a client
        #self.page is None if we have not setup.
        if not self.page:
            self.page = enstore_html.EnActiveMonitorPage(
                ["Time", "user IP", "Enstore IP",
                 "Read Rate (MB/S)", "Write Rate (MB/S)"], ticket['refresh'])
        else:
            pass #have already set up

    def recieve_measurement(self, ticket):
        self.reply_to_caller({"status" : ('ok', "")})
        self._become_html_gen_host(ticket) #setup for making html
        self.page.add_measurement(ticket["measurement"])

    def flush_measurements(self, ticket):
        self.reply_to_caller({"status" : ('ok', "")})
        self._become_html_gen_host(ticket)
        file = enstore_files.EnFile("%s/%s"%(ticket['dir'], 
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

    ms = MonitorServer(('', enstore_constants.MONITOR_PORT))

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
