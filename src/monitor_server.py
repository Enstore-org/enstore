#!/usr/bin/env python
#
# system imports
import sys
import string
import types
import os
import socket
import select
import traceback
#import pprint
import time
import fcntl
#if sys.version_info < (2, 2, 0):
#    import FCNTL #FCNTL is depricated in python 2.2 and later.
#    fcntl.F_GETFL = FCNTL.F_GETFL
#    fcntl.F_SETFL = FCNTL.F_SETFL
import errno

# enstore imports
import dispatching_worker
import generic_server
import Trace
import e_errors
import hostaddr
#import socket_ext
import callback
import enstore_html
import enstore_files
#import configuration_client
import timeofday
import enstore_constants
#import udp_client

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

There are three connections opened for this test.
  ____                                ____
 |    | --- 1 UDP request ---------> |    |
 | MC | <-- 2 TCP control socket --- | MS |
 |____| --- 3 TCP data socket -----> |____|

This mimics the connections where in a real encp:
1) encp to library manager
2) library manager to encp
3) encp to mover

"""

MY_NAME = "MNTR_SRV"

SEND_TO_SERVER = "send_to_server"
SEND_FROM_SERVER = "send_from_server"

class MonitorError(Exception):
    def __init__(self, error_message):

        Exception.__init__(self)

        self.error_message = error_message

    def __str__(self):
        return self.error_message

    def __repr__(self):
        return "MonitorError: %s"%(self.error_message,)

#SERVER_CONNECTION_ERROR = "Server connection error"
#CLIENT_CONNECTION_ERROR = "Client connection error"

class MonitorServer(dispatching_worker.DispatchingWorker,
                    generic_server.GenericServer):

    def __init__(self, csc, port=enstore_constants.MONITOR_PORT):
        self.timeout = 10
	self.running = 0
	self.print_id = MY_NAME

        Trace.trace(1, "Monitor Server at %s %s" %(csc[0], csc[1]))

        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        dispatching_worker.DispatchingWorker.__init__(self,
                                         ('', port))

	self.running = 1

        #If socket.MSG_DONTWAIT isn't there add it, because should be.
        if not hasattr(socket, "MSG_DONTWAIT"): #Python 1.5 test
            host_type = os.uname()[0]
            if host_type == 'Linux':
                socket.MSG_DONTWAIT = 64
            elif host_type[:4]=='IRIX':
                socket.MSG_DONTWAIT = 128
            else: #No flag value.
                socket.MSG_DONTWAIT = 0

        # always nice to let the user see what she has
        Trace.trace(10, repr(self.__dict__))
        self.page = None

    #This function should only be called from simulate_encp_transfer.
    #data_sock: already connected tcp socket.
    #block_size and block_count: the size of the buffer to attempt to move
    # at a time and the number of them to move.
    #function: a string with one of two possible values "send" or "recv".
    # These are for accessing the socket.send() and socket.recv() functions.
    def _test_encp_transfer(self, data_sock, block_size, block_count,function):
        bytes_transfered = 0
        sendstr = "S"*block_size
        bytes_to_transfer = block_size * block_count

        #Set args outside of the loop for performance reasons.
        if function == "send":
            args = (sendstr, socket.MSG_DONTWAIT)
            sock_read_list = []
            sock_write_list = [data_sock]
        else:  #if read
            args = (block_size,)
            sock_read_list = [data_sock]
            sock_write_list = []

        t0 = time.time() #Grab the current time.
        t1 = t0 #Reset counter to current time (aka zero).

        while bytes_transfered < bytes_to_transfer:
            #Determine how much time is needed to pass before timming out.
            # This amount to time spent inside select should be the value
            # of self.timeout.  However, it has been observed that a
            # sleep/select call specifed to wait 1 sec actually on average
            # waits 0.9997 seconds (On Linux and SUN; IRIX waits the full
            # second).  This way we can wait the entire time without the
            # possiblity of loosing connections.  The .000001 is one micro-
            # second, the smallest resolution the API specifies.
            wait_time = self.timeout - (time.time() - t1) + .000001

            try:
                r,w,ex = select.select(sock_read_list, sock_write_list,
                                       [data_sock], wait_time)
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except:
                r, w, ex = (None, None, None)


            if w or r or ex:
                #if necessary make the send string the correct (smaller) size.
                bytes_left = bytes_to_transfer - bytes_transfered
                if w and bytes_left < block_size:
                    sendstr = "S"*bytes_left
                    args = (sendstr, socket.MSG_DONTWAIT)
                try:
                    #By handling the similarities of sends and recieves the
                    # same way, a lot of duplicate code is removed.
                    transfer_function = getattr(data_sock, function)
                    return_value = apply(transfer_function, args)

                    #For reads, we only care about the length sent...
                    if r:
                        return_value = len(return_value)

                    if return_value == 0:
                        #If we get here then something bad happened.  The
                        # read and writes can return 0 values/lengths,
                        # however the select won't let the read/writes
                        # execute unless there is something there or
                        # somthing bad happened (which makes return_value
                        # equal to zero).
                        data_sock.close()
                        #raise CLIENT_CONNECTION_ERROR, \
                        #      os.strerror(errno.ECONNRESET)
                        raise MonitorError(os.strerror(errno.ECONNRESET))

                    #Get the new number of bytes sent.
                    bytes_transfered = bytes_transfered + return_value

                    t1 = time.time() #t1 is now current time

                except socket.error, detail:
                    data_sock.close()
                    #raise CLIENT_CONNECTION_ERROR, detail[1]
                    raise MonitorError("socket error: %s"%(detail[1],))

            #If there hasn't been any traffic in the last timeout number of
            # seconds, then timeout the connection.
            elif time.time() - t1 > self.timeout:
                data_sock.close()
                #raise CLIENT_CONNECTION_ERROR, os.strerror(errno.ETIMEDOUT)
                raise MonitorError(os.strerror(errno.ETIMEDOUT))

        return time.time() - t0


    #Open up the server side of the control socket.
    #client_addr: The (host, port) of the node where the client is running.
    #mover_addr: In the actual encp, the mover node is not necessarily the
    # same node that the library manager runs on.  For this test, they are
    # the same machine.
    def _open_cntl_socket(self, client_addr, mover_addr):
        #Create the socket.
	address_family = socket.getaddrinfo(client_addr[0], None)[0][0]
        try:
            sock=socket.socket(address_family, socket.SOCK_STREAM)
        except socket.error, detail:
            #raise CLIENT_CONNECTION_ERROR, detail[1]
            raise MonitorError("open_cntl_socket: %s"%(detail[1],))

        #Put the socket into non-blocking mode.
        flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(sock.fileno(), fcntl.F_SETFL,flags|os.O_NONBLOCK)

        try:
            sock.connect(client_addr) #Start the TCP handshake.
        except socket.error, detail:
            #We have seen that on IRIX, when the connection succeds, we
            # get an EISCONN error.
            if hasattr(errno, 'EISCONN') and detail[0] == errno.EISCONN:
                pass
            #The TCP handshake is in progress.
            elif detail[0] == errno.EINPROGRESS:
                pass
            #A real or fatal error has occured.  Handle accordingly.
            else:
                #raise CLIENT_CONNECTION_ERROR, detail[1]
                #raise MonitorError(detail[1])
                raise MonitorError(detail)

        #Check if the socket is open for reading and/or writing.
        r, w, unused = select.select([sock], [sock], [], self.timeout)

        if r or w:
            #Get the socket error condition...
            rtn = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        else:
            #raise CLIENT_CONNECTION_ERROR, os.strerror(errno.ETIMEDOUT)
            raise MonitorError(os.strerror(errno.ETIMEDOUT))

        #...if it is zero then success, otherwise it failed.
        if rtn != 0:
            #raise CLIENT_CONNECTION_ERROR, os.strerror(rtn)
            raise MonitorError(os.strerror(rtn))

        #Restore flag values to blocking mode.
        fcntl.fcntl(sock.fileno(), fcntl.F_SETFL, flags)

        #Create the return ticket to tell the client what addr to callback to.
        return_ticket = {'mover' :{'callback_addr': mover_addr} }
        callback.write_tcp_obj(sock, return_ticket)
        sock.close()

    #Return the socket to use for the encp rate tests.
    #mover_addr: In the actual encp, the mover node is not necessarily the
    # same node that the library manager runs on.  For this test, they are
    # the same machine.
    #listen_sock: The socket to wait for the client to connect to creating the
    # data socket.
    def _open_data_socket(self, listen_sock):

        listen_sock.listen(1)

        #wait for a response
        r, unused, unused = select.select([listen_sock], [], [listen_sock],
                                          self.timeout)
        if not r :
            listen_sock.close()
            #raise CLIENT_CONNECTION_ERROR, os.strerror(errno.ETIMEDOUT)
            raise MonitorError(os.strerror(errno.ETIMEDOUT))

        #Wait for the client to connect creating the data socket used for the
        # encp rate tests.
        data_sock, unused = listen_sock.accept()
        listen_sock.close()

        #For machines with multiple network interfaces, pick the best one.
        #interface=hostaddr.interface_name(mover_addr[0])
        #if interface:
        #    status=socket_ext.bindtodev(data_sock.fileno(),interface)
        #    if status:
        #        Trace.log(e_errors.ERROR, "bindtodev(%s): %s" %
        #                  (interface,os.strerror(status)))

        return data_sock

    #This is the function mentioned in the 'work' section of the ticket sent
    # from the client.
    def simulate_encp_transfer(self, ticket):
        reply = {'status'     : (None, None),
                 'block_size' : ticket['block_size'],
                 'block_count': ticket['block_count']}

        try:
            #A little easier to read now.
            data_ip = ticket['server_addr'][0]
            client_addr = ticket['client_addr']
        except KeyError: #backward compatibility
            data_ip = ticket['remote_interface']
            client_addr = ticket['callback_addr']

        #Get the addr to tell the client to call back to and get the listening
        # socket to listen with.
        localhost, localport, listen_sock = callback.get_callback(ip=data_ip)
        #Instead of using an actual mover, this is the addr that this server
        # must tell the client it will be listening (via listen_sock) on.
        test_mover_addr = (localhost, localport)

        #Simulate the opening and initial handshake of the control socket.
        try:
            self._open_cntl_socket(client_addr, test_mover_addr)
            data_sock = self._open_data_socket(listen_sock)

            if not data_sock:
                #raise CLIENT_CONNECTION_ERROR, "no connection established"
                raise MonitorError("no connection established")

        #except (CLIENT_CONNECTION_ERROR, SERVER_CONNECTION_ERROR):
        except MonitorError:
            Trace.log(e_errors.ERROR, "Error extablishing connection: %s"
                      % str(sys.exc_info()[:2]))
            return

        if not data_sock:
            return

        #Now that all of the socket connections have been opened, let the
        # transfers begin.
        try:
            if ticket['transfer'] == SEND_FROM_SERVER:
                self._test_encp_transfer(data_sock, ticket['block_size'],
                                         ticket['block_count'], "send")

                reply['elapsed'] = -1

            elif ticket['transfer'] == SEND_TO_SERVER:
                reply['elapsed'] = self._test_encp_transfer(
                    data_sock,ticket['block_size'], ticket['block_count'],
                    "recv")
        #except (CLIENT_CONNECTION_ERROR, SERVER_CONNECTION_ERROR):
        except MonitorError:
            print sys.exc_info()[:2]
            return

        if not data_sock:
            return

        reply['status'] = (e_errors.OK, None)
        ticket.update(reply)
        self.reply_to_caller(ticket)
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

    #After a client has executed both tests (read and write) it sends a ticket
    # with two keys.  'work' is the first and is set to 'recieve_measurement.
    # The other is 'measurement' and holds a 5-tuple.  See
    # _become_html_gen_host for more info on the 5 tuple.
    def recieve_measurement(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        self._become_html_gen_host(ticket) #setup for making html
        self.page.add_measurement(ticket["measurement"])

    def flush_measurements(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        self._become_html_gen_host(ticket)
        m_file = enstore_files.EnFile("%s/%s"%(ticket['dir'],
					     enstore_constants.NETWORKFILE))
        m_file.open()
        m_file.write(str(self.page))
        m_file.close()

class MonitorServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        #self.html_dir = None
        generic_server.GenericServerInterface.__init__(self)

    def valid_dictionaries(self):
        return (self.help_options,self.trace_options)

if __name__ == "__main__":
    Trace.init(MY_NAME)

    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    else:
        port = enstore_constants.MONITOR_PORT

    intf = MonitorServerInterface()

    ms = MonitorServer((intf.config_host, intf.config_port), port)

    #This is a server and therfore must handle things like --alive requests.
    ms.handle_generic_commands(intf)

    #Main loop.
    while 1:
        try:
            Trace.trace(6,"Monitor Server (re)starting")
            ms.serve_forever()
	except SystemExit, exit_code:
	    sys.exit(exit_code)
        except:
            Trace.handle_error()
            ms.serve_forever_error("monitor_server")
            continue

    Trace.trace(6,"Monitor Server finished (impossible)")
