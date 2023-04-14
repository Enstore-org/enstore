#!/usr/bin/env python

"""
Enstore udp server. Enstore command - response commumnications are
implemented using UDP protocol. This is the server part of enstore UDP
communications.
"""

# system imports
import errno
import time
import os
import checksum
import sys
import socket
import fcntl
import types
import threading
import cPickle

# enstore imports

import cleanUDP
import udp_common
import Trace
import e_errors
import host_config
import enstore_constants
import hostaddr

# for python 2.6 and latter use
# rawUDP_p -- process based rawUDP for better use of multiprocessor environment
# and to avoid GIL

if sys.version_info >= (2, 6, 0):
    try:
        import rawUDP_p as rawUDP
        can_use_raw = True
    except ImportError:
        can_use_raw = False
else:
    try:
        import rawUDP as rawUDP
        can_use_raw = True
    except ImportError:
        can_use_raw = False

class UDPServer:
    """
    Generic request response server class, for multiple connections.
    Note that the get_request actually reads the data from the socket.
    """

    def __init__(self, server_address, receive_timeout=60., use_raw=None):
        """
        :type server_address: :obj:`tuple`
        :arg server_address: (:obj:`str` - hostname or host IP, :obj:`int` - port number)
        :type receive_timeout: :obj:`float`
        :arg receive_timeout: wait time for receiving incoming commands
        :type use_raw: :obj:`bool`
        :arg use_raw: use raw UDP module to receive incoming commands (if True)
        """

        self.socket_type = socket.SOCK_DGRAM
        self.max_packet_size = enstore_constants.MAX_UDP_PACKET_SIZE
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self._lock = threading.Lock()
        self.current_id = None
        self.queue_size = 0L
        self.use_raw = use_raw and can_use_raw
        if use_raw:
            #self.server_address = server_address
            if getattr(self, "server_socket", None):
                self.server_address = server_address
                pass
            else:
                ip, port, self.server_socket = udp_common.get_callback(
                    server_address[0], server_address[1])
                self.server_address = (ip, port)
        else:
            try:
                #If we already have a server_socket...
                if getattr(self, "server_socket", None):
                    if type(server_address) == type(()) \
                       and type(server_address[0]) == type("") \
                       and type(server_address[1]) == type(0):
                        ssa = self.server_socket.getsockname()
                        if ssa == server_address:
                            self.server_address = ssa
                        else:
                            self.server_socket.close()
                            ip, port, self.server_socket = udp_common.get_callback(
                                server_address[0], server_address[1])
                            self.server_address = (ip, port)
                    else:
                        ip, port, self.server_socket = \
                            udp_common.get_default_callback()
                        self.server_address = (ip, port)
                #If an address was not specified.
                elif type(server_address) != type(()) or \
                   (type(server_address) != type(()) and len(server_address) != 2):
                    ip,port,self.server_socket = udp_common.get_default_callback()
                    self.server_address = (ip, port)
                #If an address was specified.
                elif (type(server_address[0]) == type("")
                     and type(server_address[1]) == type(0)):
                    ip, port, self.server_socket = udp_common.get_callback(
                        server_address[0], server_address[1])
                    self.server_address = (ip, port)
                #If an address was not specified.
                else:
                    ip,port,self.server_socket = udp_common.get_default_callback()
                    self.server_address = (ip, port)

            except socket.error, msg:
                self.server_address = ("", 0)
                self.server_socket = None
                Trace.log(e_errors.ERROR, str(msg))

        try:
            self.node_name, self.aliaslist, self.ipaddrlist = hostaddr.gethostinfo()
            cf = host_config.find_config_file()
            if cf:
                cc = host_config.read_config_file(cf)
                for i in cc.get('interface', {}).keys():
                    self.ipaddrlist.append(cc['interface'][i]['ip'])
        except (socket.error, socket.herror, socket.gaierror):
            self.node_name, self.aliaslist, self.ipaddrlist = \
                self.server_address[0], [], [self.server_address[0]]

        # used to recognize UDP retries
        self.request_dict = {}
        # keep requests in request dict for this many seconds
        #self.request_dict_ttl = 1800
        self.request_dict_ttl = 1000

        # set this socket to be closed in case of an exec
        if self.server_socket != None:
            fcntl.fcntl(self.server_socket.fileno(), fcntl.F_SETFD,
                        fcntl.FD_CLOEXEC)

        # to use receiver implemented in c
        # to increase the performance
        self.raw_requests = None;
        if self.use_raw:
            self.raw_requests = rawUDP.RawUDP(receive_timeout=self.rcv_timeout)
            self.raw_requests.init_socket(self.server_socket)

        thread = threading.currentThread()
        thread_name = thread.getName()

        print "UDP_SERVER starting in thread",thread_name

    def disable_reshuffle(self):
        """
        Disable reshuffling of duplicate requests when using rawUDP.
        This can be beneficial for mover requests,
        but may hurt encp requests.
        Call this right before starting the server.
        """

        if self.use_raw:
            self.raw_requests.disable_reshuffle()
            pass

    def set_keyword(self, keyword):
        if self.use_raw:
            self.raw_requests.set_keyword(keyword)


    # cleanup if we are done with this unique id
    def _done_cleanup(self):
        if self.current_id and self.request_dict.has_key(self.current_id):
            try:
                del self.request_dict[self.current_id]
            except KeyError:
                pass


    def __del__(self):
        self.server_socket.close()
        self.server_socket = None

    def purge_stale_entries(self):
        stale_time = time.time() - self.request_dict_ttl
        count = 0
        for key, value in self.request_dict.items():
            if  value[2] < stale_time:
                try:
                    del self.request_dict[key]
                    count = count+1
                except KeyError:
                    exc, msg = sys.exc_info()[:2]
                    Trace.trace(20, "purge_stale_entries: error %s %s"%(exc, msg))

        Trace.trace(20,"purge_stale_entries count=%d"%(count,))

    def server_bind(self):
        """

        Called by constructor to bind the socket.
        May be overridden.
        """

        Trace.trace(16,"server_bind add %s"%(self.server_address,))
        self.server_socket.bind(self.server_address)

    def handle_timeout(self):
        # override this method for specific timeout handling
        pass

    def fileno(self):
        """
        Return current socket file number.
        Interface required by select().

        :rtype: :obj:`int`

        """
        return self.server_socket.fileno()

    def get_server_address(self):
        """
        Return current server address.

        :rtype: :obj:`tuple` - (:obj:`str` - hostname or host IP, :obj:`int` - port number)

        """
        return self.server_address

    def do_request(self):
        request, client_address = self.get_message()

        if not request:
            return None

        return self.process_request(request, client_address)

    # old get_message
    def _get_message(self):
        """
        Old get_message

        :rtype: :obj:`tuple` - (:obj:`str` - stringified ticket, after CRC is removed,
                               :obj:`tuple` (:obj:`str` - host IP, :obj:`int` - port number) - client address

        There are three cases:
           1. Read from socket where crc is stripped and return address is valid.
           2. Read from pipe where there is no crc and no r.a.
           3. Time out where there is no string or r.a.
        """

        request, client_addr = '',()
        r = [self.server_socket]

        rcv_timeout = self.rcv_timeout
        r, w, x, remaining_time = cleanUDP.Select(r, [], [], rcv_timeout)

        if not r + w:
            return ('',()) #timeout

        for fd in r:
            if fd == self.server_socket:

                req, client_addr = self.server_socket.recvfrom(
                    self.max_packet_size, self.rcv_timeout)
                #print "REQ", req
                try:
                    request, inCRC = udp_common.r_eval(req)
                    Trace.trace(5,"_get_message: %s"%(request,))
                except ValueError, detail:
                    Trace.trace(5, "must be event_relay msg %s"%(detail,))
                    # must be an event relay message
                    # it has a different format
                    try:
                        request = udp_common.r_eval(req)
                        raise NameError, request # dispatching_worker will take care of this
                    except:
                        exc, msg = sys.exc_info()[:2]
                        # reraise exception
                        raise exc, msg

                except (SyntaxError, TypeError):
                    #If TypeError occurs, keep retrying.  Most likely it is
                    # an "expected string without null bytes".
                    #If SyntaxError occurs, also keep trying, most likely
                    # it is from and empty UDP datagram.
                    exc, msg = sys.exc_info()[:2]
                    try:
                        message = "%s: %s: From client %s:%s" % \
                                  (exc, msg, client_addr, request[:100])
                    except IndexError:
                        message = "%s: %s: From client %s: %s" % \
                                  (exc, msg, client_addr, request)
                    Trace.log(10, message)

                    #Set these to something.
                    request, inCRC = (None, None)

                if request == None:
                    return (request, client_addr)
                # calculate CRC
                crc = checksum.adler32(0L, request, len(request))
                if (crc != inCRC) :
                    Trace.log(e_errors.INFO,
                              "BAD CRC request: %s " % (request,))
                    Trace.log(e_errors.INFO,
                              "CRC: %s calculated CRC: %s" %
                              (repr(inCRC), repr(crc)))

                    request=None

        return (request, client_addr)

    def _get_raw_message(self):
       """
       Get message received by raw UDP module

       :rtype: :obj:`tuple` - (:obj:`str` - stringified ticket, after CRC is removed,
                              :obj:`tuple` (:obj:`str` - host IP, :obj:`int` - port number) - client address

       There are three cases:
           1. Read from socket where crc is stripped and return address is valid.
           2. Read from pipe where there is no crc and no r.a.
           3. Time out where there is no string or r.a.

       """

       request, client_addr = '',()
       rc = self.raw_requests.get()
       if rc:
           self.queue_size = self.raw_requests.queue_size
           #Trace.trace(5, "REQ %s %s %s"%(self.server_address, request,self.queue_size))
           Trace.trace(5, "REQ %s %s"%(rc[1], self.queue_size))
       else:
           rc = ('',())
           if self.queue_size != 0:
               print "Nonsense rc=%s size=%s"%(rc, self.queue_size)
               sys.exit(1)

       Trace.trace(5,"_get_raw_message %s %s" % (rc[0], rc[1]))
       return rc

    def get_message(self):
        """
        Get message from client

        :rtype: :obj:`tuple` (:obj:`str` - stringified ticket, after CRC is removed,
                             :obj:`tuple` (:obj:`str` - host IP, :obj:`int` - port number) - client address

        """

        if self.raw_requests:
            return self._get_raw_message()
        else:
            return self._get_message()


    def process_request(self, request, client_address):
        """
        Process the  request that was (generally) sent from UDPClient.send

        In some cases involving the media_changer, this function will
        process messages read from the child processes when
        DispatchingWorker.get_request() calls it.  The only major
        consequence of this is that we don't put anything into
        ticket['r_a'].  If we do, these fake values of:

        | idn = 0
        | number = 0
        | client_address = ()

        will cause reply_with_address() and reply_to_caller() to do the
        wrong thing and not send back the reply.  This is because
        the media changer places these values into ticket['ra'],
        and we don't want to have two sets of competing information.
        Note: The use of 'r_a' was choosen internally for the
        udp_server because of the pre-existing use of 'ra' between
        the media_changer and udp_server.

        :type request: :obj:`tuple`
        :arg request: (:obj:`str` - client id, :obj:`long` - message number, :obj:`dict` - ticket)
        :rtype: :obj:`dict` - ticket

        """

        try:
            idn, number, ticket = udp_common.r_eval(request)
            #Trace.log(e_errors.INFO, "process_request idn %s number %s ticket %s"%(idn, number, ticket))
        except (NameError, ValueError), detail:
            Trace.trace(5, "must be an event relay message %s"%(detail,))
            # must be an event relay message
            # it has a different format
            try:
                rq = udp_common.r_eval(request)
                self.erc.error_msg = str(rq)
                self.handle_er_msg(None)
                return None
                #raise NameError, rq # dispatching_worker will take care of this
            except:
                exc, msg = sys.exc_info()[:2]
                Trace.trace(5, "will reraise %s %s"%(exc, msg))
                # reraise exception
                raise exc, msg
        except (SyntaxError, TypeError):
            #If TypeError occurs, keep retrying.  Most likely it is
            # an "expected string without null bytes".
            #If SyntaxError occurs, also keep trying, most likely
            # it is from an empty UDP datagram.
            exc, msg = sys.exc_info()[:2]
            try:
                message = "%s: %s: From client %s:%s" % \
                          (exc, msg, client_address, request[:100])
            except IndexError:
                message = "%s: %s: From client %s: %s" % \
                          (exc, msg, client_address, request)
            # print message
            Trace.log(10, message)

            #Set these to something.
            idn, number, ticket = (None, None, None)

        if idn == None or type(ticket) != types.DictType:
            Trace.log(e_errors.ERROR,
                      "Malformed request from %s %s" %
                      (client_address, request,))
            reply = (0L,{'status': (e_errors.MALFORMED, None)},None)
            self.server_socket.sendto(repr(reply), client_address)
            return None

        reply_address = client_address
        client_number = number
        current_id = idn
        #The following are not thread safe.
        self.reply_address = client_address
        self.client_number = number
        self.current_id = idn

        #The reason we need to include this information (at least
        # temporarily) is that for a multithreaded server it would
        # be possible for this function to process multiple requests
        # before reply_with_list() could be called from another thread(s).
        # In such a situation reply_to_caller() would reply with the
        # most recent request address and not to the one that made the request.
        #Trace.log(e_errors.INFO, "process_request reply addr %s"%(reply_address,))
        if reply_address:
            ticket['r_a'] = (reply_address,
                             client_number,
                             current_id)

        if self.request_dict.has_key(idn):

            # UDPClient resends messages if it doesn't get a response
            # from us, see it we've already handled this request earlier. We've
            # handled it if we have a record of it in our dict
            try:
                lst = self.request_dict[idn]
            except KeyError:
                Trace.trace(6,
                            "process_request %s from %s: no such key in request dictionary" % \
                            (repr(idn), client_address))
                return None

            if lst[0] == number:
                Trace.trace(6,
                            "process_request %s from %s already handled" % \
                            (repr(idn), client_address))
                self.reply_with_list(lst, client_address, idn)
                return None

            # if the request number is smaller, then there has been a timing
            # race and we've already handled this as much as we are going to.
            elif number < lst[0]:
                Trace.trace(6,
                            "process_request %s from %s old news" % \
                            (repr(idn), client_address))
                return None #old news, timing race....
        self.purge_stale_entries()

        #Trace.log(e_errors.INFO, "process_request ticket %s"%(ticket,))
        return ticket


    def reply_to_caller(self, ticket):
        """
        Reply to sender with her number and ticket (which has status)
        generally, the requested user function will send its response through
        this function - this keeps the request numbers straight

        :type ticket: :obj:`dict`
        :arg ticket: ticket to send back to client
        """

        if type(ticket) == types.DictType and ticket.get("r_a", None):
            reply_address = ticket["r_a"][0]
            client_number = ticket["r_a"][1]
            current_id    = ticket["r_a"][2]

        else:
            Trace.log(e_errors.WARNING, "No reply address in reply to caller %s"%(ticket,))
            #Can we ever get here?  If we do it isn't thread safe.
            try:
                reply_address = self.reply_address
                client_number = self.client_number
                current_id    = self.current_id
            except AttributeError:
                exc, msg = sys.exc_info()[:2]
                print "reply_to_caller: error", exc, msg
                return

        reply = (client_number, ticket, time.time())
        self.reply_with_list(reply, reply_address, current_id)

    def reply_to_caller_using_interface_ip(self, ticket, interface_ip):
        """
        If a different interface is needed to send the reply on then use it.

        :type ticket: :obj:`dict`
        :arg ticket: ticket to send back to client
        :type interface_ip: :obj:`str`
        :arg interface_ip: client IP
        """
        if type(ticket) == types.DictType and ticket.get("r_a", None):
            reply_address = ticket["r_a"][0]
            client_number = ticket["r_a"][1]
            current_id    = ticket["r_a"][2]

        else:
            Trace.log(e_errors.WARNING, "No reply address in reply to caller using IP %s"%(ticket,))
            #Can we ever get here?  If we do it isn't thread safe.
            reply_address = self.reply_address
            client_number = self.client_number
            current_id    = self.current_id

        reply = (client_number, ticket, time.time())
        self.reply_with_list(reply, reply_address, current_id, interface_ip)

    def reply_with_list(self, list, reply_address, current_id,
                        interface_ip = None):
        """
        Keep a copy of request to check for later udp retries of the same
        request and then send to the user.

        :type list: :obj:`list`
        :arg list: list of ready replies held in server
        :type reply_address: :obj:`tuple`
        :arg reply_address: - (:obj:`str` - host IP, :obj:`int` - port number) - client address
        :type interface_ip: :obj:`str`
        :arg interface_ip: client IP

        """

        with self._lock:
            try:
                # there are rare cases when the following erro occurs:
                # RuntimeError: dictionary changed size during iteration
                # I do not know the reason
                # but this should help the code to proceed
                #list_copy = copy.deepcopy(list)
                list_copy = cPickle.loads(cPickle.dumps(list, -1)) # this about 5 times faster than deepcopy
            except:
                list_copy = None
                Trace.handle_error()
                Trace.log(e_errors.INFO, "Exception when doing deepcopy. List %s"%(list,))

        if not list_copy:
            # do not send a reply
            return

        self.request_dict[current_id] = list_copy

        if interface_ip != None:
            ip, port, send_socket = udp_common.get_callback(interface_ip)
            with_interface = " with interface %s" % interface_ip
        else:
            send_socket = self.server_socket
            with_interface = ""  #Give better trace message.

        # sendto() in python 2.6 raises this EMSGSIZE socket exception if
        # the message size is to long for UDP.  In python 2.4, the message
        # is silently truncated.
        wrapped_list = udp_common.r_repr(list_copy)
        if len(wrapped_list) > self.max_packet_size:
            ### A long message can now be handled by generic_client and
            ### dispatching_worker.  Don't log a traceback here.
            raise socket.error(errno.EMSGSIZE, os.strerror(errno.EMSGSIZE))

        try:
            Trace.trace(16, "udp_server (reply%s): to %s: request_dict %s" %
                        (with_interface, reply_address, current_id))

            payload = list_copy[1]
            dt=list_copy[2]-payload.get("send_ts",0)
            Trace.trace(6, "client_id %s: latency %s" %
                        (current_id,str(dt)))
            send_socket.sendto(wrapped_list, reply_address)
        except:
            ### A long message can now be handled by generic_client and
            ### dispatching_worker.  Don't log a traceback here.
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    def reply_with_address(self, ticket):
        """
        For requests that are not handled serially reply_address, current_id,
        and client_number number must be reset.  In the forking media changer
        these are in the forked child and passed back to us.

        :type ticket: :obj:`dict`
        :arg ticket: ticket to send back to client
        """

        self.reply_address = ticket["ra"][0]
        self.client_number = ticket["ra"][1]
        self.current_id    = ticket["ra"][2]
        self.reply_to_caller(ticket)

    def set_out_file(self):
        if self.use_raw:
            self.raw_requests.set_out_file()


if __name__ == "__main__":

    def monitor(udp_srv):
        import subprocess
        import os
        print "udp_server monitor starting"
        rqs1=0.
        t1=time.time()
        first = True
        f=open("udp_server_test_%s"%(os.getpid(),), "w")
        while 1:
            cmd = 'netstat -npl | grep %s'%(udp_srv.server_address[1],)
            pipeObj = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True)
            if pipeObj:
                result = pipeObj.communicate()[0]
            l=result
            l.strip()

            if l.find('udp') != -1:
                a=l.split(' ')
                c = 0
                for i in a:
                    if i == '':
                        c = c + 1
                for i in range(c):
                    a.remove('')
                r_queue = a[1]

            cmd = 'netstat -s | grep "packet receive errors"'
            pipeObj = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True)
            if pipeObj:
                result = pipeObj.communicate()[0]

            l=result
            l.strip(' ')
            if l.find('errors') != -1:
                r_err = long(l.split(' ')[4])
            t=time.time()
            if first:
                first = False
                error_rate = 0
                t1 = t
                r_err0 = r_err
            else:
                error_rate = (r_err-r_err0)/(t-t1)
                t1 = t
                r_err0 = r_err


            t=time.time()
            #print rqs
            #print rqs-rqs1
            #print t
            #print t1
            msg= '%s %s'%(time.ctime(time.time()), udp_srv.queue_size)
            msg = '%s %s'%(msg, r_queue)
            msg = '%s %s %s'%(msg, r_err, error_rate)
            f.write("%s\n"%(msg,))
            f.flush()
            time.sleep(10)

    #This test program can be run in conjuction with the udp_client.py
    # test program.  This test program will process any message send to
    # the correct port (including other tests than udp_client.py).

    if len(sys.argv) > 1:
        monitor_server = True
    else:
       monitor_server = False
    udpsrv = UDPServer(('', 7700), receive_timeout = 60.0, use_raw=1)
    #udpsrv = UDPServer(('', 7700), receive_timeout = 60.0)

    if udpsrv.use_raw:
        udpsrv.set_out_file()
        # start receiver thread or process
        udpsrv.raw_requests.receiver()


    if monitor_server:
        thread = threading.Thread(group=None, target=monitor,
                              args=(udpsrv,), kwargs={})
        thread.start()


    while 1:
        try:
            ticket = udpsrv.do_request()
        except KeyboardInterrupt:
            sys.exit(0)
        if ticket:
            #print "Message %s"%(ticket,)
            udpsrv.reply_to_caller(ticket)
            #break
    del(udpsrv)
    print "finished"
