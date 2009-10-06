#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import errno
import time
import os
#import traceback
import checksum
import sys
import socket
#import signal
#import string
import fcntl
#if sys.version_info < (2, 2, 0):
#    import FCNTL #FCNTL is depricated in python 2.2 and later.
#    fcntl.F_SETFD = FCNTL.F_SETFD
#    fcntl.FD_CLOEXEC = FCNTL.FD_CLOEXEC
import copy
import types
#import rexec
import threading

# enstore imports
#import hostaddr
import cleanUDP
import udp_common
import Trace
import e_errors
import host_config
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
'''
import rawUDP
can_use_raw = True
'''

# Generic request response server class, for multiple connections
# Note that the get_request actually read the data from the socket

class UDPServer:
    
    def __init__(self, server_address, receive_timeout=60., use_raw=None):
        self._lock = threading.Lock()

        #These variables should be used like constants.  (readonly)
        self.socket_type = socket.SOCK_DGRAM
        self.max_packet_size = 16384
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self.address_family = socket.AF_INET
        self.use_raw = use_raw and can_use_raw
        # keep requests in request dict for this many seconds
        #self.request_dict_ttl = 1800
        self.request_dict_ttl = 1000

        #These variables should be dead.
        #self.current_id = None
        #self.queue_size = 0L

        #These variables should be used with care.
        if not hasattr(self, 'tsd'):
            self.tsd = threading.local()
        self.init_thread_specific_data(server_address)

        #By default init_thread_specific_data() sets use_request_dict to
        # false, since most server threads only live to process
        # one request.  This is the MainThread, so we need to override
        # this to true.
        self.tsd.use_request_dict = True

        # to use receiver implemented in c
        # to increase the performance
        self.raw_requests = None
        self.check_request = True # check request in r_eval
        if self.use_raw:
            self.check_request = False # dont check request in r_eval, it is checked in rawUDP receiver
            self.raw_requests = rawUDP.RawUDP(receive_timeout=self.rcv_timeout)
            self.raw_requests.init_socket(self.tsd.server_socket)
            # start raw udp receiver
            # it creates internal receiver thread and runs it in a loop
            '''
            if self.raw_requests:
                #rawUDP.receiver(self.raw_requests)
                self.raw_requests.receiver()
            '''
        thread = threading.currentThread()
        thread_name = thread.getName()

        print "UDP_SERVER starting in thread",thread_name 

    def init_thread_specific_data(self, server_address = None):

        if hasattr(self.tsd, 'server_address') and \
               self.tsd.server_address == server_address:
            #Already initialized.
            return
        
        # used to recognize UDP retries
        self.tsd.request_dict = {}
        #Set if the thread should remember things in self.tsd.request_dict.
        # This is useful for servers that spawn a thread to handle making
        # one reply.
        self.tsd.use_request_dict = False

        #What does this self.use_raw section do differently from the else
        # section?  Someone will need to explain it, because it is not
        # obvious.
        if self.use_raw:
            if getattr(self.tsd, "server_socket", None):
                self.tsd.server_address = server_address
                pass
            #If an address was not specified.
            elif type(server_address) != type(()) or \
                     (type(server_address) != type(()) and len(server_address) != 2):
                ip,port,self.tsd.server_socket = udp_common.get_default_callback()
                self.tsd.server_address = (ip, port)
            else:
                ip, port, self.tsd.server_socket = udp_common.get_callback(
                    server_address[0], server_address[1])
                self.tsd.server_address = (ip, port)
        else:
            try:
                #If we already have a server_socket...
                if getattr(self.tsd, "server_socket", None):
                    if type(server_address) == type(()) \
                       and type(server_address[0]) == type("") \
                       and type(server_address[1]) == type(0):
                        ssa = self.tsd.server_socket.getsockname()
                        if ssa == server_address:
                            self.tsd.server_address = ssa
                        else:
                            self.tsd.server_socket.close()
                            ip, port, self.tsd.server_socket = udp_common.get_callback(
                                server_address[0], server_address[1])
                            self.tsd.server_address = (ip, port)
                    else:
                        ip, port, self.tsd.server_socket = \
                            udp_common.get_default_callback()
                        self.tsd.server_address = (ip, port)
                #If an address was not specified.
                elif type(server_address) != type(()) or \
                   (type(server_address) != type(()) and len(server_address) != 2):
                    ip,port,self.tsd.server_socket = udp_common.get_default_callback()
                    self.tsd.server_address = (ip, port)
                #If an address was specified.                
                elif (type(server_address[0]) == type("")
                     and type(server_address[1]) == type(0)):
                    ip, port, self.tsd.server_socket = udp_common.get_callback(
                        server_address[0], server_address[1])
                    self.tsd.server_address = (ip, port)
                #If an address was not specified.
                else:
                    ip,port,self.tsd.server_socket = udp_common.get_default_callback()
                    self.tsd.server_address = (ip, port)

            except socket.error, msg:
                self.tsd.server_address = ("", 0)
                self.tsd.server_socket = None
                Trace.log(e_errors.ERROR, str(msg))

        #Determine the list of IP addresses that we are allowed to use.
        #These should be used readonly.
        try:
            self.tsd.node_name, self.tsd.aliaslist, self.tsd.ipaddrlist = \
                socket.gethostbyname_ex(
                    socket.gethostbyaddr(self.tsd.server_address[0])[0])
            cf = host_config.find_config_file()
            if cf:
                cc = host_config.read_config_file(cf)
                for i in cc.get('interface', {}).keys():
                    self.ipaddrlist.append(cc['interface'][i]['ip'])
        except (socket.error, socket.herror, socket.gaierror, TypeError):
            #TypeError is in the list if server_address is None.
            self.tsd.node_name, self.tsd.aliaslist, self.tsd.ipaddrlist = \
                self.tsd.server_address[0], [], [self.tsd.server_address[0]]


        # set this socket to be closed in case of an exec
        if self.tsd.server_socket != None:
            fcntl.fcntl(self.tsd.server_socket.fileno(), fcntl.F_SETFD,
                        fcntl.FD_CLOEXEC)

    ####################################################################
    
    # disable reshuffling of duplicate requests when using rawUDP
    # this can be beneficial for mover requests
    # but may hurt encp requests
    # call this right before starting the server
    def disable_reshuffle(self):
        if self.use_raw:
            self.raw_requests.disable_reshuffle()
            pass

    def set_keyword(self, keyword):
        if self.use_raw:
            self.raw_requests.set_keyword(keyword)

    ####################################################################

    #Useful function to hide the format of the sub-ticket.  It returns the
    # IP address of the originating node.
    def extract_reply_address(self, ticket):
        reply_address = ticket['r_a'][0]
        return reply_address

    #Useful function to hide the format of the sub-ticket.  It returns the
    # unique message id.
    def extract_current_id(self, ticket):
        reply_address = ticket['r_a'][2]
        return reply_address
        
    # cleanup if we are done with this unique id
    def _done_cleanup(self):
        #self.current_id is not thread safe
        pass
        """
        if self.current_id and self.request_dict.has_key(self.current_id):
            try:
                del self.request_dict[self.current_id]
            except KeyError:
                pass
        """

    def purge_stale_entries(self):
        stale_time = time.time() - self.request_dict_ttl
        count = 0
        for key, value in self.get_request_dict().items():
            if  value[2] < stale_time:
                try:
                    del self.get_request_dict()[key]
                    count = count+1
                except KeyError:
                    exc, msg = sys.exc_info()[:2]
                    Trace.trace(20, "purge_stale_entries: error %s %s"%(exc, msg))

        Trace.trace(20,"purge_stale_entries count=%d"%(count,))

    #Not used???
    def server_bind(self):
        """Called by constructor to bind the socket.

        May be overridden.

        """
        print "server_bind add %s"%(self.server_address,)
        Trace.trace(16,"server_bind add %s"%(self.server_address,))
        #Since we are binding the address we are listening with, only
        # the main socket should get bound???
        self.server_socket.bind(self.server_address)

    def handle_timeout(self):
        # override this method for specific timeout hadling
        pass

    def fileno(self):
        """Return socket file number.

        Interface required by select().

        """
        return self.tsd.server_socket.fileno()

    ####################################################################

    def get_server_address(self):
        self.server_address

    def get_request_dict(self):
        if not hasattr(self.tsd, 'request_dict'):
            self.init_thread_specific_data()

        return self.tsd.request_dict

    def set_request_dict(self, id, value):
        if not getattr(self.tsd, 'use_request_dict', None):
            #Only update the recently processed responses if this thread
            # is the one that recieved the message.  If it was passed to
            # this thread from another, like the MainThread, then we
            # don't need to slowly consume more memory.
            self.get_request_dict()[id] = value

    ####################################################################
            
    def do_request(self):
        # ref udp_client.py (i.e. we may wish to have a udp_client method
        # to get this information)

        request, client_address = self.get_message()

        if not request:
            return None

        return self.process_request(request, client_address)

    # old get_message
    def _get_message(self):
        # returns  (string, socket address)
        #      string is a stringified ticket, after CRC is removed
        # There are three cases:
        #   read from socket where crc is stripped and return address is valid
        #   read from pipe where there is no crc and no r.a.     
        #   time out where there is no string or r.a.

        request, client_addr = '',()
        r = [self.tsd.server_socket]

        rcv_timeout = self.rcv_timeout
        r, w, x, remaining_time = cleanUDP.Select(r, [], [], rcv_timeout)

        if not r + w:
            return ('',()) #timeout

        for fd in r:
            if fd == self.tsd.server_socket:

                req, client_addr = self.tsd.server_socket.recvfrom(
                    self.max_packet_size, self.rcv_timeout)
                #print "REQ", req
                try:
                    request, inCRC = udp_common.r_eval(req, check=self.check_request)
                    Trace.trace(5,"_get_message: %s"%(request,)) 
                except ValueError, detail:
                    Trace.trace(5, "must be event_relay msg %s"%(detail,))
                    # must be an event relay message
                    # it has a different format
                    try:
                        request = udp_common.r_eval(req, check=self.check_request)
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
       # returns  (string, socket address)
       #      string is a stringified ticket, after CRC is removed
       # There are three cases:
       #   read from socket where crc is stripped and return address is valid
       #   read from pipe where there is no crc and no r.a.     
       #   time out where there is no string or r.a.

       request, client_addr = '',()
       rc = self.raw_requests.get()
       if rc:
           Trace.trace(5, "REQ %s %s"%(rc[1], self.raw_requests.queue_size)) 
       else:
           rc = ('',())
           if self.raw_requests.queue_size != 0:
               print "Nonsense rc=%s size=%s"%(rc, self.raw_requests.queue_size)
               sys.exit(1)

       Trace.trace(5,"_get_raw_message %s %s" % (rc[0], rc[1])) 
       return rc

    def get_message(self):
        if self.raw_requests:
            return self._get_raw_message()
        else:
            return self._get_message()
        

    # Process the  request that was (generally) sent from UDPClient.send
    def process_request(self, request, client_address):

        ### In some cases involving the media_changer, this function will
        ### process messages read from the child processes when
        ### DispatchingWorker.get_request() calls it.  The only major
        ### consecquence of this is that we don't put anything into
        ### ticket['r_a'].  If we do, these fake values of:
        ###    idn = 0
        ###    number = 0
        ###    client_address = ()
        ### will cause reply_with_address() and reply_to_caller() to do the
        ### wrong thing and not send back the reply.  This is becuase
        ### the media changer places these values into ticket['ra'],
        ### and we don't want to have two sets of competing information.
        ### Note: The use of 'r_a' was choosen internally for the
        ### udp_server becuase of the pre-existing use of 'ra' between
        ### the media_changer and udp_server.
       
        try:
            idn, number, ticket = udp_common.r_eval(request, check=self.check_request)
        except (NameError, ValueError), detail:
            Trace.trace(5, "must be an event relay message %s"%(detail,))
            # must be an event relay message
            # it has a different format
            try:
                rq = udp_common.r_eval(request, check=self.check_request)
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
            # it is from and empty UDP datagram.
            exc, msg = sys.exc_info()[:2]
            try:
                message = "%s: %s: From client %s:%s" % \
                          (exc, msg, client_address, request[:100])
            except IndexError:
                message = "%s: %s: From client %s: %s" % \
                          (exc, msg, client_address, request)
            #rint message
            Trace.log(10, message)

            #Set these to something.
            idn, number, ticket = (None, None, None)
        
        if idn == None or type(ticket) != types.DictType:
            Trace.log(e_errors.ERROR,
                      "Malformed request from %s %s" %
                      (client_address, request,))
            reply = (0L,{'status': (e_errors.MALFORMED, None)},None)
            self.tsd.server_socket.sendto(repr(reply), client_address)
            return None

        reply_address = client_address
        client_number = number
        current_id = idn
        #The following are not thread safe.
        ###self.reply_address = client_address
        ###self.client_number = number
        ###self.current_id = idn

        #The reason we need to include this information (at least
        # temporarily) is that for a multithreaded server it would
        # be possible for this function to process multiple requests
        # before repy_with_list() could be called from another thread(s).
        # In such a situation reply_to_caller() would reply with the
        # most recent request address and not to the one that made the request.
        #
        #It is tempting to use self.tsd to hold these values.  This won't
        # work because they would be stored in the main thread's thread-
        # specific-data and not necessarily the TSD of the thread that
        # will reply.
        if reply_address:
            ticket['r_a'] = (reply_address,
                             client_number,
                             current_id)

        if self.get_request_dict().has_key(idn):

            # UDPClient resends messages if it doesn't get a response
            # from us, see it we've already handled this request earlier. We've
            # handled it if we have a record of it in our dict
            try:
                lst = self.get_request_dict()[idn]
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

        return ticket

    ####################################################################

    # reply to sender with her number and ticket (which has status)
    # generally, the requested user function will send its response through
    # this function - this keeps the request numbers straight
    def reply_to_caller(self, ticket):
        if type(ticket) == types.DictType and ticket.get('r_a', None):
            reply_address = ticket['r_a'][0] 
            client_number = ticket['r_a'][1]
            current_id    = ticket['r_a'][2]

            del ticket['r_a']
        
        else:
            Trace.log(e_errors.ERROR, "request missing r_a: %s" % (ticket,))
            Trace.log_stack_trace()
            return

        reply = (client_number, ticket, time.time())
        self.reply_with_list(reply, reply_address, current_id)

    # reply to sender with her number and ticket (which has status)
    # generally, the requested user function will send its response through
    # this function - this keeps the request numbers straight
    def reply_to_caller2(self, raw_reply, ticket):
        if type(ticket) == types.DictType and ticket.get('r_a', None):
            reply_address = ticket['r_a'][0]
            client_number = ticket['r_a'][1]
            current_id    = ticket['r_a'][2]

            del ticket['r_a']

        else:
            Trace.log(e_errors.ERROR, "request missing r_a: %s" % (ticket,))
            Trace.log_stack_trace()
            return
        
        reply = (client_number, raw_reply, time.time())
        self.reply_with_list(reply, reply_address, current_id)

    # if a different interface is needed to send the reply on then use it.
    def reply_to_caller_using_interface_ip(self, ticket, interface_ip):
        if type(ticket) == types.DictType and ticket.get('r_a', None):
            reply_address = ticket['r_a'][0] 
            client_number = ticket['r_a'][1]
            current_id    = ticket['r_a'][2]

            del ticket['r_a']
        
        else:
            Trace.log(e_errors.ERROR, "request missing r_a: %s" % (ticket,))
            Trace.log_stack_trace()
            return

        reply = (client_number, ticket, time.time())
        self.reply_with_list(reply, reply_address, current_id, interface_ip)
        
    # keep a copy of request to check for later udp retries of same
    # request and then send to the user
    def reply_with_list(self, list, reply_address, current_id,
                        interface_ip = None):

        #print "reply_with_list interface_ip %s reply_address %s current_id %s"%(interface_ip, reply_address, current_id)
        self._lock.acquire()
        try:
            # there are rare cases when the following erro occurs:
            # RuntimeError: dictionary changed size during iteration
            # I do not know the reason
            # but this should help the code to proceed
            list_copy = copy.deepcopy(list)
        except:
            list_copy = None
            try:
                Trace.handle_error()
            except:
                pass
        self._lock.release()

        if not list_copy:
            # do not send a reply
            return

        #Remember the response.  If we get the same request again, we only
        # need to resend it from cache instead of recreating the response.
        # This doesn't really work for responses that use a TCP connection
        # to send the full response.
        self.set_request_dict(current_id, list_copy)
        
        if interface_ip == self.tsd.server_socket.getsockname()[0]:
            send_socket = self.tsd.server_socket
            with_interface = ""
            Trace.trace(e_errors.INFO, "reply_with_list: %s %s (same)" % (threading.current_thread().getName(), send_socket.getsockname()))
        elif interface_ip != None:
            ip, port, send_socket = udp_common.get_callback(interface_ip)
            with_interface = " with interface %s" % interface_ip
            Trace.trace(e_errors.INFO, "reply_with_list: %s %s (other)" % (threading.current_thread().getName(), send_socket.getsockname()))
        else:
            send_socket = self.tsd.server_socket
            Trace.trace(e_errors.INFO, "reply_with_list: %s %s" % (threading.current_thread().getName(), send_socket.getsockname()))
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
            send_socket.sendto(wrapped_list, reply_address)
        except:
            ### A long message can now be handled by generic_client and
            ### dispatching_worker.  Don't log a traceback here.
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    """
    # for requests that are not handled serially reply_address, current_id,
    # and client_number number must be reset.  In the forking media changer
    # these are in the forked child and passed back to us
    def reply_with_address(self, ticket):
        self.reply_address = ticket["ra"][0]
        self.client_number = ticket["ra"][1]
        self.current_id    = ticket["ra"][2]
        #reply = (self.client_number, ticket, time.time())
        #Trace.trace(19,"reply_with_address %s %s %s %s"%( 
        #    self.reply_address,
        #    self.current_id,
        #    self.client_number,
        #    reply))
        self.reply_to_caller(ticket)
    """

    ####################################################################
    
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
