###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import errno
import time
import os
import traceback
import checksum
import sys
import socket
import os
import string
import fcntl
import FCNTL
import copy
import types

#enstore imports
import cleanUDP
import Trace
import e_errors


# Generic request response server class, for multiple connections
# Note that the get_request actually read the data from the socket

class DispatchingWorker:

    
    def __init__(self, server_address):
        self.socket_type = socket.SOCK_DGRAM
        self.max_packet_size = 16384
        self.rcv_timeout = 60.   # timeout for get_request in sec.
        self.address_family = socket.AF_INET
        

        self.server_address = server_address
        self.read_fds = []    # fds that the worker/server also wants watched with select
        self.write_fds = []   # fds that the worker/server also wants watched with select
        self.callback = {} #callback functions associated with above
        self.request_dict = {} # used to recognize UDP retries
        self.request_dict_ttl = 1800 # keep requests in request dict for this many seconds
        self.interval_funcs = {} # functions to call periodically - key is function, value is [interval, last_called]

        
        ## flag for whether we are in a child process
        ## Server loops should be conditional on "self.is_child" rather than 'while 1'
        self.is_child = 0
        self.server_socket = cleanUDP.cleanUDP (self.address_family,
                                    self.socket_type)
        self.custom_error_handler = None

        # set this socket to be closed in case of an exec
        fcntl.fcntl(self.server_socket.fileno(), FCNTL.F_SETFD, FCNTL.FD_CLOEXEC)
        self.server_bind()

    def add_interval_func(self, func, interval):
        self.interval_funcs[func] = [interval, 0]
        self.rcv_timeout = min(interval, self.rcv_timeout)

    def remove_interval_func(self, func):
        del self.interval_funcs[func]
        
    def reset_interval_timer(self, func):
        self.interval_funcs[func][1] = time.time()

    def set_error_handler(self, handler):
        self.custom_error_handler = handler

    def purge_stale_entries(self):
        stale_time = time.time() - self.request_dict_ttl
        count = 0 
        for key, value in self.request_dict.items():
            if  value[2] < stale_time:
                del self.request_dict[key]
                count = count+1
        Trace.trace(20,"purge_stale_entries count=%d"%(count,))

    # check for any children that have exited (zombies) and collect them
    def collect_children(self):
        try:
            pid, status = os.waitpid(0, os.WNOHANG)
        except os.error, msg:
            if msg.errno != errno.ECHILD:
                Trace.trace(6,"collect_children %s"%(msg,))
                raise os.error, msg
        
    def fork(self):
        """Fork off a child process.  Use this instead of os.fork for safety"""
        pid = os.fork()
        
        if pid != 0:  #We're in the parent process
            self.is_child = 0
            return pid
        else:
            self.is_child = 1
            ##Should we close the control socket here?
            return 0
        
        
    def server_bind(self):
        """Called by constructor to bind the socket.

        May be overridden.

        """
        Trace.trace(16,"server_bind add %s"%(self.server_address,))
        self.server_socket.bind(self.server_address)

    
    def serve_forever(self):
        """Handle one request at a time until doomsday, unless we are in a child process"""
        ###XXX should have a global exception handler here
        count = 0
        while not self.is_child:
            self.do_one_request()
            self.collect_children()
            count = count + 1
            if count > 100:
                self.purge_stale_entries()
                count = 0
                
        if self.is_child:
            Trace.trace(6,"serve_forever, child process exiting")
            os._exit(0) ## in case the child process doesn't explicitly exit
        else:
            Trace.trace(6,"serve_forever, shouldn't get here")

    def do_one_request(self):
        """Recieve and process one request, possibly blocking."""
        # request is a "(idn,number,ticket)"
        request, client_address = self.get_request()
        now=time.time()

        for func, time_data in self.interval_funcs.items():
            interval, last_called = time_data
            if now - last_called > interval:
                self.interval_funcs[func] = [interval, now]
                func()

        if request == '':
            # nothing returned, must be timeout
            self.handle_timeout()
            return
        try:
            self.process_request(request, client_address)
        except KeyboardInterrupt:
            traceback.print_exc()
        except SystemExit, code:        # processing may fork (forked process will call exit)
            sys.exit( code )
        except:
            self.handle_error(request, client_address)



    # a server can add an fd to the server_fds list
    def add_select_fd(self, fd, write=0, callback=None):
        if fd is None:
            return
        if write:
            if fd not in self.write_fds:
                self.write_fds.append(fd)
        else:
            if fd not in self.read_fds:
                self.read_fds.append(fd)
        self.callback[fd]=callback
        
    def remove_select_fd(self, fd):
        if fd is None:
            return

        while fd in self.write_fds:
            self.write_fds.remove(fd)
        while fd in self.read_fds:
            self.read_fds.remove(fd)
        if self.callback.has_key(fd):
            del self.callback[fd]

        
    def get_request(self):
        # returns  (string, socket address)
        #      string is a stringified ticket, after CRC is removed
        # There are three cases:
        #   read from socket where crc is stripped and return address is valid
        #   read from pipe where there is no crc and no r.a.     
        #   time out where there is no string or r.a.

        gotit = 0
        while not gotit:

            r = self.read_fds + [self.server_socket]
            w = self.write_fds

            rcv_timeout = self.rcv_timeout

            if self.interval_funcs:
                now = time.time()
                for func, time_data in self.interval_funcs.items():
                    interval, last_called = time_data
                    rcv_timeout = min(rcv_timeout, interval - (now - last_called))

                rcv_timeout = max(rcv_timeout, 0)

            r, w, x, remaining_time = cleanUDP.Select(r, w, r+w, rcv_timeout)

            if not r + w:
                return ('',()) #timeout

            #handle pending I/O operations first
            for fd in r+w:
                if self.callback.has_key(fd) and self.callback[fd]:
                    self.callback[fd](fd)


            #now handle other incoming requests
            for fd in r:
                if fd in self.read_fds and self.callback[fd]==None: #XXX this is special-case code,
                                                        ##for old usage in media_changer
                    msg = os.read(fd, 8)
                    try:
                        bytecount = string.atoi(msg)
                    except:
                        Trace.trace(20,'get_request_select: bad bytecount %s %s' % (msg,len(msg)))
                        break
                    msg = ""
                    while len(msg)<bytecount:
                        tmp = os.read(fd, bytecount - len(msg))
                        if not tmp:
                            break
                        msg = msg+tmp
                    request= (msg,())                    #             if so read it
                    self.remove_select_fd(fd)
                    os.close(fd)

                    return request
                elif fd == self.server_socket:
                    req = self.server_socket.recvfrom(self.max_packet_size, self.rcv_timeout)
                    gotit = 1
                    request,inCRC = eval(req[0])
                    # calculate CRC
                    crc = checksum.adler32(0L, request, len(request))
                    if (crc != inCRC) :
                        Trace.trace(6,"handle_request - bad CRC inCRC="+repr(inCRC)+
                                    " calcCRC="+repr(crc))
                        Trace.log(e_errors.INFO, "BAD CRC request: "+request)
                        Trace.log(e_errors.INFO,
                                  "CRC: "+repr(inCRC)+" calculated CRC: "+repr(crc))
                        request=""

        return (request, req[1])

    def handle_timeout(self):
        # override this method for specific timeout hadling
        pass

    def fileno(self):
        """Return socket file number.

        Interface required by select().

        """
        return self.server_socket.fileno()

    # Process the  request that was (generally) sent from UDPClient.send
    def process_request(self, request, client_address):
        # ref udp_client.py (i.e. we may wish to have a udp_client method
        # to get this information)
        idn, number, ticket = eval(request)
        self.reply_address = client_address
        self.client_number = number
        self.current_id = idn

        
        if self.request_dict.has_key(idn):

            # UDPClient resends messages if it doesn't get a response
            # from us, see it we've already handled this request earlier. We've
            # handled it if we have a record of it in our dict
            lst = self.request_dict[idn]
            if lst[0] == number:
                Trace.trace(6,"process_request "+repr(idn)+" already handled")
                self.reply_with_list(lst)
                return

            # if the request number is smaller, then there has been a timing
            # race and we've already handled this as much as we are going to.
            elif number < lst[0]: 
                Trace.trace(6,"process_request "+repr(idn)+" old news")
                return #old news, timing race....

        # look in the ticket and figure out what work user wants
        try:
            function_name = ticket["work"]
            function = getattr(self,function_name)
        except (KeyError, AttributeError), detail:
            ticket = {'status' : (e_errors.KEYERROR, 
                                  "cannot find requested function `%s'"%(function_name,))}
            Trace.trace(6,"%s process_request %s %s"%(detail,ticket,function_name))
            self.reply_to_caller(ticket)
            return

        # call the user function
        try:
            apply(function, (ticket,))
        except TypeError, detail:
            Trace.log(e_errors.ERROR, "process request from %s: %s(%s) raised %s" %
                      (client_address, function, ticket, detail))
                      
    def handle_error(self, request, client_address):
        exc, msg, tb = sys.exc_info()
        Trace.trace(6,"handle_error %s %s"%(exc,msg))
        Trace.log(e_errors.INFO,'-'*40)
        Trace.log(e_errors.INFO,
                  'Exception during request from %s, request=%s'%
                  (client_address, request))
        e_errors.handle_error(exc, msg, tb)
        Trace.log(e_errors.INFO,'-'*40)
        if self.custom_error_handler:
            self.custom_error_handler(exc,msg,tb)
        else:
            self.reply_to_caller( {'status':(str(exc),str(msg), 'error'), 
                                   'request':request, 
                                   'exc_type':str(exc), 
                                   'exc_value':str(msg)} )

    def alive(self,ticket):
        ticket['address'] = self.server_address
        ticket['status'] = (e_errors.OK, None)
        ticket['pid'] = os.getpid()
        self.reply_to_caller(ticket)


    def do_print(self, ticket):
        Trace.do_print(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)

    def dont_print(self, ticket):
        Trace.dont_print(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)

    def do_log(self, ticket):
        Trace.do_log(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)
        
    def dont_log(self, ticket):
        Trace.dont_log(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)

    def do_alarm(self, ticket):
        Trace.do_alarm(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)
        
    def dont_alarm(self, ticket):
        Trace.dont_alarm(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)
        
        
    # quit instead of being killed
    def quit(self,ticket):
        Trace.trace(10,"quit address="+repr(self.server_address))
        ticket['address'] = self.server_address
        ticket['status'] = (e_errors.OK, None)
        ticket['pid'] = os.getpid()
        Trace.log( e_errors.INFO, 'QUITTING... via os._exit')
        self.reply_to_caller(ticket)
        os._exit(0)

    # cleanup if we are done with this unique id
    def done_cleanup(self,ticket):
        try:
            ##Trace.trace(20,"done_cleanup id="+repr(self.current_id))
            del self.request_dict[self.current_id]
        except KeyError:
            pass

    # reply to sender with her number and ticket (which has status)
    # generally, the requested user function will send its response through
    # this function - this keeps the request numbers straight
    def reply_to_caller(self, ticket):
        reply = (self.client_number, ticket, time.time()) 
        self.reply_with_list(reply)          

    # keep a copy of request to check for later udp retries of same
    # request and then send to the user
    def reply_with_list(self, list):
        self.request_dict[self.current_id] = copy.deepcopy(list)
        self.server_socket.sendto(repr(self.request_dict[self.current_id]), self.reply_address)
        
    # for requests that are not handled serially reply_address, current_id, and client_number
    # number must be reset.  In the forking media changer these are in the forked child
    # and passed back to us
    def reply_with_address(self,ticket):
        self.reply_address = ticket["ra"][0] 
        self.client_number = ticket["ra"][1]
        self.current_id    = ticket["ra"][2]
        reply = (self.client_number, ticket, time.time())
        Trace.trace(19,"reply_with_address %s %s %s %s"%( 
            self.reply_address,
            self.current_id,
            self.client_number,
            reply))
        self.reply_to_caller(ticket)
