###############################################################################
#
# $Id$
#
###############################################################################
"""
Receive client UDP requests, convert them into dictionary (enstore ticket) and
execute metod, specified by ticket['work'] in a loop.
"""

# system imports
import errno
import time
import os
import traceback
import checksum
import sys
#import socket
import signal
import threading
#import string
import copy
import types
import socket
import select

#enstore imports
import udp_server
import hostaddr
import cleanUDP
import Trace
import e_errors
import enstore_constants
import callback

MAX_THREADS = 50
MAX_CHILDREN = 32 #Do not allow forking more than this many child processes
DEFAULT_TTL = 60 #One minute lifetime for child processes

def thread_wrapper(function, args=(), after_function=None):
    t = time.time()
    Trace.trace(5,"dispatching_worker.thread_wrapper: function %s "%(function.__name__,))
    if type(args) != types.TupleType:
        apply(function, (args,))
    else:
        apply(function, args)
    if after_function:
        after_function()
    Trace.trace(5,"dispatching_worker.thread_wrapper: function %s time %s"%(function.__name__,time.time()-t))

def run_in_thread(thread_name, function, args=(), after_function=None):
    """
    Run function in a thread

    :type thread_name: :obj:`str`
    :arg thread_name: A string containing the name of the thread to use, or None.
       If thread_name is given, a limit of one is allowed.  If
       None is given, then at most MAX_THREADS number of threads
       are allowed.

    :type function: :obj:`callable`
    :arg function: The function to run in the thread.
    :type args: :obj:`tuple`
    :arg args: arguments.
    :type after_function: :obj:`callable`
    :arg after_function: function to run after function completes
    """
    # see what threads are running
    if thread_name:
        threads = threading.enumerate()
        for thread in threads:
            if ((thread.getName() == thread_name) and thread.isAlive()):
                Trace.trace(5, "thread %s is already running" % (thread_name))
                #We've exceeded the number of thread_name threads, which
                # is one.  Running it in main thread.
                thread_wrapper(function, args, after_function)
                return

    #Impose a prossess wise limit on the number of threads.
    thread_count = threading.activeCount()
    if thread_count >= MAX_THREADS:
        Trace.trace(5, "too many threads, %s, are already running" \
                    % (thread_count,))
        #We've exceeded the number of thread_name threads.
        # Running it in main thread.
        thread_wrapper(function, args, after_function)
        return

    args = (function,)+args
    if after_function:
        args = args + (after_function,)
    Trace.trace(5, "create thread: target %s name %s args %s" % (function, thread_name, args))
    thread = threading.Thread(group=None, target=thread_wrapper,
                              name=thread_name, args=args, kwargs={})
    Trace.trace(5, "starting thread %s"%(dir(thread,)))
    try:
        thread.start()
    except:
        exc, detail, tb = sys.exc_info()
        Trace.log(e_errors.ERROR, "error starting thread %s: %s" % (thread_name, detail))


class DispatchingWorker(udp_server.UDPServer):
    """
    Generic request response server class, for multiple connections
    Note that the get_request actually read the data from the socket
    """
    def __init__(self, server_address, use_raw=None):
        """

        :type server_address: :obj:`tuple`
        :arg server_address: (:obj:`str`- IP address, :obj:`int` - port) server address
        :type use_raw: :obj:`int`
        :arg use_raw: 0 - do not use intermediate raw input, > 0 - use intermediate udp
           buffering to avoid loss of incoming messages due to high rate of messages.
        """

        self.allow_callback = False
        if use_raw:
            # enable to spawn callback processing thread
            # by default in raw input mode
            self.allow_callback = True
        udp_server.UDPServer.__init__(self, server_address,
                                      receive_timeout=60.0,
                                      use_raw=use_raw)
        #If the UDPServer socket failed to open, stop the server.
        if self.server_socket == None:
            msg = "The udp server socket failed to open.  Aborting.\n"
            sys.stdout.write(msg)
            sys.exit(1)

        #Deal with multiple interfaces.

        # fds that the worker/server also wants watched with select
        self.read_fds = []
        self.write_fds = []
        # callback functions associated with above
        self.callback = {}
        # functions to call periodically -
        #  key is function, value is [interval, last_called]
        self.interval_funcs = {}

        ## Flag for whether we are in a child process.
        ## Server loops should be conditional on "self.is_child"
        ## rather than 'while 1'.
        self.is_child = 0
        self.n_children = 0
        self.kill_list = []

        self.custom_error_handler = None

    # call this right after initialization
    # when use raw is enabled (last parameter in __init__
    def enable_callback(self):
        self.allow_callback = True

    # call this right after initialization
    # when use raw is enabled (last parameter in __init__
    def disable_callback(self):
        self.allow_callback = False

    def add_interval_func(self, func, interval, one_shot=0,
                          align_interval = None):
        now = time.time()
        if align_interval:
            #Set this so that we start the intervals at prealigned times.
            # For example: if we want the interval to be 15 minutes and
            # the current time is 16:11::41; then set last_called to be
            # 11 minutes and 41 seconds ago.
            (year, month, day, hour, minutes, seconds, unsed, unused, unused) \
                   = time.localtime(now)
            day_begin = time.mktime((year, month, day, 0, 0, 0, -1 , -1 , -1))
            day_now = now - day_begin

            last_called = day_now + day_begin - \
                 (((day_now / interval) - int(day_now / interval)) * interval)

        else:
            last_called = now
        self.interval_funcs[func] = [interval, last_called, one_shot]

    def set_interval_func(self, func,interval):
        """
        Set a function which will be executed periodically

        :type func: :obj:`callable`
        :arg func: function to execute periodically
        :type interval: :obj:`int`
        :arg interval: function execution interval
        """


        #Backwards-compatibilty
        self.add_interval_func(func, interval)

    def remove_interval_func(self, func):
        """
        Remove interval function

        :type func: :obj:`callable`
        :arg func: function to remove
        """
        del self.interval_funcs[func]

    def reset_interval_timer(self, func):
        """
        Reset interval timer function

        :type func: :obj:`callable`
        :arg func: interval timer restarts for this function
        """
        self.interval_funcs[func][1] = time.time()

    def reset_interval(self, func, interval):
        """
        Reset interval for a periodic function

        :type func: :obj:`callable`
        :arg func: function to execute periodically
        :type interval: :obj:`int`
        :arg interval: function execution interval
        """
        self.interval_funcs[func][0] = interval
        self.interval_funcs[func][1] = time.time()

    def set_error_handler(self, handler):
        self.custom_error_handler = handler

    ####################################################################

    # check for any children that have exited (zombies) and collect them
    def collect_children(self):
        while self.n_children > 0:
            try:
                pid, status = os.waitpid(0, os.WNOHANG)
                if pid==0:
                    break
                self.n_children = self.n_children - 1
                if pid in self.kill_list:
                    self.kill_list.remove(pid)
                Trace.trace(6, "collect_children: collected %d, nchildren = %d"
                            % (pid, self.n_children))
            except os.error, msg:
                if msg.errno == errno.ECHILD: #No children to collect right now
                    break
                else: #Some other exception
                    Trace.trace(6,"collect_children %s"%(msg,))
                    raise os.error, msg

    def kill(self, pid, signal):
        if pid not in self.kill_list:
            return
        Trace.trace(6, "killing process %d with signal %d" % (pid,signal))
        try:
            os.kill(pid, signal)
        except os.error, msg:
            Trace.log(e_errors.ERROR, "kill %d: %s" %(pid, msg))

    def fork(self, ttl=DEFAULT_TTL):
        """Fork off a child process.  Use this instead of os.fork for safety"""
        if self.n_children >= MAX_CHILDREN:
            Trace.log(e_errors.ERROR, "Too many child processes!")
            return os.getpid()
        if self.is_child: #Don't allow double-forking
            Trace.log(e_errors.ERROR, "Cannot fork from child process!")
            return os.getpid()

        pid = os.fork()
        ### The incrementing of the number of childern should occur after
        ### the os.fork() call.  If it is before and os.fork() throws
        ### an execption, then we have a discrepencey.
        self.n_children = self.n_children + 1
        Trace.trace(6,"fork: n_children = %d"%(self.n_children,))

        if pid != 0:  #We're in the parent process
            if ttl is not None:
                self.kill_list.append(pid)
                self.add_interval_func(lambda self=self,pid=pid,sig=signal.SIGTERM: self.kill(pid,sig), ttl, one_shot=1)
                self.add_interval_func(lambda self=self,pid=pid,sig=signal.SIGKILL: self.kill(pid,sig), ttl+5, one_shot=1)
            self.is_child = 0
            return pid
        else:
            self.is_child = 1
            ##Should we close the control socket here?
            return 0

    def func_wrapper(self, function, args=(), after_function=None):
        if type(args) != types.TupleType:
            self.__invoke_function(function, (args,))
        else:
            self.__invoke_function(function, args)
        if after_function:
            after_function()

    # run_in_thread():
    # thread_name: A string containing the name of the thread to use, or None.
    #              If thread_name is given, a limit of one is allowed.  If
    #              None is given, then at most MAX_THREADS number of threads
    #              are allowed.
    # function: The function to run in the thread.  This is not the string
    #           name of the function.
    # args: Tuple of arguments.
    # after_function:
    #
    # Note: Python threads can't be killed.  Thus, there is no time-to-live
    #       aspect with threads like there is with processes.
    def run_in_thread(self, thread_name, function, args=(), after_function=None):
        """
        Run function in a thread

        :type thread_name: :obj:`str`
        :arg thread_name: A string containing the name of the thread to use, or None.
           If thread_name is given, a limit of one is allowed.  If
           None is given, then at most MAX_THREADS number of threads
           are allowed.

        :type function: :obj:`callable`
        :arg function: The function to run in the thread.
        :type args: :obj:`tuple`
        :arg args: arguments.
        :type after_function: :obj:`callable`
        :arg after_function: function to run after function completes
        """
        # see what threads are running
        if thread_name:
            threads = threading.enumerate()
            for thread in threads:
                if ((thread.getName() == thread_name) and thread.isAlive()):
                    Trace.trace(5, "thread %s is already running" % (thread_name))
                    #We've exceeded the number of thread_name threads, which
                    # is one.  Running it in main thread.
                    self.func_wrapper(function, args, after_function)
                    return

        #Impose a prossess wise limit on the number of threads.
        thread_count = threading.activeCount()
        if thread_count >= MAX_THREADS:
            Trace.trace(5, "too many threads, %s, are already running" \
                        % (thread_count,))
            #We've exceeded the number of thread_name threads.
            # Running it in main thread.
            self.func_wrapper(function, args, after_function)
            return

        args = (function,)+args
        if after_function:
            args = args + (after_function,)
        Trace.trace(5, "create thread: target %s name %s args %s" % (function, thread_name, args))
        thread = threading.Thread(group=None, target=self.func_wrapper,
                                  name=thread_name, args=args, kwargs={})
        Trace.trace(5, "starting thread %s"%(dir(thread,)))
        try:
            thread.start()
        except:
            exc, detail = sys.exc_info()[:2]
            Trace.log(e_errors.ERROR, "error starting thread %s: %s" % (thread_name, detail))

    # run_in_process():
    # process_name: A string containing the name of the thread to use, or None.
    #              If thread_name is given, a limit of one is allowed.  If
    #              None is given, then at most MAX_THREADS number of threads
    #              are allowed.
    # function: The function to run in the thread.  This is not the string
    #           name of the function.
    # args: Tuple of arguments.
    # after_function:
    #
    #Note: This is intended for responding to client messages.  If a process
    #      needs to fork a process that will live for a long time,
    #      look at the dispatching_work.fork() function instead.
    def run_in_process(self, process_name, function, args=(), after_function = None):
        """
        Run function in a process. This is implemented via fork.

        :type process_name: :obj:`str`
        :arg process_name: A string containing the name of the thread to use, or None.
           If thread_name is given, a limit of one is allowed.  If
           None is given, then at most MAX_THREADS number of threads
           are allowed.

        :type function: :obj:`callable`
        :arg function: The function to run in the thread.
        :type args: :obj:`tuple`
        :arg args: arguments.
        :type after_function: :obj:`callable`
        :arg after_function: function to run after function completes
        """

        Trace.trace(5, "create process: target %s name %s args %s" % (function, process_name, args))
        try:
            pid = os.fork()
        except OSError, msg:
            Trace.log(e_errors.ERROR, "fork() failed: %s\n" % (str(msg),))
            return
        if pid > 0:  #parent
            #Add this to the list.
            self.kill_list.append(pid)
            self.add_interval_func(lambda self=self, pid=pid,
                                   sig=signal.SIGTERM: self.kill(pid, sig),
                                   DEFAULT_TTL, one_shot = 1)
            self.add_interval_func(lambda self=self, pid=pid,
                                   sig=signal.SIGKILL: self.kill(pid,sig),
                                   DEFAULT_TTL + 5, one_shot = 1)
        else: #child
            #Clear the list of the parent's other childern.  They are
            # not the childern of this current process.
            self.kill_list = []

            try:
                self.func_wrapper(function, args, after_function)
                res = 0
            except:
                res = 1
                exc, msg, tb = sys.exc_info()
                Trace.handle_error(exc, msg, tb)
                del tb #Avoid cyclic references.
                Trace.log(e_errors.ERROR, "error starting process %s: %s" \
                          % (process_name, msg))

            os._exit(res) #child exit

    ####################################################################

    # serve callback requests
    def serve_callback(self):
        while 1:
            try:
                self.get_fd_message()
            except:
                exc, msg, tb = sys.exc_info()
                Trace.handle_error(exc, msg, tb)

    def serve_forever(self):
        """Handle one request at a time until doomsday, unless we are in a child process"""
        ###XXX should have a global exception handler here
        count = 0
        if self.use_raw:
            self.set_out_file()
            if self.allow_callback:
                Trace.trace(5, "spawning get_fd_message")
                # spawn callback processing thread (event relay messages)
                run_in_thread("call_back_proc", self.serve_callback)

            # start receiver thread or process
            self.raw_requests.receiver()
        while not self.is_child:
            self.do_one_request()
            self.collect_children()
            count = count + 1
            #if count > 100:
            if count > 20:
                self.purge_stale_entries()
                count = 0

        if self.is_child:
            Trace.trace(6,"serve_forever, child process exiting")
            os._exit(0) ## in case the child process doesn't explicitly exit
        else:
            Trace.trace(6,"serve_forever, shouldn't get here")

    def get_request(self):
        """
        Get request, coming from the client.

        :rtype: :obj:`tuple` (:obj:`str` - message, :obj:`tuple` (:obj:`str`- IP address, :obj:`int` - port) - client address)
        """
        if self.use_raw:
            request, client_address = self._get_request_multi()
        else:
            request, client_address = self._get_request_single()
        return request, client_address


    def do_one_request(self):
        """Receive and process one request, possibly blocking."""
        # request is a "(idn,number,ticket)"
        request = None
        try:
            request, client_address = self.get_request()
        except:
            exc, msg = sys.exc_info()[:2]

        now=time.time()

        for func, time_data in self.interval_funcs.items():
            interval, last_called, one_shot = time_data
            if now - last_called > interval:
                if one_shot:
                    del self.interval_funcs[func]
                else: #record last call time
                    self.interval_funcs[func][1] =  now
                Trace.trace(6, "do_one_request: calling interval_function %s"%(func,))
                func()

        if request is None: #Invalid request sent in
            return

        if request == '':
            # nothing returned, must be timeout
            self.handle_timeout()
            return
        try:
            self.process_request(request, client_address)
        except KeyboardInterrupt:
            traceback.print_exc()
        except SystemExit, code:
            # processing may fork (forked process will call exit)
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

    def read_fd(self, fd):

            raw_bytecount = os.read(fd, 8)

            #Read on the number of bytes in the message.
            try:
                bytecount = int(raw_bytecount)
            except ValueError:
                Trace.trace(20,
                            'get_request_select: bad bytecount %s %s'
                            % (raw_bytecount, len(raw_bytecount)))
                bytecount = 0

            #Read in the message.
            msg = ""
            while len(msg)<bytecount:
                tmp = os.read(fd, bytecount - len(msg))
                if not tmp:
                    break
                msg = msg+tmp

            #Finish off the communication.
            self.remove_select_fd(fd)
            os.close(fd)

            #Return the request and an empty address.
            addr = ()
            return (msg, addr)

    def _get_request_single(self):
        """
        Get request in single threaded environment,
        when use_raw is set to 0,
        hence rawUDP is not used.
        This is a copy of the old get_request.

        :rtype: :obj:`tuple` (:obj:`str` - message, :obj:`tuple` (:obj:`str`- IP address, :obj:`int` - port) - client address)

        Returned string is a stringified ticket, after CRC is removed.
        There are three cases:

           read from socket where crc is stripped and return address is valid

           read from pipe where there is no crc and no r.a.

           time out where there is no string or r.a.
        """
        while True:
            r = self.read_fds + [self.server_socket]
            w = self.write_fds

            rcv_timeout = self.rcv_timeout

            if self.interval_funcs:
                now = time.time()
                for func, time_data in self.interval_funcs.items():
                    interval, last_called, one_shot = time_data
                    rcv_timeout = min(rcv_timeout,
                                      interval - (now - last_called))

                rcv_timeout = max(rcv_timeout, 0)

            r, w, x, remaining_time = cleanUDP.Select(r, w, r+w, rcv_timeout)
            if not r + w:
                return ('',()) #timeout

            #handle pending I/O operations first
            for fd in r + w:
                if self.callback.has_key(fd) and self.callback[fd]:
                    self.callback[fd](fd)

            #now handle other incoming requests
            for fd in r:

                if (type(fd) == types.IntType and
                    fd in self.read_fds and
                    self.callback[fd]==None):
                    #XXX this is special-case code,
                    #for old usage in media_changer

                    (request, addr) = self.read_fd(fd)
                    return (request, addr)

                elif fd == self.server_socket:
                    #Get the 'raw' request and the address from whence it came.
                    (request, addr) = udp_server.UDPServer.get_message(self)

                    #Skip these if there is nothing to do.
                    if request == None or addr in [None, ()]:
                        #These conditions could be caught when
                        # hostaddr.allow() raises an exception.  Since,
                        # these are obvious conditions, we stop here to avoid
                        # the Trace.log() that would otherwise fill the
                        # log file with useless error messages.
                        return (request, addr)

                    #Determine if the address the request came from is
                    # one that we should be responding to.
                    try:
                        is_valid_address = hostaddr.allow(addr)
                    except (IndexError, TypeError), detail:
                        Trace.log(e_errors.ERROR,
                                  "hostaddr failed with %s Req.= %s, addr= %s"\
                                  % (detail, request, addr))
                        request = None
                        return (request, addr)

                    #If it should not be responded to, handle the error.
                    if not is_valid_address:
                        Trace.log(e_errors.ERROR,
                               "attempted connection from disallowed host %s" \
                                  % (addr[0],))
                        request = None
                        return (request, addr)

                    return (request, addr)

        return (None, ())

    def _get_request_multi(self):
        """
        Get request in multi threaded environment,
        when use_raw is set to 1,
        hence rawUDP is used.

        :rtype: :obj:`tuple` (:obj:`str` - message, :obj:`tuple` (:obj:`str`- IP address, :obj:`int` - port) - client address)

        Returned string is a stringified ticket, after CRC is removed.
        """

        t0 = time.time()

        while True:
            r = [self.server_socket]
            rcv_timeout = self.rcv_timeout
            if self.interval_funcs:
                now = time.time()
                for func, time_data in self.interval_funcs.items():
                    interval, last_called, one_shot = time_data
                    rcv_timeout = min(rcv_timeout,
                                      interval - (now - last_called))

                rcv_timeout = max(rcv_timeout, 0)
            try:
                rc = self.get_message()
                Trace.trace(5, "disptaching_worker!!: get_request %s"%(rc,))
            except (NameError, ValueError), detail:
                Trace.trace(5, "dispatching_worker: nameerror %s"%(detail,))
                self.erc.error_msg = str(detail)
                self.handle_er_msg(None)
                return None, None
            if rc and rc != ('',()):
                #Trace.trace(5, "disptaching_worker: get_request %s"%(rc,))
                request, addr = rc[0], rc[1]

                #Determine if the address the request came from is
                # one that we should be responding to.
                try:
                    is_valid_address = hostaddr.allow(addr)
                except (IndexError, TypeError), detail:
                    Trace.log(e_errors.ERROR,
                              "hostaddr failed with %s Req.= %s, addr= %s"\
                              % (detail, request, addr))
                    request = None
                    return (request, addr)

                #If it should not be responded to, handle the error.
                if not is_valid_address:
                    Trace.log(e_errors.ERROR,
                           "attempted connection from disallowed host %s" \
                              % (addr[0],))
                    request = None
                    return (request, addr)

                return rc
            else:
                # process timeout
                if time.time()-t0 > rcv_timeout:
                    return ('',()) #timeout

        return (None, ())


    def get_fd_message(self):
        """
        Get and process read and write fds.
        This method runs in a separate tread.

        There are three cases:
           read from socket where crc is stripped and return address is valid (event relay messages)

           read from pipe where there is no crc and no reply address (r.a.)

           time out where there is no string or r.a.
        """
        while True:
            r = self.read_fds
            w = self.write_fds

            rcv_timeout = self.rcv_timeout

            r, w, x, remaining_time = cleanUDP.Select(r, w, r+w, rcv_timeout)
            if not r + w:
                return ('',()) #timeout

            #handle pending I/O operations first
            for fd in r + w:
                if self.callback.has_key(fd) and self.callback[fd]:
                    Trace.trace(5,"get_fd_message - got one")
                    self.callback[fd](fd)

        return (None, ())

    ####################################################################

    def process_request(self, request, client_address):
        """
        Process the  request that was (generally) sent from UDPClient.send.
        Since get_request() gets requests from UDPServer.get_message()
        and self.read_fd(fd).  Thusly, some care needs to be taken
        from within UDPServer.process_request() to be tolerent of
        requests not originally read with UDPServer.get_message().

        :type request: :obj:`str`
        :arg  request: message
        :type client_address: :obj:`tuple`
        :arg client_address: (:obj:`str`- IP address, :obj:`int` - port)
        """

        ticket = udp_server.UDPServer.process_request(self, request,
                                                      client_address)

        Trace.trace(6, "dispatching_worker:process_request %s; %s"%(request, ticket,))
        #This checks help process cases where the message was repeated
        # by the client.
        if not ticket:
            Trace.trace(6, "dispatching_worker: no ticket!!!")
            return

        # look in the ticket and figure out what work user wants
        try:
            function_name = ticket["work"]
        except (KeyError, AttributeError, TypeError), detail:
            ticket = {'status' : (e_errors.KEYERROR,
                                  "cannot find any named function")}
            msg = "%s process_request %s from %s" % \
                (detail, ticket, client_address)
            Trace.trace(6, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            self._done_cleanup()
            return

        try:
            Trace.trace(5,"process_request: function %s"%(function_name,))
            function = getattr(self,function_name)
        except (KeyError, AttributeError, TypeError), detail:
            ticket = {'status' : (e_errors.KEYERROR,
                                  "cannot find requested function `%s'"
                                  % (function_name,))}
            msg = "%s process_request %s %s from %s" % \
                (detail, ticket, function_name, client_address)
            Trace.trace(6, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            self._done_cleanup()
            return

        # call the user function
        self.invoke_function(function, (ticket,))

    def __invoke_function(self, function, args=()):
        Trace.trace(5, "invoke_function: function %s" % (function.func_name,))
        t = time.time()
        apply(function, args)
        Trace.trace(5, "invoke_function: function %s time %s" \
                    % (function.func_name, time.time() - t))

    def invoke_function(self, function, args=(), after_function = None):
        """
        This function has been introduced as convenience, so
        that subclasses can override it as they see fit w/o
        touching dispatching_worker.process_request().

        :type function: :obj:`callable`
        :arg function: The function to run in the thread.
        :type args: :obj:`tuple`
        :arg args: arguments.
        :type after_function: :obj:`callable`
        :arg after_function: function to run after function completes
           after_function is only used if overloaded version in a server uses it.

        """
        self.__invoke_function(function, args)
        self._done_cleanup()

    def handle_error(self, request, client_address):
        """
        Response to error occured during request processing.
        This method logs an error and responds to the client

        :type request: :obj:`str`
        :arg  request: message
        :type client_address: :obj:`tuple`
        :arg client_address: (:obj:`str`- IP address, :obj:`int` - port)

        """
        exc, msg, tb = sys.exc_info()
        Trace.trace(6,"handle_error %s %s"%(exc,msg))
        Trace.log(e_errors.INFO,'-'*40)
        Trace.log(e_errors.INFO,
                  'Exception during request from %s, request=%s'%
                  (client_address, request))
        Trace.handle_error(exc, msg, tb)
        Trace.log(e_errors.INFO,'-'*40)
        if self.custom_error_handler:
            self.custom_error_handler(exc,msg,tb)
        else:
            self.reply_to_caller( {'status':(str(exc),str(msg), 'error'),
                                   'request':request,
                                   'exc_type':str(exc),
                                   'exc_value':str(msg)} )

    ####################################################################

    def alive(self,ticket):
        """
        Work instance. Send echo to client.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = 'alive'
        """
        ticket['address'] = self.server_address
        ticket['status'] = (e_errors.OK, None)
        ticket['pid'] = os.getpid()
        self.reply_to_caller(ticket)


    def _do_print(self, ticket):
        """
        Work instance. Turn on debugging printing to STDO. Do not confirm to client.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = '_do_print'
        """
        Trace.do_print(ticket['levels'])

    def do_print(self, ticket):
        """
        Work instance. Turn on debugging printing to STDO.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = 'do_print'
        """
        Trace.do_print(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)

    def dont_print(self, ticket):
        """
        Work instance. Turn off debugging printing to STDO.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = 'dont_print'
        """
        Trace.dont_print(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)

    def do_log(self, ticket):
        """
        Work instance. Turn on debugging logging.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = 'do_log'
        """
        Trace.do_log(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)

    def dont_log(self, ticket):
        """
        Work instance. Turn off debugging logging.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = 'dont_log'
        """
        Trace.dont_log(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)

    def do_alarm(self, ticket):
        """
        Work instance. Turn on debugging logging and alarming.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = 'do_alarm'
        """
        Trace.do_alarm(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)

    def dont_alarm(self, ticket):
        """
        Work instance. Turn off debugging logging and alarming.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = 'do_alarm'
        """
        Trace.dont_alarm(ticket['levels'])
        ticket['status']=(e_errors.OK, None)
        self.reply_to_caller(ticket)


    def quit(self,ticket):
        """
        Work instance. Quit server.

        :type ticket: :obj:`dict`
        :arg ticket: ticket from client with ticket['work'] = 'quit'
        """
        if sys.version_info >= (2, 6):
            import multiprocessing
            children = multiprocessing.active_children()
            for p in children:
                p.terminate()
        Trace.trace(10,"quit address="+repr(self.server_address))
        ticket['address'] = self.server_address
        ticket['status'] = (e_errors.OK, None)
        ticket['pid'] = os.getpid()
        Trace.log( e_errors.INFO, 'QUITTING... via os._exit')
        self.reply_to_caller(ticket)
        sys.stdout.flush()
        os._exit(0) ##MWZ: Why not sys.exit()?  No servers fork() anymore...

    # cleanup if we are done with this unique id
    def done_cleanup(self, ticket):
        #The parameter ticket is necessary since that is part of the
        # interface.  All other 'work' related functions also have it.
        __pychecker__ = "unusednames=ticket"
        self._done_cleanup()

    ####################################################################

    # send back our response
    def send_reply(self, t):
        save_copy = copy.copy(t)
        try:
            self.reply_to_caller(t)
        except:
            # even if there is an error - respond to caller so he can process it
            exc, msg = sys.exc_info()[:2]

            if isinstance(msg, socket.error) and msg.args[0] == errno.EMSGSIZE:
                self.send_reply_with_long_answer(save_copy)
            else:
                t["status"] = (str(exc), str(msg))
                self.reply_to_caller(t)
                Trace.trace(enstore_constants.DISPWORKDBG,
                            "exception in send_reply %s" % (t,))
                Trace.handle_error(exc, msg, sys.exc_info()[2])
                return

    def send_reply_with_long_answer_part1(self, ticket):
        """
        This functions uses an acitve protocol.  This function uses UDP and TCP.

        :type ticket: :obj:`dict`
        :arg ticket: client request ticket
        """

        if not e_errors.is_ok(ticket):
            #If we have an error, then we only need to reply and skip the rest.
            self.reply_to_caller(ticket)
            return None

        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)

        #The initial over UDP message needs to be small.
        small_reply = {'work' : ticket['work'],
                       'r_a' : ticket['r_a'],
                       'status' : (e_errors.OK, None),
                       'callback_addr' : (host, port),
                       'long_reply' : 1, #Tell the client the answer is long.
                       }

        #Tell the client to wait for a connection.
        small_reply_copy = copy.copy(small_reply)
        self.reply_to_caller(small_reply_copy)

        #Wait for the client to connect over TCP.
        for i in range(12):
            r, w, x = select.select([listen_socket], [], [], 5)
            if not r:
                #Tell the client to wait for a connection.
                small_reply_copy = copy.copy(small_reply)
                self.reply_to_caller(small_reply_copy)
            else:
                #We've been connected to.
                break
        else:
            #We didn't hear back from the other side.
            listen_socket.close()
            message = "connection timedout from %s" % (ticket['r_a'],)
            Trace.log(e_errors.ERROR, message)
            return None

        #Accept the servers connection.
        control_socket, address = listen_socket.accept()

        #Veify that this connection is made from an acceptable
        # IP address.
        if not hostaddr.allow(address):
            control_socket.close()
            listen_socket.close()
            message = "address %s not allowed" % (address,)
            Trace.log(e_errors.ERROR, message)
            return None

        #Socket cleanup.
        listen_socket.close()

        return control_socket

    def send_reply_with_long_answer_part2(self, control_socket, ticket):
        """
        Generalize the code to have a really large ticket be returned.
        This functions uses an acitve protocol.  This function uses UDP and TCP.

        :type ticket: :obj:`dict`
        :arg ticket: client request ticket
        """
        try:
            #Write reply on control socket.
            callback.write_tcp_obj_new(control_socket, ticket)
        except (socket.error), msg:
            message = "failed to use control socket: %s" % (str(msg),)
            Trace.log(e_errors.NET_ERROR, message)

        #Socket cleanup.
        control_socket.close()

    def send_reply_with_long_answer(self, ticket):
        """
        Generalize the code to have a really large ticket be returned.
        This functions uses an acitve protocol.  This function uses UDP and TCP.

        The 'ticket' is sent over the network.
        'long_items' is a list of elements that should be supressed in the
        initial UDP response.

        :type ticket: :obj:`dict`
        :arg ticket: client request ticket
        """
        control_socket = self.send_reply_with_long_answer_part1(ticket)
        if not control_socket:
            return

        self.send_reply_with_long_answer_part2(control_socket, ticket)

    ####################################################################

    def restricted_access(self,ticket=None):
        '''
        restricted_access(self) -- check if the service is restricted

        restricted service can only be requested on the server node
        This function simply compares self.server_address and
        self.reply_address. If they match, return None. If not, return a
        error status that can be used in reply_to_caller.
        '''
        local_reply_address=None
        if not ticket:
            local_reply_address = self.reply_address[0]
        else:
            r_a = ticket.get('r_a', None)
            if not r_a:
                local_reply_address = self.reply_address[0]
            else:
                local_reply_address = r_a[0][0]
        if local_reply_address in self.ipaddrlist:
            return None

        return (e_errors.ERROR,
                "This restricted service can only be requested from node %s"
                % (self.node_name))



