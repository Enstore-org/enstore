#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
The following functions are those intended for use by other modules:
read_obj:          Use for reading python objects from a pipe file descriptor.
read_tcp_obj:      Use for reading python objects from a tcp socket.
write_obj:         Use for writing python objects to a pipe file descriptor.
write_tcp_obj_new: Use for writing python objects to a tcp socket.  
get_callback:      Use to obtain a tcp socket.

Use of write_tcp_obj() is discouraged for new code.  It uses repr instead
of pickle.
"""

# system imports
import time
import sys
import os
import random
import select
import socket
import cPickle
import errno
import fcntl
import types

# enstore imports
import Trace
import e_errors
import checksum
import host_config
from en_eval import en_eval

def hex8(x):
    s=hex(x)[2:]  #kill the 0x
    if type(x)==type(1L): s=s[:-1]  # kill the L
    l = len(s)
    if l>8:
        raise OverflowError, x
    return '0'*(8-l)+s

def __get_socket_state(fd):
    if os.uname()[0] == "Linux":
        import stat
        try:
            #Determine the current socket state.

            #This table of values is from /usr/include/linux/tcp.h.
            tcp_states = { 1 : "ESTABLISHED",
                           2 : "SYN_SENT",
                           3 : "SYN_RECV",
                           4 : "FIN_WAIT1",
                           5 : "FIN_WAIT2",
                           6 : "TIME_WAIT",
                           7 : "CLOSE",
                           8 : "CLOSE_WAIT",
                           9 : "LAST_ACK",
                           10 : "LISTEN",
                           11 : "CLOSING"
                           }
            #First get the inode (as a string for comparison).
            inode = str(os.fstat(fd)[stat.ST_INO])[:-1]

            #Second, read the entire table of tcp sockets.
            net_tcp = open("/proc/net/tcp", "r")
            net_tcp_data = net_tcp.readlines()
            net_tcp.close()

            #Find the entry that corresponds to this socket.
            state = ""
            line = ""
            for line in net_tcp_data:
                result = line.find(inode)
                if result >= 0:
                    state = line[33:37]
                    break
            else:
                line = ""
                state = ""

            return tcp_states.get(int(state, 16), "UNKNOWN")
        except (socket.error, ValueError, IOError, OSError), msg:
            #We need to catch IOError or OSError incase the open of
            # /proc/net/tcp fails.  On 9-10-2007, an encp gave a traceback
            # opening /proc/net/tcp because of "No such file or directory".
            # How that can happen to a file in /proc, I don't know.
            Trace.log(e_errors.ERROR,
                      "__get_socket_state(): /proc/net/tcp: %s" % str(msg))
            
    return None

# get an unused tcp port for control communication
def get_callback(ip=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    config = host_config.get_config()
    if ip is None:
        if config:
            ip = config.get('hostip')
        if not ip:
            ip = host_config.get_default_interface_ip()
    s.bind((ip, 0))
    host, port = s.getsockname()
    return host, port, s

###############################################################################
###############################################################################

#send with a timeout
def timeout_send(sock,msg,timeout=15*60):
    timeout = float(timeout)
    junk,fds,junk = select.select([],[sock],[],timeout)
    if sock not in fds:
        return ""
    if type(sock) == types.IntType: #In case sock is an fd for a pipe.
        return os.write(sock, msg)
    else:
        return sock.send(msg)
    #Should never get here.
    return ""

#send a message, with bytecount and rudimentary security
## Note: Make sure to consider that sock could be a socket object, socket
##       fd or pipe fd.
def write_raw(sock,msg,timeout=15*60):
    max_pkt_size=16384
    try:
        msg_len = len(msg)
        
        msg_msg_len = "%08d" % (len(msg),)
        msg_len_len = len(str(msg_msg_len))

        salt = random.randint(11, 99)
        msg_salt = "ENSTOR%s" % (salt,)
        msg_salt_len = len(msg_salt)

        checksum_msg = hex8(checksum.adler32(salt, msg, msg_len))
        checksum_len = len(checksum_msg)

        #nwritten = timeout_send(sock, "%08d"%(len(msg),), timeout)
        nwritten = timeout_send(sock, msg_msg_len, timeout)
        if type(nwritten) != types.IntType or nwritten != msg_len_len:
            return 1, "short write: message length"

        #timeout_send(sock, "ENSTOR%s"%(salt,), timeout)
        nwritten = timeout_send(sock, msg_salt, timeout)
        if type(nwritten) != types.IntType or nwritten != msg_salt_len:
            return 1, "short write: salt"

        ptr = 0
        while ptr < msg_len:
            nwritten = timeout_send(sock, msg[ptr:ptr+max_pkt_size], timeout)
            if type(nwritten) != types.IntType or nwritten <= 0:
                break
            ptr = ptr + nwritten
        if ptr < msg_len:
            return 1, "short write: message"

        #timeout_send(sock, hex8(checksum.adler32(salt, msg, msg_len)), timeout)
        nwritten = timeout_send(sock, checksum_msg, timeout)
        if type(nwritten) != types.IntType or nwritten != checksum_len:
            return 1, "short write: checksum"

        return 0, ""
    except (socket.error, OSError), detail:
        error_string = "write_raw: socket.error %s"%(detail,)
        #Trace.log(e_errors.ERROR, error_string)
        return 1, error_string
        ##XXX Further sends will fail, our peer will notice incomplete message

write_tcp_raw = write_raw

# send a message over the network which is a Python object
def write_tcp_obj(sock,obj,timeout=15*60):
### When we want to go strictly to cPickle use the following line.
#    return write_tcp_obj_new(sock,obj,timeout)

    rtn, e = write_tcp_raw(sock,repr(obj),timeout)

    if e:
        Trace.log(e_errors.ERROR, e)
    return rtn

# send a message over the network which is a Python object
def write_tcp_obj_new(sock,obj,timeout=15*60):
    rtn, e = write_tcp_raw(sock,cPickle.dumps(obj),timeout)
    if e:
        Trace.log(e_errors.ERROR, e)
    return rtn

# send a message to a co-process which is a Python object
def write_obj(fd, obj, timeout=15*60, verbose = True):
    rtn, e = write_raw(fd, cPickle.dumps(obj), timeout)

    if e and verbose:
        Trace.log(e_errors.ERROR, e)

    return rtn

###############################################################################
###############################################################################

def record_recv_error(sock):
    #if data_string == "":
        #According to python documentation when recv() returns the empty
        # string the other end has closed the connection.

        #Log the time spent waiting.
        #Trace.log(e_errors.ERROR,
        #          "timeout_recv(): time passed: %s sec of %s sec" %
        #          (time.time() - total_start_time, timeout))

        try:
            #Verify if there is an error on the socket.
            socket_error = sock.getsockopt(socket.SOL_SOCKET,
                                           socket.SO_ERROR)
            if socket_error != 0:
                Trace.log(e_errors.ERROR,
                          "timeout_recv(): socket error: %s" % socket_error)
        except socket.error, msg:
            Trace.log(e_errors.ERROR,
                      "timeout_recv(): getsockopt(SO_ERROR): %s" % str(msg))

        try:
            #Verify if the connection is still up.
            sock.getpeername()
        except socket.error, msg:
            Trace.log(e_errors.ERROR,
                      "timeout_recv(): getpeername: %s" % str(msg))

        Trace.log(e_errors.ERROR, "timeout_recv(): received no data")

        #Log the current socket state (only works on Linux).
        socket_state = __get_socket_state(sock.fileno())
        Trace.log(e_errors.ERROR,
                  "timeout_recv(): socket state: %s" % str(socket_state))

        # It would be useful to output the number of bytes in the
        # read buffer.  Python does not yet support it.
        try:
            OPT = getattr(fcntl, "FIONREAD", None)
            if OPT == None:
                if os.uname()[0] == "Linux":
                    OPT = 0x541B  #Linux specific hack.
                if os.uname()[0][:4] == "IRIX" or \
                   os.uname()[0] == "SunOS" or \
                   os.uname()[0] == "OSF1":
                    OPT = 1074030207 #Pulled from header files.
            if OPT != None:
                import struct #Only import this when necessary.
                nbytes = struct.unpack("i",
                                       fcntl.ioctl(sock, OPT, "    "))[0]
                Trace.log(e_errors.ERROR,
                          "timeout_recv(): fcntl(FIONREAD): %s"
                          % (str(nbytes),))
        except AttributeError:
            #FIONREAD not known on this system.
            pass
        except IOError, msg:
            Trace.log(e_errors.ERROR,
                      "timeout_recv(): ioctl(FIONREAD): %s" % (str(msg),))

#recv with a timeout
def timeout_recv(sock, nbytes, timeout = 15 * 60):
    total_start_time = time.time()
    #time_left = timeout
    timeout_time = time.time() + timeout

    error_string = ""
    
    #Loop until a the timeout has passed, a hard error occurs or
    # the message has really arrived.
    #while time_left > 0.0:
    while timeout_time > time.time():
        #start_time = time.time()
        try:
            time_left = max(timeout_time - time.time(), 0.0)
            fds, junk, junk = select.select([sock], [], [], time_left)
        except select.error, msg:
            if msg.args[0] == errno.EINTR:
                #time_left = max(total_start_time + timeout - time.time(), 0.0)
                continue
            #fds = []
            error_string = "timeout_recv(): %s" % str(msg)
            #Trace.log(e_errors.ERROR, error_string)
            #Return to handle the error.
            return "", error_string
        #end_time = time.time()
        if sock not in fds:
            #error_string = "timeout_recv(): select duration: %s  fds: %s  sock: %s"
            #          % (end_time - start_time, fds, sock)
            #Trace.log(e_errors.ERROR, error_string)

            #Hopefully, this situation is different than other situations
            # that were all previously lumped together as "error".
            continue

        if type(sock) == types.IntType:
            data_string = os.read(sock, nbytes)
        else:
            data_string = sock.recv(nbytes)
        if data_string == "":
            #According to python documentation when recv() returns the empty
            # string the other end has closed the connection.
            
            #Log the time spent waiting.
            error_string = "timeout_recv(): time passed: %s sec of %s sec" % \
                           (time.time() - total_start_time, timeout)
            #Trace.log(e_errors.ERROR, error_string)

        return data_string, error_string
        
    #timedout
    error_string = "timeout_recv(): timedout"
    #Trace.log(e_errors.ERROR, error_string)
    return "", error_string

#read_raw - return tuple of message read and error string.  One or the other
# should be returned as an empty string.
def read_raw(fd, timeout=15*60):
    #Trace.log(e_errors.INFO, "read_raw: starting")
    tmp, error_string = timeout_recv(fd, 8, timeout) # the message length
    len_tmp = len(tmp)
    if len_tmp != 8:
        
        error_string = "%s; read_raw: wrong bytecount (%d) '%s'" % \
                       (error_string, len_tmp, tmp)
        return "", error_string
    try:
        bytecount = int(tmp)
    except (ValueError, TypeError):
        error_string = "%s; read_tcp_raw: bad bytecount '%s'" % \
                       (error_string, tmp,)
        return "", error_string
    tmp, error_string = timeout_recv(fd, 8, timeout) # the 'signature'
    if len(tmp)!=8 or tmp[:6] != "ENSTOR":
        error_string = "%s; read_tcp_raw: invalid signature '%s'" % \
                       (error_string, tmp,)
        return "", error_string
    salt= int(tmp[6:])
    msg = ""
    while len(msg) < bytecount:
        tmp, error_string = timeout_recv(fd, bytecount - len(msg), timeout)
        if not tmp:
            break
        msg = msg+tmp
    if len(msg)!=bytecount:
        error_string = "%s; read_tcp_raw: bytecount mismatch %s != %s" \
                       % (error_string, len(msg), bytecount)
        return "", error_string
    tmp, error_string = timeout_recv(fd, 8, timeout)
    crc = long(tmp, 16)  #XXX 
    mycrc = checksum.adler32(salt,msg,len(msg))
    if crc != mycrc:
        error_string = "%s; read_tcp_raw: checksum mismatch %s != %s" \
                        % (error_string, mycrc, crc)
        return "", error_string
    return msg, ""

read_tcp_raw = read_raw

# receive a message over the network which is a Python object
def read_tcp_obj(sock, timeout=15*60) :
    s, e = read_tcp_raw(sock, timeout)
    if not s:
        record_recv_error(sock) #Log the state of the socket.
        
        #Gather additional information and log the error.
        try:
            peername = sock.getpeername()
        except (socket.error, socket.herror, socket.gaierror):
            peername = "unknown"
        error_string = "%s from %s" % (e, peername)
        Trace.log(e_errors.ERROR, error_string)
        
        raise e_errors.TCP_EXCEPTION

    try:
        obj = cPickle.loads(s)
    except (cPickle.PickleError, cPickle.PicklingError,
            cPickle.UnpickleableError, cPickle.UnpicklingError):
        try:
            obj = en_eval(s)
        except SyntaxError:
            obj = None
    
    return obj

# receive a message over the network which is a Python object
def read_tcp_obj_new(sock, timeout=15*60):
    s, e = read_tcp_raw(sock, timeout)
    if not s:
        record_recv_error(sock) #Log the state of the socket.
        
        #Gather additional information and log the error.
        try:
            peername = sock.getpeername()
        except (socket.error, socket.herror, socket.gaierror):
            peername = "unknown"
        error_string = "%s from %s" % (e, peername)
        Trace.log(e_errors.ERROR, error_string)
        
	raise e_errors.TCP_EXCEPTION
    
    return cPickle.loads(s)

# receive a message from a co-process which is a Python object
def read_obj(fd, timeout=15*60, verbose = True):
    s, e = read_raw(fd, timeout)
    if not s:
        if verbose:
            Trace.log(e_errors.ERROR, e)
        
        raise e_errors.TCP_EXCEPTION #What should this be?
    return cPickle.loads(s)
    

if __name__ == "__main__" :
    Trace.init("CALLBACK")
    Trace.trace(6,"callback called with args "+repr(sys.argv))

    c = get_callback()
    Trace.log(e_errors.INFO,"callback exit ok callback="+repr(c))





