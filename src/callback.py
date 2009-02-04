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
import Interfaces

def hex8(x):
    s=hex(x)[2:]  #kill the 0x
    if type(x)==type(1L): s=s[:-1]  # kill the L
    l = len(s)
    if l>8:
        raise OverflowError, x
    return '0'*(8-l)+s

def get_socket_read_queue_length(sock):
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

    else:
        raise AttributeError("FIONREAD")

    return nbytes

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

def log_socket_state(sock):
        #According to python documentation when recv() returns the empty
        # string the other end has closed the connection.

        try:
            #Verify if there is an error on the socket.
            socket_error = sock.getsockopt(socket.SOL_SOCKET,
                                           socket.SO_ERROR)
            if socket_error != 0:
                Trace.log(e_errors.ERROR,
                          "socket state: pending SO_ERROR: %s" \
                          % (socket_error,))
        except socket.error, msg:
            Trace.log(e_errors.ERROR,
                      "socket state: getsockopt(SO_ERROR): %s" \
                      % (str(msg),))

        try:
            #Verify if the connection is still up.
            peer_name = sock.getpeername()
        except socket.error, msg:
            peer_name = None
            Trace.log(e_errors.ERROR,
                      "socket state: getpeername(): %s" % str(msg))

        Trace.log(e_errors.ERROR,
                  "socket state: received no data from %s" % (peer_name,))

        #Log the current socket state (only works on Linux).
        socket_state = __get_socket_state(sock.fileno())
        Trace.log(e_errors.ERROR,
                  "socket state: socket state: %s" % str(socket_state))

        # It would be useful to output the number of bytes in the
        # read buffer.  Python does not yet support it.
        try:
            nbytes = get_socket_read_queue_length(sock)

            Trace.log(e_errors.ERROR,
                      "socket state: fcntl(FIONREAD): %s"
                      % (str(nbytes),))
        except AttributeError:
            #FIONREAD not known on this system.
            pass
        except IOError, msg:
            Trace.log(e_errors.ERROR,
                      "socket state: ioctl(FIONREAD): %s" % (str(msg),))

        #Get any mac addresses.
        arpGetFunc = getattr(Interfaces, "arpGet", None)
        if peer_name and arpGetFunc:
            try:
                mac_addresses = arpGetFunc(peer_name[0])
                if mac_addresses:
                    Trace.log(e_errors.ERROR, 
                              "socket state: arp get[%s]: %s" % \
                              (peer_name[0], str(mac_addresses)))

            except:
                Trace.log(e_errors.ERROR,
                          "socket state: arp get[%s]: %s" % \
                          (peer_name[0], str(msg)))

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
        #Send to the debug log the current stack trace.  Hopefully, this will
        # be useful in debugging hanging connections.
        try:
            if "log_stack_trace" in dir(Trace):
                Trace.log_stack_trace()
        except:
            Trace.handle_error()
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
        #Determine the length of the payload.
        msg_len = len(msg)

        #First part of the message sent is 8 characters consisiting of the
        # length of the payload part of the message.  Put these bytes together
        # and determine the length.
        msg_msg_len = "%08d" % (msg_len,)
        msg_len_len = len(str(msg_msg_len))

        #Second part of the message sent is 8 characters of Enstore
        # specific bytes.  Put these bytes together and determine the length.
        salt = random.randint(11, 99)
        msg_signature = "ENSTOR%s" % (salt,)
        msg_signature_len = len(msg_signature)

        #Forth part of the message sent is the 32bit adler32 CRC of the
        # payload of the message.  Put these bytes together and determine
        # the length.
        checksum_msg = hex8(checksum.adler32(salt, msg, msg_len))
        checksum_len = len(checksum_msg)

        #Now actually write out the length of the payload to the socket.
        ptr = 0
        while ptr < msg_len_len:
            nwritten = timeout_send(sock, msg_msg_len[ptr:], timeout)
            if type(nwritten) != types.IntType or nwritten <= 0:
                break
            ptr = ptr + nwritten
        if ptr < msg_len_len:
            return 1, "short write: message length (expected 8, sent %s)" \
                   % (ptr,)
        if ptr != msg_len_len:
            return 1, "bad write: message length (expected 8, sent %s)" \
                   % (ptr,)

        #This time write out the 'signature'.
        ptr = 0
        while ptr < msg_signature_len:
            nwritten = timeout_send(sock, msg_signature[ptr:], timeout)
            if type(nwritten) != types.IntType or nwritten <= 0:
                break
            ptr = ptr + nwritten
        if ptr < msg_signature_len:
            return 1, "short write: salt (expected 8, sent %s)" % (ptr,)
        if ptr != msg_signature_len:
            return 1, "bad write: salt (expected 8, sent %s)" % (ptr,)

        #Write the payload to the socket.
        ptr = 0
        while ptr < msg_len:
            nwritten = timeout_send(sock, msg[ptr:ptr+max_pkt_size], timeout)
            if type(nwritten) != types.IntType or nwritten <= 0:
                break
            ptr = ptr + nwritten
        if ptr < msg_len:
            return 1, "short write: message (expected %d, sent %s)" \
                   % (msg_len, ptr,)
        if ptr != msg_len:
            return 1, "bad write: message (expected %d, sent %s)" \
                   % (msg_len, ptr,)

        #Lastly, write out the checksum of the payload.
        ptr = 0
        while ptr < checksum_len:
            nwritten = timeout_send(sock, checksum_msg[ptr:], timeout)
            if type(nwritten) != types.IntType or nwritten <= 0:
                break
            ptr = ptr + nwritten
        if ptr < checksum_len:
            return 1, "short write: checksum (expected 4, sent %s)" \
                   % (ptr,)
        if ptr != checksum_len:
            return 1, "bad write: checksum (expected 4, sent %s)" \
                   % (ptr,)
            

        return 0, ""
    except (socket.error, select.error, OSError), detail:
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
        log_socket_state(sock) #Log the state of the socket.
        Trace.log(e_errors.ERROR, e)
        raise e_errors.TCP_EXCEPTION

    return rtn

# send a message over the network which is a Python object
def write_tcp_obj_new(sock,obj,timeout=15*60):
    rtn, e = write_tcp_raw(sock,cPickle.dumps(obj),timeout)
    if e:
        log_socket_state(sock) #Log the state of the socket.
        Trace.log(e_errors.ERROR, e)
        raise e_errors.TCP_EXCEPTION

    return rtn

# send a message to a co-process which is a Python object
def write_obj(fd, obj, timeout=15*60, verbose = True):
    rtn, e = write_raw(fd, cPickle.dumps(obj), timeout)

    if e and verbose:
        Trace.log(e_errors.ERROR, e)
        raise e_errors.TCP_EXCEPTION #What should this be?
    
    return rtn

###############################################################################
###############################################################################
        
#recv with a timeout
def timeout_recv(sock, nbytes, timeout = 15 * 60):
    total_start_time = time.time()
    #time_left = timeout
    timeout_time = time.time() + timeout

    error_string = ""
    data_string = ""
    
    #Loop until a the timeout has passed, a hard error occurs or
    # the message has really arrived.
    while timeout_time > time.time():
        try:
            time_left = max(timeout_time - time.time(), 0.0)
            fds, junk, junk = select.select([sock], [], [], time_left)
        except select.error, msg:
            if msg.args[0] == errno.EINTR:
                continue
            error_string = "timeout_recv(): %s" % str(msg)
            #Return to handle the error.
            return "", error_string
        if sock not in fds:
            #Hopefully, this situation is different than other situations
            # that were all previously lumped together as "error".
            continue

        read_len = nbytes - len(data_string)
        if type(sock) == types.IntType:
            data_string = data_string + os.read(sock, read_len)
        else:
            data_string = data_string + sock.recv(read_len)

        if data_string == "":
            #According to python documentation when recv() returns the empty
            # string the other end has closed the connection.
            
            #Log the time spent waiting.
            error_string = "timeout_recv(): time passed: %s sec of %s sec" % \
                           (time.time() - total_start_time, timeout)
            #Trace.log(e_errors.ERROR, error_string)
        if len(data_string) != nbytes:
            #Keep trying until we get everything we want.
            continue

        return data_string, error_string
        
    #timedout
    error_string = "timeout_recv(): timedout"
    return "", error_string

#read_raw - return tuple of message read and error string.  One or the other
# should be returned as an empty string.
def read_raw(fd, timeout=15*60):
    # Read in the length of the payload part of the message.
    tmp, error_string = timeout_recv(fd, 8, timeout) # the message length
    len_tmp = len(tmp)
    if len_tmp != 8:
        
        error_string = "%s; read_raw: wrong bytecount (%d) '%s'" % \
                       (error_string, len_tmp, tmp)
        return "", error_string
    try:
        bytecount = int(tmp)
    except (ValueError, TypeError):
        error_string = "%s; read_raw: bad bytecount '%s'" % \
                       (error_string, tmp,)
        return "", error_string

    #Read in the signature.
    tmp, error_string = timeout_recv(fd, 8, timeout) # the 'signature'
    if len(tmp)!=8 or tmp[:6] != "ENSTOR":
        error_string = "%s; read_raw: invalid signature '%s'" % \
                       (error_string, tmp,)
        return "", error_string

    #Extract the salt from the signature.
    salt= int(tmp[6:])

    #Read in the payload and verify it is consistant with what we expected.
    msg = ""
    while len(msg) < bytecount:
        tmp, error_string = timeout_recv(fd, bytecount - len(msg), timeout)
        if not tmp:
            break
        msg = msg+tmp
    if len(msg)!=bytecount:
        error_string = "%s; read_raw: bytecount mismatch %s != %s" \
                       % (error_string, len(msg), bytecount)
        return "", error_string

    #Read in the adler32 CRC and verify it is consistant with what we
    # expected.
    tmp, error_string = timeout_recv(fd, 8, timeout)
    crc = long(tmp, 16)  #XXX 
    mycrc = checksum.adler32(salt,msg,len(msg))
    if crc != mycrc:
        error_string = "%s; read_raw: checksum mismatch %s != %s" \
                        % (error_string, mycrc, crc)
        return "", error_string
    return msg, ""

read_tcp_raw = read_raw

# receive a message over the network which is a Python object
def read_tcp_obj(sock, timeout=15*60) :
    s, e = read_tcp_raw(sock, timeout)
    if not s:
        log_socket_state(sock) #Log the state of the socket.

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
        log_socket_state(sock) #Log the state of the socket.

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





