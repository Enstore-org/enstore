#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import time
import sys
import os
#import string
import random
import select
#import errno
import socket
import cPickle
import rexec
import errno
import fcntl

_rexec = rexec.RExec()
def _eval(stuff):
    return _rexec.r_eval(stuff)

# enstore imports
import Trace
import e_errors
import checksum
#import hostaddr
import host_config

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
        except socket.error, msg:
            Trace.log(e_errors.ERROR,
                      "timeout_recv(): /proc/net/tcp: %s" % str(msg))
            
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

#recv with a timeout
def timeout_send(sock,msg,timeout=15*60):
    timeout = float(timeout)
    junk,fds,junk = select.select([],[sock],[],timeout)
    if sock not in fds:
        return ""
    return sock.send(msg)

#send a message, with bytecount and rudimentary security
def write_tcp_raw(sock,msg,timeout=15*60):
    max_pkt_size=16384
    try:
        l = len(msg)
        ptr=0
        #sock.send("%08d"%(len(msg),))
        timeout_send(sock, "%08d"%(len(msg),), timeout)
        salt=random.randint(11,99)
        #sock.send("ENSTOR%s"%(salt,))
        timeout_send(sock, "ENSTOR%s"%(salt,), timeout)
        while ptr<l:
            #nwritten=sock.send(msg[ptr:ptr+max_pkt_size])
            nwritten=timeout_send(sock, msg[ptr:ptr+max_pkt_size], timeout)
            if nwritten<=0:
                break
            ptr = ptr+nwritten
        #sock.send(hex8(checksum.adler32(salt,msg,l)))
        timeout_send(sock, hex8(checksum.adler32(salt,msg,l)), timeout)
        return 0
    except socket.error, detail:
        Trace.log(e_errors.ERROR,"write_tcp_raw: socket.error %s"%(detail,))
        return 1
        ##XXX Further sends will fail, our peer will notice incomplete message


# send a message which is a Python object
def write_tcp_obj(sock,obj,timeout=15*60):
    return write_tcp_raw(sock,repr(obj),timeout)

# send a message which is a Python object
def write_tcp_obj_new(sock,obj,timeout=15*60):
    return write_tcp_raw(sock,cPickle.dumps(obj),timeout)


#recv with a timeout
def timeout_recv(sock, nbytes, timeout = 15 * 60):
    total_start_time = time.time()
    #time_left = timeout
    timeout_time = time.time() + timeout
    
    #Loop until a the timeout has passed, a hard error occurs or
    # the message has really arrived.
    #while time_left > 0.0:
    while timeout_time > time.time():
        start_time = time.time()
        try:
            time_left = max(timeout_time - time.time(), 0.0)
            fds, junk, junk = select.select([sock], [], [], time_left)
        except select.error, msg:
            if msg.args[0] == errno.EINTR:
                #time_left = max(total_start_time + timeout - time.time(), 0.0)
                continue
            #fds = []
            Trace.log(e_errors.ERROR, "timeout_recv(): %s" % str(msg))
            #Return to handle the error.
            return ""
        end_time = time.time()
        if sock not in fds:
            Trace.log(e_errors.ERROR,
                      "timeout_recv(): select duration: %s  fds: %s  sock: %s"
                      % (end_time - start_time, fds, sock))
            #return ""
            #time_left = max(total_start_time + timeout - time.time(), 0.0)
            #Hopefully, this situation is different than other situations
            # that were all previously lumped together as "error".
            continue

        data_string = sock.recv(nbytes)
        if data_string == "":
            #According to python documentation when recv() returns the empty
            # string the other end has closed the connection.

            #Log the time spent waiting.
            Trace.log(e_errors.ERROR,
                      "timeout_recv(): time passed: %s sec of %s sec" %
                      (time.time() - total_start_time, timeout))

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
            
        return data_string
        
    #timedout
    Trace.log(e_errors.ERROR, "timeout_recv(): timedout")
    return ""

# read a complete message
def read_tcp_raw(sock, timeout=15*60):
    #Trace.log(e_errors.INFO, "read_tcp_raw: starting")
    tmp = timeout_recv(sock, 8, timeout)
    try:
        bytecount = int(tmp)
    except (ValueError, TypeError):
        #bytecount = None
        Trace.log(e_errors.ERROR, "read_tcp_raw: bad bytecount %s" % (tmp,))
        return ""
    if len(tmp) != 8:
        try:
            Trace.log(e_errors.ERROR,"read_tcp_raw: wrong bytecount %s"%(tmp,))
        except ValueError, msg:
            Trace.log(e_errors.ERROR,"read_tcp_raw: %s"%(msg,))
        return ""
    tmp = timeout_recv(sock,8, timeout) # the 'signature'
    if len(tmp)!=8 or tmp[:6] != "ENSTOR":
        Trace.log(e_errors.ERROR,"read_tcp_raw: invalid signature %s"%(tmp,))
        return ""
    salt= int(tmp[6:])
    msg = ""
    while len(msg) < bytecount:
        tmp = timeout_recv(sock,bytecount - len(msg), timeout)
        if not tmp:
            break
        msg = msg+tmp
    if len(msg)!=bytecount:
        Trace.log(e_errors.ERROR,"read_tcp_raw: bytecount mismatch %s != %s"%(len(msg),bytecount))
        return ""
    tmp = timeout_recv(sock,8, timeout)
    crc = long(tmp, 16)  #XXX 
    mycrc = checksum.adler32(salt,msg,len(msg))
    if crc != mycrc:
        Trace.log(e_errors.ERROR,"read_tcp_raw: checksum mismatch %s != %s"%(mycrc, crc))
        return ""
    return msg


def read_tcp_obj(sock, timeout=15*60) :
    s=read_tcp_raw(sock, timeout)
    if not s:
        raise e_errors.TCP_EXCEPTION
    return _eval(s)

def read_tcp_obj_new(sock, timeout=15*60) :
    s=read_tcp_raw(sock, timeout)
    if not s:
	raise e_errors.TCP_EXCEPTION
    return cPickle.loads(s)


if __name__ == "__main__" :
    Trace.init("CALLBACK")
    Trace.trace(6,"callback called with args "+repr(sys.argv))

    c = get_callback()
    Trace.log(e_errors.INFO,"callback exit ok callback="+repr(c))





