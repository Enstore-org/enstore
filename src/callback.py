###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import sys, os
import string
import random
import select
import errno
import socket
import cPickle
import rexec

_rexec = rexec.RExec()
def eval(stuff):
    return _rexec.r_eval(stuff)

# enstore imports
import Trace
import e_errors
import checksum
import hostaddr
import host_config

def hex8(x):
    s=hex(x)[2:]  #kill the 0x
    if type(x)==type(1L): s=s[:-1]  # kill the L
    l = len(s)
    if l>8:
        raise OverflowError, x
    return '0'*(8-l)+s

# get an unused tcp port for control communication
def get_callback(verbose=0, ip=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    config = host_config.get_config()
    if ip is None:
        if config:
            ip = config.get('hostip')
        if not ip:
            hostname, junk, ips = hostaddr.gethostinfo()
            ip = ips[0]
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
    except socket.error, detail:
        Trace.log(e_errors.ERROR,"write_tcp_raw: socket.error %s"%(detail,))
        ##XXX Further sends will fail, our peer will notice incomplete message


# send a message which is a Python object
def write_tcp_obj(sock,obj,timeout=15*60):
    return write_tcp_raw(sock,repr(obj),timeout)

# send a message which is a Python object
def write_tcp_obj_new(sock,obj,timeout=15*60):
    return write_tcp_raw(sock,cPickle.dumps(obj),timeout)


#recv with a timeout
def timeout_recv(sock,nbytes,timeout=15*60):
    timeout = float(timeout)
    fds,junk,junk = select.select([sock],[],[],timeout)
    if sock not in fds:
        return ""
    return sock.recv(nbytes)
    

# read a complete message
def read_tcp_raw(sock, timeout=15*60):
    tmp = timeout_recv(sock,8, timeout) 
    try:
        bytecount = string.atoi(tmp)
    except:
        bytecount = None
    if len(tmp)!=8 or bytecount is None:
        Trace.log(e_errors.ERROR,"read_tcp_raw: bad bytecount %s"%(tmp,))
        return ""
    tmp = timeout_recv(sock,8, timeout) # the 'signature'
    if len(tmp)!=8 or tmp[:6] != "ENSTOR":
        Trace.log(e_errors.ERROR,"read_tcp_raw: invalid signature %s"%(tmp,))
        return ""
    salt=string.atoi(tmp[6:])
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
    crc = string.atol(tmp, 16)  #XXX 
    mycrc = checksum.adler32(salt,msg,len(msg))
    if crc != mycrc:
        Trace.log(e_errors.ERROR,"read_tcp_raw: checksum mismatch %s != %s"%(mycrc, crc))
        return ""
    return msg


def read_tcp_obj(sock, timeout=15*60) :
    s=read_tcp_raw(sock, timeout)
    if not s:
        raise e_errors.TCP_EXCEPTION
    return eval(s)

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





