import time
import lockfile
import dict_to_a
import errno
import Trace

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses the
# SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

# see if we can bind to the selected tcp host/port
def try_a_port(host, port) :
    Trace.trace(16,'Entering try_a_port host='+repr(host)+" port="+repr(port))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(host, port)
    except:
        sock.close()
        Trace.trace(16,'Leaving try_a_port FAILURE')
        return (0 , sock)
    Trace.trace(16,'Leaving try_a_port sock='+repr(sock))
    return 1 , sock

# get an unused tcp port for communication
def get_callback() :
    Trace.trace(16,"Entering get_callback")
    (host,ca,ci) = socket.gethostbyaddr(socket.gethostname())

    # First acquire the hunt lock.  Once we have it, we have the exlusive right
    # to hunt for a port.  Hunt lock will (I hope) properly serlialze the
    # waiters so that they will be services in the order of arrival.
    # Because we use file locks instead of semaphores, the system will
    # properly clean up, even on kill -9s.
    lockf = open ("/var/lock/hsm/lockfile", "w")
    Trace.trace(20,"get_callback - trying to get lock")
    lockfile.writelock(lockf)  #holding write lock = right to hunt for a port.
    Trace.trace(20,"get_callback - got the lock - hunting for port")

    # now check for a port we can use
    while  1:
        # remember, only person with lock is pounding  hard on this
        port1 = 7600
        port2 = 7650
        for port in range (port1,port2) :
            success, mysocket = try_a_port (host, port)
            # if we got a lock, give up the hunt lock and return port
            if success :
                lockfile.unlock(lockf)
                lockf.close()
                Trace.trace(16,"Leaving get_callback host="+repr(host)+\
                            " port="+repr(port)+" mysocket="+repr(mysocket))
                return host, port, mysocket
        #  otherwise, we tried all ports, try later.
        sleeptime = 1
        msg = "get_callback: all ports from "+repr(port1)+' to '+repr(port2)+\
              " used. waiting"+repr(sleeptime)+" seconds"
        print msg
        Trace.trace(2,msg)
        time.sleep (sleeptime)


# return a mover tcp socket
def mover_callback_socket(ticket) :
    Trace.trace(16,'Entering mover_callback_socket host='+\
                repr(ticket['mover_callback_host'])+" port="+\
                repr(ticket['mover_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['mover_callback_host'], ticket['mover_callback_port'])
    Trace.trace(16,"Leaving mover_callback_socket sock="+repr(sock))
    return sock

# return a library manager tcp socket
def library_manager_callback_socket(ticket) :
    Trace.trace(16,'Entering library_manager_callback_socket host='+\
                repr(ticket['library_manager_callback_host'])+" port="+\
                repr(ticket['library_manager_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['library_manager_callback_host'], \
                 ticket['library_manager_callback_port'])
    Trace.trace(16,"Leaving library_manager_callback_socket sock="+repr(sock))
    return sock

# return a library manager tcp socket
def volume_clerk_callback_socket(ticket) :
    Trace.trace(16,'Entering volume_clerk_callback_socket host='+\
                repr(ticket['volume_clerk_callback_host'])+" port="+\
                repr(ticket['volume_clerk_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['volume_clerk_callback_host'], \
                 ticket['volume_clerk_callback_port'])
    Trace.trace(16,"Leaving volume_clerk_callback_socket sock="+repr(sock))
    return sock

# return a file clerk tcp socket
def file_clerk_callback_socket(ticket) :
    Trace.trace(16,'Entering file_clerk_callback_socket host='+\
                repr(ticket['file_clerk_callback_host'])+" port="+\
                repr(ticket['file_clerk_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['file_clerk_callback_host'], \
                 ticket['file_clerk_callback_port'])
    Trace.trace(16,"Leaving file_clerk_callback_socket sock="+repr(sock))
    return sock

# return and admin clerk tcp socket
def admin_clerk_callback_socket(ticket) :
    Trace.trace(16,'Entering admin_clerk_callback_socket host='+\
                repr(ticket['admin_clerk_callback_host'])+" port="+\
                repr(ticket['admin_clerk_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['admin_clerk_callback_host'], \
                 ticket['admin_clerk_callback_port'])
    Trace.trace(16,"Leaving admin_clerk_callback_socket sock="+repr(sock))
    return sock

# send ticket/message on user tcp socket and return user tcp socket
def user_callback_socket(ticket) :
    Trace.trace(16,'Entering user_callback_socket host='+\
                repr(ticket['user_callback_host'])+" port="+\
                repr(ticket['user_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['user_callback_host'], ticket['user_callback_port'])
    write_tcp_socket(sock,ticket,"callback user_callback_socket")
    Trace.trace(16,"Leaving user_callback_socket sock="+repr(sock))
    return sock

# send ticket/message on tcp socket
def send_to_user_callback(ticket) :
    Trace.trace(16,'Entering send_to_user_callback host='+\
                repr(ticket['user_callback_host'])+" port="+\
                repr(ticket['user_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['user_callback_host'], ticket['user_callback_port'])
    write_tcp_socket(sock,ticket,"callback send_to_user_callback")
    sock.close()
    Trace.trace(16,"Leaving send_to_user_callback")

def write_tcp_buf(sock,buffer,errmsg=""):
    badsock = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if badsock != 0 :
        print errmsg,"pre-send error:", errno.errorcode[badsock]
    sock.send(buffer)
    badsock = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if badsock != 0 :
        print errmsg,"pre-send error:", errno.errorcode[badsock]
# send a message on a tcp socket
def write_tcp_socket(sock,buffer,errmsg=""):
    badsock = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if badsock != 0 :
        print errmsg,"pre-send error:", errno.errorcode[badsock]
    sock.send(dict_to_a.dict_to_a(buffer))
    badsock = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if badsock != 0 :
        print errmsg,"pre-send error:", errno.errorcode[badsock]

# read a complete message in a  tcp socket
def read_tcp_buf(sock,errmsg="") :
    badsock = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if badsock != 0 :
       print errmsg,"pre-recv error:", errno.errorcode[badsock]
    buf = sock.recv(65536*4)
    badsock = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if badsock != 0 :
       print errmsg,"post-recv error:", errno.errorcode[badsock]
    return buf
def read_tcp_socket(sock,errmsg="") :
    workmsg = ""
    while 1:
        badsock = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if badsock != 0 :
            print errmsg,"pre-recv error:", errno.errorcode[badsock]
        buf = sock.recv(65536*4)
        badsock = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if badsock != 0 :
            print errmsg,"post-recv error:", errno.errorcode[badsock]
        if len(buf) == 0 :
            break
        workmsg = workmsg+buf
        try:
            worklist = dict_to_a.a_to_dict(workmsg)
            return worklist
        except SyntaxError:
            #print "SyntaxError on translating:",repr(workmsg),"\nretrying"
            err = 1
            continue
    try:
        worklist = dict_to_a.a_to_dict(workmsg)
        return worklist
    except SyntaxError:
        raise IOError,"Error handling message"+repr(workmsg)

if __name__ == "__main__" :
    print get_callback()
