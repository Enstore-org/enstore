###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import sys

# enstore imports
import lockfile
import dict_to_a
import Trace
import generic_cs

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses the
# SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS
    socket = SOCKS
except ImportError:
    import socket


HUNT_PORT_LOCK = "/tmp/enstore/hunt_port_lock"

# see if we can bind to the selected tcp host/port
def try_a_port(host, port) :
    Trace.trace(16,'{try_a_port host='+repr(host)+" port="+repr(port))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# COMMENTED OUT FOR RIP TESTS sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(host, port)
    except:
	try:
            sock.close()
        except:
            pass
        Trace.trace(16,'}try_a_port FAILURE')
        return (0 , 0)
    Trace.trace(16,'}try_a_port sock='+repr(sock))
    return 1 , sock

# get an unused tcp port for communication
def get_callback_port(start,end):
    Trace.trace(16,"{get_callback_port")
    (host_name,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    host = ci[0]

    # First acquire the hunt lock.  Once we have it, we have the exlusive right
    # to hunt for a port.  Hunt lock will (I hope) properly serlialze the
    # waiters so that they will be services in the order of arrival.
    # Because we use file locks instead of semaphores, the system will
    # properly clean up, even on kill -9s.
    #lockf = open ("/var/lock/hsm/lockfile", "w")
    lockf = open (HUNT_PORT_LOCK, "w")
    Trace.trace(20,"get_callback_port - trying to get lock on node"+repr(host_name)+" "+repr(ca))
    lockfile.writelock(lockf)  #holding write lock = right to hunt for a port.
    Trace.trace(20,"get_callback_port - got the lock - hunting for port")

    # now check for a port we can use
    while  1:
        # remember, only person with lock is pounding  hard on this
        for port in range (start,end) :
            success, mysocket = try_a_port (host, port)
            # if we got a lock, give up the hunt lock and return port
            if success :
                lockfile.unlock(lockf)
                lockf.close()
                Trace.trace(16,"}get_callback_port host="+repr(host)+\
                            " port="+repr(port)+" mysocket="+repr(mysocket))
                return host, port, mysocket
        #  otherwise, we tried all ports, try later.
        sleeptime = 1
        msg = "get_callback_port: all ports from "+repr(start)+' to ' \
	      +repr(end) + " used. waiting"+repr(sleeptime)+" seconds"
        generic_cs.enprint(msg)
        Trace.trace(2,msg)
        time.sleep (sleeptime)


# get an unused tcp port for control communication
def get_callback():
    return get_callback_port( 7600, 7640 )

# get an unused tcp port for data communication - called by mover
def get_data_callback():
    return get_callback_port( 7640, 7650 )

def write_tcp_buf(sock,buffer,errmsg=""): #can delete errmsg, given new trace mechanism
    del errmsg # quiet the linter
    r = None
    try:
        r = sock.send(buffer)
    except socket.error, detail:
        Trace.trace(0,"write_tcp_buf: socket.error"+str(detail))
        ##XXX Further sends will fail, our peer will notice incomplete message
    return r

# send a message on a tcp socket
def write_tcp_socket(sock,buffer,errmsg=""):
    return write_tcp_buf(sock,dict_to_a.dict_to_a(buffer),errmsg)
    
# read a complete message in a  tcp socket
def read_tcp_buf(sock,errmsg="") :
    del errmsg # quiet the linter
    Trace.trace(16,"{read_tcp_buf")
    buf = sock.recv(65536*4)
    Trace.trace(16,"}read_tcp_buf len="+repr(len(buf)))
    return buf



def read_tcp_socket(sock,errmsg="") :
    del errmsg #quiet the linter
    Trace.trace(16,"{read_tcp_socket")
    workmsg = ""
    depth = 0

    while depth or not workmsg:
        c = sock.recv(1)
        if not c:
            break
        
        if c=='{':  ### XXX Kludge in absence of a byte-count on message
            depth = depth+1
        elif c=='}':
            depth = depth-1

        workmsg = workmsg + c

    try:
        worklist = dict_to_a.a_to_dict(workmsg)
        Trace.trace(16,"}read_tcp_socket len="+repr(len(worklist)))
        return worklist
    except SyntaxError:
        Trace.trace(0,"read_tcp_socket Error handling message"+repr(workmsg))
        raise IOError,"Error handling message"+repr(workmsg)

    
# return a mover tcp socket
def mover_callback_socket(ticket) :
    host, port = ticket['mover']['callback_addr']    
    Trace.trace(16,'{mover_callback_socket host='+\
                repr(host)+" port="+\
                repr(port))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(host, port)
    Trace.trace(16,"}mover_callback_socket sock="+repr(sock))
    return sock

# return a library manager tcp socket
def library_manager_callback_socket(ticket) :
    Trace.trace(16,'{library_manager_server_callback_socket host='+\
                repr(ticket['library_manager_callback_host'])+" port="+\
                repr(ticket['library_manager_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['library_manager_callback_host'], \
                 ticket['library_manager_callback_port'])
    Trace.trace(16,"}library_manager_server_callback_socket sock="+repr(sock))
    return sock

# return a library manager tcp socket
def volume_server_callback_socket(ticket) :
    Trace.trace(16,'{volume_server_callback_socket host='+\
                repr(ticket['volume_clerk_callback_host'])+" port="+\
                repr(ticket['volume_clerk_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['volume_clerk_callback_host'], \
                 ticket['volume_clerk_callback_port'])
    Trace.trace(16,"}volume_server_callback_socket sock="+repr(sock))
    return sock

# return a file clerk tcp socket
def file_server_callback_socket(ticket) :
    Trace.trace(16,'{file_server_callback_socket host='+\
                repr(ticket['file_clerk_callback_host'])+" port="+\
                repr(ticket['file_clerk_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['file_clerk_callback_host'], \
                 ticket['file_clerk_callback_port'])
    Trace.trace(16,"}file_server_callback_socket sock="+repr(sock))
    return sock

# return and admin clerk tcp socket
def admin_server_callback_socket(ticket) :
    Trace.trace(16,'{admin_server_callback_socket host='+\
                repr(ticket['admin_clerk_callback_host'])+" port="+\
                repr(ticket['admin_clerk_callback_port']))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['admin_clerk_callback_host'], \
                 ticket['admin_clerk_callback_port'])
    Trace.trace(16,"}admin_sever_callback_socket sock="+repr(sock))
    return sock

# send ticket/message on user tcp socket and return user tcp socket
def user_callback_socket(ticket) :
    host, port = ticket['callback_addr']
    Trace.trace(16,'{user_callback_socket host='+\
                repr(host)+" port="+\
                repr(port))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(host, port)
    write_tcp_socket(sock,ticket,"callback user_callback_socket")
    Trace.trace(16,"}user_callback_socket sock="+repr(sock))
    return sock

# send ticket/message on tcp socket
def send_to_user_callback(ticket) :
    host, port = ticket['callback_addr']
    Trace.trace(16,'{send_to_user_callback host='+\
                repr(host)+" port="+\
                repr(port))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(host, port)
    write_tcp_socket(sock,ticket,"callback send_to_user_callback")
    sock.close()
    Trace.trace(16,"}send_to_user_callback")

if __name__ == "__main__" :
    import sys
    Trace.init("callback")
    Trace.trace(1,"callback called with args "+repr(sys.argv))

    c = get_callback()
    generic_cs.enprint(c)
    Trace.trace(1,"callback exit ok callback="+repr(c))





