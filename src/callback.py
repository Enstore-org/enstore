import time
import lockfile
import dict_to_a
import errno

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
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(host, port)
    except:
        sock.close()
        return (0 , sock)
    return 1 , sock

# get an unused tcp port for communication
def get_callback() :
    #host = 'localhost'
    (host,ca,ci) = socket.gethostbyaddr(socket.gethostname())

    # First acquire the hunt lock.  Once we have it, we have the exlusive right
    # to hunt for a port.  Hunt lock will (I hope) properly serlialze the
    # waiters so that they will be services in the order of arrival.
    # Because we use file locks instead of semaphores, the system will
    # properly clean up, even on kill -9s.
    lockf = open ("/var/lock/hsm/lockfile", "w")
    lockfile.writelock(lockf)  #holding write lock = right to hunt for a port.

    # now check for a port we can use
    while  1:
        # remember, only person with lock is pounding  hard on this
        for port in range (7600, 7650) :
            success, mysocket = try_a_port (host, port)
            # if we got a lock, give up the hunt lock and return port
            if success :
                lockfile.unlock(lockf)
                lockf.close()
                return host, port, mysocket
        #  otherwise, we tried all ports, try later.
        time.sleep (1)


# return a mover tcp socket
def mover_callback_socket(ticket) :
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['mover_callback_host'], ticket['mover_callback_port'])
    return sock

# return a library manager tcp socket
def library_manager_callback_socket(ticket) :
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['library_manager_callback_host'], \
                 ticket['library_manager_callback_port'])
    return sock

# return a library manager tcp socket
def volume_clerk_callback_socket(ticket) :
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['volume_clerk_callback_host'], \
                 ticket['volume_clerk_callback_port'])
    return sock

# send ticket/message on user tcp socket and return user tcp socket
def user_callback_socket(ticket) :
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['user_callback_host'], ticket['user_callback_port'])
    write_tcp_socket(sock,ticket,"callback user_callback_socket")
    return sock

# send ticket/message on tcp socket
def send_to_user_callback(ticket) :
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ticket['user_callback_host'], ticket['user_callback_port'])
    write_tcp_socket(sock,ticket,"callback send_to_user_callback")
    sock.close()

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
