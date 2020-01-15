#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import socket
import sys
import exceptions
import errno

# enstore imports
import host_config
import cleanUDP
import Interfaces
import en_eval

def __get_callback(host, port):
    if host == '':
        # discover primary address family
        hostname = socket.gethostname()
        hostinfo = socket.getaddrinfo(hostname, socket.AF_INET)
        # hostinfo is the list of tuples
        # [(Address_Family, Socket_Type, Protocol, Cacnonical_Name, Addres), ....]
        # For details see https://docs.python.org/2/library/socket.html#module-socket
        # Check the entries usually the first one is the primary address family,
        # But just to make sure.
        af_inet6 = False
        af_inet = False
        for e in hostinfo:
            if e[0] == socket.AF_INET:
                af_inet = True
            if e[0] == socket.AF_INET6:
                af_inet6 = True
        if af_inet6:
            address_family = socket.AF_INET6
        else:
            address_family = socket.AF_INET
    else:
        address_family = socket.getaddrinfo(host, None)[0][0]

    sock = cleanUDP.cleanUDP(address_family, socket.SOCK_DGRAM)
    try:
        sock.socket.bind((host, port))
    except socket.error, msg:
        if msg.args[0] == errno.EADDRNOTAVAIL:
            error_message = "%s: %s" % (msg.args[1], host)

            #If we get EADDRNOTAVAIL, we should check to see if the interfaces
            # list returns information about the defualt IP.  If the
            # default IP is not currently configured then there is an
            # inconsistency between /etc/hosts and ifconfig.
            interfaces_dict = Interfaces.interfacesGet()
            for intf_dict in interfaces_dict.values():
                if intf_dict['ip'] == host:
                    break
            else:
                error_message = "%s\n%s" % (error_message,
                                            "Check /etc/hosts and ifconfig -a"
                                            " for inconsistent information.")
        elif msg.args[0] == errno.EADDRINUSE:
            #We should include the address information since we know it
            # is currently in use by another process.
            error_message = "%s: %s" % (msg.args[1], (host, port))
        else:
            error_message = msg.args[1]

        #sys.stdout.write("%s\n" % error_message)
        sys.stdout.write("MY %s\n" % error_message)
        sys.exit(1)
    if   address_family == socket.AF_INET6:
        host, port, junk, junk = sock.socket.getsockname()
    else:
        host, port = sock.socket.getsockname()
    return host, port, sock

# try to get a port from a range of possibilities
def get_default_callback(use_port=0, receiver_ip=None):
    host = host_config.get_default_interface(receiver_ip)['ip']
    return __get_callback(host, use_port)

# try to get a port from a range of possibilities
def get_callback(use_host=None, use_port=0):
    #if use_host not in [None,'']:
    if use_host not in [None]:
        host = use_host
    else:
        host = host_config.choose_interface()['ip']
    return __get_callback(host, use_port)

### These function deal with encoding and decoding the raw bytes from
### udp messages.

def r_eval(message_to_decode, check=True, compile=False):
    try:
        #This is uses the restricted eval.  The unstricted eval could have
        #  been used by doing: return eval(message_to_decode)
        #t=time.time()
        rc = en_eval.en_eval(message_to_decode, check=check, compile=compile)
        #t1=time.time()
        #Trace.trace(5,"r_eval %s %s %s"%(t1-t,check, compile))
        return rc
        #return en_eval(message_to_decode)
    except:
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

def r_repr(message_to_encode):
    # We could have done something like "return `message_to_encode`" too.
    return repr(message_to_encode)
