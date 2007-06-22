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
import rexec

# enstore imports
import host_config
import cleanUDP
import Trace
import e_errors

# try to get a port from a range of possibilities
def get_default_callback(use_port=0):
    host = host_config.get_default_interface()['ip']
    sock = cleanUDP.cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
    sock.socket.bind((host, use_port))
    host, port = sock.socket.getsockname()
    return host, port, sock

# try to get a port from a range of possibilities
def get_callback(use_host=None, use_port=0):
    if use_host != None:
        host = use_host
    else:
        host = host_config.choose_interface()['ip']
    sock = cleanUDP.cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
    sock.socket.bind((host, use_port))
    host, port = sock.socket.getsockname()
    return host, port, sock

### These function deal with encoding and decoding the raw bytes from
### udp messages.

_eval = rexec.RExec().r_eval

def r_eval(message_to_decode):
    try:
        #This is uses the restricted eval.  The unstricted eval could have
        #  been used by doing: return eval(message_to_decode)
        return _eval(message_to_decode)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        exc, msg = sys.exc_info()[:2]
        logmsg="udp_common.r_reply %s %s"%(exc, msg)
        if exc == exceptions.SyntaxError: #msg size> max datagram size?
            logmsg=logmsg+"Truncated message?"
        elif exc == exceptions.TypeError:
            logmsg = logmsg + ": " + message_to_decode
        Trace.log(e_errors.ERROR, logmsg)
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    #return number, out, t

def r_repr(message_to_encode):
    # We could have done something like "return `message_to_encode`" too.
    return repr(message_to_encode)
