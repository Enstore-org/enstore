#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import socket

# enstore imports
import host_config
import cleanUDP

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
