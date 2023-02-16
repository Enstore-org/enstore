#!/usr/bin/env python
######################################################################
# $Id
######################################################################
# This is a simple udp client used along with udp_srvr.py for testing
# server port is 6700
# command line arguments: host_name port message_length
import sys
import  udp_client

class Client:
    def __init__(self):
        self.udpc = udp_client.UDPClient()


if __name__ == "__main__":   # pragma: no cover
    cl = Client()
    host = sys.argv[1]
    port = int(sys.argv[2])
    m_len = int(sys.argv[3])
    ticket = {'work':'echo', 'args':"*"*m_len}
    addr = (host, port)

    ret = cl.udpc.send(ticket, addr, rcv_timeout=10, max_send=1)
    print ret

