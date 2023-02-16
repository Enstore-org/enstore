#!/usr/bin/env python
######################################################################
# $Id
######################################################################
# This is a simple udp server used along with udp_cl.py for testing

import socket
import dispatching_worker


class Server(dispatching_worker.DispatchingWorker):

    def __init__(self):
        self.hostname = socket.gethostname()
        dispatching_worker.DispatchingWorker.__init__(self, (self.hostname,
                                                             6700))
    def echo(self, ticket):
        print "received %s len %s"%(ticket, len(ticket))
        self.reply_to_caller(ticket)


if __name__ == "__main__":   # pragma: no cover
    srv = Server()
    srv.serve_forever()

