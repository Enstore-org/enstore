#!/usr/bin/env python

# $Id$

import select
import socket

import driver
import e_errors
import strbuffer

class NetDriver(driver.Driver):

    def __init__(self):
        self.sock = -1
        
    def fdopen(self, sock):
        self.sock = sock
        size = 1024*1024
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                sock.setsockopt(socket.SOL_SOCKET, opt, size)
                print "tcp buffer size",  opt, sock.getsockopt(
                    socket.SOL_SOCKET, opt)
            except socket.error, msg:
                print "set buffer size", opt, msg
                    
        print "fdopen", self.sock, self.sock.fileno()
        return self.sock
        
    def fileno(self):
        return self.sock.fileno()
    
    def close(self):
        r = self.sock.close()
        self.sock = -1
        return r

    def read(self, buf, offset, nbytes):
        return strbuffer.buf_recv(self.fileno(), buf, offset, nbytes)
                                  
    def write(self, buf, offset, nbytes):
        return strbuffer.buf_send_dontwait(self.fileno(), buf, offset, nbytes)
        
    def ready_to_read(self):
        r,w,x = select.select([self], [], [], 0)
        return r

    def ready_to_write(self):
        r,w,x = select.select([], [self],  [], 0)
        return w
    
            
        
