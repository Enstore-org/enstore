#!/usr/bin/env python

import os
import socket
import select
import time
import string

DEFAULT_PORT = 55510

class Relay:

    client_timeout = 15*60 #clients recieve messages for this long
    
    def __init__(self, myport=DEFAULT_PORT):
        self.clients = {} # key is (host,port), value is time connected
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        myaddr = ("", myport)
        self.listen_socket.bind(myaddr)
        
    def mainloop(self):
        while 1:
            readable, junk, junk = select.select([self.listen_socket], [], [], 15)
            if not readable:
                continue
            msg = self.listen_socket.recv(1024)
            now = time.time()
            if not msg:
                continue
            tok = string.split(msg)
            if not tok:
                continue
            if tok[0]=='notify':
                try:
                    ip = tok[1]
                    port = int(tok[2])
                    self.clients[(ip, port)] = now
                except:
                    print "cannot handle request", msg
            else:
                for addr, t0 in self.clients.items():
                    if now - t0 > self.client_timeout:
                        del self.clients[addr]
                    else:
                        try:
                            self.send_socket.sendto(msg, addr)
                        except:
                            print "send failed", addr
                            
                
if __name__ == '__main__':
    R = Relay()
    R.mainloop()
