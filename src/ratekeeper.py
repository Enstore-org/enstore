#!/usr/bin/env python

# $Id$

import os
import sys
import socket
import select
import string
import time

def endswith(s1,s2):
    return s1[-len(s2):] == s2

def atol(s):
    if s[-1] == 'L':
        s = s[:-1] #chop off any trailing "L"
    return string.atol(s)

def next_minute(t=None):
    if t is None:
        t = time.time()
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(t)
    m = (m+1)%60
    if m==0:
        h=(h+1)%24
        if h==0:
            D=D+1
            wd=wd+1
            jd=jd+1
            ##I'm not going to worry about end-of-month.  Sue me!
    t = time.mktime((Y, M, D, h, m, 0, wd, jd, dst))
    return t
        

class Ratekeeper:
    interval = 15
    resubscribe_interval = 10*60 
    def __init__(self, event_relay_addr, filename_base, output_dir='/tmp/RATES'):
        self.event_relay_addr = event_relay_addr
        self.filename_base = filename_base
        self.output_dir = output_dir
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.outfile = None
        self.ymd = None #Year, month, date
        self.last_ymd = None
        hostname = os.uname()[1]
        self.sock.bind((hostname, 0))
        self.addr = self.sock.getsockname()
        self.subscribe_time = 0
        self.mover_msg = {} #key is mover, value is last (num, denom)
        
    def subscribe(self):
        self.sock.sendto("notify %s %s" % (self.addr),
                         (self.event_relay_addr))

    def check_outfile(self, now=None):
        if now is None:
            now = time.time()
        tup = time.localtime(now)
        self.ymd = tup[:3]
        if self.ymd != self.last_ymd:
            self.last_ymd = self.ymd
            if self.outfile:
                try:
                    self.outfile.close()
                except:
                    sys.stderr.write("Can't open file\n")

            year, month, day = self.ymd
            outfile_name = os.path.join(self.output_dir, "%s.RATES.%04d%02d%02d" %
                                        (self.filename_base, year, month, day))
            self.outfile=open(outfile_name, 'w')

    
    def main(self):
        now = time.time()
        self.start_time = next_minute(now)
        wait = self.start_time - now
        sys.stderr.write("waiting %.2f seconds\n" % wait)
        time.sleep(wait)
        sys.stderr.write("starting\n")
        N = 1L
        bytes_read = 0L
        bytes_written = 0L
        while 1:
            now = time.time()
            self.check_outfile(now)
            if now - self.subscribe_time > self.resubscribe_interval:
                self.subscribe()
                self.subscribe_time = now
                
            end_time = self.start_time + N * self.interval
            remaining = end_time - now
            if remaining <= 0:
                try:
                    self.outfile.write( "%s %d %d\n" % (time.strftime("%T", time.localtime(now)),
                                                        bytes_read, bytes_written))
                    self.outfile.flush()
                except:
                    sys.stderr.write("Can't write to output file\n")
                    
                bytes_read = 0L
                bytes_written = 0L
                N = N + 1
                end_time = self.start_time + N * self.interval
                remaining = end_time - now

            r, w, x = select.select([self.sock], [], [], remaining)
            
            if not r:
                continue

            r=r[0]

            try:
                cmd = r.recv(1024)
            except:
                cmd = None
                
            if not cmd:
                continue
            cmd = string.strip(cmd)
            words = string.split(cmd)
            if not words:
                continue
            if words[0] != 'transfer':
                continue
            mover = words[1]
            mover = string.upper(mover)
            if string.find(mover, 'NULL')>=0:
                continue

            num = atol(words[2])  #NB -bytes = read;  +bytes=write
            writing = num>0
            num = abs(num)
            denom = atol(words[3])
            
            prev = self.mover_msg.get(mover)
            self.mover_msg[mover] = (num, denom)
            
            if not prev:
                continue

            num_0, denom_0 = prev
            if num < num_0 or denom != denom_0:
                #consider this the beginning of a new transfer
                continue
            bytes = num - num_0
            if writing:
                bytes_written = bytes_written + bytes
            else:
                bytes_read = bytes_read + bytes


if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        event_relay_host = sys.argv[1]
    if event_relay_host[:2]=='d0':
        event_relay_host = 'd0ensrv2.fnal.gov'
        filename_base = 'd0en'
    elif event_relay_host[:3]=='stk':
        event_relay_host = 'stkensrv2.fnal.gov'
        filename_base = 'stken'
    else:
        event_relay_host = os.environ.get("ENSTORE_CONFIG_HOST")
        filename_base = event_relay_host
        
    event_relay_port = 55510

    rk = Ratekeeper((event_relay_host, event_relay_port), filename_base)
    rk.main()
    
            
