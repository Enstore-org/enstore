#!/usr/bin/env python

# $Id$

from Tkinter import *
import tkFont

import os
import sys
import socket
import select
import string
import time
from cmath import pi, exp


def dtr(degrees):
    return pi * degrees/180.0

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
            d=d+1
            wd=wd+1
            jd=jd+1
            ##I'm not going to worry about end-of-month.  Sue me!
    t = time.mktime((Y, M, D, h, m, 0, wd, jd, dst))
    return t


class Meter(Canvas):
    angle_range = 160
    fancy_needle = 1
    avg_interval = 5
    def __init__(self, master, system_name, **attributes):
        Canvas.__init__(self, master)
        self.system_name = system_name
        title = "Ratemeter: "+system_name
        Tk.title(self.master, title)
        self.pack(expand=1, fill=BOTH)
        self.width, self.height = 0,0
        self.scale_max = 50.0
        self.r = []
        self.w = []
        self.tr = 0.0
        self.tw = 0.0
        self.needle1 = None
        self.needle2 = None
        self.labels = []
        
        self.bottom_margin = 20
        
    def update_needles(self, r, w): 
        self.tr = self.tr + r
        self.r.insert(0,r)
        while len(self.r)>self.avg_interval:
            self.tr = self.tr - self.r.pop()
        self.tw = self.tw + w
        self.w.insert(0,w)
        while len(self.w)>self.avg_interval:
            self.tw = self.tw - self.w.pop()
        ar = self.tr / self.avg_interval
        aw = self.tw / self.avg_interval

        self.maybe_resize()
        self.draw_needles(ar, aw)
        self.update()

    def draw_needles(self, r, w):

        width, height = self.width, self.height
        bottom = self.bottom_margin
        
        I = complex(0,1)
        size  = 0.8 * self.width/4.0
        start_angle = dtr(180-self.angle_range)/2
        angle_range = dtr(self.angle_range)
        
        ang = start_angle + angle_range * (1 - r/self.scale_max)
        p0 = complex(width/4.0, bottom)
        p1 = p0 + size * exp(I*ang)
        p2 = p0 + 3*exp(I*(ang-pi/2))
        p3 = p0 + 3*exp(I*(ang+pi/2))
        if self.needle1:
            self.delete(self.needle1)
        if self.fancy_needle:
            self.needle1 = self.create_polygon(p1.real, height-p1.imag,
                                               p2.real, height-p2.imag,
                                               p3.real, height-p3.imag,
                                               fill='red')
        else:
            self.needle1 = self.create_line(p0.real, height-p0.imag,
                                            p1.real, height-p1.imag,
                                            width=3, fill='red')
            
        ang = start_angle + angle_range * (1 - w/self.scale_max)
        p0 = complex(3*width/4.0, bottom)
        p1 = p0 + size * exp(I*ang)
        p2 = p0 + 3*exp(I*(ang-pi/2))
        p3 = p0 + 3*exp(I*(ang+pi/2))
        if self.needle2:
            self.delete(self.needle2)

        if self.fancy_needle:
            self.needle2 = self.create_polygon(p1.real, height-p1.imag,
                                               p2.real, height-p2.imag,
                                               p3.real, height-p3.imag,
                                               fill='red')
        else:
            self.needle2 = self.create_line(p0.real, height-p0.imag,
                                            p1.real, height-p1.imag,
                                            width=3, fill='red')
        
        
    def maybe_resize(self):
        width =  self.winfo_width()
        height = self.winfo_height()

        if (width, height) == (self.width, self.height):
            return

        self.width, self.height = width, height
        bottom = self.bottom_margin
        
        for l in self.labels:
            self.delete(l)

        large_font = tkFont.Font(family='Helvetica',size=24)
        self.labels.append(self.create_text(width/2, 12,
                                            text=self.system_name + " rates",
                                            font=large_font))
        
        self.labels.append(self.create_text(width/4, height-7, text="Read MB/S"))
        self.labels.append(self.create_text(3*width/4, height-7, text="Write MB/S"))

        size = 0.9 * width / 4.0
        self.labels.append(self.create_rectangle(width/4 - size, height - bottom - size,
                                                 width/4 + size, height-bottom+3, fill='white'))

        self.labels.append(self.create_rectangle(3*width/4 - size, height - bottom - size,
                                                 3*width/4 + size, height-bottom+3, fill='white'))

        size = 0.8 * width / 4.0
        start_angle = (180-self.angle_range)/2
        self.labels.append(self.create_arc(width/4 - size, height - bottom - size,
                                           width/4 + size, height - bottom + size,
                                           start=start_angle, extent=self.angle_range, width=2, style='arc'))
        
        self.labels.append(self.create_arc(3*width/4 - size, height - bottom - size,
                                           3*width/4 + size, height - bottom + size,
                                           start=start_angle, extent=self.angle_range, width=2, style='arc'))

        self.labels.append(self.create_oval(width/4 - 3, height - bottom - 3,
                                            width/4 + 3, height - bottom + 3,
                                            fill='red'))

        self.labels.append(self.create_oval(3*width/4 - 3, height - bottom - 3,
                                            3*width/4 + 3, height - bottom + 3,
                                            fill='red'))

        I = complex(0,1)
        start_angle = dtr(start_angle)
        angle_range = dtr(self.angle_range)
        for x in range(11):
            label = "%.2g" % (x*self.scale_max/10.0)
            ang = start_angle + angle_range * (1 - x/10.0)
            p0 = complex(width/4.0, bottom)
            p1 = p0 + (0.75*width/4.0) * exp(I*ang)
            p2 = p0 + (0.8*width/4.0) * exp(I*ang)
            p3 = p0 + (0.85*width/4.0) * exp(I*ang)
            self.labels.append(self.create_line(p1.real, height-p1.imag, p2.real, height-p2.imag))
            self.labels.append(self.create_text(p3.real, height-p3.imag, text=label))
            p0 = complex(3*width/4.0, bottom)
            p1 = p0 + (0.75*width/4.0) * exp(I*ang)
            p2 = p0 + (0.8*width/4.0) * exp(I*ang)
            p3 = p0 + (0.85*width/4.0) * exp(I*ang)
            self.labels.append(self.create_line(p1.real, height-p1.imag, p2.real, height-p2.imag))
            self.labels.append(self.create_text(p3.real, height-p3.imag, text=label))
            
class Ratemeter:
    interval = 1
    resubscribe_interval = 10*60 

    def __init__(self, event_relay_addr, system_name, output_dir='/tmp'):
        self.event_relay_addr = event_relay_addr
        self.system_name = system_name
        self.output_dir = output_dir
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        hostname = os.uname()[1]
        self.sock.bind((hostname, 0))
        self.addr = self.sock.getsockname()
        self.subscribe_time = 0
        self.mover_msg = {} #key is mover, value is last (num, denom)
        self.meter = Meter(master=None, system_name = system_name)
        
    def subscribe(self):
        self.sock.sendto("notify %s %s" % (self.addr),
                         (self.event_relay_addr))

    def main(self):
        now = time.time()
        self.start_time = now
        N = 1L
        MB = 1024*1024
        bytes_read = 0.0
        bytes_written = 0.0
        while 1:
            now = time.time()

            if now - self.subscribe_time > self.resubscribe_interval:
                self.subscribe()
                self.subscribe_time = now
                
            end_time = self.start_time + N * self.interval
            remaining = end_time - now
            if remaining <= 0:
                self.meter.update_needles(bytes_read/MB, bytes_written/MB)

                bytes_read = 0.0
                bytes_written = 0.0
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
            if denom != denom_0 or num<num_0:
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
        system_name = 'd0en'
    elif event_relay_host[:3]=='stk':
        event_relay_host = 'stkensrv2.fnal.gov'
        system_name = 'stken'
    else:
        event_relay_host = os.environ.get("ENSTORE_CONFIG_HOST")
        system_name = event_relay_host
        
    event_relay_port = 55510

    rm = Ratemeter((event_relay_host, event_relay_port), system_name)
    rm.main()
    
            
