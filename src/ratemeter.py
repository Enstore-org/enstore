#!/usr/bin/env python

# $Id$

from cmath import pi, exp
import time
import string
import select
import socket
import tkFont
import Tkinter
import os
import sys

# Set up paths to find our private copy of tcl/tk 8.3
ENSTORE_DIR = os.environ.get("ENSTORE_DIR")
if ENSTORE_DIR:
    TCLTK_DIR = os.path.join(ENSTORE_DIR, 'etc', 'TclTk')
else:
    TCLTK_DIR = os.path.normpath(
        os.path.join(
            os.getcwd(),
            '..',
            'etc',
            'TclTk'))
os.environ["TCL_LIBRARY"] = os.path.join(TCLTK_DIR, 'tcl8.3')
os.environ["TK_LIBRARY"] = os.path.join(TCLTK_DIR, 'tk8.3')
sys.path.insert(0, os.path.join(TCLTK_DIR, sys.platform))


def dtr(degrees):
    return pi * degrees / 180.0


def endswith(s1, s2):
    return s1[-len(s2):] == s2


def atol(s):
    if s[-1] == 'L':
        s = s[:-1]  # chop off any trailing "L"
    return string.atol(s)


I = complex(0, 1)


class Meter:
    angle_range = 160
    fancy_needle = 1
    avg_interval = 5

    def __init__(self, panel, left, top, right, bottom, scale_max=2.5):
        self.panel = panel
        self.left = left
        self.top = top
        self.right = right
        self.bottom = bottom
        self.scale_max = scale_max
        self.values = []
        self.total = 0.0
        self.needle = None
        self.labels = []

    def resize(self, left, top, right, bottom):
        self.left = left
        self.top = top
        self.right = right
        self.bottom = bottom
        self.redraw_labels()

    def update(self, value):
        self.total = self.total + value
        self.values.insert(0, value)
        while len(self.values) > self.avg_interval:
            self.total = self.total - self.values.pop()
        avg = self.total / self.avg_interval
        self.draw_needle(avg)

    def draw_needle(self, value):
        left, top, right, bottom = self.left, self.top, self.right, self.bottom
        size = (right - left) / 2.
        mid_x = (left + right) / 2.

        bottom = bottom - 3  # adjustment

        if value > self.scale_max:
            while value > self.scale_max:
                self.scale_max = self.scale_max * 2
            self.redraw_labels()

        start_angle = (180 - self.angle_range) / 2
        angle_range = self.angle_range
        radius = 0.85 * size
        ang = dtr(start_angle + angle_range * (1 - value / self.scale_max))
        p0 = complex(mid_x, 0)
        p1 = p0 + radius * exp(I * ang)
        p2 = p0 + 3 * exp(I * (ang - pi / 2))
        p3 = p0 + 3 * exp(I * (ang + pi / 2))
        if self.needle:
            self.panel.delete(self.needle)
        if self.fancy_needle:
            self.needle = self.panel.create_polygon(p1.real, bottom - p1.imag,
                                                    p2.real, bottom - p2.imag,
                                                    p3.real, bottom - p3.imag,
                                                    fill='red')
        else:
            self.needle1 = self.panel.create_line(p0.real, bottom - p0.imag,
                                                  p1.real, bottom - p1.imag,
                                                  width=3, fill='red')

    def redraw_labels(self):
        left, top, right, bottom = self.left, self.top, self.right, self.bottom
        mid_x = (left + right) / 2.
        size = (right - left) / 2.
        start_angle = (180 - self.angle_range) / 2.
        angle_range = self.angle_range
        for l in self.labels:
            self.panel.delete(l)

        self.labels.append(self.panel.create_rectangle(left, top - 3,  # adjustment
                                                       right, bottom,
                                                       fill='white'))

        bottom = bottom - 3  # adjustment
        radius = 0.85 * size
        self.labels.append(self.panel.create_arc(mid_x - radius, bottom - radius,
                                                 mid_x + radius, bottom + radius,
                                                 start=start_angle, extent=angle_range, width=2, style='arc'))

        self.labels.append(self.panel.create_oval(mid_x - 3, bottom - 3,
                                                  mid_x + 3, bottom + 3,
                                                  fill='red'))

        p0 = complex(mid_x, 0)
        r0 = 0.80 * size
        r1 = 0.85 * size
        r2 = 0.93 * size
        for x in range(11):
            label = "%.3g" % (x * self.scale_max / 10.0)
            ang = start_angle + angle_range * (1 - x / 10.0)
            p1 = p0 + r0 * exp(dtr(I * ang))
            p2 = p0 + r1 * exp(dtr(I * ang))
            p3 = p0 + r2 * exp(dtr(I * ang))
            self.labels.append(
                self.panel.create_line(
                    p1.real,
                    bottom - p1.imag,
                    p2.real,
                    bottom - p2.imag))
            self.labels.append(
                self.panel.create_text(
                    p3.real,
                    bottom - p3.imag,
                    text=label))


class MeterPanel(Tkinter.Canvas):
    bottom_margin = 20

    def __init__(self, master, system_name, **attributes):
        width, height = 475, 150
        Tkinter.Canvas.__init__(self, master, width=width, height=height)
        self.system_name = system_name
        title = "Ratemeter: " + system_name
        Tkinter.Tk.title(self.master, title)
        self.pack(expand=1, fill=Tkinter.BOTH)
        self.width, self.height = width, height
        bottom = height - self.bottom_margin
        meter_size = 0.9 * width / 2.
        x0 = (width / 2. - meter_size) / 2.
        y0 = bottom - meter_size / 2.
        self.meter1 = Meter(self, x0, y0, x0 + meter_size, bottom)
        x0 = x0 + self.width / 2.
        self.meter2 = Meter(self, x0, y0, x0 + meter_size, bottom)
        self.labels = []

    def update_meters(self, r, w):
        self.maybe_resize()
        self.meter1.update(r)
        self.meter2.update(w)
        self.update()

    def maybe_resize(self):
        width = self.winfo_width()
        height = self.winfo_height()

        if (width, height) == (self.width, self.height):
            return

        self.width, self.height = width, height
        bottom = height - self.bottom_margin

        for l in self.labels:
            self.delete(l)

        large_font = tkFont.Font(family='Helvetica', size=24)
        self.labels.append(self.create_text(width / 2, 12,
                                            text=self.system_name + " rates",
                                            font=large_font))

        self.labels.append(
            self.create_text(
                width / 4,
                height - 7,
                text="Read MB/S"))
        self.labels.append(
            self.create_text(
                3 * width / 4,
                height - 7,
                text="Write MB/S"))

        meter_size = 0.9 * self.width / 2.
        x0 = 0.05 * self.width / 2.
        y0 = bottom - meter_size / 2.
        self.meter1.resize(x0, y0, x0 + meter_size, bottom)
        x0 = x0 + self.width / 2.
        self.meter2.resize(x0, y0, x0 + meter_size, bottom)
        self.update()


class Ratemeter:
    interval = 1
    resubscribe_interval = 10 * 60

    def __init__(self, event_relay_addr, system_name, output_dir='/tmp'):
        self.event_relay_addr = event_relay_addr
        self.system_name = system_name
        self.output_dir = output_dir
        hostname = os.uname()[1]
        address_family = socket.getaddrinfo(hostname, None)[0][0]
        self.sock = socket.socket(address_family, socket.SOCK_DGRAM)
        if hostname == 'sirius.net.home':
            hostname = 'cgw-sirius.dyndns.org'
        self.sock.bind((hostname, 0))
        self.addr = self.sock.getsockname()
        self.subscribe_time = 0
        self.mover_msg = {}  # key is mover, value is last (num, denom)
        self.meter_panel = MeterPanel(master=None, system_name=system_name)

    def subscribe(self):
        self.sock.sendto("notify %s %s" % (self.addr),
                         (self.event_relay_addr))

    def main(self):
        now = time.time()
        self.start_time = now
        N = 1
        MB = 1024 * 1024.
        bytes_read = bytes_written = 0.0
        while True:
            now = time.time()

            if now - self.subscribe_time > self.resubscribe_interval:
                self.subscribe()
                self.subscribe_time = now

            end_time = self.start_time + N * self.interval
            remaining = end_time - now
            while remaining <= 0:
                self.meter_panel.update_meters(
                    bytes_read / MB, bytes_written / MB)
                bytes_read = bytes_written = 0.0
                N = N + 1
                end_time = self.start_time + N * self.interval
                now = time.time()
                remaining = end_time - now

            r, w, x = select.select([self.sock], [], [], remaining)

            if not r:
                continue

            r = r[0]

            try:
                cmd = r.recv(1024)
            except BaseException:
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
            if string.find(mover, 'NULL') >= 0:
                continue

            num = atol(words[2])  # NB -bytes = read;  +bytes=write
            writing = num > 0
            num = abs(num)
            denom = atol(words[3])
            prev = self.mover_msg.get(mover)
            self.mover_msg[mover] = (num, denom)

            if not prev:
                continue
            num_0, denom_0 = prev
            if num < num_0 or denom != denom_0:
                # consider this the beginning of a new transfer
                continue
            bytes = num - num_0
            if writing:
                bytes_written = bytes_written + bytes
            else:
                bytes_read = bytes_read + bytes


if __name__ == "__main__":

    if len(sys.argv) > 1:
        event_relay_host = sys.argv[1]
    if event_relay_host[:2] == 'd0':
        event_relay_host = 'd0ensrv2.fnal.gov'
        system_name = 'd0en'
    elif event_relay_host[:3] == 'stk':
        event_relay_host = 'stkensrv2.fnal.gov'
        system_name = 'stken'
    else:
        event_relay_host = os.environ.get("ENSTORE_CONFIG_HOST")
        system_name = event_relay_host

    event_relay_port = 55510

    rm = Ratemeter((event_relay_host, event_relay_port), system_name)
    rm.main()
