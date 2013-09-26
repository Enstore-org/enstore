#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import os
import errno
import time

# enstore imports
#import enstore_plotter_framework

class EnstorePlotterModule:
    def __init__(self,name,isActive=True):
        self.name=name
        self.is_active=isActive
        self.parameters = {}
    def isActive(self):
        return self.is_active
    def setActive(self,isActive=True):
        self.is_active=isActive
    def book(self,frame):
        print "Booking ",self.name
    def fill(self,frame):
        print "Filling ",self.name
    def plot(self):
        print "Plotting ",self.name
    def install(self):
        print "Installing:",self.name
    def add_parameter(self,par_name,par_value):
        self.parameters[par_name]=par_value
    def get_parameter(self,name):
        return self.parameters.get(name)
    def move(self, src, dst):
        #Open the input and output files.
        src_fd = os.open(src, os.O_RDONLY)
        try:
            dst_fd = os.open(dst, os.O_WRONLY | os.O_CREAT)
        except OSError, msg:
            if msg.args[0] in [errno.EISDIR]:
                fname = os.path.join(dst, os.path.basename(src))
                dst_fd = os.open(fname, os.O_WRONLY | os.O_CREAT)
            else:
                raise msg
        #Loop over the files copying the data.
        to_move = -1
        while to_move:
            to_move = os.read(src_fd, 1048576)
            if to_move:
                os.write(dst_fd, to_move)
        #Close the source and destination files.
        os.close(src_fd)
        os.close(dst_fd)
        #Remove the source file.
        os.remove(src)

def roundtime(seconds, rounding=None):
    """
    Round the provided time and return it.

    `seconds`: time in seconds.
    `rounding`: None, 'floor' or 'ceil'.
    """

    Y, M, D, h, m, s, wd, jd, dst = time.localtime(seconds)
    hms = {None: (h, m, s), 'floor': (0, 0, 0), 'ceil': (23, 59, 59)}
    h, m, s = hms[rounding]
    seconds_rounded = time.mktime((Y, M, D, h, m, s, wd, jd, dst))
    return seconds_rounded
