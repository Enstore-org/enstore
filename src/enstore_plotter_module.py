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
    """Base class for all Enstore plotter modules."""
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
        """ Add a parameter to the modules parameter dictionary.  
        If the parameter already exists, it will be overwritten.

        Args:
            par_name(str): The name of the parameter.
            par_value(any): The value of the parameter.
        Returns:
            None
        """
        self.parameters[par_name]=par_value

    def get_parameter(self,name):
        """ get a parameter from the modules parameter dictionary.
        
        Args:
            name(str): The name of the parameter.
        Returns:
            The value of the parameter or None if the parameter does not exist.
        """
        return self.parameters.get(name)

    def move(self, src, dst):
        """ Move a file from src to dst.  If dst is a directory, move the file
        into the directory.  If dst is a file, overwrite it.  If dst is a
        directory and the file already exists, append a number to the file
        name.  If the file already exists and the number is 999, raise an
        exception.  
        
        Args:
            src(str): The source file.
            dst(str): The destination file or directory.
        Returnns:
            None
        Raises:
            OSError: If the destination file already exists and the number is
                     999.
        """
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

    Args:
        seconds: time in seconds.
        rounding: None, 'floor' or 'ceil'.
    Returns:
        time in seconds with indicated rounding applied .
        'floor' rounds down to the nearest day.
        'ceil' rounds up to the nearest day.
    """

    Y, M, D, h, m, s, wd, jd, dst = time.localtime(seconds)
    hms = {None: (h, m, s), 'floor': (0, 0, 0), 'ceil': (23, 59, 59)}
    h, m, s = hms[rounding]
    seconds_rounded = time.mktime((Y, M, D, h, m, s, wd, jd, dst))
    return seconds_rounded
