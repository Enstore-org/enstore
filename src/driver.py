#!/usr/bin/env python

# $Id$

""" Abstract base class for driver classes"""


class Driver:

    def fileno(self):
        raise NotImplementedError

    def tell(self):
        raise NotImplementedError

    def open(self, device, mode):
        raise NotImplementedError

    def reopen(self, device, mode):
        raise NotImplementedError
    
    def flush(self, device):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError

    def rewind(self):
        raise NotImplementedError
    
    def seek(self, where):
        raise NotImplementedError

    def get_status(self):
        raise NotImplementedError

    def set_mode(self, density=None, compression=None, blocksize=None):
        raise NotImplementedError
    
    def ready_to_read(self):
        raise NotImplementedError

    def ready_to_write(self):
        raise NotImplementedError
    
    def rates(self):
        raise NotImplementedError
    
