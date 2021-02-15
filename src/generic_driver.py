#!/usr/bin/env python

# $Id$


""" Abstract base class for driver classes"""

import exceptions


class DriverError(exceptions.Exception):
    def __init__(self, arg):
        exceptions.Exception.__init__(self, arg)


class Driver:

    mount_delay = 0

    def fileno(self):
        raise NotImplementedError

    def tell(self):
        raise NotImplementedError

    def open(self, device, mode, retry_count=10):
        raise NotImplementedError

    def flush(self, device):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def rewind(self):
        raise NotImplementedError

    def seek(self, where, eot_ok=0):
        raise NotImplementedError

    def skipfm(self, n):
        raise NotImplementedError

    def get_status(self):
        raise NotImplementedError

    def verify_label(self, volume_label, mode, expected_length=80):
        raise NotImplementedError

    def set_mode(self, density=None, compression=None, blocksize=None):
        raise NotImplementedError

    def rates(self):
        raise NotImplementedError

    def get_cleaning_bit(self):
        return 0
