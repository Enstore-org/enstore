#!/usr/bin/env python

##############################################################################
#
# $Id$
##############################################################################

"""
    Enstore File Cache errors and exceptions
"""

class EnCacheException(Exception):
    """Enstore Cache Exception"""

class EnCacheWrongCommand(EnCacheException):
    """Wrong Command"""
    def __init__(self, c):
        self.command = c
    def __str__(self):
        # @todo make it more mnemonic when printed in the logs
        return "unrecognized command " +repr(self.command)
