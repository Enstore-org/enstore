#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################

"""
Purpose: use it when a resource is accessed concurrently for read and write
         (e.g. file).
         write blocks new read accesses, requires old reads to be released
         read does not block new reads
"""
import threading

DEFAULT_TIMEOUT = 30


class ReadWriteConditionVariable:
    def __init__(self):
        self.__read_ready = threading.Condition(threading.Lock())
        self.__readers = 0

    def acquire_read(self):
        self.__read_ready.acquire()
        self.__readers = self.__readers + 1
        self.__read_ready.release()

    def release_read(self):
        self.__read_ready.acquire()
        try:
            if not self.__readers:
                return
            self.__readers = self.__readers - 1
            if not self.__readers:
                self.__read_ready.notifyAll()
        finally:
            self.__read_ready.release()

    def acquire_write(self, timeout=None):
        self.__read_ready.acquire()
        while self.__readers > 0:
            try:
                to = DEFAULT_TIMEOUT
                if timeout:
                    to = timeout
                self.__read_ready.wait(to)
            except RuntimeError:
                self.__readers = 0

    def release_write(self):
        self.__read_ready.release()
