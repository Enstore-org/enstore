#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################


import threading

class ReadWriteLock:
    def __init__(self):
        self.__read_ready = threading.Condition(threading.Lock())
        self.__readers = 0

    def acquire_read(self):
        self.__read_ready.acquire()
        try:
            self.__readers = self.__readers + 1
        finally:
            self.__read_ready.release()

    def release_read(self):
        self.__read_ready.acquire()
        try:
            self.__readers = self.__readers - 1
            if not self.__readers:
                self.__read_ready.notifyAll()
        finally:
            self.__read_ready.release()

    def acquire_write(self):
        self.__read_ready.acquire()
        while self.__readers > 0:
            self.__read_ready.wait()

    def release_write(self):
        self.__read_ready.release()
