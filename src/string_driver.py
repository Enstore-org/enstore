#!/usr/bin/env python

# $Id$

import setpath
import generic_driver
import strbuffer


class StringDriver(generic_driver.Driver):

    def __init__(self, src):
        self.src = src

    def read(self, buf, offset, nbytes):
        strbuffer.buf_read_string(self.src, buf, offset, nbytes)
        return nbytes
