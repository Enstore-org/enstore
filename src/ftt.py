#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""A more OO-style interface to Fermi Tape Tools"""

from future.utils import raise_
import sys
import exceptions
import string
import types

#import setpath
import ftt2

# _ftt is the old module name (from the modules directory).  In supporting
# swig 1.3 we had a naming collision over the module name _ftt.  Thus
# the C module is called _ftt2module.so and the python wrapper is ftt2.py.
_ftt = ftt2

ascii_error = _ftt.cvar.ftt_ascii_error

# grab all the constants, which start with "FTT_"

for k in dir(_ftt):
    if k[:4] == 'FTT_':
        exec('%s=_ftt.%s' % (k[4:], k))


def cleanup(s):
    return string.join(
        map(string.strip, string.split(s, '\n')),
        ' ')


class FTTError(exceptions.Exception):
    def __init__(self, args):
        self.strerror = cleanup(args[0])
        self.errno = args[1]
        self.value = args[2]

    def __str__(self):
        return ascii_error[self.errno]


def raise_ftt(err=None, value=None):
    if err is None:
        err = _ftt.ftt_get_error()
    if err and isinstance(err, list):
        # beginning with swig 1.3.21
        # the list is returned instead of tuple
        # this workaround fixes the incompatibility
        err = tuple(err)
    err = err + (value,)
    if err[1] != 0:
        raise FTTError(err)
    else:
        raise FTTError(("Unspecified error", 0, value))


class stat_buf:
    def __init__(self):
        self.b = _ftt.ftt_alloc_stat()

    def __del__(self):
        try:
            _ftt.ftt_free_stat(self.b)
        except BaseException:
            pass

    def __getitem__(self, key):
        r = _ftt.ftt_extract_stats(self.b, key)
        return r

    def dump(self, file=sys.stdout):
        return _ftt.ftt_dump_stats(self.b, file)

    def __add__(self, other):
        res = stat_buf()
        _ftt.ftt_add_stats(self.b, other.b, res.b)
        return res

    def __rsub__(self, other):
        res = stat_buf()
        _ftt.ftt_sub_stats(self.b, other.b, res.b)
        return res


def check(r, e=-1):
    """first arg is return code from _ftt call, second arg is value or values to be
    considered error returns"""
    if not isinstance(e, tuple):
        e = (e,)
    if r not in e:
        return r
    else:
        raise_ftt(value=r)
    return r


class FTT:

    def __init__(self, descriptor, name=None):
        self.d = descriptor
        self.name = name

    def __del__(self):
        try:
            self.close()
        except BaseException:
            pass

    def __repr__(self):
        return "<FTT: %s>" % ((self.name or id(self)),)

    def open_dev(self):
        return check(_ftt.ftt_open_dev(self.d))

    def close(self):
        if self.d is None:
            return 0
        ret = check(_ftt.ftt_close(self.d))
        self.d = None
        return ret

    def close_dev(self):
        return check(_ftt.ftt_close_dev(self.d))

    def read(self, buf, length):
        return check(_ftt.ftt_read(self.d, buf, length), (0, -1))

    def write(self, buf, length):
        return check(_ftt.ftt_write(self.d, buf, length))

    def writefm(self):
        return check(_ftt.ftt_writefm(self.d))

    def writefm_buffered(self):
        return check(_ftt.ftt_writefm_buffered(self.d))

    def flush_data(self):
        return check(_ftt.ftt_flush_data(self.d))

    def write2fm(self):
        return check(_ftt.ftt_write2fm(self.d))

    def retry(self): raise NotImplementedError

    def skip_fm(self, count):
        return check(_ftt.ftt_skip_fm(self.d, count))

    def skip_rec(self, count):
        return check(_ftt.ftt_skip_rec(self.d, count))

    def skip_to_double_fm(self):
        return check(_ftt.ftt_skip_to_double_fm(self.d))

    def rewind(self):
        return check(_ftt.ftt_rewind(self.d))

    def retension(self):
        return check(_ftt.ftt_retension(self.d))

    def unload(self):
        return check(_ftt.ftt_unload(self.d))

    def erase(self):
        return check(_ftt.ftt_erase(self.d))

    def set_mode(self, density, compression, blocksize):
        return check(_ftt.ftt_set_mode(
            self.d, density, compression, blocksize))

    def get_mode(self):
        return _ftt.ftt_get_mode(self.d)

    def avail_mode(self, density, compression, fixed):
        return _ftt.ftt_avail_mode(self.d, density, compression, fixed)

    def density_to_name(self, density):
        return _ftt.ftt_density_to_name(self.d, density)

    def name_to_density(self, name):
        return _ftt.ftt_name_to_density(self.d, name)

    def get_max_blocksize(self):
        return _ftt.ftt_get_max_blocksize(self.d)

    def set_data_direction(self, n):
        return check(_ftt.ftt_set_data_direction(
            self.d, n))

    def list_all(self):
        return _ftt.ftt_list_all(self.d)

    def chall(self, uid, gid, mode):
        return check(_ftt.ftt_chall(self.d, uid, gid, mode))

    def get_mode_dev(self, devname):
        return _ftt.ftt_get_mode_dev(self.d, devname)

    def set_mode_dev(self, devname, force=0):
        return check(_ftt.ftt_set_mode_dev(self.d, devname, force))

    def describe_dev(self, devname, file=sys.stdout):
        return check(_ftt.ftt_describe_dev(self.d, devname, file))

    def get_basename(self):
        return _ftt.ftt_get_basename(self.d)

    def list_supported(self, file=sys.stdout):
        return check(_ftt.ftt_list_supported(self.d, file))

    def status(self, timeout=60):
        return _ftt.ftt_status(self.d, timeout)
    # interface to do_scsi_command
    # use this for scsi commands which transfer to device

    def do_write_scsi_command(self, OpName, CmdBuff,
                              CmdBufSize, RdWrBuf, RdWrBuffSize, Delay):
        # OpName - arbitrary name of operation (example: "mode sense, page 0x1e"
        # CmdBuff - command list according to scsi command description
        # CmdBufSize - size (length) of CmdBuff
        # RdWrBuf - Data read write list
        # RdWrBuffSize - size (length) of RdWrBuf
        # Delay - scsi command timeout in seconds
        # returns result of scsi command
        return check(_ftt.ftt_do_scsi_command(self.d,
                                              OpName,
                                              CmdBuff,
                                              CmdBufSize,
                                              RdWrBuf,
                                              RdWrBuffSize,
                                              Delay,
                                              1)
                     )
    # interface to do_scsi_command
    # use this for scsi commands which transfer from device

    def do_read_scsi_command(self, OpName, CmdBuff,
                             CmdBufSize, RdWrBuffSize, Delay):
        # OpName - arbitrary name of operation (example: "mode sense, page 0x1e"
        # CmdBuff - command list according to scsi command description
        # CmdBufSize - size (length) of CmdBuff
        # RdWrBuf - Data read write list
        # RdWrBuffSize - size (length) of RdWrBuf
        # Delay - scsi command timeout in seconds
        # returns data as list of numbers
        return check(_ftt.do_read_scsi_command(self.d,
                                               OpName,
                                               CmdBuff,
                                               CmdBufSize,
                                               RdWrBuffSize,
                                               Delay))

    def get_position(self):
        try:
            status, file_position, block_position = _ftt.ftt_get_position(
                self.d)
        except TypeError as detail:
            # do not know how this might happen
            raise_(TypeError, detail)

        if status:
            raise_ftt(value=status)
        else:
            return file_position, block_position

    def get_stats(self):
        b = stat_buf()
        check(_ftt.ftt_get_stats(self.d, b.b))
        return b


def open(basename, rdonly):
    desc = _ftt.ftt_open(basename, rdonly)
    # XXX check return!
    return FTT(desc, name=basename)


def open_logical(basename, os, driveid, rdonly):
    desc = _ftt.ftt_open_logical(basename, rdonly)
    # XXX check return!
    return FTT(desc, name=basename)
