#!/usr/bin/env python

# $Id$
    
"""A more OO-style interface to Fermi Tape Tools"""

import sys
import exceptions
import string

import setpath
import ftt_low

ascii_error = ftt_low.cvar.ftt_ascii_error

#grab all the constants, which start with "FTT_"

for k in dir(ftt_low):
    if k[:4]=='FTT_':
        exec('%s=ftt_low.%s' % (k[4:], k))

def cleanup(s):
    return string.join(
        map(string.strip, string.split(s,'\n')),
        ' ')


class FTTError(exceptions.Exception):
    def __init__(self, args):
        self.strerror = cleanup(args[0])
        self.errno = args[1]
    def __str__(self):
        return ascii_error[self.errno]

def raise_ftt(err=None):
    if err is None:
        err=ftt_low.ftt_get_error()
    if err[1]!=0:
        raise FTTError(err)
    else:
        raise FTTError(("Unspecified error", 0))

    

class stat_buf:
    def __init__(self):
        self.b = ftt_low.ftt_alloc_stat()

        
    def __del__(self):
        try:
            ftt_low.ftt_free_stat(self.b)
        except:
            pass

    def __getitem__(self, key):
        r = ftt_low.ftt_extract_stats(self.b,key)
        return r

    def dump(self, file=sys.stdout):
        return ftt_low.ftt_dump_stats(self.b, file)

    def __add__(self, other):
        res = stat_buf()
        ftt_low.ftt_add_stats(self.b, other.b, res.b)
        return res
    
    def __rsub__(self, other):
        res = stat_buf()
        ftt_low.ftt_sub_stats(self.b, other.b, res.b)
        return res
    
    
def check(r, e=-1):
    if type(e) is not type(()):
        e=(e,)
    if r not in e:
        return r
    else:
        raise_ftt()
    return r


class FTT:

    def __init__(self, descriptor):
        self.d = descriptor

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def open_dev(self):
        return check(ftt_low.ftt_open_dev(self.d))
    def close(self): 
        return check(ftt_low.ftt_close(self.d))
    def close_dev(self): 
        return check(ftt_low.ftt_close_dev(self.d))
    def read(self, buf, length): 
        return check(ftt_low.ftt_read(self.d, buf, length), (0,-1))
    def write(self, buf, length): 
        return check(ftt_low.ftt_write(self.d, buf, length))
    def writefm(self): 
        return check(ftt_low.ftt_writefm(self.d))
    def write2fm(self): 
        return check(ftt_low.ftt_write2fm(self.d))
    def retry(self): raise NotImplementedError
    def skip_fm(self, count): 
        return check(ftt_low.ftt_skip_fm(self.d, count))
    def skip_rec(self, count): 
        return check(ftt_low.ftt_skip_rec(self.d, count))
    def skip_to_double_fm(self): 
        return check(ftt_low.ftt_skip_to_double_fm(self.d))
    def rewind(self): 
        return check(ftt_low.ftt_rewind(self.d))
    def retension(self): 
        return check(ftt_low.ftt_retension(self.d))
    def unload(self): 
        return check(ftt_low.ftt_unload(self.d))
    def erase(self): 
        return check(ftt_low.ftt_erase(self.d))
    def set_mode(self, density, compression, blocksize):
        return check(ftt_low.ftt_set_mode(self.d, density, compression, blocksize))
    def get_mode(self): 
        return ftt_low.ftt_get_mode(self.d)
    def avail_mode(self, density, compression, fixed):
        return ftt_low.ftt_avail_mode(self.d, density, compression, fixed)
    def density_to_name(self, density):
        return ftt_low.ftt_density_to_name(self.d, density)
    def name_to_density(self, name):
        return ftt_low.ftt_name_to_density(self.d, name)
    def get_max_blocksize(self): 
        return ftt_low.ftt_get_max_blocksize(self.d)
    def set_data_direction(self, n): 
        return check(ftt_low.ftt_set_data_direction(
        self.d, n))
    def list_all(self):
        return ftt_low.ftt_list_all(self.d)
    def chall(self, uid, gid, mode):
        return check(ftt_low.ftt_chall(self.d, uid, gid, mode))
    def get_mode_dev(self, devname):
        return ftt_low.ftt_get_mode_dev(self.d, devname)
    def set_mode_dev(self, devname, force=0):
        return check(ftt_low.ftt_set_mode_dev(self.d, devname, force))
    def describe_dev(self, devname, file=sys.stdout):
        return check(ftt_low.ftt_describe_dev(self.d, devname, file))
    def get_basename(self):
        return ftt_low.ftt_get_basename(self.d)
    def list_supported(self, file=sys.stdout):
        return check(ftt_low.ftt_list_supported(self.d, file))
    def status(self, timeout=60):
        return ftt_low.ftt_status(self.d, timeout)
    def get_position(self):
        status, file, block = ftt_low.ftt_get_position(self.d)
        if status:
            raise_ftt()
        else:
            return file, block
    def get_stats(self):
        b = stat_buf()
        check(ftt_low.ftt_get_stats(self.d, b.b))
        return b
    
def open(basename, rdonly):
    desc = ftt_low.ftt_open(basename, rdonly)
    #XXX check return!
    return FTT(desc)

def open_logical (basename, os, driveid, rdonly):
    desc = ftt_low.ftt_open_logical(basename, rdonly)
    #XXX check return!
    return FTT(desc)

