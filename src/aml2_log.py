#! /usr/bin/env python

# $Id$

#system imports
import os
import sys
import types
import ftplib
import stat
import time

#enstore imports
import access

record_size = 15+256

def fetch_log_file(addr, month, day, filename):
    # Use ftp to grab log for "date"  from robot  at "addr"
    # It would be nice to use a "reget" but the OS/2 ftp server on
    # the ADIC robot doesn't suppor this
    ftp_obj = ftplib.FTP(addr,"anonymous")
    ofile = open(filename,'w')
    infile = "log%02d%02d.001" % (day, month)
    ftp_obj.cwd("amu")
    ftp_obj.cwd("logs-trc")
    ftp_obj.retrbinary("RETR "+infile, ofile.write)
    ofile.close()
    ftp_obj.quit()
    
def n_records(filename):
    # how many records in log file
    sbuf = os.stat(filename)
    return sbuf[stat.ST_SIZE]/record_size

def get_record(file, n):
    if type(file) == types.StringType:
        file = open(file,'r')
    # return n'th record from file, as
    # (time,  [list of strings])
    file.seek(n*record_size)
    data = file.read(record_size)
    return decode(data)
    
def format_record(timecode, code1, code2, msg):
    #nice ASCII format
    timetuple = time.localtime(timecode) #XXX check DST
    year, month, day, hour, minute, second = timetuple[:6]
    return "%4d/%02d/%02d %02d:%02d:%02d %s %s %s" % (
        year, month, day, hour, minute, second, code1, code2, msg)
    
def dump_log_file(addr, month, day, output=sys.stdout, update=0):
    prefix="/tmp/adiclog"
    if not os.path.isdir(prefix):
        os.mkdir(prefix)
    filename = os.path.join(prefix, "%s.%02d.%02d.log"%(addr, month, day))
    if update or not access.access(filename, access.R_OK):
        fetch_log_file(addr, month, day, filename)
    f = open(filename, 'r')
    for n in range(n_records(filename)):
        tup = get_record(f, n)
        fmt = apply(format_record, tup)
        output.write(fmt+"\n")
        

###internal support functions    
def decode(raw_msg):
    timecode = int4(raw_msg[:4])
    if time.daylight: timecode = timecode + 3600 ## XXX guesswork!
    code1 = raw_msg[4:7]
    code2 = raw_msg[7:15]
    msg = raw_msg[15:]
    #clean it up
    code1=hexify(code1)
    code2=squash_nulls(code2,1)
    msg=squash_nulls(msg)
    
    return timecode,code1,code2,msg

def hexify(s):
    r='0x'
    for c in map(ord,s):
        h=hex(c)[2:]
        if len(h)==1: h='0'+h
        r=r+h
    return r

def int2(s): #turns 2 byte string into a 16-bit int, assuming lsb first
    return ord(s[1])*256 + ord(s[0])

def int4(s): #turn 4 byte string into 32-bit int, as above
    return 65536L*int2(s[2:4]) + int2(s[:2])

def squash_nulls(s, allow_repeat=0):
    nul=0
    res = ''
    for c in s:
        if c=='\0':
            if allow_repeat or not nul:
                res = res+' '
            nul=1
        else:
            nul=0
            res = res+c
    return res
        

    
if __name__ == "__main__":
    y,m,d = time.localtime(time.time())[:3]
    dump_log_file("adic2.fnal.gov", m, d)
    
