#!/usr/bin/env python

# $Id$

import sys
import string
import time

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
        
def decode(raw_msg):
    timecode = int4(raw_msg[:4])
    code1 = raw_msg[4:7]
    code2 = raw_msg[7:15]
    msg = raw_msg[15:]
    #clean it up
    timetuple = time.localtime(timecode)
    code1=hexify(code1)
    code2=squash_nulls(code2,1)
    msg=squash_nulls(msg)
    
    return timetuple,code1,code2,msg


def dumpfile(logfile):
    data = open(logfile,'r').read()
    ptr=0
    chunksize=256+15
    while ptr<len(data):
        raw_msg=data[ptr:ptr+chunksize]
        timetuple,code1,code2,msg=decode(raw_msg)
        year, month, day, hour, minute, second = timetuple[:6]
        print "%4d/%02d/%02d %02d:%02d:%02d %s %s %s" % (
            year, month, day, hour, minute, second, code1, code2, msg)
        ptr=ptr+chunksize
    
if __name__=="__main__":
    for file in sys.argv[1:]:
        dumpfile(file)
        
