#!/usr/bin/env python

# $Id$

import sys
import string


def hexify(s):
    r='0x'
    for c in map(ord,s):
        h=hex(c)[2:]
        if len(h)==1: h='0'+h
        r=r+h
    return r

def decode(raw_msg):
    time = raw_msg[:5]
    disc = raw_msg[5:7]
    code = raw_msg[7:15]
    msg = raw_msg[15:]
    #clean it up
    time2=hexify(time)
    disc2=hexify(disc)
    code2=''
    for c in code:
        if c=='\0':
            code2=code2+' '
        else: code2=code2+c
    msg2=''
    nul=0
    for c in msg:
        if c=='\0':
            if not nul:
                msg2=msg2+' '
            nul=1
        else:
            nul=0
            msg2=msg2+c
            
    return time2,disc2,code2,msg2


def dumpfile(logfile):
    data = open(logfile,'r').read()
    ptr=0
    chunksize=256+15
    while ptr<len(data):
        raw_msg=data[ptr:ptr+chunksize]
        time,disc,code,msg=decode(raw_msg)
        print time,disc,code,msg
        ptr=ptr+chunksize
        count=count+1
    
if __name__=="__main__":
    for file in sys.argv[1:]:
        dumpfile(file)
        
