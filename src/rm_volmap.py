#!/usr/bin/env python
# $Id$
import sys
import os
import string
import time
import getopt

OK = 0
FAIL = 1

def Print(msg,output):
    output.write(msg)
    print msg[:-1]

def CleanJunk(msg):
    table='                                 !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~                                                                                                                                 '
    return string.strip(string.translate(msg,table," \"'{}"))

def generate_volume_list():
    cwd=os.getcwd()
    p = os.popen('ls', 'r')
    r = p.readlines()
    ret = []
    for line in r:
        l = os.path.join(cwd,line[:-1])
        ret.append(l)
    return ret

def generate_file_list(cwd):
    p = os.popen('ls %s'%(cwd,), 'r')
    r = p.readlines()
    ret = []
    for line in r:
        ret.append(line[:-1])
    return ret


def writelayer(layer,value, filepath=None):
    if filepath:
        (directory, file) = os.path.split(filepath)
    else:
        return
    fname = os.path.join(directory, ".(use)(%s)(%s)"%(layer, file))

    #If the value isn't a string, make it one.
    if type(value)!=type(''):
        value=str(value)

    f = open(fname,'w')
    f.write(value)
    f.close()

def rm(filename=None):
    if not filename:
        return

    writelayer(4,"", filename)
    writelayer(1,"", filename)
    writelayer(2,"", filename)
    writelayer(3,"", filename)

    # It would be better to move the file to some trash space.
    # I don't know how right now.
    os.remove(filename)

def readlayer(fullname,layer,ferr):
    (fdir,fname)=os.path.split(fullname)
    fname = "%s/.(use)(%s)(%s)"%(fdir,layer,fname)
    try:
        f = open(fname,'r')
        l = f.readlines()
        f.close()
        return (l,OK)
    except :
        exc, msg, tb = sys.exc_info()
        Print( "Error ERL: Can't read file '%s', errors : exc='%s' msg='%s'\n" %
                     (fname,str(exc),str(msg)),ferr )
        l = []
        return (l,FAIL)
  

if __name__ == "__main__":   # pragma: no cover
    volume_list=generate_volume_list()
    for vol in volume_list:
        print vol
        files=generate_file_list(vol)
        os.chdir(vol)
        for file in files:
            print file
            rm(file)
        os.chdir('..')
        print "removing", vol
        os.rmdir(vol)
        
            
