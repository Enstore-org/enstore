#!/usr/bin/env python

# $Id$

# Caches DNS information so we don't keep hitting the DNS server by calling socket.gethostname()

# Also implements multiple-ethernet load balancing

#system imports
import os
import stat
import socket
import string
import time

#enstore imports
import access


hostinfo=None

def gethostinfo(verbose=0):
    global hostinfo
    if not hostinfo:
        hostname=socket.gethostname()
        uname = os.uname()[1]
        if hostname != uname:
            if verbose:
                print "Warning:  gethostname returns %s, uname returns %s\n" % (
                    hostname, uname)
        if verbose:
            print "sending DNS request"
        hostinfo=socket.gethostbyaddr(hostname)
    return hostinfo

known_ips = {}
def address_to_name(addr):
    ## this will return the address if it can't be resolved into a hostname
    if addr in known_ips.keys():
        return known_ips[addr]
    try:
        name = socket.gethostbyaddr(addr)[0]
    except socket.error:
        name = addr
    known_ips[addr] = name
    return name
    

known_names = {}
def name_to_address(name):
    if name in known_names.keys():
        return known_names[name]
    try:
        addr = socket.gethostbyname(name)
    except socket.error:
        addr = name
    known_names[name] = addr
    return addr
    
def get_interface_file_name(verbose=0):
    hostname, junk, junk = gethostinfo()
    if '.' in hostname:
        hostname=string.split(hostname,'.')[0]
    filename = "%s.interface.conf"%hostname
    trydirs = []
    for envvar in "ENSTORE_DIR", "ENCP_DIR":
        if envvar in os.environ.keys():
            trydirs.append(os.path.join(os.environ[envvar], "etc"))
    trydirs.append("/etc") # fallback
    for trydir in trydirs:
        trypath=os.path.join(trydir, filename)
        if verbose: print "searching", trydir, "for", filename
        if access.access(trypath, access.R_OK):
            if verbose: print "found it"
            return trypath
    if verbose: print "not found"
    return None
        
        
multi_interface_table=None
last_mtime = 0

def get_multiple_interfaces(verbose=0):
    global multi_interface_table
    global last_mtime

    filename = get_interface_file_name(verbose)

    #see if we previously loaded the file, if so, check the mtime
    if multi_interface_table:
        if verbose: print "table already loaded, checking for file update"
        if filename:
            try:
                mtime=os.stat(filename)[stat.ST_MTIME]
                if mtime == last_mtime:
                    if verbose: print "not changed"
                    return multi_interface_table
            except os.error: # file must have been deleted
                if verbose: print "cannot stat", filename
                multi_interface_table = None
                last_mtime = 0
        else:
             multi_interface_table = None
             last_mtime = 0
            
    table = []

    if filename:
        file = open(filename,'r')
        last_mtime=os.stat(filename)[stat.ST_MTIME]
        if verbose: print "reading", filename
        ##this table will be  a list of tuples,  (IP#.  Relative B/W)
        for line in file.readlines():
            words = string.split(line)
            if not words:
                continue
            if words[0][0]=='#':
                continue
            if len(words)==2:
                ip,bw=words
                table.append((ip,int(bw)))
            else:
                print "Configuration file error: %s %s" % (filename, line)
    if not table:
        if verbose: print "no multiple interface table"
        junk, junk, ips = gethostinfo()
        table = [(ips[0], 1)]
    multi_interface_table = table
    return multi_interface_table

if __name__ == "__main__":
    print gethostinfo(1)
    print gethostinfo(1)
    print get_multiple_interfaces(1)
    print get_multiple_interfaces(1)
    print address_to_name('127.0.0.1')
    print address_to_name('1.1.1.1')
    print address_to_name('131.225.84.156')
    print name_to_address('rip8.fnal.gov')
    while 1:
        print get_multiple_interfaces(1)
        time.sleep(1)
    
    

    
                        
    


