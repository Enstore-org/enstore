#!/usr/bin/env python

# $Id$

# Caches DNS information so we don't keep hitting the DNS server by calling socket.gethostname()

# Also implements multiple-ethernet load balancing

#system imports
import os
import socket
import string

#enstore imports
import access


hostinfo=None

def gethostinfo(verbose=0):
    global hostinfo
    if not hostinfo:
        hostname=socket.gethostname()
        uname = os.uname()[1]
        if hostname != uname:
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
    

def get_interface_file_name():
    hostname, junk, junk = gethostinfo()
    filename = "%s.interface.conf"%hostname
    trydirs = []
    for envvar in "ENSTORE_DIR", "ENCP_DIR":
        if envvar in os.environ.keys():
            trydirs.append(os.path.join(os.environ[envvar], "etc"))
    trydirs.append("/etc") # fallback
    for trydir in trydirs:
        trypath=os.path.join(trydir, filename)
        if access.access(trypath, access.R_OK):
            return trypath
    return None
        
        
multi_interface_table=None
        
def get_multiple_interfaces(verbose=0):
    global multi_interface_table
    if multi_interface_table:
        return multi_interface_table

    table = []
    filename = get_interface_file_name()

    if filename:
        file = open(filename,'r')

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
        junk, junk, ips = gethostinfo()
        table = (ips[0], 1)
    multiple_interface_table = table
    return multiple_interface_table

if __name__ == "__main__":
    print gethostinfo(1)
    print gethostinfo(1)
    print get_multiple_interfaces()
    print get_multiple_interfaces()
    print address_to_name('127.0.0.1')
    print address_to_name('1.1.1.1')
    print address_to_name('131.225.84.156')

    
    

    
                        
    


