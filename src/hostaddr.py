#!/usr/bin/env python

# $Id$

# Caches DNS information so we don't keep hitting the DNS server by calling socket.gethostname()

# Also implements multiple-ethernet load balancing

#system imports
import os, sys
import stat
import socket
import string,re
import time


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
    if verbose: sys.stdout.flush()
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
    filename = "%s.interface.conf"%(hostname,)
    trydirs = []
    for envvar in "ENSTORE_DIR", "ENCP_DIR":
        if envvar in os.environ.keys():
            trydirs.append(os.environ[envvar])
            trydirs.append(os.path.join(os.environ[envvar], "etc"))
    trydirs.append("/etc") # fallback
    for trydir in trydirs:
        trypath=os.path.join(trydir, filename)
        if verbose: print "searching", trydir, "for", filename
        if os.access(trypath, os.R_OK):
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
    if verbose:
        sys.stdout.flush()
    return multi_interface_table

ifconfig_command=None
ifinfo={}
def find_ifconfig_command():
    global ifconfig_command
    if ifconfig_command:
        return ifconfig_command
    for testpath in '/sbin', '/usr/sbin','/etc','/usr/etc','/bin','/usr/bin':
        tryit = os.path.join(testpath,'ifconfig')
        if os.access(tryit,os.X_OK):
            ifconfig_command=tryit
            return ifconfig_command
    return None
        
def interface_name(ip):
    if not ifinfo or not ifinfo.has_key(ip):
        find_ifconfig_command()
        if not ifconfig_command:
            return None
        p=os.popen(ifconfig_command+' -a','r')
        text=p.readlines()
        status=p.close()
        if status:
            return None
        interface=None

        #a regular expression to match an IP address in dotted-quad format
        ip_re = re.compile(r'([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')
        for line in text:
            if not line:
                interface=None
                continue
            tokens=string.split(line)
            if not tokens:
                interface=None
                continue
            if line[0] not in ' \t':
                interface=tokens[0]
                tokens=tokens[1:]
                if interface[-1]==':':
                    interface=interface[:-1]
            for tok in tokens:
                match=ip_re.search(tok)
                if match:
                    ifinfo[match.group(1)]=interface
                
    return ifinfo.get(ip)
    
    
    
if __name__ == "__main__":
    print gethostinfo(1)
    print gethostinfo(1)
    print get_multiple_interfaces(1)
    print get_multiple_interfaces(1)
    print address_to_name('127.0.0.1')
    print address_to_name('1.1.1.1')
    print address_to_name('131.225.84.156')
    print name_to_address('rip8.fnal.gov')
    print interface_name('127.0.0.1')
    while 1:
        print get_multiple_interfaces(1)
        time.sleep(1)
    


    
                        
    


