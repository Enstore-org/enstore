#!/usr/bin/env python

# $Id$

# Caches DNS information so we don't keep hitting the DNS server by calling socket.gethostname()

#system imports
import os, sys
import stat
import socket
import string,re
import time


#Enstore imports
import Trace

hostinfo=None

#Only allow connections from these domains
valid_domains = ('131.225', '127.0', '198.124.212', '198.124.213')


####  XXX Get preferred interface from config file if present, else use hostname.

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

def allow(addr):
    Trace.trace(19, "allow: checking address %s" % (addr,))
    if not addr:
        Trace.trace(19, "allow: not allowing %s" % (addr,))
        return 0
    if type(addr) is type(()):
        if len(addr)==2:
            addr = addr[0]
        else:
            raise TypeError
    if addr[0] not in string.digits:
        addr = name_to_address(addr)
    if addr[0] not in string.digits:
        Trace.trace(19, "allow: not allowing %s" % (addr,))
        return 0
    tok = string.split(addr, '.')
    if len(tok) != 4:
        Trace.trace(19, "allow: not allowing %s" % (addr,))
        return 0
    for v in valid_domains:
        vtok = string.split(v, '.')
        if tok[:len(vtok)] == vtok:
            Trace.trace(19, "allow: allowing %s" % (addr,))
            return 1
    Trace.trace(19, "allow: not allowing %s" % (addr,))
    return 0

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
    if not ip:
        return
    if ip[0] not in string.digits:
        ip = name_to_address(ip)
    if not ip:
        return
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
        ip_re = re.compile('([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)')
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
    print address_to_name('127.0.0.1')
    print address_to_name('1.1.1.1')
    print address_to_name('131.225.84.156')
    print name_to_address('rip8.fnal.gov')
    print interface_name('127.0.0.1')
    


    
                        
    


