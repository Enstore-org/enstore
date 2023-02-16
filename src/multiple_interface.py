#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################


import os
import sys
import re
import string
import time

cmds = {}
ifinfo = {}
_uname = ""

def uname():
    global _uname
    if _uname:
        return _uname
    s =string.lower( os.uname()[0])
    _uname = ''
    for c in s:
        if c not in string.letters:
            break
        _uname = _uname + c
    return _uname

def _find_command(cmd):
    if cmds.has_key(cmd):
        return cmds[cmd]

    for testpath in '/sbin', '/usr/sbin','/etc','/usr/etc','/bin','/usr/bin':
        tryit = os.path.join(testpath,cmd)
        if os.access(tryit,os.X_OK):
            cmds[cmd] = tryit
            return tryit
        
    return None

def _getifinfo():
    global ifinfo
    # use command depending on OS type
    ostype, junk, osversion, junk, junk = os.uname()
    if ostype == 'IRIX64' and osversion == '6.2':
	o_s = 'IRIX6.2'
    else:
	o_s = 'other'
    if o_s == 'IRIX6.2':
	ifconfig_command = _find_command('netstat')
	if ifconfig_command:
	   ifconfig_command =  ifconfig_command+' -in'
    else:
	ifconfig_command = _find_command('ifconfig')
	if ifconfig_command:
	   ifconfig_command =  ifconfig_command+' -a'

    if not ifconfig_command:
        return None
    p=os.popen(ifconfig_command,'r')
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


def interface_name(ip):
    if not ifinfo or not ifinfo.has_key(ip):
        _getifinfo()
    return ifinfo.get(ip)

def uniq(l):
    d={}
    for i in l:
        d[i]=0
    k=d.keys()
    k.sort()
    return k

def interfaces():
    if not ifinfo:
        _getifinfo()
    return uniq( ifinfo.values())

#functions to extract from the output of 'netstat -i' the number of packets
# sent and recieved

def _parse_linux(tok):
    return  long(tok[3]), long(tok[7])

def _parse_irix(tok):
    return long(tok[4]), long(tok[6])

def _parse_osf(tok):
    return long(tok[4]), long(tok[6])

def _parse_sunos(tok):
    return long(tok[4]), long(tok[6])

def _parse_default(tok):
    #The variable tok is necessary as part of a standard interface to the
    # _parse_*() functions.
    __pychecker__ = "unusednames=tok"
    
    return 0L, 0L

def stats(interfaces):
    netstat_cmd = _find_command('netstat')
    try:
        #Previously, the correct _parse_*() function was found by doing
        # the following:
        #       import multiple_interface
        #       _parse = getattr(multiple_interface, "_parse_"+uname())
        # This import from within the multiple_interface module was a really
        # bad design.  This new way avoids having to do that.
        _parse = getattr(sys.modules[__name__], "_parse_"+uname())
    except:
        try:
            sys.stderr.write("Unrecognized platform: %s\n" % uname())
            sys.stderr.flush()
        except IOError:
            pass
        _parse = _parse_default

    p = os.popen(netstat_cmd + " -i", 'r')
    data = p.readlines()
    status = p.close()
    if status:
        ret = None
    else:
        ret = {}
        for d in data:
            tok = string.split(d)
            for interface in interfaces:
                #Sometimes we use aliases (eg. eth0:0) get the stats for
                # the real interface.  We get the following "netstat -i"
                # output on SLF 3.0.5 (for the alias interfaces):
                #    eth0:0     1500   0      - no statistics available -  BMRU
                # The "no statistics available" don't parse as the integers
                # expected.  Thus, we split on ":" and only look at the
                # real interface name for statistics.  We continue to use
                # the alias name elsewhere.
                real_interface = interface.split(":")[0]
                
                if tok and tok[0] == real_interface:
                    ret[interface] = _parse(tok)
                elif tok and tok[0][-1] == "*" and tok[0][:-1] == interface:
                    try:
                        sys.stderr.write("Interface %s is down\n" % tok[0][:-1])
                        sys.stderr.flush()
                    except IOError:
                        pass
    return ret
    
def rates(interfaces):
    st0, t0 = stats(interfaces), time.time()
    time.sleep(1)
    st1, t1 = stats(interfaces), time.time()
    tdiff = t1-t0
    ret = {}
    for interface in interfaces:
        try:
            r0, s0 = st0[interface]
            r1, s1 = st1[interface] 
            rrate = (r1-r0)/tdiff
            srate = (s1-s0)/tdiff
            ret[interface] = (rrate, srate)
        except KeyError:
            pass
    return ret

if __name__ == "__main__":   # pragma: no cover
    ifs = interfaces()
    print rates(ifs)
    
