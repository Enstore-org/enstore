#!/usr/bin/env python

import os
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
    ifconfig_command = _find_command('ifconfig')
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
    return 0, 0

def stats(interfaces):
    netstat_cmd = _find_command('netstat')
    import multiple_interface
    try:
        _parse = getattr(multiple_interface, "_parse_"+uname())
    except:
        print "Unrecognized platform", uname()
        _parse = _parse_default()
        
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
                if tok and tok[0] == interface:
                    ret[interface] = _parse(tok)
    return ret
    
def rates(interfaces):
    st0, t0 = stats(interfaces), time.time()
    time.sleep(1)
    st1, t1 = stats(interfaces), time.time()
    tdiff = t1-t0
    ret = {}
    for interface in interfaces:
        r0, s0 = st0[interface]
        r1, s1 = st1[interface]
        rrate = (r1-r0)/tdiff
        srate = (s1-s0)/tdiff
        ret[interface] = (rrate, srate)
    return ret

if __name__ == '__main__':
    ifs = interfaces()
    print rates(ifs)
    
