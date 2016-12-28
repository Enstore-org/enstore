#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# Caches DNS information so we don't keep hitting the DNS server by calling
# socket.gethostname()

#system imports
import os, sys
import socket
import string, re
import types
import errno
import time

#Enstore imports
import Trace
import Interfaces


#Return true if the string is an ipv4 dotted-decimal address.
def is_ip(ip):
    if type(ip) != type(""):
        raise TypeError, "Expected string type, not %s." % type(ip)

    if re.match("[0-9]{1,3}(\.[0-9]{1,3}){0,3}", ip):
        return 1

    return 0

####  XXX Get preferred interface from config file if present,
####      else use hostname.

hostinfo = None
def gethostinfo(verbose=0):
    __pychecker__="unusednames=verbose"  #Some modules still pass verbose...

    global hostinfo
    if not hostinfo:
        hostname = socket.gethostname()
        uname = os.uname()[1]
        if hostname != uname:
            message = "Warning:  gethostname returns %s, uname returns %s\n" \
                      % (hostname, uname)
            try:
                sys.stderr.write(message)
                sys.stderr.flush()
            except IOError:
                pass
        #hostinfo=socket.gethostbyname_ex(hostname)
        # For compatibility with IPV6 use socket.getaddrinfo
        hostinfo1 = socket.getaddrinfo(hostname, socket.AF_INET)
        # hostinfo1 is like:
        # [(2, 1, 6, '', ('131.225.13.15', 2)),
        # (2, 2, 17, '', ('131.225.13.15', 2)),
        # (2, 3, 0, '', ('131.225.13.15', 2))]
        #The following if is necessary for nodes (probably laptops) that
        # have 'problematic' /etc/hosts files.  This is because they contain
        # lines in their /etc/hosts file that look like:
        #  127.0.0.1  sleet.dhcp.fnal.gov sleet localhost.localdomain localhost
        # The ip address of 127.0.0.1 is 'wrong' for the sleet.dhcp hostname
        # and the sleet alias.

        # For compatibility with earlier implementation convert hostinfo1
        # to presentation retrned by socket.gethostbyname_ex(hostname)
        hostinfo = [hostname, [hostname.split('.')[0]], [hostinfo1[0][4][0]]]
        if hostinfo[2] == ["127.0.0.1"]:
            intf_ips = []
            for intf_dict in Interfaces.interfacesGet().values():
                intf_ips.append(intf_dict['ip'])
            hostinfo = (hostinfo[0], hostinfo[1], intf_ips)
    return hostinfo

#Return the domain name of the current network.
def getdomainname():
    fqdn = socket.getfqdn()
    words = fqdn.split(".")
    if len(words) >= 3:
        return string.join(words[1:], ".")

    return None

#Return the domain address of the current network.
def getdomainaddr():
    host_info = socket.getaddrinfo(socket.gethostname(), None)
    if host_info:
        ip = host_info[0][4][0]
        words = ip.split(".")
        if len(words) == 4:
            first_byte = int(words[0])
            if 0 >= first_byte and first_byte < 128:
                #Class A network.  (8 bits network, 24 bits host)
                return string.join(words[0:1], ".")
            elif 128 <= first_byte and first_byte < 192:
                #Class B network.  (16 bits network, 16 bits host)
                return string.join(words[0:2], ".")
            elif 192 <= first_byte and first_byte < 224:
                #Class C network.  (24 bits network, 8 bits host)
                return string.join(words[0:3], ".")

    return None

def __my_gethostbyaddr(addr):
    try_count = 0
    while try_count < 60:
        try:
            rc = socket.gethostbyaddr(addr)
            Trace.trace(19, "__my_gethostbyaddr: socket.gethostbyaddr returned %s"%(rc,))
            return rc
        except socket.error, msg:
            #One known way to get here is to run out of file
            # descriptors.  I'm sure there are others.
            this_errno = msg.args[0]
            Trace.trace(19, "__my_gethostbyaddr: socket.error %s"%(msg,))
            if this_errno == errno.EAGAIN or this_errno == errno.EINTR:
                try_count = try_count + 1
                time.sleep(1)
            else:
                break
        except (socket.gaierror, socket.herror), msg:
            this_herrno = msg.args[0]
            Trace.trace(19, "__my_gethostbyaddr: exception %s"%(this_herrno,))
            if this_herrno == socket.EAI_AGAIN:
                try_count = try_count + 1
                time.sleep(1)
            else:
                break
        except Exception as e:
            Trace.trace(19, "__my_gethostbyaddr: Exception %s"%(str(e),))
            break
    return None

def __my_gethostbyname(name):
    try_count = 0
    addr = None
    while try_count < 60:
        try:
	    hostinfo = socket.getaddrinfo(name, None)
            #return socket.gethostbyname(name)
	    addr = hostinfo[0][4][0]
	    break
        except socket.error, msg:
            #One known way to get here is to run out of file
            # descriptors.  I'm sure there are others.
            this_errno = msg.args[0]
            if this_errno == errno.EAGAIN or this_errno == errno.EINTR:
                try_count = try_count + 1
                time.sleep(1)
            else:
                return None
        except (socket.gaierror, socket.herror), msg:
            this_herrno = msg.args[0]
            if this_herrno == socket.EAI_AGAIN:
                try_count = try_count + 1
                time.sleep(1)
            else:
                return None
    return addr

known_ips = {}
def address_to_name(addr):
    ## this will return the address if it can't be resolved into a hostname
    if addr in known_ips.keys():
        return known_ips[addr]

    host_info = __my_gethostbyaddr(addr)
    if host_info != None:
        name = host_info[0]
    else:
        name = addr

    known_ips[addr] = name
    return name


known_names = {}
def name_to_address(name):
    ## this will return the hostname if it can't be resolved into an address
    if name in known_names.keys():
        return known_names[name]

    addr = __my_gethostbyname(name)
    if addr == None:
        addr = name

    known_names[name] = addr
    return addr


known_domains = { 'invalid_domains' : {},
                  'valid_domains' : {'default' : [getdomainaddr(),
                                                  "127.0.0"] } }
#This needs to be called by all servers (done in generic_server.py).  Also,
# all long lived clients that need to care about multiple systems do to.
# Short lived clients (that only care about the default Enstore system)
# will have this information automaticaly pulled down.
#
#Note: The configuration_server is different from the other servers.
# It needs to call this function directly.
def update_domains(csc_or_dict):
    Trace.trace(19, "update_domains parameter: %s" % (csc_or_dict,))
    global known_domains

    #Determine the source.  The dict argument type is necessary for the
    # configuration server since it can't create a csc to itself.
    if type(csc_or_dict) == types.InstanceType:
        domains = csc_or_dict.get('domains', 3, 3)
        system_name = csc_or_dict.get_enstore_system(3, 3)
    elif type(csc_or_dict) == types.DictType:
        domains = csc_or_dict
        system_name = domains.get('system_name', "default2")
    else:
        return

    valid_domains = domains.get('valid_domains', [])
    invalid_domains = domains.get('invalid_domains', [])

    known_domains['valid_domains'][system_name] = valid_domains
    known_domains['invalid_domains'][system_name] = invalid_domains

    Trace.trace(19, "valid_domains: %s" % known_domains['valid_domains'])
    Trace.trace(19, "invalid_domains: %s" % known_domains['invalid_domains'])

#Return None if no matching rule is explicity found.  Return True if this
# is a valid address and False if it is not.
def _allow(addr):
    # always allow requests from local host
    if socket.getfqdn() == socket.gethostbyaddr(addr)[0]:
        return 1
    valid_domains_dict = known_domains.get('valid_domains', {})
    invalid_domains_dict = known_domains.get('invalid_domains', {})

    hostinfo = socket.getaddrinfo(addr, None)
    address_family = hostinfo[0][0]
    #Get the address.
    if address_family == socket.AF_INET6:
        try:
            tok = string.split(addr, ':')
        except (AttributeError):
            tok = ""
        if len(tok) == 0:
            Trace.trace(19, "allow: not allowing %s" % (addr,))
            return 0
    else:  # IPV4
        try:
            tok = string.split(addr, '.')
        except (AttributeError):
            tok = ""
        if len(tok) != 4:
            Trace.trace(19, "allow: not allowing %s" % (addr,))
            return 0
    #Return false if the ip is in a domain we are not allowed to reply to.
    for invalid_domains in invalid_domains_dict.values():
        for v in invalid_domains:
            try:
                if address_family == socket.AF_INET6:
                    vtok = string.split(v, ':')
                else:
                    vtok = string.split(v, '.')
            except (AttributeError):
                continue
            if tok[:len(vtok)] == vtok:
                Trace.trace(19, "allow: not allowing %s" % (addr,))
                return 0

    #Return true if the ip is in a domain we are allowed to reply to.
    for valid_domains in valid_domains_dict.values():
        for v in valid_domains:
            try:
                if address_family == socket.AF_INET6:
                    vtok = string.split(v, ':')
                else:
                    vtok = string.split(v, '.')
            except (AttributeError):
                continue
            if tok[:len(vtok)] == vtok:
                Trace.trace(19, "allow: allowing %s" % (addr,))
                return 1

    Trace.trace(19, "allow: not allowing %s" % (addr,))
    return None

def allow(addr):
    Trace.trace(19, "allow: checking address %s %s" % (addr, len(addr)))
    client_addr = list(addr)
    # If message comes with IPV4 address on IPV6 configured receiver its format is like:
    # '::ffff:193.109.174.113'.
    # Single out IPV4 address.
    a_list = addr[0].split(':')
    le = a_list[-1]
    if len(le.split('.')) == 4:
        client_addr = [le, addr[1]] # IPV4 over IPV6 connection
    #Check if the address is of a valid type.  The two valid types are
    # a string (of either the hostname or ip) or a 2-tuple with a string
    # as the first item (tha has the hostname or ip).
    hostinfo = socket.getaddrinfo(client_addr[0], None)
    Trace.trace(19, "allow: hostinfo %s" % (hostinfo))

    address_family = hostinfo[0][0]
    if type(addr) is type(()):
        if len(client_addr)==2:
            addr = client_addr[0]
        elif address_family == socket.AF_INET6:
            addr = hostinfo[0][4][0]
        else:
            raise TypeError, "Tuple addr has wrong length %s." % len(addr)
    if type(addr) != type(""):
        raise TypeError, "Variable addr is of type %s." % type(addr)

    #If we do not have anything to test return false.
    if not addr:
        Trace.trace(19, "allow: not allowing %s" % (addr,))
        return 0

    #If the address is not in dotted-decimal form, convert it.
    try:
        if not is_ip(addr):
            addr = name_to_address(addr)
    except IndexError:
        Trace.trace(19, "allow: not allowing %s" % (addr,))
        return 0
    #Call the helper _allow() function that test the address against what is
    # in known_domains.
    result = _allow(addr)
    return result


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
    lh = '127.0.0.1'  #lh = LocalHost

    print gethostinfo()
    print lh, "->", address_to_name(lh), "->", interface_name(lh)
    for ip in gethostinfo()[2]:
        print ip, "->", address_to_name(ip), "->", interface_name(ip)
    print gethostinfo()[0], "->", name_to_address(gethostinfo()[0])
    for level in range(1,20):
        Trace.print_levels[level]=1
    print __my_gethostbyaddr('')
    print __my_gethostbyaddr('aaa')








