#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# Caches DNS information, so we don't keep hitting the DNS server by calling
# socket.gethostname()

import errno
# system imports
import os
import re
import socket
import string
import sys
import time
import types

import Interfaces

# Enstore imports
import Trace
import e_errors


# Return true if the string is an ipv4 dotted-decimal address.
def is_ip(check_ip):
    if not isinstance(check_ip, type("")):
        raise TypeError("Expected string type, not %s." % type(check_ip))

    if re.match(r"[0-9]{1,3}(\.[0-9]{1,3}){0,3}", check_ip):
        return 1

    return 0


####  XXX Get preferred interface from config file if present,
####      else use hostname.

GL_hostinfo = None


def gethostinfo(_=None):
    global GL_hostinfo
    if not GL_hostinfo:
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
        # hostinfo=socket.gethostbyname_ex(hostname)
        # For compatibility with IPV6 use socket.getaddrinfo
        hostinfo1 = socket.getaddrinfo(hostname, socket.AF_INET)
        # hostinfo1 is like:
        # [(2, 1, 6, '', ('131.225.13.15', 2)),
        # (2, 2, 17, '', ('131.225.13.15', 2)),
        # (2, 3, 0, '', ('131.225.13.15', 2))]
        # The following if is necessary for nodes (probably laptops) that
        # have 'problematic' /etc/hosts files.  This is because they contain
        # lines in their /etc/hosts file that look like:
        #  127.0.0.1  sleet.dhcp.fnal.gov sleet localhost.localdomain localhost
        # The ip address of 127.0.0.1 is 'wrong' for the sleet.dhcp hostname
        # and the sleet alias.

        # For compatibility with earlier implementation convert hostinfo1
        # to presentation returned by socket.gethostbyname_ex(hostname)
        GL_hostinfo = [hostname, [hostname.split('.')[0]], [hostinfo1[0][4][0]]]
        if GL_hostinfo[2] == ["127.0.0.1"]:
            intf_ips = []
            for intf_dict in Interfaces.interfacesGet().values():
                intf_ips.append(intf_dict['ip'])
            GL_hostinfo = (GL_hostinfo[0], GL_hostinfo[1], intf_ips)
    return GL_hostinfo


# Return the domain name of the current network.
def getdomainname():
    fqdn = socket.getfqdn()
    words = fqdn.split(".")
    if len(words) >= 3:
        return string.join(words[1:], ".")

    return None


def _getdomainaddr(host_info):
    """
    Return the domain address of the current network.
    For dual IP configuration the list of addresses may be returned by socket.gethostname(), like:
    [(10, 1, 6, '', ('2620:6a:0:8421::96', 0, 0, 0)),
    (10, 2, 17, '', ('2620:6a:0:8421::96', 0, 0, 0)),
    (10, 3, 0, '', ('2620:6a:0:8421::96', 0, 0, 0)),
    (2, 1, 6, '', ('131.225.191.96', 0)),
    (2, 2, 17, '', ('131.225.191.96', 0)),
    (2, 3, 0, '', ('131.225.191.96', 0))]
    This is to return domain name for a single entry.
    """

    address_family = host_info[0]
    rc = None
    host_ip = host_info[4][0]
    if address_family == socket.AF_INET6:
        rc = ':'.join(host_ip.split(':')[:3])
    else:
        words = host_ip.split(".")
        if len(words) == 4:
            first_byte = int(words[0])
            if 0 <= first_byte < 128:
                # Class A network.  (8 bits network, 24 bits host)
                rc = '.'.join(words[:1])
            elif 128 <= first_byte < 192:
                # Class B network.  (16 bits network, 16 bits host)
                rc = '.'.join(words[:2])
            elif 192 <= first_byte < 224:
                # Class C network.  (24 bits network, 8 bits host)
                rc = '.'.join(words[:3])
    return rc


# Return the domain address of the current network.
def getdomainaddr():
    host_info = socket.getaddrinfo(socket.gethostname(), None)
    rc = None
    if host_info:
        if isinstance(host_info, list):
            rc = []
            for el in host_info:
                da = _getdomainaddr(el)
                if da and da not in rc:
                    rc.append(da)
        else:
            rc = _getdomainaddr(host_info)
    return rc


def __my_gethostbyaddr(addr):
    try_count = 0
    while try_count < 60:
        try:
            rc = socket.gethostbyaddr(addr)
            Trace.trace(19, "__my_gethostbyaddr: socket.gethostbyaddr returned %s" % (rc,))
            return rc
        except socket.error, msg:
            # One known way to get here is to run out of file
            # descriptors.  I'm sure there are others.
            this_errno = msg.args[0]
            Trace.trace(19, "__my_gethostbyaddr: socket.error %s" % (msg,))
            if this_errno == errno.EAGAIN or this_errno == errno.EINTR:
                try_count = try_count + 1
                time.sleep(1)
            else:
                break
        except (socket.gaierror, socket.herror), msg:
            this_herrno = msg.args[0]
            Trace.trace(19, "__my_gethostbyaddr: exception %s" % (this_herrno,))
            if this_herrno == socket.EAI_AGAIN:
                try_count = try_count + 1
                time.sleep(1)
            else:
                break
        except Exception as e:
            Trace.trace(19, "__my_gethostbyaddr: Exception %s" % (str(e),))
            break
    return None


def __my_gethostbyname(name):
    try_count = 0
    addr = None
    while try_count < 60:
        try:
            host_info = socket.getaddrinfo(name, None)
            # return socket.gethostbyname(name)
            addr = host_info[0][4][0]
            break
        except socket.error, msg:
            # One known way to get here is to run out of file
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


GL_known_ips = {}


def address_to_name(addr):
    ## this will return the address if it can't be resolved into a hostname
    global GL_known_ips
    if addr in GL_known_ips.keys():
        return GL_known_ips[addr]

    host_info = __my_gethostbyaddr(addr)
    if host_info is not None:
        name = host_info[0]
    else:
        name = addr

    GL_known_ips[addr] = name
    return name


GL_known_names = {}


def name_to_address(name):
    ## this will return the hostname if it can't be resolved into an address
    global GL_known_names
    if name in GL_known_names.keys():
        return GL_known_names[name]

    addr = __my_gethostbyname(name)
    if addr is None:
        addr = name

    GL_known_names[name] = addr
    return addr


GL_domains = getdomainaddr()
if isinstance(GL_domains, list):
    GL_domains.append('127.0.0')
else:
    GL_domains = [GL_domains, '127.0.0']
GL_known_domains = {'invalid_domains': {},
                    'valid_domains': {'default': GL_domains}}


# This needs to be called by all servers (done in generic_server.py).  Also,
# all long-lived clients that need to care about multiple systems do to.
# Short-lived clients (that only care about the default Enstore system)
# will have this information automatically pulled down.
#
# Note: The configuration_server is different from the other servers.
# It needs to call this function directly.
def update_domains(csc_or_dict):
    Trace.trace(19, "update_domains parameter: %s" % (csc_or_dict,))
    global GL_known_domains

    # Determine the source.  The dict argument type is necessary for the
    # configuration server since it can't create a csc to itself.
    if isinstance(csc_or_dict, types.InstanceType):
        dict_domains = csc_or_dict.get('domains', 3, 3)
        system_name = csc_or_dict.get_enstore_system(3, 3)
    elif isinstance(csc_or_dict, types.DictType):
        dict_domains = csc_or_dict
        system_name = dict_domains.get('system_name', "default2")
    else:
        return

    valid_domains = dict_domains.get('valid_domains', [])
    invalid_domains = dict_domains.get('invalid_domains', [])

    GL_known_domains['valid_domains'][system_name] = valid_domains
    GL_known_domains['invalid_domains'][system_name] = invalid_domains

    Trace.trace(19, "valid_domains: %s" % GL_known_domains['valid_domains'])
    Trace.trace(19, "invalid_domains: %s" % GL_known_domains['invalid_domains'])


# Return None if no matching rule is explicitly found.  Return True if this
# is a valid address and False if it is not.
GL_host_name = None
GL_localhost_addresses = {}


def _allow(addr):
    global GL_host_name
    global GL_localhost_addresses
    global GL_known_domains
    if not GL_host_name:
        try:
            GL_host_name = socket.getfqdn()
        except Exception as e:
            Trace.log(e_errors.ERROR, '_allow: getfqdn failed: %s' % (e,))
            return 0
    # always allow requests from local host
    try:
        if addr not in GL_localhost_addresses:
            GL_localhost_addresses[addr] = (GL_host_name == socket.gethostbyaddr(addr)[0])
        if GL_localhost_addresses[addr]:
            return 1
    except Exception as e:
        Trace.log(e_errors.ERROR, '_allow: gethostbyaddr failed for %s: %s' % (addr, e,))
        return 0
    valid_domains_dict = GL_known_domains.get('valid_domains', {})
    invalid_domains_dict = GL_known_domains.get('invalid_domains', {})

    try:
        host_info = socket.getaddrinfo(addr, None)
    except Exception as e:
        Trace.log(e_errors.ERROR, '_allow: gettaddrinfo failed for %s: %s' % (addr, e,))
        return 0

    address_family = host_info[0][0]
    # Get the address.
    if address_family == socket.AF_INET6:
        try:
            tok = string.split(addr, ':')
        except AttributeError:
            tok = ""
        if len(tok) == 0:
            Trace.trace(19, "allow: not allowing 1 %s" % (addr,))
            return 0
    else:  # IPV4
        try:
            tok = string.split(addr, '.')
        except AttributeError:
            tok = ""
        if len(tok) != 4:
            Trace.trace(19, "allow: not allowing 2 %s" % (addr,))
            return 0
    # Return false if the ip is in a domain we are not allowed to reply to.
    Trace.trace(19, 'Invalid domains: %s' % (invalid_domains_dict,))
    for invalid_domains in invalid_domains_dict.values():
        for v in invalid_domains:
            try:
                if address_family == socket.AF_INET6:
                    vtok = string.split(v, ':')
                else:
                    vtok = string.split(v, '.')
            except AttributeError:
                Trace.log(19, 'allow: AttributeError')
                continue
            if tok[:len(vtok)] == vtok:
                Trace.log(e_errors.INFO, "allow: in invalid domains, not allowing %s" % (addr,))
                return 0

    # Return true if the ip is in a domain we are allowed to reply to.
    Trace.trace(19, 'Valid domains: %s' % (valid_domains_dict,))
    for valid_domains in valid_domains_dict.values():
        for v in valid_domains:
            try:
                if address_family == socket.AF_INET6:
                    vtok = string.split(v, ':')
                else:
                    vtok = string.split(v, '.')
            except AttributeError:
                continue
            if tok[:len(vtok)] == vtok:
                Trace.trace(19, "allow: allowing %s" % (addr,))
                return 1

    Trace.trace(19, "allow: not allowing 3 %s" % (addr,))
    return None


def allow(addr):
    Trace.trace(19, "allow: checking address %s %s" % (addr, len(addr)))
    client_addr = list(addr)
    # If message comes with IPV4 address on IPV6 configured receiver its format is like:
    # '::fff:193.109.174.113'.
    # Single out IPV4 address.
    a_list = addr[0].split(':')
    le = a_list[-1]
    if len(le.split('.')) == 4:
        client_addr = [le, addr[1]]  # IPV4 over IPV6 connection
    # Check if the address is of a valid type.  The two valid types are
    # a string (of either the hostname or ip) or a 2-tuple with a string
    # as the first item (tha has the hostname or ip).
    host_info = socket.getaddrinfo(client_addr[0], None)
    Trace.trace(19, "allow: host_info %s" % host_info)

    address_family = host_info[0][0]
    if isinstance(addr, type(())):
        if len(client_addr) == 2:
            addr = client_addr[0]
        elif address_family == socket.AF_INET6:
            addr = host_info[0][4][0]
        else:
            raise TypeError("Tuple addr has wrong length %s." % len(addr))
    if not isinstance(addr, type("")):
        raise TypeError("Variable addr is of type %s." % type(addr))

    # If we do not have anything to test return false.
    if not addr:
        Trace.trace(19, "allow: not allowing 4 %s" % (addr,))
        return 0

    # If the address is not in dotted-decimal form, convert it.
    try:
        if not is_ip(addr):
            addr = name_to_address(addr)
    except IndexError:
        Trace.trace(19, "allow: not allowing 5 %s" % (addr,))
        return 0
    # Call the helper _allow() function that test the address against what is
    # in GL_known_domains.
    result = _allow(addr)
    return result


GL_ifconfig_command = None
GL_ifinfo = {}


def find_ifconfig_command():
    global GL_ifconfig_command
    if GL_ifconfig_command:
        return GL_ifconfig_command
    for testpath in '/sbin', '/usr/sbin', '/etc', '/usr/etc', '/bin', '/usr/bin':
        tryit = os.path.join(testpath, 'ifconfig')
        if os.access(tryit, os.X_OK):
            GL_ifconfig_command = tryit
            return GL_ifconfig_command
    return None


def interface_name(arg_ip):
    global GL_ifconfig_command
    global GL_ifinfo
    if not arg_ip:
        return
    if arg_ip[0] not in string.digits:
        arg_ip = name_to_address(arg_ip)
    if not arg_ip:
        return
    if not GL_ifinfo or arg_ip not in GL_ifinfo:
        find_ifconfig_command()
        if not GL_ifconfig_command:
            return None
        p = os.popen(GL_ifconfig_command + ' -a', 'r')
        text = p.readlines()
        status = p.close()
        if status:
            return None
        interface = None

        # a regular expression to match an IP address in dotted-quad format
        ip_re = re.compile(r'([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)')
        for line in text:
            if not line:
                interface = None
                continue
            tokens = string.split(line)
            if not tokens:
                interface = None
                continue
            if line[0] not in ' \t':
                interface = tokens[0]
                tokens = tokens[1:]
                if interface[-1] == ':':
                    interface = interface[:-1]
            for tok in tokens:
                match = ip_re.search(tok)
                if match:
                    GL_ifinfo[match.group(1)] = interface

    return GL_ifinfo.get(arg_ip)


if __name__ == "__main__":  # pragma: no cover
    lh = '127.0.0.1'  # lh = LocalHost

    print gethostinfo()
    print lh, "->", address_to_name(lh), "->", interface_name(lh)
    for ip in gethostinfo()[2]:
        print ip, "->", address_to_name(ip), "->", interface_name(ip)
    print gethostinfo()[0], "->", name_to_address(gethostinfo()[0])
    for level in range(1, 20):
        Trace.print_levels[level] = 1
    print __my_gethostbyaddr('')
    print __my_gethostbyaddr('aaa')
