#!/usr/bin/env python
#
# $Id$
#

"""The client-side configuration file will be called /etc/enstore.conf but this can
be overridden with env. var. ENSTORE_CONF"""

import os
import sys
import string
import random
import socket

import Trace
import e_errors
import multiple_interface
import enroute
import runon
import pdb

#UDP_fixed_route = 0

##############################################################################
# The following three functions read in the enstore.conf file.
##############################################################################

def find_config_file():
    config_host = os.environ.get("ENSTORE_CONFIG_HOST", None)
    if config_host:
        filename = '/etc/'+config_host+'.enstore.conf'
    	if not os.path.exists(filename):
            filename = '/etc/enstore.conf'
    else:
        filename = '/etc/enstore.conf'
    filename = os.environ.get("ENSTORE_CONF", filename)
    if os.path.exists(filename):
        return filename
    return None

def read_config_file(filename):
    if not filename:
        return None
    config = {}
    try:
        f = open(filename, 'r')
    except:
        sys.stderr.write("Can't open %s" % (filename,))
        return None
    for line in f.readlines():
        comment = string.find(line, "#")
        if comment >= 0:
            line = line[:comment]
        line = string.strip(line)
        if not line:
            continue
        tokens = string.split(line)
        ntokens = len(tokens)
        first = 1
        for token in tokens:
            eq = string.find(token,'=')
            if eq<=0:
                sys.stderr.write("%s: syntax error, %s"%(filename, token))
                f.close()
                return None
            key, value = token[:eq], token[eq+1:]
            try:
                value=int(value)
            except ValueError:
                try:
                    value = float(value)
                except:
                    pass
            if first:
                first = 0
                if ntokens == 1:
                    config[key] = value
                else:
                    config[key] = config.get(key,{})
                    subdict = {}
                    config[key][value]=subdict
            else:
                subdict[key]=value
    f.close()
    return config

_cached_config = None

def get_config():
    global _cached_config
    if not _cached_config:
        _cached_config = read_config_file(find_config_file())
    return _cached_config

##############################################################################
# The following two functions parse the config dictionary.
##############################################################################

#Returns the 'interface' sub dictionary.
def get_interfaces():
    config = get_config()
    if not config:
        return
    interface_dict = config.get('interface')
    if not interface_dict:
        return
    interfaces = interface_dict.keys()
    if not interfaces:
        return

    return interfaces

#Returns the dictionary, that represents one interface line in entore.conf.
def get_interface_info(interface):
    config = get_config()
    if not config:
        return
    interface_dict = config.get('interface')
    if not interface_dict:
        return

    return interface_dict[interface]

##############################################################################
# The following function selects which CPU to run the process on.
##############################################################################

def runon_cpu(interface):
    config = get_config()
    if not config:
        return
    interface_dict = config.get('interface')
    interface_details = interface_dict[interface]
    cpu = interface_details.get('cpu')
    if cpu is not None:
        err = runon.runon(cpu)
        if err:
            Trace.log(e_errors.ERROR, "runon(%s): failed, err=%s" % (cpu, err))
        else:
            Trace.log(e_errors.INFO, "runon(%s)" % (cpu,))

##############################################################################
# The following two functions manipulate the routing table.
##############################################################################

def set_route(dest, interface_ip):
    config = get_config()
    if not config:
        return

    for interface in get_interfaces():
	if interface_ip == config['interface'][interface]['ip']:
	    break
    else:
	return

    for interface in get_interfaces():
	if dest == config['interface'][interface]['ip']:
	    break
    else:
	return

    err=enroute.routeAdd(dest, interface_ip)
    if err:
        Trace.log(e_errors.ERROR,
		  "set_route(%s, %s) failed"%(dest, interface_ip))

def unset_route(dest):
    config = get_config()
    if not config:
        return

    for interface in get_interfaces():
	if dest == config['interface'][interface]['ip']:
	    break
    else:
	return

    err=enroute.routeDel(dest)
    if err:
        Trace.log(e_errors.ERROR,
		  "unset_route(%s) failed"%(dest,))

##############################################################################
# The following three functions select an interface based on various criteria.
##############################################################################

#Return the hostip, as a string, that appears on the 'hostip=' line in
# the enstore.conf file.
def get_default_interface_ip():
    config = get_config()
    if not config:
        return socket.gethostbyname(socket.gethostname())
    hostip = config.get('hostip', None)
    if hostip:
        return hostip
    else:
        return socket.gethostbyname(socket.gethostname())

def choose_interface(dest=None):
    interfaces = get_interfaces()
    if not interfaces:
        return None
    
    choose = []
    for interface in interfaces:
        weight = get_interface_info(interface).get('weight', 1.0)
        choose.append((-weight, random.random(), interface))
    choose.sort()
    junk, junk, junk, interface = choose[0]
    return get_interface_info(interface)

def check_load_balance(mode = None, dest = None):
    #mode should be 0 or 1 for "read" or "write"
    config = get_config()
    if not config:
        return
    interface_dict = config.get('interface')
    if not interface_dict:
        return
    interfaces = interface_dict.keys()
    if not interfaces:
        return
    #Trace.log(e_errors.INFO, "probing network to select interface")
    rate_dict = multiple_interface.rates(interfaces)
    #Trace.log(e_errors.INFO, "interface rates: %s" % (rate_dict,))
    choose = []
    for interface in interfaces:
        weight = interface_dict[interface].get('weight', 1.0)
        recv_rate, send_rate = rate_dict[interface]
        recv_rate = recv_rate/weight
        send_rate = send_rate/weight
	total_rate = (recv_rate + send_rate)/weight
        if mode==1: #writing
            #If rates are equal on different interfaces, randomize!
            choose.append((send_rate, -weight, random.random(), interface))
	elif mode==0: #reading
	    choose.append((recv_rate, -weight, random.random(), interface))
        else:
            choose.append((total_rate, -weight, random.random(), interface))
    choose.sort()
    junk, junk, junk, interface = choose[0]
    return get_interface_info(interface)

##############################################################################
# The following function sets up an interface for outgoing data.
##############################################################################

def setup_interface(dest, interface_ip):
    #Some architecures (like IRIX) attach a network card to a processor.
    # make sure the process runs on the correct cpu for the interface selected.
    interface_dict = get_config().get('interface')
    for interface in interface_dict.keys():
	if interface_dict[interface]['ip'] == interface_ip:
	    #pass in the interface (ie. eg0, eth0)
	    runon_cpu(interface)

    unset_route(dest)
    set_route(dest, interface_ip)

