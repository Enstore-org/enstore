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
import pprint
import re
import errno
import time

import Trace
#import e_errors
import multiple_interface
import enroute
import runon
#import pdb

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
    #Make sure that the specified file exists and is a regular file.
    #Note: If the $ENSTORE_CONF environmental variable specifies a file
    # that exists and is not an actual enstore.conf file, there will be
    # some serious problems later on.
    if os.path.exists(filename) and os.path.isfile(filename):
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
                    subdict = {key:value}
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

#Force an update of the cached version of the enstore.conf file.
def update_cached_config():
    global _cached_config
    _cached_config = read_config_file(find_config_file())
    return _cached_config

##############################################################################
# The following function determines the default ip.
##############################################################################

#Return the hostip, as a string, that appears on the 'hostip=' line in
# the enstore.conf file.
def get_default_interface_ip():
    __pychecker__ = "unusednames=i"

    hostip = ""
    msg = None
    default = None

    #Try to determine the detfault ip to use for the local connection.
    # The minute loop is necessary when the DNS server is rebooted.
    for i in range(0, 60):
        try:
            default=socket.gethostbyname(socket.getfqdn(socket.gethostname()))
            break
        except socket.error, msg:
            time.sleep(1)
            continue

    #If an error occured for the entire minute print to screen if specified.
    if msg and not hostip:    
        Trace.trace(10, str(msg))

    #Determine if the user specified the default in the /etc/enstore.conf file
    # or just use the system default.
    config = get_config()
    if not config:
        return default
    hostip = config.get('hostip', None)
    if hostip:
        return hostip
    else:
        return default

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
        return {'ip':get_default_interface_ip(), 'interface':interface}
    interface_dict = config.get('interface')
    if not interface_dict:
        return {'ip':get_default_interface_ip(), 'interface':interface}

    return interface_dict[interface]

#Returns the dictionary, that represents one interface line in entore.conf.
def get_interface_info_by_ip(interface_ip):
    config = get_config()
    if not config:
        return {'ip':interface_ip}
    interface_dict = config.get('interface')
    if not interface_dict:
        return {'ip':interface_ip}

    for interface in interface_dict.keys():
        if interface_dict[interface]['ip'] == interface_ip:
            return interface_dict[interface]
    return {'ip':interface_ip}

##############################################################################
# The following functions return information about the routing table.
##############################################################################

#The return value is a list of dictionaries.  One element in the list is
# one line in the routing table.  The keys for each dictionary are the titles
# for each column in the routing table.  WARNING: The titles are very system
# dependent.
def get_netstat_r():
    #Obtain the netstat information needed (netstat -rn).  The option -r is
    # to specify displaying the routing table.  The option -n will display
    # the -r routing table output in FQDN form.  This is desirable, since
    # each interface has a unique ip and not ip alias.  Also, netstat -r will
    # truncate long user names.  This avoid having to convert a truncated
    # hostname to the desired ip address.
    netstat_cmd = multiple_interface._find_command("netstat")
    if os.uname()[0] == "Linux":
        netstat_cmd = netstat_cmd + " -rne"
    else:
        netstat_cmd = netstat_cmd + " -rn"

    #Should any of the following three functions generate an exception
    # it is the calling functions job (save get_routes() and
    # update_cached_routes) to catch it.  It should be noted that modules
    # should not call this function directly, but call get_routes() and/or
    # update_cached_routes().

    # fork and exec the netstat command
    p = os.popen(netstat_cmd, "r")
    # obtain the info from the netstat command
    data = p.readlines()
    # release the child netstat command
    status = p.close()

    if status:
        return None
    if not data:
        return None

    #regular expresion to 
    simplify = re.compile( ' +')

    #Determine the number of columns in the output.
    for line in data:
        titles = simplify.sub( ' ', line.strip()).split(" ")
        #On all of the observed platforms there is a title line that contains
        # as the first non-whitespace characters "Destination".
        if titles[0] == "Destination":
            columns = len(titles)
            break
    else:
        return None

    output = []
    dotted_decimal = re.compile("([0-9]{1,3}.){0,3}[0-9]{1,3}")
    flags = re.compile("[UHGRDMACS]")
    #Strip out the valid lines of the table.
    for line in data:
        info = simplify.sub( ' ', line.strip()).split(" ")
        #If the line begins with xxx.xxx.xxx.xxx format, use it.
        #Look for lines that have the same number of columns as the title line.
        #If the destination is default
        #Skip the title line however.
        if ( len(info) == columns or \
             dotted_decimal.match(info[0]) or \
             info[0] == "default" ) \
             and info != titles and info[0][:5] != "Route":
            #Pad the possiblity of empty columns before the "Flags" column.
            flags_index = titles.index("Flags")
            while not flags.match(info[flags_index]):
                info.insert(2, "")
            #Create the dictionary for this route in the routing table.
            tmp = {}
            for i in range(len(titles)):
                tmp[titles[i]] = info[i]
            output.append(tmp)
    return output

_cached_netstat = None

def get_routes():
    global _cached_netstat
    if not _cached_netstat:
        _cached_netstat = get_netstat_r()
    return _cached_netstat

def update_cached_routes():
    global _cached_netstat
    _cached_netstat = get_netstat_r()
    return _cached_netstat

def clear_cached_routes():
    global _cached_netstat
    _cached_netstat = None
    return None

#Rerturns true if the destination is already in the routing table.  False,
# otherwise.
def is_route_in_table(dest):
    
    #if no routing is required, return true.
    if not get_config():
	return 1

    ip = socket.gethostbyname(dest)
    route_table = get_routes()
    for route in route_table:
        #Since netstat -rn gives the numerical address, coversions are not
        # necessary.
        if route['Destination'] == ip:
            return 1
        #Test to see if the subnet route already exists.
        existing_rt = route['Destination'].split("/")[0] #hack for OSF1 
        rt = string.join(existing_rt.split(".")[:3], ".")
        rt = string.split(rt, "/")[0]
        sn = string.join(ip.split(".")[:-1], ".")
        if rt == existing_rt and rt == sn:
            return 1
    return 0

#Return a dictionary where the keys are the intefaces and the values are
# the number of occurances in the netstat -r output.
"""
def connections():
    if os.uname()[0] == "Linux":
	search_string = "Iface"
    else:
	search_string = "Interface"

    #mode should be 0 or 1 for "read" or "write"
    config = get_config()

    interface_dict = config.get('interface')
    
    interfaces = interface_dict.keys()

    ret = {}
    for item in interfaces:
	ret[item] = 0

    for item2 in get_netstat_r():
	#This screwy piece of code determines if the route in question is
	# to a mover node.  This is nececcary to remove any extras the
	# control sockets setup.
	try:
	    if socket.gethostbyaddr(item['Destination'])[0].find("mvr") < 0:
		continue
	    
	    print socket.gethostbyaddr(item['Destination'])[0]
        except KeyboardInterrupt, msg:
	    raise msg
	except:
	    continue
	
        if item2[search_string] in interfaces:
            #Add one to the number of connection to one interface.
            ret[item2[search_string]] = ret.get(item2[search_string], 0) + 1

    return ret
"""

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
            sys.stdout.write("runon(%s): failed, err=%s" % (cpu,err))
            #Trace.log(e_errors.ERROR, "runon(%s): failed, err=%s" % (cpu,err))

##############################################################################
# The following two functions manipulate the routing table.
##############################################################################

def set_route(dest, interface_ip):
    config = get_config()
    if not config:
        return
    interfaces = get_interfaces()
    if not interfaces:
        return
    
    for interface in interfaces: #get_interfaces():
    	if interface_ip == config['interface'][interface]['ip']:
    	    gateway = config['interface'][interface]['gw']
            if_name = interface
	    break
    else:
	return

    #Attempt to set the new route.
    try:
        err=enroute.routeAdd(dest, gateway, if_name)
    except TypeError:
        #If we get here, then it is likely that the changes in enroute
        # and enroute2 have not been compiled recently.  This is likely
        # due to the change to support passing the local interface to be
        # used from encp to enroute2.
        sys.stderr.write("Unable to change route.  Update, recompile and"
                         " try again.\n")
        return

    if err == 1: #Not called from encp/enstore.  (should never see this)
	raise OSError(errno.EPERM, "Routing:" + enroute.errstr(err))
    elif err == 2: #Not supported.
        raise OSError(errno.ENOPROTOOPT, "Routing:" + enroute.errstr(err))
    elif err == 3: #Not permitted.
        raise OSError(errno.EACCES, "Routing:" + enroute.errstr(err))
    elif err == 4: #Not valid parameters.
        raise OSError(errno.EINVAL, "Routing:" + enroute.errstr(err))
    elif err == 5: #Return code if route selection is not supported.
        pass
    elif err == 6: #Route change failed.
	raise OSError(errno.EINVAL, "Routing: " + enroute.errstr(err))

def update_route(dest, interface_ip):
    config = get_config()
    if not config:
        return
    interfaces = get_interfaces()
    if not interfaces:
        return
    
    for interface in interfaces: #get_interfaces():
    	if interface_ip == config['interface'][interface]['ip']:
    	    gateway = config['interface'][interface]['gw']
            if_name = interface
	    break
    else:
	return

    #Attempt to reset an existing route.
    try:
        err=enroute.routeChange(dest, gateway, if_name)
    except TypeError:
        #If we get here, then it is likely that the changes in enroute
        # and enroute2 have not been compiled recently.  This is likely
        # due to the change to support passing the local interface to be
        # used from encp to enroute2.
        sys.stderr.write("Unable to change route.  Update, recompile and"
                         " try again.\n")

    if err == 1: #Not called from encp/enstore.  (should never see this)
	raise OSError(errno.EPERM, "Routing: " + enroute.errstr(err))
    elif err == 2: #Not supported.
        raise OSError(errno.ENOPROTOOPT, "Routing: " + enroute.errstr(err))
    elif err == 3: #Not permitted.
        raise OSError(errno.EACCES, "Routing: " + enroute.errstr(err))
    elif err == 4: #Not valid parameters.
        raise OSError(errno.EINVAL, "Routing: " + enroute.errstr(err))
    elif err == 5: #Return code if route selection is not supported.
        pass
    elif err == 6: #Route change failed.
	raise OSError(errno.EINVAL, "Routing: " + enroute.errstr(err))

def unset_route(dest):
    config = get_config()
    if not config:
        return
    interfaces = get_interfaces()
    if not interfaces:
        return

    #Attempt to remove the route.
    try:
        err=enroute.routeDel(dest)
    except TypeError:
        #If we get here, then it is likely that the changes in enroute
        # and enroute2 have not been compiled recently.  This is likely
        # due to the change to support passing the local interface to be
        # used from encp to enroute2.
        sys.stderr.write("Unable to change route.  Update, recompile and"
                         " try again.\n")

    if err == 1: #Not called from encp/enstore.  (should never see this)
	raise OSError(errno.EPERM, "Routing: " + enroute.errstr(err))
    elif err == 2: #Not supported.
        raise OSError(errno.ENOPROTOOPT, "Routing: " + enroute.errstr(err))
    elif err == 3: #Not permitted.
        raise OSError(errno.EACCES, "Routing: " + enroute.errstr(err))
    elif err == 4: #Not valid parameters.
        raise OSError(errno.EINVAL, "Routing: " + enroute.errstr(err))
    elif err == 5: #Return code if route selection is not supported.
        pass
    elif err == 6: #Route change failed.
	raise OSError(errno.EINVAL, "Routing: " + enroute.errstr(err))

##############################################################################
# The following three functions select an interface based on various criteria.
##############################################################################

def get_default_interface():
    return get_interface_info_by_ip(get_default_interface_ip())

def choose_interface():
    interfaces = get_interfaces()
    if not interfaces:
        return get_default_interface()
    
    choose = []
    for interface in interfaces:
        weight = get_interface_info(interface).get('weight', 1.0)
        choose.append((-weight, random.random(), interface))
    choose.sort()
    unused, unused, interface = choose[0]
    return get_interface_info(interface)

def check_load_balance(mode = None):
    #mode should be 0 or 1 for "read" or "write"
    config = get_config()
    if not config:
        return get_default_interface()
    interface_dict = config.get('interface')
    if not interface_dict:
        return get_default_interface()
    interfaces = interface_dict.keys()
    if not interfaces:
        return get_default_interface()

    rate_dict = multiple_interface.rates(interfaces)
    #connections_dict = connections()
    
    choose = []
    for interface in interfaces:

        #Get the weight of the interface.
        weight = interface_dict[interface].get('weight', 1.0)

        #Get the rates of the current interface.
        try: 
            recv_rate, send_rate = rate_dict[interface]
            total_rate = (recv_rate + send_rate)
        except KeyError:
            continue

        #MWZ 12-5-2003:  Why would we do this?  What does this gain us?
        recv_rate = recv_rate/weight
        send_rate = send_rate/weight
	total_rate = (recv_rate + send_rate)/weight

        #Get the number of connections (static routes) for the interface.
        #try:
        #    conn_in_progress = connections_dict[interface]
        #except KeyError:
        #    continue

        #Assemble the load balancing criteria.
        if mode==1: #writing
            #If rates are equal on different interfaces, randomize!
            #choose.append((conn_in_progress, send_rate, -weight,
            #               random.random(), interface))
	    choose.append((send_rate, -weight, random.random(), interface))
	elif mode==0: #reading
	    #choose.append((conn_in_progress, recv_rate, -weight,
            #               random.random(), interface))
	    choose.append((recv_rate, -weight, random.random(), interface))
        else:
            #choose.append((conn_in_progress, total_rate, -weight,
            #               random.random(), interface))
	    choose.append((total_rate, -weight, random.random(), interface))

    #By the magic of python, the first item in the list will be the
    # best choice for load balancing.
    choose.sort()
    unused, unused, unused, interface = choose[0]
    return get_interface_info(interface)

##############################################################################
# The following function sets up an interface for outgoing data.
##############################################################################

def setup_interface(dest, interface_ip):
    config = get_config()
    if not config:
        return
    interface_dict = config.get('interface')
    if not interface_dict:
        return
    
    #If we are already on the machine, we don't need to do set routes.
    if socket.gethostbyaddr(socket.gethostname())[0] == \
       socket.gethostbyaddr(dest)[0]:
        return

    #Some architecures (like IRIX) attach a network card to a processor.
    # make sure the process runs on the correct cpu for the interface
    # selected.
    for interface in interface_dict.keys():
        if interface_dict[interface]['ip'] == interface_ip:
            #pass in the interface (ie. eg0, eth0)
            runon_cpu(interface)

    #If the route is already in the table, remove it.  If it is not present,
    # then there is no need to try and delete it.
    if is_route_in_table(dest):
        unset_route(dest)
    #Set the static route.
    set_route(dest, interface_ip)

    #Since the routing table just changed, the cached version needs updating.
    update_cached_routes()

if __name__ == '__main__':

    #pprint.pprint(connections())
    pprint.pprint(check_load_balance())
