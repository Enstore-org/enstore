#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import sys
import string
import socket
import select
import pprint
import time
import errno
import re
import time

# enstore imports
import configuration_client
import library_manager_client
import volume_clerk_client
import option
import e_errors
import callback
import Trace
import host_config
import udp_server
import hostaddr

_counter = 0

def generate_unique_id():
    global _counter
    thishost = hostaddr.gethostinfo()[0]
    ret = "%s-%d-%d-%d" % (thishost, int(time.time()),_counter, os.getpid())
    _counter = _counter + 1
    return ret

def get_callback_addr(ip=None):
    # get a port to talk on and listen for connections
    (host, port, listen_socket) = callback.get_callback(
        verbose=0, ip=ip)
    callback_addr = (host, port)
    listen_socket.listen(4)

    Trace.message(1,
                  "Waiting for mover(s) to call back on (%s, %s)." %
                  callback_addr)

    return callback_addr, listen_socket

def get_routing_callback_addr(udps=None):
    # get a port to talk on and listen for connections
    if udps == None:
        udps = udp_server.UDPServer(None,
                                    receive_timeout=900)
    else:
        udps.__init__(None, receive_timeout=900)
        
    route_callback_addr = (udps.server_address[0], udps.server_address[1])
    
    Trace.message(1,
                  "Waiting for mover(s) to send route back on (%s, %s)." %
                  route_callback_addr)

    return route_callback_addr, udps

def parse_file(filename):
    file=open(filename, "r")
    data=map(string.strip, file.readlines())
    tmp = []
    for item in data:
	try:
	    tmp.append(string.split(item)[0])
	except IndexError:
	    continue #This happens for blank lines

    file.close()
    return tmp

def get_vcc_list():
    #Determine the entire valid list of configuration servers.
    csc = configuration_client.ConfigurationClient()
    config_server_addr_list = csc.get('known_config_servers')
    if config_server_addr_list['status'] != (e_errors.OK, None):
        print config_server_addr_list['status']
        sys.exit(1)
    #Add this hosts default csc.
    config_server_addr_list[socket.gethostname()] = csc.server_address
    #Remove status.
    del config_server_addr_list['status']

    csc_list = []
    vcc_list = []
    for config in config_server_addr_list.values():
        _csc = configuration_client.ConfigurationClient(config)
        csc_list.append(_csc)
        vcc_list.append(volume_clerk_client.VolumeClerkClient(_csc))

    return csc_list, vcc_list

############################################################################
############################################################################

def open_routing_socket(route_server, unique_id_list, mover_timeout):

    route_ticket = None

    if not route_server:
        return

    start_time = time.time()
    while(time.time() - start_time < mover_timeout):

        try:
            route_ticket = route_server.process_request()
        except socket.error:
            continue
        
        #If route_server.process_request() fails it returns None.
        if not route_ticket:
            continue
        #If route_server.process_request() returns incorrect value.
        elif route_ticket == type({}) and hasattr(route_ticket, 'unique_id') \
           and route_ticket['unique_id'] not in unique_id_list:
            continue
        #It is what we were looking for.
        else:
            break
    else:
        raise socket.error(errno.ETIMEDOUT, "Mover did not call back.")

    #set up any special network load-balancing voodoo
    interface=host_config.check_load_balance(mode=0)
    #load balencing...
    if interface:
        ip = interface.get('ip')
        if ip and route_ticket.get('mover_ip', None):
	    #With this loop, give another encp 10 seconds to delete the route
	    # it is using.  After this time, it will be assumed that the encp
	    # died before it deleted the route.
	    start_time = time.time()
	    while(time.time() - start_time < 10):

		host_config.update_cached_routes()
		route_list = host_config.get_routes()
		for route in route_list:
		    if route_ticket['mover_ip'] == route['Destination']:
			break
		else:
		    break

		time.sleep(1)
	    
            #This is were the interface selection magic occurs.
            host_config.setup_interface(route_ticket['mover_ip'], ip)


    (route_ticket['callback_addr'], listen_socket) = \
				    get_callback_addr(ip=ip)
    route_server.reply_to_caller_using_interface_ip(route_ticket, ip)

    return route_ticket, listen_socket

    ##########################################################################

def open_control_socket(listen_socket, mover_timeout):


    read_fds,write_fds,exc_fds=select.select([listen_socket], [], [],
                                             mover_timeout)

    #If there are no successful connected sockets, then select timedout.
    if not read_fds:
        raise socket.error(errno.ETIMEDOUT,
                           "Mover did not call back.")
    
    control_socket, address = listen_socket.accept()

    if not hostaddr.allow(address):
        control_socket.close()
        raise socket.error(errno.EPERM, "host %s not allowed" % address[0])

    read_fds,write_fds,exc_fds=select.select([control_socket], [], [],
                                             mover_timeout)

    try:
        ticket = callback.read_tcp_obj(control_socket)
    except e_errors.TCP_EXCEPTION:
        raise socket.error(errno.EPROTO, "Unable to obtain mover responce")
    
    return control_socket, address, ticket

    ##########################################################################

def receive_final_dialog(control_socket):
    # File has been sent - wait for final dialog with mover. 
    # We know the file has hit some sort of media.... 
    
    try:
        done_ticket = callback.read_tcp_obj(control_socket)
    except e_errors.TCP_EXCEPTION, msg:
        done_ticket = {'status':(e_errors.TCP_EXCEPTION,
                                 msg)}
        
    return done_ticket

############################################################################
############################################################################

def main():
    #Read in the list of vols.
    vol_list = parse_file(sys.argv[1])

    #The list of volume clerks to check.
    csc_list, vcc_list = get_vcc_list()

    #Determine the calback address.
    callback_addr, listen_socket = get_callback_addr()
    #Determine the routing callback address.
    config = host_config.get_config()
    if config and config.get('interface', None):
        route_selection = 1
        routing_callback_addr, udp_server = get_routing_callback_addr()
    else:
        route_selection = 0
        routing_callback_addr, udp_server = None, None

    unique_id_list = []
    for vol in vol_list:
	e_msg = None #clear this error variable.
        for i in range(len(vcc_list)):
            vc = vcc_list[i].inquire_vol(vol)

	    #If the volume has a bad state, skip it.
            if vc['status'] != (e_errors.OK, None):
		if e_msg: #If error is already set, skip it.
		    continue
		e_msg = "Volume %s has state %s and unassertable." % \
			(vol, vc['status'])
                continue
	    #If the volume is not a tape, skip it.
	    if vc['media_type'] == "null" or vc['media_type'] == "disk":
		if e_msg: #If error is already set, skip it.
		    continue
		e_msg = "Volume %s is a %s volume and unassertable." % \
			(vol, vc['media_type'])
		continue

	    #Create the ticket to submit to the library manager.
            ticket = {}
            ticket['unique_id'] = generate_unique_id()
            ticket['callback_addr'] = callback_addr
            ticket['routing_callback_addr'] = routing_callback_addr
	    ticket['route_selection'] = route_selection
            ticket['vc'] = vc
            ticket['vc']['address'] = vcc_list[i].server_address  #vcc instance
            #easier to do this than modify the mover.
	    ticket['fc'] = {}
	    ticket['times'] = {}
	    ticket['times']['t0'] = time.time()
	    ticket['encp'] = {}
	    ticket['encp']['adminpri'] = -1
	    ticket['encp']['basepri'] = 1

	    print "Submitting assert request for %s volume %s." % \
		  (ticket['vc']['media_type'], vol)

            lmc = library_manager_client.LibraryManagerClient(
                csc_list[i], ticket['vc']['library'] + ".library_manager")
            responce_ticket = lmc.volume_assert(ticket, 10, 1)

	    if responce_ticket['status'] != (e_errors.OK, None):
		print "Submittion for %s failed: %s" % \
		      (vol, responce_ticket['status'])
		continue

	    unique_id_list.append(ticket['unique_id'])

            break #When the correct vcc is found skip the rest.
	
	else:
	    print e_msg

    for i in range(len(unique_id_list)):
        if route_selection == 1:
	    #There is no need to do this on a non-multihomed machine.
            route_ticket, listen_socket = open_routing_socket(
		udp_server, unique_id_list, 900)
        socket, addr, callback_ticket = open_control_socket(listen_socket, 900)

        #print "RESPONCE TICKET"
        #pprint.pprint(callback_ticket)

        print "Asserting volume %s." % callback_ticket['vc']['external_label']

        if callback_ticket['status'][0] != e_errors.OK:
            continue

        done_ticket = receive_final_dialog(socket)

        #print "DONE TICKET"
        #pprint.pprint(done_ticket)

        print "Volume status is %s" % (done_ticket['status'],)

        socket.close()
        
if __name__ == "__main__":
    try:
	main()
    except KeyboardInterrupt:
        sys.stderr.write("KeyboardInterrupt\n")
        sys.stderr.flush()
