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
import types

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
import encp
import generic_client
import delete_at_exit
#import enstore_functions

MY_NAME = "ASSERT"

############################################################################
############################################################################

#Parse the file containing the volumes to be asserted.  It expects the first
# word on each line to be the volume.  Reamining text on a line is ignored.
# This is so that a multi-line copy-paste from "enstore vol --vols" doesn't
# need to be pased down to the volume names.  Also, any line beginning with
# a "#" is a comment and ignored.
def parse_file(filename):
    #paranoid check
    if type(filename) != types.StringType:
        return []
    #Handle file access problems gracefully.
    try:
        file=open(filename, "r")
    except OSError, msg:
        sys.stderr.write(msg + "\n")
        sys.exit(1)
        
    data=map(string.strip, file.readlines())
    tmp = []
    for item in data:
	try:
            line = string.split(item)[0]
            line.strip()
            if line[0] != "#":
                tmp.append(string.split(item)[0])
	except IndexError:
	    continue #This happens for blank lines

    file.close()
    return tmp

def parse_vol_list(comma_seperated_string):
    #paranoid check
    if type(comma_seperated_string) != types.StringType:
        return []

    split_on_commas = comma_seperated_string.split(",")

    #If the string began or ended with a comma remove the blank label name.
    if split_on_commas[0] == "":
        del split_on_commas[0]
    if split_on_commas[-1] == "":
        del split_on_commas[-1]

    return split_on_commas

def get_vcc_list():
    #Determine the entire valid list of configuration servers.
    csc = configuration_client.ConfigurationClient()
    config_server_addr_list = csc.get('known_config_servers', 10, 6)
    if not e_errors.is_ok(config_server_addr_list['status']):
        sys.stderr.write(str(config_server_addr_list['status']) + "\n")
        sys.exit(1)
        
    #Add this hosts default csc.
    config_server_addr_list[socket.gethostname()] = csc.server_address
    #Remove status.
    del config_server_addr_list['status']

    #For all systems that respond get the volume clerk and configuration
    # server clients.
    csc_list = []
    vcc_list = []
    for config in config_server_addr_list.values():
        _csc = configuration_client.ConfigurationClient(config)
        csc_list.append(_csc)
        vcc_list.append(volume_clerk_client.VolumeClerkClient(_csc))

    return csc_list, vcc_list

def create_assert_list(vol_list, intf):

    #The list of volume clerks to check.
    csc_list, vcc_list = get_vcc_list()

    #Determine the calback address.
    callback_addr, listen_socket = encp.get_callback_addr(intf)
    #Determine the routing callback address.
    config = host_config.get_config()
    if config and config.get('interface', None):
        route_selection = 1
        routing_callback_addr, udp_server = \
                               encp.get_routing_callback_addr(intf)
    else:
        route_selection = 0
        routing_callback_addr, udp_server = None, None

    #For each volume in the list, determine which system it belongs in by
    # asking each volume clerk until one responds positively.  When one does
    # build the ticket and add it to the list to assert.
    assert_list = []
    for vol in vol_list:
	e_msg = None #clear this error variable.
        for i in range(len(vcc_list)):
            vc = vcc_list[i].inquire_vol(vol)

	    #If the volume has a bad state, skip it.
            if not e_errors.is_ok(vc['status']):
		if e_msg: #If error is already set, skip it.
		    continue
		e_msg = "Volume %s has state %s and unassertable.\n" % \
			(vol, vc['status'])
                continue
	    #If the volume is not a tape, skip it.
	    if vc['media_type'] == "null" or vc['media_type'] == "disk":
		if e_msg: #If error is already set, skip it.
		    continue
		e_msg = "Volume %s is a %s volume and unassertable.\n" % \
			(vol, vc['media_type'])
		continue

	    #Create the ticket to submit to the library manager.
            ticket = {}
            ticket['unique_id'] = encp.generate_unique_id()
            ticket['callback_addr'] = callback_addr
            ticket['routing_callback_addr'] = routing_callback_addr
	    ticket['route_selection'] = route_selection
            ticket['vc'] = vc
            ticket['vc']['address'] = vcc_list[i].server_address  #vcc instance
            #Easier to do this than modify the mover.
	    ticket['fc'] = {}
            #Internally used values.
            ticket['_csc'] = csc_list[i].server_address
            #The following are for the inquisitor.
            ticket['fc']['external_label'] = vc['external_label']
            ticket['fc']['location_cookie'] = "0000_000000000_0000000"
	    ticket['times'] = {}
	    ticket['times']['t0'] = time.time()
	    ticket['encp'] = {}
	    ticket['encp']['adminpri'] = -1
	    ticket['encp']['basepri'] = 1
            ticket['encp']['curpri'] = 0  #For transfers, this is set by LM.
            ticket['encp']['delpri'] = 0
            ticket['encp']['agetime'] = 0
            ticket['infile'] = ""
            ticket['outfile'] = ""
            ticket['wrapper'] = encp.get_uinfo()
            ticket['wrapper']['size_bytes'] = 0
            ticket['wrapper']['machine'] = os.uname()
            ticket["wrapper"]["pnfsFilename"] = ""
            ticket["wrapper"]["fullname"] = ""

            #Add the assert work ticket to the list of volume asserts.
            assert_list.append(ticket)

            break  #When the correct vcc is found skip the rest.

        else:
            sys.stderr.write(e_msg)

    return assert_list, listen_socket, udp_server

def submit_assert_requests(assert_list):
    unique_id_list = []

    #Submit each request to the library manager.  The necessary information
    # is in the ticket.  While submiting the assert work requests, create
    # a list of the unique_ids created.
    for ticket in assert_list:
        Trace.trace(1, "Submitting assert request for %s volume %s." % \
              (ticket['vc']['media_type'], ticket['vc']['external_label']))

        #Instantiate the library_manager_client class and send it the
        # volume assert.
        lmc = library_manager_client.LibraryManagerClient(
            ticket['_csc'], ticket['vc']['library'] + ".library_manager")
        responce_ticket = lmc.volume_assert(ticket, 10, 1)

        if not e_errors.is_ok(responce_ticket['status']):
            sys.stderr.write("Submittion for %s failed: %s\n" % \
                             (ticket['vc']['external_label'],
                              responce_ticket['status']))
            continue

        unique_id_list.append(ticket['unique_id'])

    return unique_id_list  #return the list of unique ids to wait for.

#Unique_id_list is a list of just the unique ids.  Assert_list is a list of
# the complete tickets.
def handle_assert_requests(unique_id_list, assert_list, listen_socket,
                           udp_server, intf):

    error_id_list = []
    completed_id_list = []
    
    while len(error_id_list) + len(completed_id_list) < len(assert_list):
        
        try:
            #Obtain the control socket and if necessary the routing socket.
            if udp_server:
	        #There is no need to do this on a non-multihomed machine.
                route_ticket, listen_socket = encp.open_routing_socket(
                    udp_server, unique_id_list, intf)
            socket, addr, callback_ticket = \
                    encp.open_control_socket(listen_socket, intf.mover_timeout)
        except KeyboardInterrupt:
            raise sys.exc_info()
        except:
            #Output a message.
            exc, msg = sys.exc_info()[:2]
            sys.stderr.write(str(msg) + "\n")

            uncompleted_list = []
            for i in range(len(assert_list)):
                if assert_list[i]['unique_id'] not in error_id_list or \
                   assert_list[i]['unique_id'] not in completed_id_list:
                    #If an error occured, update the unique id.
                    if msg.errno != errno.ETIMEDOUT:
                        assert_list[i]['unique_id'] = encp.generate_unique_id()
                    uncompleted_list.append(assert_list[i])
            #Resend or resubmit the volume request.
            submit_assert_requests(uncompleted_list)
            continue

        Trace.trace(10, "RESPONCE TICKET")
        Trace.trace(10, pprint.pformat(callback_ticket))

        Trace.trace(1, "Asserting volume %s." % \
                    callback_ticket['vc']['external_label'])

        if not e_errors.is_ok(callback_ticket['status']):
            #Output a message.
            sys.stderr.write("Early error for %s: %s\n" %
                             callback_ticket['vc']['external_label'],
                             str(callback_ticket['status']))
            #Do not retry from error.
            error_id_list.append(callback_ticket['unique_id'])
            continue

        try:
            #Obtain the results of the volume assert by the mover.
            done_ticket = encp.receive_final_dialog(socket)
        except KeyboardInterrupt:
            raise sys.exc_info()
        except:
            #Output a message.
            exc, msg = sys.exc_info()[:2]
            sys.stderr.write("Encountered final dialog error for %s: %s\n" %
                             callback_ticket['vc']['external_label'], str(msg))
            #Do not retry from error.
            error_id_list.append(callback_ticket['unique_id'])
            continue
        
        Trace.trace(5, "DONE TICKET")
        Trace.trace(5, pprint.pformat(done_ticket))

        #Print message if requested.  If an error occured print that to stderr.
        message = "Volume %s status is %s" % (
            done_ticket['vc']['external_label'],
            done_ticket['status'],)
        if e_errors.is_ok(done_ticket['status']):
            Trace.trace(1, message)
        else:
            sys.stderr.write(message + "\n")

        completed_id_list.append(done_ticket['unique_id'])

        #Close the socket or risk crashing with to many open files.
        try:
            socket.close()
        except socket.error:
            pass
        
    #This is still open.  Close it for good programming technique.
    try:
        listen_socket.close()
    except socket.error:
        pass

############################################################################
############################################################################


class VolumeAssertInterface(option.Interface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        self.verbose = 0
        self.volume = ""
        self.mover_timeout = 60*60
        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.volume_assert_options)
    
    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

    parameters = ["volume_list_file"]
    
    volume_assert_options = {
        option.VERBOSE:{option.HELP_STRING:"print out information.",
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_TYPE:option.INTEGER,
                        option.USER_LEVEL:option.USER,},
        option.MOVER_TIMEOUT:{option.HELP_STRING:"set mover timeout period "\
                              " in seconds (default 1 hour)",
                              option.VALUE_USAGE:option.REQUIRED,
                              option.VALUE_TYPE:option.INTEGER,
                              option.USER_LEVEL:option.USER,},
        option.VOLUME:{option.HELP_STRING:"assert specific volume(s), " \
                       "seperate multiple volumes with commas",
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_TYPE:option.STRING,
                       option.USER_LEVEL:option.USER,},
        }

############################################################################
############################################################################
    
def main(intf):

    Trace.init(MY_NAME)
    for x in xrange(0, intf.verbose+1):
        Trace.do_print(x)
    Trace.trace(3, 'Volume assert called with args: %s'%(sys.argv,))
    
    #Read in the list of vols.
    if intf.args:  #read from file.
        vol_list = parse_file(intf.args[0])
    elif intf.volume: #read from command line argument.
        vol_list = parse_vol_list(intf.volume)
    else:
        sys.stderr.write("No volume labels given.\n")
        sys.exit(1)

    #Create the list of assert work requests.
    assert_list, listen_socket, udp_server = create_assert_list(vol_list, intf)

    #Submit the work requests to the library manager.
    unique_id_list = submit_assert_requests(assert_list)

    #Wait for mover to call back with the volume assert status.
    handle_assert_requests(unique_id_list, assert_list,
                           listen_socket, udp_server, intf)
    
############################################################################
############################################################################
        
if __name__ == "__main__":

    delete_at_exit.setup_signal_handling()

    intf = VolumeAssertInterface(user_mode=0)

    intf._mode = "admin"

    try:
	main(intf)
    except KeyboardInterrupt:
        sys.stderr.write("KeyboardInterrupt\n")
        sys.stderr.flush()
