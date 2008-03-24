#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
import os
import time
import pprint
import select
import socket
import errno
import copy
import types
import stat

# enstore modules
import encp
import pnfs
import e_errors
import delete_at_exit
import host_config
import Trace
#import checksum
import option
#import enstore_functions3


#Completion status field values.
SUCCESS = encp.SUCCESS    #"SUCCESS"
FAILURE = encp.FAILURE    #"FAILURE"

#Return values to know if get should stop or keep going.
CONTINUE_FROM_BEGINNING = encp.CONTINUE_FROM_BEGINNING
CONTINUE = encp.CONTINUE
STOP = encp.STOP

DONE_LEVEL     = encp.DONE_LEVEL
ERROR_LEVEL    = encp.ERROR_LEVEL
TRANSFER_LEVEL = encp.TRANSFER_LEVEL
TO_GO_LEVEL    = encp.TO_GO_LEVEL
INFO_LEVEL     = encp.INFO_LEVEL
CONFIG_LEVEL   = encp.CONFIG_LEVEL
TIME_LEVEL     = encp.TIME_LEVEL
TICKET_LEVEL   = encp.TICKET_LEVEL
TICKET_1_LEVEL = encp.TICKET_1_LEVEL

def get_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "v1_53  CVS $Revision$ "
    get_file = globals().get('__file__', "")
    if get_file: version_string = version_string + get_file
    return version_string

def setup_get_interface():

    encp.EncpInterface.get = 1
    
    encp.EncpInterface.parameters = [
        "--volume <volume> <destination dir>",
        "--read-to-end-of-tape --volume <volume> <source dir> <destination dir>",
        "<source file> [source file [...]] <destination directory>"
        ]
    
    #encp.encp_client_version = get_client_version

    encp.EncpInterface.encp_options[option.LIST] = {
        option.HELP_STRING: "Takes in a filename of a file containing a list "
                            "of locations and filenames.",
        option.VALUE_USAGE:option.REQUIRED,
        option.VALUE_TYPE:option.STRING,
        option.VALUE_LABEL:"name_of_list_file",
        option.USER_LEVEL:option.USER,}
    encp.EncpInterface.encp_options[option.SEQUENTIAL_FILENAMES] = {
        option.HELP_STRING: "Override known filenames and use sequentially "
                            "numbered filenames.",
        option.VALUE_USAGE:option.IGNORED,
        option.VALUE_TYPE:option.INTEGER,
        option.USER_LEVEL:option.USER,}
    encp.EncpInterface.encp_options[option.SKIP_DELETED_FILES] = {
        option.HELP_STRING: "Skip over deleted files.",
        option.VALUE_USAGE:option.IGNORED,
        option.VALUE_TYPE:option.INTEGER,
        option.USER_LEVEL:option.USER,}
    encp.EncpInterface.encp_options[option.READ_TO_END_OF_TAPE] = {
        option.HELP_STRING: "After the last file known is read keep reading "
                            "until EOD or EOT.",
        option.VALUE_USAGE:option.IGNORED,
        option.VALUE_TYPE:option.INTEGER,
        option.USER_LEVEL:option.USER,}
    try:
        del encp.EncpInterface.encp_options[option.EPHEMERAL]
    except KeyError:
        pass
    try:
        del encp.EncpInterface.encp_options[option.FILE_FAMILY]
    except KeyError:
        pass
    
    encp.EncpInterface.list = None                # Used for "get" only.
    encp.EncpInterface.skip_deleted_files = None  # Used for "get" only.
    encp.EncpInterface.read_to_end_of_tape = None # Used for "get" only.

def error_output(request):
    #Get the info.
    lc = request.get('fc', {}).get("location_cookie", None)
    file_number = encp.extract_file_number(lc)
    message = request.get("status", (e_errors.UNKNOWN, None))
    #Format the output.
    msg = "error_output %s %s\n" % (file_number, message)
    #Print the output.
    try:
        sys.stderr.write(msg)
        sys.stderr.flush()
    except IOError:
        pass

def halt(exit_code=1):
    Trace.message(DONE_LEVEL, "Get exit status: %s" % (exit_code,))
    Trace.log(e_errors.INFO, "Get exit status: %s" % (exit_code,))
    delete_at_exit.quit(exit_code)

def untried_output(requests):

    #Turn a list of requests for a single volume, into a dictionary
    # of request lists based on volume.
    if type(requests) == types.ListType:
        requests = {'' : requests}

    for request_list in requests.values():
        for request in request_list:
            #For each item in the list, print this if it was not tried.
            if request.get('completion_status', None) == None:
                request['status'] = (e_errors.UNKNOWN,
                                     "File transfer not attempted.")
                error_output(request)

def mover_handshake(listen_socket, udp_socket, request, encp_intf):
    #Grab a new clean udp_socket.
    udp_callback_addr, unused = encp.get_udp_callback_addr(encp_intf,
                                                           udp_socket)
    #The ticket item of 'routing_callback_addr' is a legacy name.
    request['routing_callback_addr'] = udp_callback_addr

    #Open the routing socket.
    try:

	    Trace.message(TRANSFER_LEVEL, "Opening udp socket.")
	    Trace.log(e_errors.INFO, "Opening udp socket.")
	    Trace.log(e_errors.INFO,
		      "Listening for udp message at: %s." % \
		      str(udp_socket.server_socket.getsockname()))

            #Keep looping until one of these two messages arives.
            # Ignore any other that my be received.
            uticket = encp.open_udp_socket(udp_socket,
                                           [request['unique_id']],
                                           encp_intf)

            #If requested output the raw message.
            Trace.message(TICKET_LEVEL, "RTICKET MESSAGE:")
            Trace.message(TICKET_LEVEL, pprint.pformat(uticket))
		
	    if not e_errors.is_ok(uticket):
		#Log the error.
		Trace.log(e_errors.ERROR,
			  "Unable to connect udp socket: %s" %
			  (str(uticket['status'])))
		uticket = encp.combine_dict(uticket, request)
		return None, uticket

	    Trace.message(TRANSFER_LEVEL, "Opened udp socket.")
	    Trace.log(e_errors.INFO, "Opened udp socket.")
    except (encp.EncpError,), detail:
	if getattr(detail, "errno", None) == errno.ETIMEDOUT:
	    #Handle retries needs to be called to update various values
	    # and to perfrom the resubmition itself.
	    request['status'] = (e_errors.RESUBMITTING,
				 request['unique_id'])
	else:
	    #Handle retries needs to be called to update various values
	    # and to perfrom the resubmition itself.
	    request['status'] = (detail.type, str(detail))

	#Log the error.
	Trace.log(e_errors.ERROR, "Unable to connect udp socket: %s" %
		  (str(request['status']),))
	return None, request

    #Print out the final ticket.
    Trace.message(TICKET_LEVEL, "UDP TICKET:")
    Trace.message(TICKET_LEVEL, pprint.pformat(uticket))

    message = "Listening for control socket at: %s" \
              % str(listen_socket.getsockname())
    Trace.message(DONE_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    try:
	#The listen socket used depends on if route selection is
	# enabled or disabled.  If enabled, then the listening socket
	# returned from open_udp_socket is used.  Otherwise, the
	# original udp socket opened and the beginning is used.
	#If the routes were changed, then only wait WAIT_TIME sec. before
	# initiating the retry.
        WAIT_TIME = 120.0 #in seconds
        SELECT_TIME = 5.0 #in seconds
	i = 0
	#while i < int(encp_intf.mover_timeout/SELECT_TIME):
        while i < int(WAIT_TIME / SELECT_TIME):
	    try:
		control_socket, mover_address, ticket = \
				encp.open_control_socket(
				    listen_socket, SELECT_TIME)
		break
	    except (socket.error, select.error, encp.EncpError), msg:
		#If a select (or other call) was interupted,
		# this is not an error, but should continue.
		if msg.args[0] == errno.EINTR:
		    continue
		#If the error was timeout, resend the reply
		# Since, there was an exception, "uticket" is still
		# the ticket returned from the routing call.
		elif msg.args[0] == errno.ETIMEDOUT:
		    #udp_socket.reply_to_caller_using_interface_ip(
                    #rticket, listen_socket.getsockname()[0])
                    udp_socket.reply_to_caller(uticket)
		else:
		    if isinstance(msg, (socket.error, select.error)):
			ticket = {'status' : (e_errors.NET_ERROR,
					      str(msg))}
		    else: #EncpError
			ticket = {'status' : (msg.type, str(msg))}

		    #Force an exit from the loop.
		    break

	    #Increment the count.
	    i = i + 1
	else:
	    #If we get here then we had encp_intf.max_retry timeouts
	    # occur.  Giving up.
	    ticket = {'status' : (e_errors.RESUBMITTING, None)}

	if not e_errors.is_ok(ticket):
	    #Log the error.
	    Trace.log(e_errors.ERROR,
		      "Unable to connect control socket: %s" %
		      (str(ticket['status'])))
	    ticket = encp.combine_dict(ticket, request)
	    return None, ticket

    except (encp.EncpError,), detail:
	if getattr(detail, "errno", None) == errno.ETIMEDOUT:
	    #Handle retries needs to be called to update various values
	    # and to perfrom the resubmition itself.
	    request['status'] = (e_errors.RESUBMITTING, request['unique_id'])
	else:
	    #Handle retries needs to be called to update various values
	    # and to perfrom the resubmition itself.
	    request['status'] = (detail.type, str(detail))

	#Log the error.
	Trace.log(e_errors.ERROR, "Unable to connect control socket: %s" %
		  (str(request['status']),))
	return None, request

    #Print out the final ticket.
    Trace.message(TICKET_LEVEL, "MOVER HANDSHAKE (CONTROL):")
    Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
    #Recored the receiving of the first control socket message.
    message = "Received callback ticket from mover %s for transfer %s." % \
              (ticket.get('mover', {}).get('name', "Unknown"),
               ticket.get('unique_id', "Unknown"))
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    #Compare expected unique id with the returned unique id.
    if ticket.get('unique_id', None) != request['unique_id']:
	#Build the error messages.
	msg = "Unexpected unique_id received from %s.  Expected " \
	      "unique id %s, received %s instead." % \
	      (mover_address, request['unique_id'],
	       ticket.get('unique_id', None))
	ticket['status'] = (e_errors.EPROTO, str(msg))
	#Report the errors.
        try:
            sys.stderr.write("%s\n" % str(msg))
            sys.stderr.flush()
        except IOError:
            pass
	Trace.log(e_errors.ERROR, str(msg))

    #Keep the udp socket queues clear, while waiting for the mover
    # ready message.
    start_time = time.time()
    Trace.message(TRANSFER_LEVEL, "Waiting for mover ready message.")
    Trace.log(e_errors.INFO, "Waiting for mover ready message.")
    while time.time() < start_time + encp_intf.mover_timeout:
        #Keep looping until the message arives.
        mover_ready = udp_socket.do_request()

        #If requested output the raw message.
        Trace.trace(11, "UDP MOVER READY MESSAGE:")
        Trace.trace(11, pprint.pformat(mover_ready))

        #Make sure the messages are what we expect.
        if mover_ready == None:
            continue  #Something happened, keep trying.
        elif mover_ready.get('work', None) == "mover_idle":
            break
        elif type(mover_ready.get('work', None)) == types.StringType:
            continue  #Something happened, keep trying.                
        else:
            break
    else:
        #We timed out.  Handle the error.
        error_ticket = {'status' : (e_errors.RESUBMITTING, None)}
        mover_ready = encp.handle_retries([request], request,
                                           error_ticket, encp_intf)

    Trace.message(TRANSFER_LEVEL, "Received mover ready message.")
    Trace.log(e_errors.INFO, "Received mover ready message.")

    if not e_errors.is_ok(mover_ready):
        ticket = encp.combine_dict(mover_ready, ticket)

    return control_socket, ticket

def mover_handshake2(work_ticket, udp_socket, e):
        prepare_for_transfer_start_time = time.time()

        #This is an evil hack to modify work_ticket outside of
        # create_read_requests().
        work_ticket['method'] = "read_next"
        #Grab a new clean udp_socket.
        udp_callback_addr, unused = encp.get_udp_callback_addr(e, udp_socket)
        #The ticket item of 'routing_callback_addr' is a legacy name.
        work_ticket['routing_callback_addr'] = udp_callback_addr

        #Record the event of sending the request to the mover.
        message = "Sending file %s request to the mover." % \
                encp.extract_file_number(work_ticket['fc']['location_cookie'])
        Trace.message(TRANSFER_LEVEL, message)
        Trace.log(e_errors.INFO, message)

        #Record this for posterity, if requested.
        Trace.message(TICKET_LEVEL, "MOVER_REQUEST_SUBMISSION:")
        Trace.message(TICKET_LEVEL, pprint.pformat(work_ticket))

        #Send the actual request to the mover.
        udp_socket.reply_to_caller(work_ticket)

	try:
	    mover_addr = work_ticket['mover']['callback_addr']
	except KeyError:
	    msg = sys.exc_info()[1]
            try:
                sys.stderr.write("Sub ticket 'mover' not found.\n")
                sys.stderr.write("%s: %s\n" % (e_errors.KEYERROR, str(msg)))
                sys.stderr.write(pprint.pformat(work_ticket)+"\n")
                sys.stderr.flush()
            except IOError:
                pass
	    if e_errors.is_ok(work_ticket.get('status', (None, None))):
		work_ticket['status'] = (e_errors.KEYERROR, str(msg))
	    return None, work_ticket

	#Set the route that the data socket will use.
	try:
	    #There is no need to do this on a non-multihomed machine.
	    config = host_config.get_config()
	    if config and config.get('interface', None):
		local_intf_ip = encp.open_routing_socket(mover_addr[0], e)
	    else:
		local_intf_ip = work_ticket['callback_addr'][0]
	except (encp.EncpError,), msg:
	    work_ticket['status'] = (e_errors.EPROTO, str(msg))
	    return None, work_ticket

        Trace.message(TRANSFER_LEVEL, "Opening the data socket.")
        Trace.log(e_errors.INFO, "Opening the data socket.")

        #Open the data socket.
        try:
            data_path_socket = encp.open_data_socket(mover_addr, local_intf_ip)

            if not data_path_socket:
                raise socket.error(errno.ENOTCONN,
                                   errno.errorcode[errno.ENOTCONN])

            work_ticket['status'] = (e_errors.OK, None)
	    #We need to specifiy which interface will be used on the encp side.
	    work_ticket['encp_ip'] = data_path_socket.getsockname()[0]
            Trace.message(TRANSFER_LEVEL, "Opened the data socket.")
            Trace.log(e_errors.INFO, "Opened the data socket.")
        except (encp.EncpError, socket.error), detail:
            msg = "Unable to open data socket with mover: %s" % (str(detail),)
            try:
                sys.stderr.write("%s\n" % str(msg))
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR, str(msg))
            work_ticket['status'] = (e_errors.NET_ERROR, str(msg))

        # Verify that everything went ok with the transfer.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                          work_ticket, e)

        if not e_errors.is_ok(result_dict):
            #Log the error.
            Trace.log(e_errors.ERROR, "Unable to connect data socket: %s %s" %
                      (str(work_ticket['status']), str(result_dict['status'])))
            
            #Don't loose the non-retirable error.
            if e_errors.is_non_retriable(result_dict):
                work_ticket = encp.combine_dict(result_dict, work_ticket)
            # Close these descriptors before they are forgotten about.
            #encp.close_descriptors(out_fd)

            return None, work_ticket

        message = "Data socket is connected to mover %s for %s. " % \
                      (work_ticket.get('mover', {}).get('name', "Unknown"),
                      work_ticket.get('unique_id', "Unknown"))
        Trace.message(TRANSFER_LEVEL, message)
        Trace.log(e_errors.INFO, message)

        Trace.message(TIME_LEVEL, "Time to prepare for transfer: %s sec." %
                      (time.time() - prepare_for_transfer_start_time,))

        return data_path_socket, work_ticket


def set_metadata(ticket, intf):

    #Set these now so the metadata can be set correctly.
    ticket['wrapper']['fullname'] = ticket['outfile']
    ticket['wrapper']['pnfsFilename'] = ticket['infile']
    try:
        ticket['file_size'] = ticket['exfer'].get("bytes", 0L)
    except:
        try:
            sys.stderr.write("Unexpected error setting metadata.\n")
            sys.stderr.write(pprint.pformat(ticket))
            sys.stderr.flush()
        except IOError:
            pass
        exc, msg, tb = sys.exc_info()
        raise exc, msg, tb

    #Create the pnfs file.
    try:
        #encp.create_zero_length_files(ticket['infile'], raise_error = 1)
        encp.create_zero_length_pnfs_files(ticket)
        Trace.log(e_errors.INFO, "Pnfs file created for %s."%ticket['infile'])
    except OSError, detail:
        msg = "Pnfs file create failed for %s: %s" % (ticket['infile'],
                                                      str(detail))
        Trace.message(TRANSFER_LEVEL, msg)
        Trace.log(e_errors.ERROR, msg)
        return

    Trace.message(TICKET_LEVEL, "SETTING METADATA WITH:")
    Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
    
    #Set the metadata for this new file.
    encp.set_pnfs_settings(ticket, intf)

    if not e_errors.is_ok(ticket):
        msg = "Metadata update failed for %s: %s" % (ticket['infile'],
                                                     ticket['status'])
        Trace.message(ERROR_LEVEL, msg)
        Trace.log(e_errors.ERROR, msg)

        #Be sure to cleanup after the metadata error.
        encp.clear_layers_1_and_4(ticket)
    else:
        delete_at_exit.unregister(ticket['infile']) #Don't delete good file.
        msg = "Successfully updated %s metadata." % ticket['infile']
        Trace.message(INFO_LEVEL, msg)
        Trace.log(e_errors.INFO, msg)

def end_session(udp_socket, control_socket):

    nowork_ticket = {'work': "nowork", 'method' : "no_work"}
    #try:
    #    done_ticket = callback.write_tcp_obj(control_socket, nowork_ticket)
    #except e_errors.TCP_EXCEPTION:
    #    sys.stderr.write("Unable to terminate communication "
    #                     "with mover cleanly.\n")
    #    halt(1)

    #We are done, tell the mover.
    udp_socket.reply_to_caller(nowork_ticket)

    #Either success or failure; this can be closed.
    encp.close_descriptors(control_socket)

#Return the number of files in the list left to transfer.
def requests_outstanding(request_list):

    files_left = 0

    for request in request_list:
        completion_status = request.get('completion_status', None)
        if completion_status == None: # or completion_status == EOD: 
            files_left = files_left + 1

    return files_left

##############################################################################

#Update the ticket so that next file can be read.
def next_request_update(work_ticket, file_number, encp_intf):

    #Update the location cookie with the new file mark posistion.
    lc = "0000_000000000_%07d" % file_number
    
    #Clear this file information.
    work_ticket['fc']['bfid'] = None
    work_ticket['fc']['complete_crc'] = None
    work_ticket['fc']['deleted'] = None
    work_ticket['fc']['drive'] = None
    work_ticket['fc']['location_cookie'] = lc
    work_ticket['fc']['pnfs_mapname'] = None
    work_ticket['fc']['pnfs_name0'] = None
    work_ticket['fc']['pnfsid'] = None
    work_ticket['fc']['pnfsvid'] = None
    work_ticket['fc']['sanity_cookie'] = None
    work_ticket['fc']['size'] = None

    #Clear this information too.
    work_ticket['file_size'] = None
    work_ticket['bfid'] = None
    work_ticket['completion_status'] = None

    #If 'exfer' not deleted; it clobbers new data when returned from the mover.
    del work_ticket['exfer']

    # Update the tickets filename fields for the next file.
    work_ticket['infile'] = \
                     os.path.join(os.path.dirname(work_ticket['infile']), lc)
    work_ticket['wrapper']['pnfsFilename'] = work_ticket['infile']
    if work_ticket['outfile'] != "/dev/null":
        #If the outfile is /dev/null, don't change these.
        work_ticket['outfile'] = \
                     os.path.join(os.path.dirname(work_ticket['outfile']), lc)
        work_ticket['wrapper']['fullname'] = work_ticket['outfile']

    #Get the stat of the parent directory where the pnfs file will be placed.
    if not encp_intf.pnfs_is_automounted:
        stats = os.stat(os.path.dirname(work_ticket['infile']))
    else:
        # automaticall retry 6 times, one second delay each
        i = 0
        dirname = os.path.dirname(work_ticket['infile'])
        while i < 6:
            try:
                stats = os.stat(dirname)
                break
            except OSError:
                time.sleep(1)
                i = i + 1
        else:
            stats = os.stat(dirname)

    #Clear this wrapper information.
    work_ticket['wrapper']['inode'] = 0
    # Note: The math for fixing mode comes from pnfs.py in pstat_decode().
    work_ticket['wrapper']['mode'] = (stats[stat.ST_MODE] % 0777) | 0100000
    work_ticket['wrapper']['pstat'] = stats
    work_ticket['wrapper']['size_bytes'] = None

    #Update the unique id for the LM.
    #work_ticket['unique_id'] = encp.generate_unique_id()

    return work_ticket

#Return the next uncompleted transfer.
def get_next_request(request_list, e): #, filenumber = None):

    for i in range(len(request_list)):
        completion_status = request_list[i].get('completion_status', None)
        if completion_status == None:
            return request_list[i], i
    else:
        if e.list or not e.read_to_end_of_tape:
            return None, 0
        else:
            filenumber = encp.extract_file_number(request_list[-1]['fc']['location_cookie'])
            request = next_request_update(copy.deepcopy(request_list[0]),
                                          filenumber + 1, e)
            request_list.append(request)
            return request, (len(request_list) - 1)

##############################################################################

#
def finish_request(done_ticket, request_list, index, e):
    #Everything is fine.
    if e_errors.is_ok(done_ticket):

        #Tell the user what happend.
        Trace.message(e_errors.INFO,
               "File %s copied successfully." % done_ticket['infile'])
        Trace.log(e_errors.INFO,
               "File %s copied successfully." % done_ticket['infile'])
        #Remember the completed transfer.
        #files_transfered = files_transfered + 1

        #Set the metadata if it has not already been set.
        try:
            p = pnfs.Pnfs(done_ticket['infile'])
            p.get_bit_file_id()
        except (IOError, OSError, TypeError):
            Trace.message(TRANSFER_LEVEL, "Updating metadata for %s." %
                          done_ticket['infile'])
            set_metadata(done_ticket, e)

        #Set completion status to successful.
        done_ticket['completion_status'] = SUCCESS
        done_ticket['exit_status'] = 0

        #Store these changes back into the master list.
        request_list[index] = done_ticket

        #Get the next request before continueing.
        #request, index = get_next_request(request_list,e)
        #If the read mode is "read until end of data", we need to
        # create the new output file.
        #if request.get('completion_status', None) == "EOD":
        #if not e.list or e.read_to_end_of_tape:
        #    if not os.path.exists(request['outfile']):
        #        encp.create_zero_length_local_files(request)

        return CONTINUE
        #continue

    #The requested file does not exist on the tape.  (i.e. the
    # tape has only 6 files and the seventh file was requested.)
    elif done_ticket['status'] == (e_errors.READ_ERROR,
                                   e_errors.READ_EOD):

        if e.list:
            #Tell the user what happend.
            message = "File %s read failed: %s" % \
                      (done_ticket['infile'], done_ticket['status'])
            Trace.message(DONE_LEVEL, message)
            Trace.log(e_errors.ERROR, message)

            #Set completion status to failure.
            done_ticket['completion_status'] = FAILURE
            done_ticket['exit_status'] = 1
            #request['status'] = done_ticket['status']
            #exit_status = 1

            #Tell the calling process, this file failed.
            error_output(done_ticket)
        else:
            #Tell the user what happend.
            message = "Reached EOD at location %s." % \
                      (done_ticket['fc']['location_cookie'],)
            Trace.message(DONE_LEVEL, message)
            Trace.log(e_errors.INFO, message)

            #If --list was not used this is a success.
            done_ticket['completion_status'] = SUCCESS
            done_ticket['status'] = (e_errors.OK, None)
            done_ticket['exit_status'] = 0

        #Store these changes back into the master list.
        request_list[index] = done_ticket

        #We are done with this mover.
        #end_session(udp_socket, control_socket)
        #Tell the calling process, of those files not attempted.
        untried_output(request_list)

        #Perform any necessary file cleanup.
        #return request
        return STOP

    #Give up on this file.  If a persistant media problem occurs
    # skip this and go to the next file.
    elif done_ticket['status'][0] in [e_errors.POSITIONING_ERROR,
                                      e_errors.READ_ERROR,
                                      ]:
        #Tell the user what happend.
        message = "File %s read failed: %s" % \
                      (done_ticket['infile'], done_ticket['status'])
        Trace.message(DONE_LEVEL, message)
        Trace.log(e_errors.ERROR, message)

        #Set completion status to failure.
        done_ticket['completion_status'] = FAILURE
        done_ticket['exit_status'] = 1
        #done_ticket['status'] = done_ticket['status']
        #exit_status = 1
        #Store these changes back into the master list.
        request_list[index] = done_ticket

        #We are done with this mover.
        #end_session(udp_socket, control_socket)
        #Tell the calling process, this file failed.
        error_output(done_ticket)

        #break
        return CONTINUE_FROM_BEGINNING

    #Give up.
    elif e_errors.is_non_retriable(done_ticket['status'][0]):
        #Tell the user what happend.
        message = "File %s read failed: %s" % \
                  (done_ticket['infile'], done_ticket['status'])
        Trace.message(DONE_LEVEL, message)
        Trace.log(e_errors.ERROR, message)

        #Set completion status to failure.
        done_ticket['completion_status'] = FAILURE
        #request['status'] = done_ticket['status']
        done_ticket['exit_status'] = 2
        #exit_status = 2
        #Store these changes back into the master list.
        request_list[index] = done_ticket

        #We are done with this mover.
        #end_session(udp_socket, control_socket)
        #Tell the calling process, this file failed.
        error_output(done_ticket)
        #Tell the calling process, of those files not attempted.
        untried_output(request_list)

        #return done_ticket
        return STOP

    #Keep trying.
    elif e_errors.is_retriable(done_ticket['status'][0]):
        #On retriable error go back and resubmit what is left
        # to the LM.

        #Record the intermidiate error.
        Trace.log(e_errors.WARNING, "File %s read failed: %s" %
                      (done_ticket['infile'], done_ticket['status']))

        #We are done with this mover.
        #end_session(udp_socket, control_socket)

        #break
        return CONTINUE_FROM_BEGINNING

    ### Should never get here!!!
    return None

def readtape_from_hsm(e, tinfo):

    Trace.trace(16,"readtape_from_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" % (e.input, e.output, e.verbose,
                                      e.chk_crc, tinfo['encp_start_time']))
    
    #This needs to be defined somewhere.
    #files_transfered = 0
    bytes = 0L #Sum of bytes all transfered (when transfering multiple files).
    exit_status = 0 #Used to determine the final message text.
    number_of_files = 0 #Total number of files where a transfer was attempted.

    """
    #Get an ip and port to listen for the mover address for
    # routing purposes.
    udp_callback_addr, udp_socket = encp.get_udp_callback_addr(e)
    #If the socket does not exist, do not continue.
    if udp_socket.server_socket == None:
        done_ticket = {'exit_status' : 2,
                       'status':(e_errors.NET_ERROR,
                                 "Unable to obtain udp socket.")}
        return done_ticket
    """
    
    # Get the list of files to read.
    done_ticket, listen_socket, udp_socket, requests_per_vol = \
                 encp.prepare_read_from_hsm(tinfo, e)

    if not e_errors.is_ok(done_ticket):
        pprint.pprint(done_ticket)
        #Tell the calling process, this file failed.
        error_output(done_ticket)
        #Tell the calling process, of those files not attempted.
        untried_output(requests_per_vol)
        
        done_ticket['exit_status'] = 2
        return done_ticket
    
    ######################################################################
    # Time to start reading some files.
    ######################################################################

    vols = requests_per_vol.keys()
    vols.sort()
    for vol in vols:
        request_list = requests_per_vol[vol]
        number_of_files = number_of_files + len(request_list)

        #Get the next volume in the list to transfer.
        request, index = get_next_request(request_list, e)

        #If the read mode is "read until end of data", we need to
        # create the new output file.
        if not e.list or e.read_to_end_of_tape:
            if not os.path.exists(request['outfile']):
                encp.create_zero_length_local_files(request)

        #Submit the request to the library manager.
        submitted, reply_ticket = encp.submit_read_requests([request], e)
        Trace.message(TRANSFER_LEVEL, "Read tape submission sent to LM.")

        if not e_errors.is_ok(reply_ticket):
            #Tell the calling process, this file failed.
            error_output(reply_ticket)
            #Tell the calling process, of those files not attempted.
            untried_output(request_list)

            reply_ticket['exit_status'] = 2
            return reply_ticket
        if submitted != 1:
            request['status'] = (e_errors.UNKNOWN,
                                 "Unknown failure submitting request for " \
                                 "file %s on volume %s." % \
                                 (request['infile'], vol))

            #Tell the calling process, this file failed.
            error_output(reply_ticket)
            #Tell the calling process, of those files not attempted.
            untried_output(request_list)

            request['exit_status'] = 2
            return request


        while requests_outstanding(request_list):

            # Establish control socket connection with the mover.
            control_socket, ticket = mover_handshake(listen_socket, udp_socket,
                                                     request, e)

            # Verify that everything went ok with the handshake.
            external_label = request.get('fc', {}).get('external_label', None)
            result_dict = encp.handle_retries([request], request,
                                              ticket, e,
                                              external_label = external_label)

            if not e_errors.is_ok(result_dict):
                # Close these descriptors before they are forgotten about.
                if control_socket != None:
                    encp.close_descriptors(control_socket)
                #Don't loose the non-retriable error.
                if e_errors.is_non_retriable(result_dict):
                    request = encp.combine_dict(result_dict, request)
                    return request

                continue

            #maybe this isn't a good idea...
            request = encp.combine_dict(ticket, request)
                      
            use_unique_id = request['unique_id']

            # Keep looping until the READ_EOD error occurs.
            while requests_outstanding(request_list):

                #Flush out the standard output and error descriptors.  This
                # should help in some cases when they are redirected to a file
                # and the bytes get stuck in buffer(s).
                sys.stdout.flush()
                sys.stderr.flush()

                #Get the next file in the list to transfer.
                request, index = get_next_request(request_list, e)

                #Grab a new clean udp_socket.
                ### Note: This should not be necessary after a bug in the
                ### mover was fixed long ago.
                #udp_callback_addr, unused = encp.get_udp_callback_addr(
                #    e, udp_socket)

                #Combine the ticket from the mover with the current
                # information.  Remember the ealier dictionaries 'win'
                # in setting values.  encp.combine_dict() is insufficent
                # for this dictionary munge.  It must be done by hand
                # because both tickets have correct pieces of information
                # that is old in the other ticket.
                request['mover'] = ticket['mover']
                request['callback_addr'] = listen_socket.getsockname()
                #The ticket item of 'routing_callback_addr' is a legacy name.
                request['routing_callback_addr'] = \
                                             udp_socket.get_server_address()
                #Encp create_read_request() gives each file a new unique id.
                # The LM can't deal with multiple mover file requests from one
                # LM request.  Thus, we need to set this back to the last
                # unique id sent to the library manager.
                request['unique_id'] = use_unique_id
                #Store these changes back into the master list.
                requests_per_vol[vol][index] = request

                message = "Preparing to read %s." % (request['infile'],)
                Trace.message(TRANSFER_LEVEL, message)
                Trace.log(e_errors.INFO, message)

                data_path_socket, done_ticket = mover_handshake2(request,
                                                                 udp_socket, e)
                #Give up.
                if e_errors.is_non_retriable(done_ticket['status'][0]):
                    #Tell the user what happend.
                    message = "File %s read failed: %s" % \
                              (request['infile'], done_ticket['status'])
                    Trace.message(DONE_LEVEL, message)
                    Trace.log(e_errors.ERROR, message)

                    #We are done with this mover.
                    end_session(udp_socket, control_socket)
                    #Set completion status to failure.
                    request['completion_status'] = FAILURE
                    request['status'] = done_ticket['status']
                    request['exit_status'] = 2

                    #Tell the calling process, this file failed.
                    error_output(request)
                    #Tell the calling process, of those files not attempted.
                    untried_output(requests_per_vol[vol])
                    #Perform any necessary file cleanup.
                    return request

                #Keep trying.
                elif e_errors.is_retriable(done_ticket['status'][0]):
                    #On retriable error go back and resubmit what is left
                    # to the LM.

                    #Record the intermidiate error.
                    message = "File %s read failed: %s" % \
                              (request['infile'], done_ticket['status'])
                    Trace.log(e_errors.WARNING, message)

                    #We are done with this mover.
                    end_session(udp_socket, control_socket)
                    break

                #############################################################
                #In this function call is where most of the work in transfering
                # a single file is done.

                done_ticket = encp.read_hsm_file(request, control_socket,
                                                 data_path_socket, [request],
                                                 tinfo, e,
                                                 udp_socket = udp_socket)
                #############################################################

                # Close these descriptors before they are forgotten about.
                encp.close_descriptors(data_path_socket)

                #Sum up the total amount of bytes transfered.
                exfer_ticket = done_ticket.get('exfer',
                                               {'bytes_transfered' : 0L})
                bytes = bytes + exfer_ticket.get('bytes_transfered', 0L)

                #Combine the tickets.
                request = encp.combine_dict(done_ticket, request)
                #Store these changes back into the master list.
                requests_per_vol[vol][index] = request

                #The completion_status is modified in the request ticket.
                # what_to_do = 0 for stop
                #            = 1 for continue
                #            = 2 for continue after retry
                what_to_do = finish_request(request, requests_per_vol[vol],
                                            index, e)

                #If on non-success exit status was returned from
                # finish_request(), keep it around for later.
                if request['exit_status']:
                    #We get here only on an error.  If the value is 1, then
                    # the error should be transient.  If the value is 2, then
                    # the error will likely require human intervention to
                    # resolve.
                    exit_status = request['exit_status']
                # Do what finish_request() says to do.
                if what_to_do == STOP:
                    #We get here only on a non-retriable error.
                    end_session(udp_socket, control_socket)
                    return done_ticket
                elif what_to_do == CONTINUE_FROM_BEGINNING:
                    #We get here only on a retriable error.
                    end_session(udp_socket, control_socket)
                    break
                
                """
                #Everything is fine.
                if e_errors.is_ok(done_ticket):

                    #Tell the user what happend.
                    Trace.message(e_errors.INFO,
                           "File %s copied successfully." % request['infile'])
                    Trace.log(e_errors.INFO,
                           "File %s copied successfully." % request['infile'])
                    #Remember the completed transfer.
                    #files_transfered = files_transfered + 1

                    #Set the metadata if it has not already been set.
                    try:
                        p = pnfs.Pnfs(request['infile'])
                        p.get_bit_file_id()
                    except (IOError, OSError, TypeError):
                        Trace.message(5, "Updating metadata for %s." %
                                      request['infile'])
                        set_metadata(request, e)

                    #Set completion status to successful.
                    request['completion_status'] = SUCCESS
                    request['exit_status'] = 0

                    #Store these changes back into the master list.
                    requests_per_vol[vol][index] = request
                    #Get the next request before continueing.
                    request, index = get_next_request(requests_per_vol[vol],e)

                    #If the read mode is "read until end of data", we need to
                    # create the new output file.
                    #if request.get('completion_status', None) == "EOD":
                    if not e.list or e.read_to_end_of_tape:
                        if not os.path.exists(request['outfile']):
                            encp.create_zero_length_local_files(request)

                    continue

                #The requested file does not exist on the tape.  (i.e. the
                # tape has only 6 files and the seventh file was requested.)
                elif done_ticket['status'] == (e_errors.READ_ERROR,
                                               e_errors.READ_EOD):

                    if e.list:
                        #Tell the user what happend.
                        message = "File %s read failed: %s" % \
                                  (request['infile'], done_ticket['status'])
                        Trace.message(1, message)
                        Trace.log(e_errors.ERROR, message)

                        #Set completion status to failure.
                        request['completion_status'] = FAILURE
                        request['status'] = done_ticket['status']
                        request['exit_status'] = 1
                        exit_status = 1

                        #Tell the calling process, this file failed.
                        error_output(request)
                    else:
                        #Tell the user what happend.
                        message = "Reached EOD at location %s." % \
                                  (request['fc']['location_cookie'],)
                        Trace.message(1, message)
                        Trace.log(e_errors.INFO, message)

                        #If --list was not used this is a success.
                        request['completion_status'] = SUCCESS
                        request['status'] = (e_errors.OK, None)
                        request['exit_status'] = 0

                    #We are done with this mover.
                    end_session(udp_socket, control_socket)
                    #Tell the calling process, of those files not attempted.
                    untried_output(requests_per_vol[vol])

                    #Perform any necessary file cleanup.
                    return request

                #Give up on this file.  If a persistant media problem occurs
                # skip this and go to the next file.
                elif done_ticket['status'][0] in [e_errors.POSITIONING_ERROR,
                                                  e_errors.READ_ERROR,
                                                  ]:
                    #Tell the user what happend.
                    message = "File %s read failed: %s" % \
                                  (request['infile'], done_ticket['status'])
                    Trace.message(1, message)
                    Trace.log(e_errors.ERROR, message)

                    #Set completion status to failure.
                    request['completion_status'] = FAILURE
                    request['status'] = done_ticket['status']
                    request['exit_status'] = 1
                    exit_status = 1
                    #Store these changes back into the master list.
                    requests_per_vol[vol][index] = request

                    #We are done with this mover.
                    end_session(udp_socket, control_socket)
                    #Tell the calling process, this file failed.
                    error_output(request)

                    break

                #Give up.
                elif e_errors.is_non_retriable(done_ticket['status'][0]):
                    #Tell the user what happend.
                    message = "File %s read failed: %s" % \
                              (request['infile'], done_ticket['status'])
                    Trace.message(1, message)
                    Trace.log(e_errors.ERROR, message)
                    
                    #Set completion status to failure.
                    request['completion_status'] = FAILURE
                    request['status'] = done_ticket['status']
                    request['exit_status'] = 2
                    exit_status = 2
                    #Store these changes back into the master list.
                    requests_per_vol[vol][index] = request

                    #We are done with this mover.
                    end_session(udp_socket, control_socket)
                    #Tell the calling process, this file failed.
                    error_output(request)
                    #Tell the calling process, of those files not attempted.
                    untried_output(requests_per_vol[vol])

                    return request
                    
                #Keep trying.
                elif e_errors.is_retriable(done_ticket['status'][0]):
                    #On retriable error go back and resubmit what is left
                    # to the LM.

                    #Record the intermidiate error.
                    Trace.log(e_errors.WARNING, "File %s read failed: %s" %
                                  (request['infile'], done_ticket['status']))

                    #We are done with this mover.
                    end_session(udp_socket, control_socket)

                    break
                """
        else:
            #If we get here, then, we should have a success.
            end_session(udp_socket, control_socket)

    # we are done transferring - close out the listen socket
    encp.close_descriptors(listen_socket)

    #Print to screen the exit status.
    Trace.message(TO_GO_LEVEL, "EXIT STATUS: %d" % exit_status)

    #Finishing up with a few of these things.
    calc_ticket = encp.calculate_final_statistics(bytes, number_of_files,
                                                  exit_status, tinfo)

    #Volume one ticket is the last request that was processed.
    if e.data_access_layer:
        list_done_ticket = encp.combine_dict(calc_ticket, done_ticket)
    else:
        list_done_ticket = encp.combine_dict(calc_ticket, {})

    Trace.message(TICKET_LEVEL, "LIST DONE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(list_done_ticket))

    return list_done_ticket

##############################################################################
##############################################################################

def main(intf):
    #Snag the start time.  t0 is needed by the mover, but its name conveys
    # less meaning.
    t0 = time.time()
    tinfo = {'t0' : t0, 'encp_start_time' : t0}

    #Initialize the Trace module.
    Trace.init("GET")
    for x in xrange(6, intf.verbose + 1):
        Trace.do_print(x)
    for x in xrange(1, intf.verbose + 1):
        Trace.do_message(x)

    #Some globals are expected to exists for normal operation (i.e. a logger
    # client).  Create them.
    status_ticket = encp.clients(intf)
    if not e_errors.is_ok(status_ticket):
        return encp.final_say(intf, status_ticket)

    #Log/print the starting encp information.  This depends on the log
    # from the clients() call, thus it should always be after clients().
    # This function should never give a fatal error.
    encp.log_encp_start(tinfo, intf)

    if intf.data_access_layer:
        #global data_access_layer_requested
        encp.data_access_layer_requested = intf.data_access_layer
        #data_access_layer_requested.set()
    elif intf.verbose < 0:
        #Turn off all output to stdout and stderr.
        encp.data_access_layer_requested = -1
        intf.verbose = 0
        
    done_ticket = readtape_from_hsm(intf, tinfo)

    return encp.final_say(intf, done_ticket)
    

def do_work(intf):
    delete_at_exit.setup_signal_handling()

    try:
        exit_status = main(intf)
	halt(exit_status)
    except (SystemExit, KeyboardInterrupt):
	halt(1)
    except:
        #Get the uncaught exception.
        exc, msg, tb = sys.exc_info()
        ticket = {'status' : (e_errors.UNCAUGHT_EXCEPTION,
                              "%s: %s" % (str(exc), str(msg)))}

        #Print the data access layer and send the information to the
        # accounting server (if possible).
        encp.print_data_access_layer_format(None, None, None, ticket)
        #Send to the log server the traceback dump.  If unsuccessful,
        # print the traceback to standard error.
        Trace.handle_error(exc, msg, tb)
        del tb #No cyclic references.
        #Remove any zero-length files left haning around.  Also, return
        # a non-zero exit status to the calling program/shell.
        halt(1)

if __name__ == '__main__':

    setup_get_interface()

    #First handle an incorrect command line.
    #if len(sys.argv) < 4:
    #    intf_of_encp = encp.EncpInterface(sys.argv, 1) #one = user
    #    intf_of_encp.print_usage()

    #intf_of_encp = encp.EncpInterface(sys.argv[:-3] + sys.argv[-2:], 0)
    intf_of_encp = encp.EncpInterface(sys.argv, 0)
    #intf_of_encp.volume = sys.argv[-3] #Hackish
    #intf_of_encp.argv = sys.argv[:] #Hackish

    """
    if not enstore_functions3.is_volume(sys.argv[-3]):
        try:
            sys.stderr.write("First argument is not a volume name.\n")
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)
    """

    if intf_of_encp.volume and intf_of_encp.read_to_end_of_tape and \
       ( not os.path.exists(sys.argv[-2]) or not os.path.isdir(sys.argv[-2]) \
         or not pnfs.is_pnfs_path(sys.argv[-2]) ):
        try:
            sys.stderr.write("Second argument is not an input directory.\n")
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)

    if sys.argv[-1] == "/dev/null":
        pass  #If the output is /dev/null, this is okay.
    elif not os.path.exists(sys.argv[-1]) or not os.path.isdir(sys.argv[-1]):
        try:
            sys.stderr.write("Third argument is not an output directory.\n")
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)

    #print encp.format_class_for_print(intf_of_encp, "intf_of_encp")
    
    do_work(intf_of_encp)

