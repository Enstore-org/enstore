#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
from future.utils import raise_
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
import enstore_functions3
import callback

# Completion status field values.
SUCCESS = encp.SUCCESS  # "SUCCESS"
FAILURE = encp.FAILURE  # "FAILURE"

# Return values to know if get should stop or keep going.
CONTINUE_FROM_BEGINNING = encp.CONTINUE_FROM_BEGINNING
CONTINUE = encp.CONTINUE
STOP = encp.STOP

DONE_LEVEL = encp.DONE_LEVEL
ERROR_LEVEL = encp.ERROR_LEVEL
TRANSFER_LEVEL = encp.TRANSFER_LEVEL
TO_GO_LEVEL = encp.TO_GO_LEVEL
INFO_LEVEL = encp.INFO_LEVEL
CONFIG_LEVEL = encp.CONFIG_LEVEL
TIME_LEVEL = encp.TIME_LEVEL
TICKET_LEVEL = encp.TICKET_LEVEL
TICKET_1_LEVEL = encp.TICKET_1_LEVEL


def get_client_version():
    # this gets changed automatically in {enstore,encp}Cut
    # You can edit it manually, but do not change the syntax
    version_string = "v1_53  CVS $Revision$ "
    get_file = globals().get('__file__', "")
    if get_file:
        version_string = version_string + get_file
    return version_string

#encp.encp_client_version = get_client_version


class GetInterface(encp.EncpInterface):
    #  define our specific parameters
    user_parameters = [
        "--volume <volume> <destination dir>",
        "--read-to-end-of-tape --volume <volume> <source dir> <destination dir>",
        "<source file> [source file [...]] <destination directory>"
    ]
    admin_parameters = user_parameters
    parameters = user_parameters  # gets overridden in __init__().

    def __init__(self, args=sys.argv, user_mode=0):

        # Get a copy, so we don't modifiy encp's interface class too.
        # This is an issue only if migration:
        # 1) uses get for reads and encp for writes
        # 2) uses encp for reads and put for writes
        # 3) uses get for reads and put for writes
        # If migration uses encp for read and writes there is not a conflict.
        self.encp_options = copy.deepcopy(self.encp_options)

        self.encp_options[option.LIST] = {
            option.HELP_STRING: "Takes in a filename of a file containing a list "
                                "of locations and filenames.",
            option.VALUE_USAGE: option.REQUIRED,
            option.VALUE_TYPE: option.STRING,
            option.VALUE_LABEL: "name_of_list_file",
            option.USER_LEVEL: option.USER, }
        self.encp_options[option.SEQUENTIAL_FILENAMES] = {
            option.HELP_STRING: "Override known filenames and use sequentially "
                                "numbered filenames.",
            option.VALUE_USAGE: option.IGNORED,
            option.VALUE_TYPE: option.INTEGER,
            option.USER_LEVEL: option.USER, }
        self.encp_options[option.SKIP_DELETED_FILES] = {
            option.HELP_STRING: "Skip over deleted files.",
            option.VALUE_USAGE: option.IGNORED,
            option.VALUE_TYPE: option.INTEGER,
            option.USER_LEVEL: option.USER, }
        self.encp_options[option.READ_TO_END_OF_TAPE] = {
            option.HELP_STRING: "After the last file known is read keep reading "
                                "until EOD or EOT.",
            option.VALUE_USAGE: option.IGNORED,
            option.VALUE_TYPE: option.INTEGER,
            option.USER_LEVEL: option.USER, }
        try:
            del self.encp_options[option.EPHEMERAL]
        except KeyError:
            pass
        try:
            del self.encp_options[option.FILE_FAMILY]
        except KeyError:
            pass

        self.list = None                # Used for "get" only.
        self.skip_deleted_files = None  # Used for "get" only.
        self.read_to_end_of_tape = None  # Used for "get" only.

        encp.EncpInterface.__init__(self, args=args, user_mode=user_mode)

        self.get = 1

        if self.volume and self.read_to_end_of_tape and \
            (not os.path.exists(self.args[-2]) \
                # or not os.path.isdir(self.args[-2]) \
             or not pnfs.is_pnfs_path(self.args[-2])):
            try:
                message = "Second to last argument is not an input file or directory.\n"
                sys.stderr.write(message)
                sys.stderr.flush()
            except IOError:
                pass
            sys.exit(1)

        if self.args[-1] == "/dev/null":
            pass  # If the output is /dev/null, this is okay.
        elif not os.path.exists(self.args[-1]) or not os.path.isdir(self.args[-1]):
            try:
                message = "Last argument is not an output directory.\n"
                sys.stderr.write(message)
                sys.stderr.flush()
            except IOError:
                pass
            sys.exit(1)


def error_output(request):
    # Get the info.
    lc = request.get('fc', {}).get("location_cookie", None)
    file_number = enstore_functions3.extract_file_number(lc)
    message = request.get("status", (e_errors.UNKNOWN, None))
    # Format the output.
    msg = "error_output %s %s\n" % (file_number, message)
    # Print the output.
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

    # Turn a list of requests for a single volume, into a dictionary
    # of request lists based on volume.
    if isinstance(requests, list):
        requests = {'': requests}

    for request_list in requests.values():
        for request in request_list:
            # For each item in the list, print this if it was not tried.
            if request.get('completion_status', None) is None:
                request['status'] = (e_errors.UNKNOWN,
                                     "File transfer not attempted.")
                error_output(request)


def mover_handshake_new(listen_socket, work_ticket, encp_intf):
    use_listen_socket = listen_socket
    ticket = {}

    message = "Listening for control socket at: %s" \
              % str(listen_socket.getsockname())
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    start_time = time.time()
    while time.time() < start_time + encp_intf.resubmit_timeout:
        # Attempt to get the control socket connected with the mover.
        duration = max(
            start_time +
            encp_intf.resubmit_timeout -
            time.time(),
            0)
        try:
            control_socket, mover_address, ticket = \
                encp.open_control_socket(
                    use_listen_socket, duration)
        except (socket.error, select.error, encp.EncpError) as msg:

            if msg.args[0] == errno.EINTR or msg.args[0] == errno.EAGAIN:
                # If a select (or other call) was interupted,
                # this is not an error, but should continue.
                continue
            elif msg.args[0] == errno.ETIMEDOUT:
                # Setting the error to RESUBMITTING is important.  If this is
                # not done, then it would be returned as ETIMEDOUT.
                # ETIMEDOUT is a retriable error; meaning it would retry
                # the request to the LM, but it will fail since the ticket only
                # contains the 'status' field (as set below).  When
                # handle_retries() is called after mover_handshake() by
                # having the error be RESUBMITTING, encp will resubmit all
                # pending requests (instead of failing on retrying one
                # request).
                ticket['status'] = (e_errors.RESUBMITTING, None)
            elif hasattr(msg, "type"):
                ticket['status'] = (msg.type, str(msg))
            else:
                ticket['status'] = (e_errors.NET_ERROR, str(msg))

            # Combine the dictionaries (if possible).
            if getattr(msg, 'ticket', None) is not None:
                # Do the initial munge.
                ticket = encp.combine_dict(ticket, msg.ticket, work_ticket)

            # Since an error occured, just return it.
            return None, ticket

        Trace.message(TICKET_LEVEL, "MOVER HANDSHAKE (CONTROL):")
        Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
        # Recored the receiving of the first control socket message.
        message = "Received callback ticket from mover %s for transfer %s." % \
                  (ticket.get('mover', {}).get('name', "Unknown"),
                   ticket.get('unique_id', "Unknown")) + encp.elapsed_string()
        Trace.message(INFO_LEVEL, message)
        Trace.log(e_errors.INFO, message)

        # verify that the id is one that we are excpeting and not one that got
        # lost in the ether.
        if ticket['unique_id'] != work_ticket['unique_id']:
            encp.close_descriptors(control_socket)
            Trace.log(e_errors.INFO,
                      "mover handshake: mover impostor called back"
                      "   mover address: %s   got id: %s   expected: %s"
                      "   ticket=%s" %
                      (mover_address, ticket['unique_id'],
                       work_ticket['unique_id'], ticket))
            continue

        # ok, we've been called back with a matched id - how's the status?
        if not e_errors.is_ok(ticket['status']):
            return None, ticket

        return control_socket, ticket


def mover_handshake_original(listen_socket, udp_serv, work_tickets, encp_intf):

    control_socket, ticket = encp.mover_handshake_part1(
        listen_socket, udp_serv, work_tickets, encp_intf)

    if not e_errors.is_ok(ticket):
        return None, ticket

    """

    ticket = {}
    #config = host_config.get_config()
    unique_id_list = []
    for work_ticket in work_list:
        unique_id_list.append(work_ticket['unique_id'])

    ##################################################################
    #This udp_server code is depricated.
    ##################################################################
    #Grab a new clean udp_socket.
    ###udp_callback_addr, unused = encp.get_udp_callback_addr(encp_intf,
    ###                                                       udp_serv)
    #The ticket item of 'routing_callback_addr' is a legacy name.
    ###request['routing_callback_addr'] = udp_callback_addr

    #Open the routing socket.
    if udp_serv:
        try:

	    Trace.message(TRANSFER_LEVEL, "Opening udp socket.")
	    Trace.log(e_errors.INFO, "Opening udp socket.")
	    Trace.log(e_errors.INFO,
		      "Listening for udp message at: %s." % \
		      str(udp_serv.server_socket.getsockname()))

            #Keep looping until one of these two messages arives.
            # Ignore any other that my be received.
            uticket = encp.open_udp_socket(udp_serv,
                                           unique_id_list,
                                           encp_intf)

            #If requested output the raw message.
            Trace.message(TICKET_LEVEL, "RTICKET MESSAGE:")
            Trace.message(TICKET_LEVEL, pprint.pformat(uticket))

	    if not e_errors.is_ok(uticket):
		#Log the error.
		Trace.log(e_errors.ERROR,
			  "Unable to connect udp socket: %s" %
			  (str(uticket['status'])))
		return None, uticket

	    Trace.message(TRANSFER_LEVEL, "Opened udp socket.")
	    Trace.log(e_errors.INFO, "Opened udp socket.")
        except (encp.EncpError,), detail:
            if getattr(detail, "errno", None) == errno.ETIMEDOUT:
	        #Handle retries needs to be called to update various values
                # and to perfrom the resubmition itself.
                ticket['status'] = (e_errors.RESUBMITTING, None)
            else:
	        #Handle retries needs to be called to update various values
                # and to perfrom the resubmition itself.
                ticket['status'] = (detail.type, str(detail))

	    #Log the error.
            Trace.log(e_errors.ERROR, "Unable to connect udp socket: %s" %
                      (str(ticket['status']),))
            return None, ticket

        #Print out the final ticket.
        Trace.message(TICKET_LEVEL, "UDP TICKET:")
        Trace.message(TICKET_LEVEL, pprint.pformat(uticket))
    ##################################################################
    #End of depricated udp_server code.
    ##################################################################

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
                    udp_serv.reply_to_caller(uticket)
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
	    return None, ticket

    except (encp.EncpError,), detail:
	if getattr(detail, "errno", None) == errno.ETIMEDOUT:
	    #Handle retries needs to be called to update various values
	    # and to perfrom the resubmition itself.
	    ticket['status'] = (e_errors.RESUBMITTING, ticket['unique_id'])
	else:
	    #Handle retries needs to be called to update various values
	    # and to perfrom the resubmition itself.
	    ticket['status'] = (detail.type, str(detail))

	#Log the error.
	Trace.log(e_errors.ERROR, "Unable to connect control socket: %s" %
		  (str(ticket['status']),))
	return None, ticket

    """
    # Print out the final ticket.
    Trace.message(TICKET_LEVEL, "MOVER HANDSHAKE (CONTROL):")
    Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
    # Recored the receiving of the first control socket message.
    message = "Received callback ticket from mover %s for transfer %s." % \
              (ticket.get('mover', {}).get('name', "Unknown"),
               ticket.get('unique_id', "Unknown"))
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    """
    #Compare expected unique id with the returned unique id.
    if ticket.get('unique_id', None) not in unique_id_list:
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
    """

    # Keep the udp socket queues clear, while waiting for the mover
    # ready message.
    start_time = time.time()
    Trace.message(TRANSFER_LEVEL, "Waiting for mover ready message.")
    Trace.log(e_errors.INFO, "Waiting for mover ready message.")
    while time.time() < start_time + encp_intf.mover_timeout:
        # Keep looping until the message arives.
        mover_ready = udp_serv.do_request()

        # If requested output the raw message.
        Trace.trace(11, "UDP MOVER READY MESSAGE:")
        Trace.trace(11, pprint.pformat(mover_ready))

        # Make sure the messages are what we expect.
        if mover_ready is None:
            continue  # Something happened, keep trying.
        elif mover_ready.get('work', None) == "mover_idle":
            break
        elif isinstance(mover_ready.get('work', None), bytes):
            continue  # Something happened, keep trying.
        else:
            break
    else:
        # We timed out.  Handle the error.
        error_ticket = {'status': (e_errors.RESUBMITTING, None)}
        mover_ready = encp.handle_retries([ticket], ticket,
                                          error_ticket, encp_intf)

    Trace.message(TRANSFER_LEVEL, "Received mover ready message.")
    Trace.log(e_errors.INFO, "Received mover ready message.")

    if not e_errors.is_ok(mover_ready):
        ticket = encp.combine_dict(mover_ready, ticket)

    return control_socket, ticket


def mover_handshake2_new(control_socket, work_ticket, e):

    prepare_for_transfer_start_time = time.time()

    try:
        callback.read_tcp_obj(control_socket, work_ticket)

        # Output the info.
        message = "Sent file request to mover." + encp.elapsed_string()
        Trace.log(e_errors.INFO, message)
        Trace.message(TRANSFER_LEVEL, message)
    except (select.error, socket.error) as msg:
        work_ticket['status'] = (e_errors.NET_ERROR, str(msg))
        return None, work_ticket
        # Looks as the code below is broken (wrong indentation, but will not fix it now
    # except e_errors.TCP_EXCEPTION as msg:
    #    work_ticket['status'] = (e_errors.TCP_EXCEPTION, str(msg))
    #    return None, work_ticket

        try:
            mover_addr = work_ticket['mover']['callback_addr']
        except KeyError:
            msg = sys.exc_info()[1]
            try:
                sys.stderr.write("Sub ticket 'mover' not found.\n")
                sys.stderr.write("%s: %s\n" % (e_errors.KEYERROR, str(msg)))
                sys.stderr.write(pprint.pformat(work_ticket) + "\n")
                sys.stderr.flush()
            except IOError:
                pass
            if e_errors.is_ok(work_ticket.get('status', (None, None))):
                work_ticket['status'] = (e_errors.KEYERROR, str(msg))
            return None, work_ticket

        # Set the route that the data socket will use.
        try:
            # There is no need to do this on a non-multihomed machine.
            config = host_config.get_config()
            if config and config.get('interface', None):
                local_intf_ip = encp.open_routing_socket(mover_addr[0], e)
            else:
                local_intf_ip = work_ticket['callback_addr'][0]
        except (encp.EncpError,) as msg:
            work_ticket['status'] = (e_errors.EPROTO, str(msg))
            return None, work_ticket

        Trace.message(TRANSFER_LEVEL, "Opening the data socket.")
        Trace.log(e_errors.INFO, "Opening the data socket.")

        # Open the data socket.
        try:
            data_path_socket = encp.open_data_socket(mover_addr, local_intf_ip)

            if not data_path_socket:
                raise socket.error(errno.ENOTCONN,
                                   errno.errorcode[errno.ENOTCONN])

            work_ticket['status'] = (e_errors.OK, None)
            # We need to specifiy which interface will be used on the encp
            # side.
            work_ticket['encp_ip'] = data_path_socket.getsockname()[0]
            Trace.message(TRANSFER_LEVEL, "Opened the data socket.")
            Trace.log(e_errors.INFO, "Opened the data socket.")
        except (encp.EncpError, socket.error) as detail:
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
            # Log the error.
            Trace.log(e_errors.ERROR, "Unable to connect data socket: %s %s" %
                      (str(work_ticket['status']), str(result_dict['status'])))

            # Don't loose the non-retirable error.
            if e_errors.is_non_retriable(result_dict):
                work_ticket = encp.combine_dict(result_dict, work_ticket)
            # Close these descriptors before they are forgotten about.
            # encp.close_descriptors(out_fd)

            return None, work_ticket

        message = "Data socket is connected to mover %s for %s. " % \
            (work_ticket.get('mover', {}).get('name', "Unknown"),
             work_ticket.get('unique_id', "Unknown"))
        Trace.message(TRANSFER_LEVEL, message)
        Trace.log(e_errors.INFO, message)

        Trace.message(TIME_LEVEL, "Time to prepare for transfer: %s sec." %
                      (time.time() - prepare_for_transfer_start_time,))

        return data_path_socket, work_ticket


def mover_handshake2_original(work_ticket, udp_socket, e):
    prepare_for_transfer_start_time = time.time()

    # This is an evil hack to modify work_ticket outside of
    # create_read_requests().
    work_ticket['method'] = "read_next"
    # Grab a new clean udp_socket.
    udp_callback_addr, unused = encp.get_udp_callback_addr(e, udp_socket)
    # The ticket item of 'routing_callback_addr' is a legacy name.
    work_ticket['routing_callback_addr'] = udp_callback_addr

    # Record the event of sending the request to the mover.
    message = "Sending file %s request to the mover." % \
        enstore_functions3.extract_file_number(
            work_ticket['fc']['location_cookie'])
    Trace.message(TRANSFER_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    # Record this for posterity, if requested.
    Trace.message(TICKET_LEVEL, "MOVER_REQUEST_SUBMISSION:")
    Trace.message(TICKET_LEVEL, pprint.pformat(work_ticket))

    # Send the actual request to the mover.
    udp_socket.reply_to_caller(work_ticket)

    try:
        mover_addr = work_ticket['mover']['callback_addr']
    except KeyError:
        msg = sys.exc_info()[1]
        try:
            sys.stderr.write("Sub ticket 'mover' not found.\n")
            sys.stderr.write("%s: %s\n" % (e_errors.KEYERROR, str(msg)))
            sys.stderr.write(pprint.pformat(work_ticket) + "\n")
            sys.stderr.flush()
        except IOError:
            pass
        if e_errors.is_ok(work_ticket.get('status', (None, None))):
            work_ticket['status'] = (e_errors.KEYERROR, str(msg))
        return None, work_ticket

    # Set the route that the data socket will use.
    try:
        # There is no need to do this on a non-multihomed machine.
        config = host_config.get_config()
        if config and config.get('interface', None):
            local_intf_ip = encp.open_routing_socket(mover_addr[0], e)
        else:
            local_intf_ip = work_ticket['callback_addr'][0]
    except (encp.EncpError,) as msg:
        work_ticket['status'] = (e_errors.EPROTO, str(msg))
        return None, work_ticket

    Trace.message(TRANSFER_LEVEL, "Opening the data socket.")
    Trace.log(e_errors.INFO, "Opening the data socket.")

    # Open the data socket.
    try:
        data_path_socket = encp.open_data_socket(mover_addr, local_intf_ip)

        if not data_path_socket:
            raise socket.error(errno.ENOTCONN,
                               errno.errorcode[errno.ENOTCONN])

        work_ticket['status'] = (e_errors.OK, None)
        # We need to specifiy which interface will be used on the encp side.
        work_ticket['encp_ip'] = data_path_socket.getsockname()[0]
        Trace.message(TRANSFER_LEVEL, "Opened the data socket.")
        Trace.log(e_errors.INFO, "Opened the data socket.")
    except (encp.EncpError, socket.error) as detail:
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
        # Log the error.
        Trace.log(e_errors.ERROR, "Unable to connect data socket: %s %s" %
                  (str(work_ticket['status']), str(result_dict['status'])))

        # Don't loose the non-retirable error.
        if e_errors.is_non_retriable(result_dict):
            work_ticket = encp.combine_dict(result_dict, work_ticket)
        # Close these descriptors before they are forgotten about.
        # encp.close_descriptors(out_fd)

        return None, work_ticket

    message = "Data socket is connected to mover %s for %s. " % \
        (work_ticket.get('mover', {}).get('name', "Unknown"),
         work_ticket.get('unique_id', "Unknown"))
    Trace.message(TRANSFER_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    Trace.message(TIME_LEVEL, "Time to prepare for transfer: %s sec." %
                  (time.time() - prepare_for_transfer_start_time,))

    return data_path_socket, work_ticket


mover_handshake = mover_handshake_original
# encp.mover_handshake = mover_handshake  #For encp.wait_for_message().
mover_handshake2 = mover_handshake2_original


def set_metadata(ticket, intf):

    # Set these now so the metadata can be set correctly.
    ticket['wrapper']['fullname'] = ticket['outfilepath']
    ticket['wrapper']['pnfsFilename'] = ticket['infilepath']
    try:
        ticket['file_size'] = ticket['exfer'].get("bytes", 0)
    except BaseException:
        try:
            sys.stderr.write("Unexpected error setting metadata.\n")
            sys.stderr.write(pprint.pformat(ticket))
            sys.stderr.flush()
        except IOError:
            pass
        exc, msg, tb = sys.exc_info()
        raise_(exc, msg, tb)

    # Create the pnfs file.
    try:
        #encp.create_zero_length_files(ticket['infile'], raise_error = 1)
        encp.create_zero_length_pnfs_files(ticket['infile'])
        Trace.log(
            e_errors.INFO,
            "Pnfs file created for %s." %
            ticket['infilepath'])
    except OSError as detail:
        msg = "Pnfs file create failed for %s: %s" % (ticket['infilepath'],
                                                      str(detail))
        Trace.message(TRANSFER_LEVEL, msg)
        Trace.log(e_errors.ERROR, msg)
        return

    Trace.message(TICKET_LEVEL, "SETTING METADATA WITH:")
    Trace.message(TICKET_LEVEL, pprint.pformat(ticket))

    # Set the metadata for this new file.
    encp.set_pnfs_settings(ticket, intf)

    if not e_errors.is_ok(ticket):
        msg = "Metadata update failed for %s: %s" % (ticket['infilepath'],
                                                     ticket['status'])
        Trace.message(ERROR_LEVEL, msg)
        Trace.log(e_errors.ERROR, msg)

        # Be sure to cleanup after the metadata error.
        encp.clear_layers_1_and_4(ticket)
    else:
        # Don't delete good file.
        delete_at_exit.unregister(ticket['infilepath'])
        msg = "Successfully updated %s metadata." % ticket['infilepath']
        Trace.message(INFO_LEVEL, msg)
        Trace.log(e_errors.INFO, msg)


def end_session(udp_socket, control_socket):

    nowork_ticket = {'work': "nowork", 'method': "no_work"}

    # We are done, tell the mover.
    udp_socket.reply_to_caller(nowork_ticket)

    # Either success or failure; this can be closed.
    encp.close_descriptors(control_socket)

# Return the number of files in the list left to transfer.


def requests_outstanding(request_list):

    files_left = 0

    for request in request_list:
        completion_status = request.get('completion_status', None)
        if completion_status is None:  # or completion_status == EOD:
            files_left = files_left + 1

    return files_left

##############################################################################

# Update the ticket so that next file can be read.


def next_request_update(work_ticket, file_number, encp_intf):

    # Update the location cookie with the new file mark posistion.
    lc = "0000_000000000_%07d" % file_number

    # Clear this file information.
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

    # Clear this information too.
    work_ticket['file_size'] = None
    work_ticket['bfid'] = None
    work_ticket['completion_status'] = None

    # If 'exfer' not deleted; it clobbers new data when returned from the
    # mover.
    del work_ticket['exfer']

    # Update the tickets filename fields for the next file.
    work_ticket['infile'] = \
        os.path.join(os.path.dirname(work_ticket['infile']), lc)
    work_ticket['wrapper']['pnfsFilename'] = work_ticket['infile']
    if work_ticket['outfile'] != "/dev/null":
        # If the outfile is /dev/null, don't change these.
        work_ticket['outfile'] = \
            os.path.join(os.path.dirname(work_ticket['outfile']), lc)
        work_ticket['wrapper']['fullname'] = work_ticket['outfile']

    # Get the stat of the parent directory where the pnfs file will be placed.
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

    # Clear this wrapper information.
    work_ticket['wrapper']['inode'] = 0
    # Note: The math for fixing mode comes from pnfs.py in pstat_decode().
    work_ticket['wrapper']['mode'] = (stats[stat.ST_MODE] % 0o777) | 0o100000
    work_ticket['wrapper']['pstat'] = stats
    work_ticket['wrapper']['size_bytes'] = None

    # Update the unique id for the LM.
    #work_ticket['unique_id'] = encp.generate_unique_id()

    return work_ticket

# Return the next uncompleted transfer.


def get_next_request(request_list, e):  # , filenumber = None):

    for i in range(len(request_list)):
        completion_status = request_list[i].get('completion_status', None)
        if completion_status is None:
            return request_list[i], i
    else:
        if e.list or not e.read_to_end_of_tape:
            return None, 0
        else:
            filenumber = enstore_functions3.extract_file_number(
                request_list[-1]['fc']['location_cookie'])
            request = next_request_update(copy.deepcopy(request_list[0]),
                                          filenumber + 1, e)
            request_list.append(request)
            return request, (len(request_list) - 1)

##############################################################################

#


def finish_request(done_ticket, request_list, index, e):
    # Everything is fine.
    if e_errors.is_ok(done_ticket):
        # Set the metadata if it has not already been set.
        if done_ticket['fc']['deleted'] == 'no':
            try:
                p = pnfs.Pnfs(done_ticket['infilepath'])
                p.get_bit_file_id()
            except (IOError, OSError, TypeError):
                Trace.message(TRANSFER_LEVEL, "Updating metadata for %s." %
                              done_ticket['infilepath'])
                set_metadata(done_ticket, e)

        return encp.finish_request(done_ticket, request_list, index, e)

        """
        if index == None:
            #How can we succed at a transfer, that is not in the
            # request list?
            message = "Successfully transfered a file that " \
                      "is not in the file transfer list."
            try:
                sys.stderr.write(message + "\n")
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR,
                      message + "  " + str(done_ticket))

        else:
            #Tell the user what happend.
            message = "File %s copied successfully." % \
                      (done_ticket['outfilepath'],)
            Trace.message(e_errors.INFO, message)
            Trace.log(e_errors.INFO, message)

            #Set the metadata if it has not already been set.
            try:
                p = pnfs.Pnfs(done_ticket['infilepath'])
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

        return CONTINUE
        """

    # The requested file does not exist on the tape.  (i.e. the
    # tape has only 6 files and the seventh file was requested.)
    elif done_ticket['status'] == (e_errors.READ_ERROR,
                                   e_errors.READ_EOD):

        if e.list:
            # Tell the user what happend.
            message = "File %s read failed: %s" % \
                      (done_ticket['infile'], done_ticket['status'])
            Trace.message(DONE_LEVEL, message)
            Trace.log(e_errors.ERROR, message)

            # Set completion status to failure.
            done_ticket['completion_status'] = FAILURE
            done_ticket['exit_status'] = 1

            # Tell the calling process, this file failed.
            error_output(done_ticket)
        else:
            # Tell the user what happend.
            message = "Reached EOD at location %s." % \
                      (done_ticket['fc']['location_cookie'],)
            Trace.message(DONE_LEVEL, message)
            Trace.log(e_errors.INFO, message)

            # If --list was not used this is a success.
            done_ticket['completion_status'] = SUCCESS
            done_ticket['status'] = (e_errors.OK, None)
            done_ticket['exit_status'] = 0

        # Store these changes back into the master list.
        request_list[index] = done_ticket

        # Tell the calling process, of those files not attempted.
        untried_output(request_list)

        # Perform any necessary file cleanup.
        return STOP

    # Give up on this file.  If a persistant media problem occurs
    # skip this and go to the next file.
    elif done_ticket['status'][0] in [e_errors.POSITIONING_ERROR,
                                      e_errors.READ_ERROR,
                                      ]:
        # Tell the user what happend.
        message = "File %s read failed: %s" % \
            (done_ticket['infile'], done_ticket['status'])
        Trace.message(DONE_LEVEL, message)
        Trace.log(e_errors.ERROR, message)

        # Set completion status to failure.
        done_ticket['completion_status'] = FAILURE
        done_ticket['exit_status'] = 1
        # Store these changes back into the master list.
        request_list[index] = done_ticket

        # Tell the calling process, this file failed.
        error_output(done_ticket)

        return CONTINUE_FROM_BEGINNING

    # Give up.
    elif e_errors.is_non_retriable(done_ticket['status'][0]):
        encp.finish_request(done_ticket, request_list, index, e)

        """
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
        """

        # Tell the calling process, this file failed.
        error_output(done_ticket)
        # Tell the calling process, of those files not attempted.
        untried_output(request_list)

        return STOP

    # Keep trying.
    elif e_errors.is_retriable(done_ticket['status'][0]):
        encp.finish_request(done_ticket, request_list, index, e)

        """
        #On retriable error go back and resubmit what is left
        # to the LM.

        #Record the intermidiate error.
        Trace.log(e_errors.WARNING, "File %s read failed: %s" %
                      (done_ticket['infile'], done_ticket['status']))

        #We are done with this mover.
        #end_session(udp_socket, control_socket)

        #break
        """

        return CONTINUE_FROM_BEGINNING

    # Should never get here!!!
    return None


def readtape_from_hsm(e, tinfo):

    Trace.trace(16, "readtape_from_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" % (e.input, e.output, e.verbose,
                                      e.chk_crc, tinfo['encp_start_time']))

    # This needs to be defined somewhere.
    #files_transfered = 0
    byte_sum = 0  # Sum of bytes transfered (when transfering multiple files).
    exit_status = 0  # Used to determine the final message text.
    # Total number of files where a transfer was attempted.
    number_of_files = 0

    """
    #Get an ip and port to listen for the mover address for
    # routing purposes.
    udp_callback_addr, udp_serv = encp.get_udp_callback_addr(e)
    #If the socket does not exist, do not continue.
    if udp_serv.server_socket == None:
        done_ticket = {'exit_status' : 2,
                       'status':(e_errors.NET_ERROR,
                                 "Unable to obtain udp socket.")}
        return done_ticket
    """

    # Get the list of files to read.
    done_ticket, listen_socket, udp_serv, requests_per_vol = \
        encp.prepare_read_from_hsm(tinfo, e)

    if e.check:
        return done_ticket
    if not e_errors.is_ok(done_ticket):
        # Tell the calling process, this file failed.
        error_output(done_ticket)
        # Tell the calling process, of those files not attempted.
        untried_output(requests_per_vol)

        done_ticket['exit_status'] = 2
        return done_ticket

    ######################################################################
    # Time to start reading some files.
    ######################################################################

    vols = sorted(requests_per_vol.keys())
    for vol in vols:
        request_list = requests_per_vol[vol]
        number_of_files = number_of_files + len(request_list)

        # Get the first request on the next volume to transfer.
        request, index = get_next_request(request_list, e)

        # If the read mode is "read until end of data", we need to
        # create the new output file.
        if not e.list or e.read_to_end_of_tape:
            if not os.path.exists(request['outfile']):
                encp.create_zero_length_local_files(request)

        # Submit the request to the library manager.
        request['method'] = "read_tape_start"
        submitted, reply_ticket, lmc = encp.submit_read_requests([request], e)

        Trace.message(TRANSFER_LEVEL, "Read tape submission sent to LM.")

        if not e_errors.is_ok(reply_ticket):
            # Tell the calling process, this file failed.
            error_output(reply_ticket)
            # Tell the calling process, of those files not attempted.
            untried_output(request_list)

            reply_ticket['exit_status'] = 2
            return reply_ticket
        if submitted != 1:
            request['status'] = (e_errors.UNKNOWN,
                                 "Unknown failure submitting request for "
                                 "file %s on volume %s." %
                                 (request['infile'], vol))

            # Tell the calling process, this file failed.
            error_output(reply_ticket)
            # Tell the calling process, of those files not attempted.
            untried_output(request_list)

            request['exit_status'] = 2
            return request

        # If encp.USE_NEW_EVENT_LOOP is true, we need this cleared.
        transaction_id_list = []

        while requests_outstanding(request_list):

            ticket, control_socket, unused = \
                encp.wait_for_message(listen_socket, lmc,
                                      request_list,
                                      transaction_id_list, e,
                                      udp_serv=udp_serv,
                                      mover_handshake=mover_handshake)

            """
            # Establish control socket connection with the mover.
            control_socket, ticket = mover_handshake(listen_socket, udp_serv,
                                                     request, e)
            """

            # Verify that everything went ok with the handshake.
            external_label = request.get('fc', {}).get('external_label', None)
            result_dict = encp.handle_retries([request], request,
                                              ticket, e,
                                              external_label=external_label,
                                              udp_serv=udp_serv)

            # If USE_NEW_EVENT_LOOP is true, we need these ids.
            transaction_id_list = result_dict.get('transaction_id_list', [])

            # For LM submission errors (i.e. tape went NOACCESS), use
            # any request information in result_dict to identify which
            # request gave an error.
            done_ticket = encp.combine_dict(result_dict, done_ticket)

            if e_errors.is_non_retriable(result_dict):

                # Regardless if index is None or not, make sure that
                # exit_status gets set to failure.
                exit_status = 1

                if index is None:
                    message = "Unknown transfer failed."
                    try:
                        sys.stderr.write(message + "\n")
                        sys.stderr.flush()
                    except IOError:
                        pass
                    Trace.log(e_errors.ERROR,
                              message + "  " + str(done_ticket))

                else:
                    # Combine the dictionaries.
                    work_ticket = encp.combine_dict(done_ticket,
                                                    request_list[index])
                    # Set completion status to successful.
                    work_ticket['completion_status'] = FAILURE
                    # Store these changes back into the master list.
                    request_list[index] = work_ticket

            if not e_errors.is_ok(result_dict):
                # Close these descriptors before they are forgotten about.
                if control_socket is not None:
                    encp.close_descriptors(control_socket)
                # Don't loose the non-retriable error.
                if e_errors.is_non_retriable(result_dict):
                    request = encp.combine_dict(result_dict, request)
                    return request

                continue

            if not control_socket:
                # We only got a response from the LM, we did not connect
                # with the mover yet.
                continue

            # If encp.USE_NEW_EVENT_LOOP is true, we need these ids.
            transaction_id_list = result_dict.get('transaction_id_list', [])

            # maybe this isn't a good idea...
            request = encp.combine_dict(ticket, request)

            use_unique_id = request['unique_id']

            # Keep looping until the READ_EOD error occurs.
            while requests_outstanding(request_list):

                # Flush out the standard output and error descriptors.  This
                # should help in some cases when they are redirected to a file
                # and the bytes get stuck in buffer(s).
                sys.stdout.flush()
                sys.stderr.flush()

                # Get the next file in the list to transfer.
                request, index = get_next_request(request_list, e)

                # Grab a new clean udp_server.
                # Note: This should not be necessary after a bug in the
                # mover was fixed long ago.
                # udp_callback_addr, unused = encp.get_udp_callback_addr(
                #    e, udp_serv)

                # Combine the ticket from the mover with the current
                # information.  Remember the ealier dictionaries 'win'
                # in setting values.  encp.combine_dict() is insufficent
                # for this dictionary munge.  It must be done by hand
                # because both tickets have correct pieces of information
                # that is old in the other ticket.
                request['mover'] = ticket['mover']
                request['callback_addr'] = listen_socket.getsockname()
                # The ticket item of 'routing_callback_addr' is a legacy name.
                request['routing_callback_addr'] = \
                    udp_serv.get_server_address()
                # Encp create_read_request() gives each file a new unique id.
                # The LM can't deal with multiple mover file requests from one
                # LM request.  Thus, we need to set this back to the last
                # unique id sent to the library manager.
                request['unique_id'] = use_unique_id
                # Store these changes back into the master list.
                requests_per_vol[vol][index] = request

                message = "Preparing to read %s." % (request['infile'],)
                Trace.message(TRANSFER_LEVEL, message)
                Trace.log(e_errors.INFO, message)

                data_path_socket, done_ticket = mover_handshake2(request,
                                                                 udp_serv, e)
                # Give up.
                if e_errors.is_non_retriable(done_ticket['status'][0]):
                    # Tell the user what happend.
                    message = "File %s read failed: %s" % \
                              (request['infile'], done_ticket['status'])
                    Trace.message(DONE_LEVEL, message)
                    Trace.log(e_errors.ERROR, message)

                    # We are done with this mover.
                    end_session(udp_serv, control_socket)
                    # Set completion status to failure.
                    request['completion_status'] = FAILURE
                    request['status'] = done_ticket['status']
                    request['exit_status'] = 2

                    # Tell the calling process, this file failed.
                    error_output(request)
                    # Tell the calling process, of those files not attempted.
                    untried_output(requests_per_vol[vol])
                    # Perform any necessary file cleanup.
                    return request

                # Keep trying.
                elif e_errors.is_retriable(done_ticket['status'][0]):
                    # On retriable error go back and resubmit what is left
                    # to the LM.

                    # Record the intermidiate error.
                    message = "File %s read failed: %s" % \
                              (request['infile'], done_ticket['status'])
                    Trace.log(e_errors.WARNING, message)

                    # We are done with this mover.
                    end_session(udp_serv, control_socket)
                    break

                #############################################################
                # In this function call is where most of the work in transfering
                # a single file is done.

                done_ticket = encp.read_hsm_file(request, control_socket,
                                                 data_path_socket, [request],
                                                 tinfo, e,
                                                 udp_serv=udp_serv)
                #############################################################

                # Close these descriptors before they are forgotten about.
                encp.close_descriptors(data_path_socket)

                # Sum up the total amount of bytes transfered.
                exfer_ticket = done_ticket.get('exfer',
                                               {'bytes_transfered': 0})
                byte_sum = byte_sum + exfer_ticket.get('bytes_transfered', 0)

                # Combine the tickets.
                request = encp.combine_dict(done_ticket, request)
                # Store these changes back into the master list.
                requests_per_vol[vol][index] = request

                # The completion_status is modified in the request ticket.
                # what_to_do = 0 for stop
                #            = 1 for continue
                #            = 2 for continue after retry
                what_to_do = finish_request(request, requests_per_vol[vol],
                                            index, e)

                # If on non-success exit status was returned from
                # finish_request(), keep it around for later.
                if request.get('exit_status', None):
                    # We get here only on an error.  If the value is 1, then
                    # the error should be transient.  If the value is 2, then
                    # the error will likely require human intervention to
                    # resolve.
                    exit_status = request['exit_status']
                # Do what finish_request() says to do.
                if what_to_do == STOP:
                    # We get here only on a non-retriable error.
                    end_session(udp_serv, control_socket)
                    return done_ticket
                elif what_to_do == CONTINUE_FROM_BEGINNING:
                    # We get here only on a retriable error.
                    end_session(udp_serv, control_socket)
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
            # If we get here, then, we should have a success.
            end_session(udp_serv, control_socket)

    # we are done transferring - close out the listen socket
    encp.close_descriptors(listen_socket)

    # Print to screen the exit status.
    Trace.message(TO_GO_LEVEL, "EXIT STATUS: %d" % exit_status)

    # Finishing up with a few of these things.
    calc_ticket = encp.calculate_final_statistics(byte_sum, number_of_files,
                                                  exit_status, tinfo)

    # Volume one ticket is the last request that was processed.
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
    # Snag the start time.  t0 is needed by the mover, but its name conveys
    # less meaning.
    t0 = time.time()
    tinfo = {'t0': t0, 'encp_start_time': t0}

    # Initialize the Trace module.
    Trace.init("GET")
    for x in xrange(6, intf.verbose + 1):
        Trace.do_print(x)
    for x in xrange(1, intf.verbose + 1):
        Trace.do_message(x)

    # Some globals are expected to exists for normal operation (i.e. a logger
    # client).  Create them.
    status_ticket = encp.clients(intf)
    if not e_errors.is_ok(status_ticket):
        return encp.final_say(intf, status_ticket)

    # Log/print the starting encp information.  This depends on the log
    # from the clients() call, thus it should always be after clients().
    # This function should never give a fatal error.
    encp.log_encp_start(tinfo, intf)

    if intf.data_access_layer:
        #global data_access_layer_requested
        encp.data_access_layer_requested = intf.data_access_layer
        # data_access_layer_requested.set()
    elif intf.verbose < 0:
        # Turn off all output to stdout and stderr.
        encp.data_access_layer_requested = -1
        intf.verbose = 0

    done_ticket = readtape_from_hsm(intf, tinfo)

    return encp.final_say(intf, done_ticket)


if __name__ == '__main__':

    #intf_of_get = GetInterface(sys.argv, 0)

    """
    if intf_of_get.volume and intf_of_get.read_to_end_of_tape and \
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
    """

    #print encp.format_class_for_print(intf_of_get, "intf_of_get")

    delete_at_exit.quit(encp.start(0, encp.do_work, main, GetInterface))
