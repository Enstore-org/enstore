#!/usr/bin/env python
#
# $Id$
#

import sys
import os
import time
import pprint
import select
import socket
import errno
import copy
import string
import types

import encp
import pnfs
import e_errors
import delete_at_exit
import callback
import host_config
import Trace
import udp_client
import checksum
import udp_server
import cleanUDP
import volume_family

#Completion status field values.
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
EOD = "EOD"  #Don't stop until EOD is reached.

def print_usage():
    print "Usage:", os.path.basename(sys.argv[0]), \
          "[--verbose <level>] [--list <file>] " \
          "<Volume> <Input Dir.> <Output Dir.>"

def error_output(request):
    #Get the info.
    lc = request['fc'].get("location_cookie", None)
    file_number = encp.extract_file_number(lc)
    message = request.get("status", (e_errors.UNKNOWN, None))
    #Format the output.
    msg = "error_output %s %s\n" % (file_number, message)
    #Print the output.
    sys.stderr.write(msg)

def untried_output(request_list):

    for request in request_list:
        #For each item in the list, print this if it was not tried.
        if request.get('completion_status', None) == None:
            request['status'] = (e_errors.UNKNOWN,
                                 "File transfer not attempted.")
            error_output(request)

def transfer_file(in_fd, out_fd):

    bytes = 0L #Number of bytes transfered.
    crc = 0L

    while 1:

        r, w, unused = select.select([in_fd], [], [], 15 * 60)

        if in_fd not in r:
            status = (e_errors.TIMEDOUT, "Read")
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}
            break

        #READ in the data.
        try:
            data = os.read(in_fd, 1048576)
        except OSError, detail:
            status = (e_errors.OSERROR, str(detail))
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}
        except IOError, detail:
            status = (e_errors.IOERROR, str(detail))
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}

        if len(data) == 0: #If read the EOF, return number of bytes transfered.
            status = (e_errors.OK, None)
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}
            break

        r, w, unused = select.select([], [out_fd], [], 15 * 60)

        if out_fd not in w:
            status = (e_errors.TIMEDOUT, "Write")
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}
            break

        #WRITE out the data.
        try:
            bytes = bytes + os.write(out_fd, data)
        except OSError, detail:
            #Handle the ENOSPC error differently.
            if detail.get('errno', None) == errno.ENOSPC:
                status = (e_errors.NOSPACE, str(detail))
            #Handle OSErrors
            else:
                status = (e_errors.OSERROR, str(detail))
            #The status gets returned in the same way.
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}
        except IOError, detail:
            status = (e_errors.IOERROR, str(detail))
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}

        #Calculate the checksum
        crc = checksum.adler32(crc, data, len(data))

    return {'status' : (e_errors.BROKEN, "Reached unreachable code."),
            'bytes' : bytes, 'encp_crc' : crc}


def get_single_file(work_ticket, tinfo, control_socket, udp_socket, e):

    #Loop around in case the file transfer needs to be retried.
    #while work_ticket.get('retry', 0) <= e.max_retry:

        Trace.message(5, "Opening local file.")
        Trace.log(e_errors.INFO, "Opening local file.")

        #If necessary, create the file.
        if work_ticket.get('completion_status', None) == EOD and \
           not os.path.exists(work_ticket['outfile']):
            encp.create_zero_length_files(work_ticket['outfile'])
        # Open the local file.
        done_ticket = encp.open_local_file(work_ticket['outfile'], e)
        
        if not e_errors.is_ok(done_ticket):
            sys.stderr.write("Unable to open local file %s: %s\n" %
                             (work_ticket['outfile'], done_ticket['status'],))
            encp.quit(1)
        else:
            out_fd = done_ticket['fd']

        Trace.message(5, "Sending next file request to the mover.")
        Trace.log(e_errors.INFO, "Sending next file request to the mover.")

        #This is an evil hack to modify work_ticket outside of
        # create_read_requests().
        work_ticket['method'] = "read_next"
        #Send the actual request to the mover.
        Trace.message(10, "MOVER_REQUEST_SUBMISSION:")
        Trace.message(10, pprint.pformat(work_ticket))
        udp_socket.reply_to_caller(work_ticket)

        overall_start = time.time() #----------------------------Overall Start
        
        Trace.message(5, "Opening the data socket.")
        Trace.log(e_errors.INFO, "Opening the data socket.")

        #Open the data socket.
        try:
            data_path_socket = encp.open_data_socket(
                work_ticket['mover']['callback_addr'],
                work_ticket['callback_addr'][0])

            if not data_path_socket:
                raise socket.error(errno.ENOTCONN,
                                   errno.errorcode[errno.ENOTCONN])

            work_ticket['status'] = (e_errors.OK, None)
        except (encp.EncpError, socket.error), detail:
            #sys.stderr.write("Unable to open data socket with mover: %s\n" %
            #                 (str(detail),))
            #encp.quit(1)
            work_ticket['status'] = (e_errors.NET_ERROR, str(detail))

        # Verify that everything went ok with the transfer.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                          work_ticket, None,
                                          None, None, e)

        if not e_errors.is_ok(result_dict):
            #Don't loose the non-retirable error.
            if e_errors.is_non_retriable(result_dict):
                work_ticket = encp.combine_dict(result_dict, work_ticket)
            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd)

            #Log the error and return.
            Trace.log(e_errors.ERROR, "Unable to connect data socket: %s" %
                      str(result_dict['status']))
            return work_ticket

        Trace.message(5, "Waiting for data")
        Trace.log(e_errors.INFO, "Waiting for data")

        # Stall starting the count until the first byte is ready for reading.
        read_fd, unused, unused = select.select([data_path_socket], [], [],
                                                e.mover_timeout)

        if data_path_socket not in read_fd:
            work_ticket['status'] = (e_errors.TIMEDOUT, "No data received")
        # Verify that everything went ok with the transfer.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                          work_ticket, None,
                                          None, None, e)

        if not e_errors.is_ok(result_dict):
            #Don't loose the non-retirable error.
            if e_errors.is_non_retriable(result_dict):
                work_ticket = encp.combine_dict(result_dict, work_ticket)
            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd)

            #Log the error and return.
            Trace.log(e_errors.ERROR, "Waiting for data from open data "
                      "socket failed: %s" % str(result_dict['status']))
            return work_ticket

        Trace.message(5, "Reading data from tape.")
        Trace.log(e_errors.INFO, "Reading data from tape.")
                  
        lap_time = time.time() #------------------------------------------Start
        
        # Read the file from the mover.
        done_ticket = transfer_file(data_path_socket.fileno(), out_fd)

        tstring = '%s_transfer_time' % work_ticket['unique_id']
        tinfo[tstring] = time.time() - lap_time #--------------------------End

        #Store what there is in the 'exfer' sub-ticket.
        work_ticket['exfer'] = done_ticket

        Trace.message(5, "Completed reading from tape.")
        Trace.log(e_errors.INFO, "Completed reading from tape.")
        Trace.message(10, "DONE_TICKET (transfer_file):")
        Trace.message(10, pprint.pformat(done_ticket))

        # Verify that everything went ok with the transfer.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                      done_ticket, None,
                                     None, None, e)

        if not e_errors.is_ok(result_dict):
            #Don't loose the non-retirable error.
            if e_errors.is_non_retriable(result_dict):
                work_ticket = encp.combine_dict(result_dict, work_ticket)
            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd, data_path_socket)

            #Log the error and return.
            Trace.log(e_errors.ERROR, "File transfer failed: %s" %
                      str(result_dict['status']))
            return work_ticket

        #We should not need this process_request() call.  The mover shoule
        # only send one message on the udp
        #
        #Pretend we are the library manager.
        #request = udp_socket.process_request()
        #Trace.message(10, "LM MESSAGE:")
        #Trace.message(10, pprint.pformat(request))

        #Get the final success/failure message from the mover.  If this side
        # has an error, don't wait for the mover in case the mover is waiting
        # for "Get" to do something.
        Trace.message(5, "Waiting for final dialog (1).")
        mover_done_ticket = encp.receive_final_dialog(control_socket)
        Trace.message(5, "Received final dialog (1).")
        Trace.message(10, "FINAL DIALOG (tcp):")
        Trace.message(10, pprint.pformat(mover_done_ticket))
        Trace.log(e_errors.INFO, "Received final dialog (1).")
        #Keep the udp socket queues clear.
        start_time = time.time()
        Trace.message(5, "Waiting for final dialog (2).")
        while time.time() < start_time + e.mover_timeout:
            #Keep looping until one of these two messages arives.  Ignore
            # any other that my be received.
            mover_udp_done_ticket = udp_socket.process_request()

            #If requested output the raw 
            Trace.trace(11, "UDP MOVER MESSAGE:")
            Trace.trace(11, pprint.pformat(mover_udp_done_ticket))

            #Make sure the messages are what we expect.
            if mover_udp_done_ticket == None: #Something happened, keep trying.
                continue
            if mover_udp_done_ticket['work'] == 'mover_bound_volume' or \
               mover_udp_done_ticket['work'] == 'mover_error':
                break
        Trace.message(5, "Received final dialog (2).")
        Trace.message(10, "FINAL DIALOG (udp):")
        Trace.message(10, pprint.pformat(mover_udp_done_ticket))
        Trace.log(e_errors.INFO, "Received final dialog (2).")

        #Combine these tickets.
        work_ticket = encp.combine_dict(mover_done_ticket, work_ticket)
        
        #For the case where we don't know how many files exist and we tried
        # to read passed the last file, we don't want to handle any retries
        # because we are done.
        if mover_done_ticket['status'] == (e_errors.READ_ERROR,
                                           e_errors.READ_EOD):
            #Don't loose the non-retirable error.
            if e_errors.is_non_retriable(mover_done_ticket):
                work_ticket = encp.combine_dict(mover_done_ticket, work_ticket)
            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd, data_path_socket)

            #Log the error and return.
            Trace.log(e_errors.INFO, "Dectected End Of Data: %s" %
                      str(mover_done_ticket['status']))
            return work_ticket
        
        # Verify that everything went ok with the transfer.
        mover_result_dict = encp.handle_retries([work_ticket], work_ticket,
                                                mover_done_ticket, None,
                                                None, None, e)

        if not e_errors.is_ok(mover_result_dict):
            #Don't loose the non-retirable error.
            if e_errors.is_non_retriable(mover_result_dict):
                work_ticket = encp.combine_dict(mover_result_dict, work_ticket)
            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd, data_path_socket)

            #Log the error and return.
            Trace.log(e_errors.ERROR, "Final dialog error: %s" %
                      str(mover_done_ticket['status']))
            return work_ticket

        ####################################################################
        #This is a work-around for a file_clerk and mover communication bug.
        # Occasionally the fc/mv does not send the updated fc sub-ticket.
        ####################################################################
        if len(work_ticket.get('fc', {}).keys()) > 0 and \
           work_ticket.get('fc', {}).get('bfid', "no_bfid") == None:

            #Get the file info list dump of the tape.
            unused, fcc = encp.get_clerks(e.volume)
            tape_ticket = fcc.tape_list(e.volume)
            
            #First check that everything is okay.
            if e_errors.is_ok(tape_ticket):
                #Locate the record for this file based on its location.
                for item in tape_ticket.get("tape_list", {}).values():
                    if item.get('location_cookie', "no_lc") == \
                       work_ticket.get('fc', {}).get('location_cookie', None):

                        Trace.message(2, "Applying GETs workaround for fc/mv "
                                      " communication error.")
                        Trace.log(e_errors.ERROR,
                                  "Applying GETs workaround for fc/mv "
                                  " communication error.")
                        
                        #Store the correct items from the file clerk.
                        work_ticket['fc']['bfid'] = item['bfid']
                        work_ticket['fc']['location_cookie'] = \
                                                    item['location_cookie']
                        work_ticket['fc']['complete_crc'] = \
                                                    item['complete_crc']
                        work_ticket['fc']['deleted'] = item['deleted']
                        work_ticket['fc']['drive'] = item['drive']
                        work_ticket['fc']['sanity_cookie'] = \
                                                    item['sanity_cookie']
                        work_ticket['fc']['size'] = item['size']
                        break

        ###################################################################

        #Before continueing, double check the number bytes said to be
        # moved by the mover and encp.
        encp_size = work_ticket.get('exfer', {}).get('bytes', None)
        mover_size = work_ticket.get('fc', {}).get('size', None)
        if encp_size == None:
            work_ticket['status'] = (e_errors.UNKNOWN,
                                     "Get does not know number of bytes"
                                     " transfered.")
            #return work_ticket
        elif mover_size == None:
            work_ticket['status'] = (e_errors.UNKNOWN,
                                     "Mover did not report how many bytes"
                                     " where transfered.")
            #return work_ticket
        elif long(encp_size) != long(mover_size):
            #We get here if the two sizes to not match.  This is a very bad
            # thing to occur.
            msg = (e_errors.CONFLICT,
                   "Encp bytes read (%s) do not match the mover "
                   "bytes written (%s)." % (encp_size, mover_size))
            work_ticket['status'] = (e_errors.CONFLICT, msg)
            #return work_ticket
        else:
            if work_ticket['file_size'] == None:
                #If the number of bytes transfered is consistant with Get and
                # the mover, then set this value.  This is only necessary
                # when no file information is available.
                work_ticket['file_size'] = long(encp_size)

        # Verify that the sizes reported from the mover and encp are good.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                          work_ticket, None,
                                          None, None, e)
        
        if not e_errors.is_ok(result_dict):
            #Don't loose the non-retirable error.
            if e_errors.is_non_retriable(result_dict):
                work_ticket = encp.combine_dict(result_dict, work_ticket)

            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd, data_path_socket)

            #Log the error and return.
            Trace.log(e_errors.ERROR, "Filesize comparison error: %s" %
                      str(work_ticket['status']))
            return work_ticket
        
        #Check the crc.
        encp.check_crc(work_ticket, e, out_fd)

        # Close these descriptors before they are forgotten about.
        encp.close_descriptors(out_fd, data_path_socket)

        # Verify that the crcs are correct and handle occordingly.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                          work_ticket, None,
                                          None, None, e)

        if not e_errors.is_ok(result_dict):
            #Don't loose the non-retirable error.
            if e_errors.is_non_retriable(mover_result_dict):
                work_ticket = encp.combine_dict(result_dict, work_ticket)
            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd, data_path_socket)

            #Log the error and return.
            Trace.log(e_errors.ERROR, "CRC comparison error: %s" %
                      str(work_ticket['status']))
            return work_ticket

        tstring = '%s_overall_time' % work_ticket['unique_id']
        tinfo[tstring] = time.time() - overall_start #-------------Overall End

        #Give the user some numbers on how fast things went.
        encp.calculate_rate(work_ticket, tinfo)
        
        #work_ticket = encp.combine_dict(result_dict, work_ticket)
        work_ticket['status'] = (e_errors.OK, None) #Success.
        return work_ticket
        
def set_metadata(ticket, intf):

    #Set these now so the metadata can be set correctly.
    ticket['wrapper']['fullname'] = ticket['outfile']
    ticket['wrapper']['pnfsFilename'] = ticket['infile']
    try:
        ticket['file_size'] = ticket['exfer'].get("bytes", 0L)
    except:
        sys.stderr.write("Unexpected error setting metadata.\n")
        sys.stderr.write(pprint.pformat(ticket))
        exc, msg, tb = sys.exc_info()
        raise exc, msg, tb

    #Create the pnfs file.
    try:
        encp.create_zero_length_files(ticket['infile'], raise_error = 1)
        Trace.log(e_errors.INFO, "Pnfs file created for %s."%ticket['infile'])
    except OSError, detail:
        msg = "Pnfs file create failed for %s: %s" % (ticket['infile'],
                                                      str(detail))
        Trace.message(5, msg)
        Trace.log(e_errors.ERROR, msg)
        return

    Trace.message(10, "SETTING METADATA WITH:")
    Trace.message(10, pprint.pformat(ticket))
    
    #Set the metadata for this new file.
    encp.set_pnfs_settings(ticket, intf)

    if not e_errors.is_ok(ticket):
        msg = "Metadata update failed for %s: %s" % (ticket['infile'],
                                                     ticket['status'])
        Trace.message(5, msg)
        Trace.log(e_errors.ERROR, msg)
    else:
        delete_at_exit.unregister(ticket['infile']) #Don't delete good file.
        msg = "Successfully updated %s metadata." % ticket['infile']
        Trace.message(5, msg)
        Trace.log(e_errors.INFO, msg)

def end_session(udp_socket, control_socket):

    nowork_ticket = {'work': "nowork", 'method' : "no_work"}
    #try:
    #    done_ticket = callback.write_tcp_obj(control_socket, nowork_ticket)
    #except e_errors.TCP_EXCEPTION:
    #    sys.stderr.write("Unable to terminate communication "
    #                     "with mover cleanly.\n")
    #    encp.quit(1)

    #We are done, tell the mover.
    udp_socket.reply_to_caller(nowork_ticket)

    #Either success or failure; this can be closed.
    encp.close_descriptors(control_socket)

#Return the number of files in the list left to transfer.
def requests_outstanding(request_list):

    files_left = 0

    for request in request_list:
        completion_status = request.get('completion_status', None)
        if completion_status == None or completion_status == EOD: 
            files_left = files_left + 1

    return files_left

#Update the ticket so that next file can be read.
def next_request_update(work_ticket, file_number):

    #Update the fields with the new location cookie.
    lc = "0000_000000000_%07d" % file_number
    work_ticket['fc']['location_cookie'] = lc

    #Clear this file information.
    work_ticket['fc']['bfid'] = None
    work_ticket['fc']['complete_crc'] = None
    work_ticket['fc']['deleted'] = None
    work_ticket['fc']['drive'] = None
    work_ticket['fc']['pnfs_mapname'] = None
    work_ticket['fc']['pnfs_name0'] = None
    work_ticket['fc']['pnfsid'] = None
    work_ticket['fc']['pnfsvid'] = None
    work_ticket['fc']['sanity_cookie'] = None
    work_ticket['fc']['size'] = None
    #Clear this information too.
    work_ticket['file_size'] = None
    work_ticket['bfid'] = None
    #If 'exfer' not deleted; it clobbers new data when returned from the mover.
    del work_ticket['exfer']

    #Only update these fields if a filename is not known already.
    #This could be a problem if a file is the same name as its location cookie.
    if encp.is_location_cookie(os.path.basename(work_ticket['infile'])):
        # Update the tickets fields for the next file.
        work_ticket['infile'] = \
                    os.path.join(os.path.dirname(work_ticket['infile']), lc)
        work_ticket['outfile'] = \
                    os.path.join(os.path.dirname(work_ticket['outfile']), lc)
        work_ticket['wrapper']['fullname'] = work_ticket['infile']
        work_ticket['wrapper']['pnfsFilename'] = work_ticket['outfile']

    #Update the unique id for the LM.
    #work_ticket['unique_id'] = encp.generate_unique_id()

    return work_ticket

#Return the next uncompleted transfer.
def get_next_request(request_list, filenumber = None):

    #If we know nothing about this tape.  (i.e. not metadata and --list was
    # not used.)
    if request_list[0].get('completion_status', None) == EOD and \
       filenumber != None:
        request = next_request_update(copy.deepcopy(request_list[0]),
                                      filenumber)
        request_list.append(request)
        return request, (len(request_list) - 1)

    #For all cases were metadata is known.
    for i in range(len(request_list)):
        completion_status = request_list[i].get('completion_status', None)
        if completion_status == None or completion_status == EOD: 
            return request_list[i], i

    return None, 0
            
def main(e):

    t0 = time.time()
    tinfo = {'t0' : t0, 'encp_start_time' : t0}

    #Some globals are expected to exists for normal operation (i.e. a logger
    # client).  Create them.
    encp.clients()

    #Initialize the Trace module.
    Trace.init("GET")
    for x in xrange(6, e.verbose+1):
        Trace.do_print(x)
    for x in xrange(1, e.verbose+1):
        Trace.do_message(x)

    command_line = "Command line: %s" % (string.join(sys.argv),)
    Trace.log(e_errors.INFO, command_line)

    #exit_status = 0 #For rembering failures.
    files_transfered = 0

    #Get a port to talk on and listen for connections
    callback_addr, listen_socket = encp.get_callback_addr(e)
    #Get an ip and port to listen for the mover address for
    # routing purposes.
    routing_addr, udp_socket = encp.get_routing_callback_addr(e)

    #Create all of the request dictionaries.
    Trace.message(4, "Creating read requests.")
    requests_per_vol = encp.create_read_requests(callback_addr, routing_addr,
                                                 tinfo, e)

    #If this is the case, don't worry about anything.
    if (len(requests_per_vol) == 0):
        encp.quit()

    #Set the max attempts that can be made on a transfer.
    check_lib = requests_per_vol.keys()
    encp.max_attempts(requests_per_vol[check_lib[0]][0]['vc']['library'], e)

    #Make sure that we are not clobbering files.
    Trace.message(4, "Checking status of files.")
    Trace.log(e_errors.INFO, "Checking status of files.")
    for request in requests_per_vol[e.volume]:
        #This might be a problem when zero information is available...
        encp.outputfile_check(request['infile'], request['outfile'], e)

    #Create the zero length file entries.
    Trace.message(4, "Creating zero length output files.")
    Trace.log(e_errors.INFO, "Creating zero length output files.")
    for request in requests_per_vol[e.volume]:
        encp.create_zero_length_files(request['outfile'])

    #Only the first submission goes to the LM for volume reads.  On errors,
    # encp.handle_retries() will send the request to the LM.  Thus this
    # code should only be done once.
    for request in requests_per_vol[e.volume]:
        #Set this for each request.
        request['method'] = "read_tape_start" #evil hacks
        request['route_selection'] = 1 #On for Get..
        request['submitted'] = None #Failures won't be re-sent if not None.

    ######################################################################
    # No more "for request in requests_per_vol[e.volume]:" pieces of code
    # should appear below this comment.
    ######################################################################

    #Get the next volume in the list to transfer.
    request, index = get_next_request(requests_per_vol[e.volume])

    Trace.message(10, "LM SUBMISSION TICKET:")
    Trace.message(10, pprint.pformat(request))
    submitted, reply_ticket = encp.submit_read_requests([request], tinfo, e)
    Trace.message(10, "LM RESPONCE TICKET:")
    Trace.message(10, pprint.pformat(reply_ticket))
    Trace.message(4, "Read tape submission sent to LM.")
    
    if not e_errors.is_ok(reply_ticket):
        msg = "Unable to read volume %s: %s" % \
              (e.volume, reply_ticket['status'])
        sys.stderr.write("%s\n" % msg)
        Trace.log(e_errors.ERROR, msg)
        encp.quit(1)
    if submitted != 1:
        msg = "Unknown failure subitting request for file %s on volume %s." % \
              (request['infile'], e.volume)
        sys.stderr.write("%s\n" % msg)
        Trace.log(e_errors.ERROR, msg)
        encp.quit(1)

    while requests_outstanding(requests_per_vol[e.volume]):
        
        #Open the routing socket.
        #config = host_config.get_config()
        use_listen_socket = listen_socket.dup()
        try:
            #There is no need to do this on a non-multihomed machine.
            #if config and config.get('interface', None):
 
            #Keep the udp socket queues clear.
            start_time = time.time()
            Trace.message(4, "Opening routing socket.")
            Trace.log(e_errors.INFO, "Opening routing socket.")
            while time.time() < start_time + e.mover_timeout:
                #Keep looping until one of these two messages arives.  Ignore
                # any other that my be received.
                rticket, use_listen_socket = encp.open_routing_socket(
                    udp_socket, [request['unique_id']], e)

                #If requested output the raw message.
                Trace.trace(11, "RTICKET MESSAGE:")
                Trace.trace(11, pprint.pformat(rticket))

                #Make sure the messages are what we expect.
                if rticket == None: #Something happened, keep trying.
                    continue
                elif not e_errors.is_ok(rticket.get('status', None)):
                    continue
                else:
                    break

            #Print out the final ticket.
            Trace.message(10, "ROUTING TICKET:")
            Trace.message(10, pprint.pformat(rticket))

            # Verify that everything went ok with the transfer.
            result_dict = encp.handle_retries([request], request,
                                              rticket, None,
                                              None, None, e)

            if e_errors.is_non_retriable(result_dict):
                #work_ticket = encp.combine_dict(result_dict, request)    
                #Log the error.
                msg = "Unable to handle routing: %s\n" % (rticket['status'],)
                sys.stderr.write("%s\n" % msg)
                Trace.log(e_errors.ERROR, msg)
                #Close these before they are forgotten about.
                encp.close_descriptors(use_listen_socket)
                encp.quit(1)
            elif not e_errors.is_ok(result_dict):
                #Log the error and continue.
                msg = "Unable to handle routing: %s\n" % (rticket['status'],)
                #sys.stderr.write("%s\n" % msg)
                Trace.log(e_errors.WARNING, msg)
                #Close these before they are forgotten about.
                encp.close_descriptors(use_listen_socket)
                continue

            Trace.message(4, "Opened routing socket.")
            Trace.log(e_errors.INFO, "Opened routing socket.")
        except (encp.EncpError,), detail:
            if detail.errno == errno.ETIMEDOUT:
                #Log the error and continue.
                msg = (detail.type,
                       "Non-fatal routing socket error: %s\n" % (str(detail),))
                sys.stderr.write("%s\n" % msg)
                Trace.log(e_errors.WARNING, msg)
                #Handle retries needs to be called to update various values
                # and to perfrom the resubmition itself.
                the_error = {'status' : (e_errors.RESUBMITTING,
                                         request['unique_id'])}
                encp.handle_retries([request], request,
                                    the_error, None,
                                    None, None, e)
                continue
            else:
                msg = "Fatal routing socket error: %s\n" % (str(detail),)
                sys.stderr.write("%s\n" % msg)
                Trace.log(e_errors.ERROR, msg)
                encp.quit(1)

        #Open the control socket.
        Trace.message(4, "Opening control socket.")
        Trace.log(e_errors.INFO, "Opening control socket.")
        try:
            control_socket, mover_address, ticket = \
                            encp.open_control_socket(use_listen_socket, 15*60)

            #Print out the final ticket.
            Trace.message(10, "CONTROL SOCKET TICKET:")
            Trace.message(10, pprint.pformat(ticket))

            # Verify that everything went ok with the transfer.
            result_dict = encp.handle_retries([request], request,
                                              ticket, None,
                                              None, None, e)

            if e_errors.is_non_retriable(result_dict):
                #work_ticket = encp.combine_dict(result_dict, request)    
                #Log the error.
                msg = "Unable to open control socket: %s\n" % \
                      (ticket['status'],)
                sys.stderr.write("%s\n" % msg)
                Trace.log(e_errors.ERROR, msg)
                #Close these before they are forgotten about.
                encp.close_descriptors(use_listen_socket, control_socket)
                encp.quit(1)
            elif not e_errors.is_ok(result_dict):
                #Log the error and continue.
                msg = "Unable to open control socket: %s\n" % \
                      (ticket['status'],)
                #sys.stderr.write("%s\n" % msg)
                Trace.log(e_errors.WARNING, msg)
                #Close these before they are forgotten about.
                encp.close_descriptors(use_listen_socket, control_socket)
                continue

        except (encp.EncpError,), detail:
            if detail.errno == errno.ETIMEDOUT:
                #Log the error and continue.
                msg = (detail.type,
                       "Non-fatal control socket error: %s\n" % (str(detail),))
                sys.stderr.write("%s\n" % msg)
                Trace.log(e_errors.WARNING, msg)
                #Handle retries needs to be called to update various values
                # and to perfrom the resubmition itself.
                the_error = {'status' : (e_errors.RESUBMITTING,
                                         request['unique_id'])}
                encp.handle_retries([request], request,
                                    the_error, None,
                                    None, None, e)
                #Close these before they are forgotten about.
                encp.close_descriptors(use_listen_socket)
                continue
            else:
                msg = "Unable to open control socket with mover: %s\n" % \
                      (str(detail),)
                sys.stderr.write("%s\n" % msg)
                Trace.log(e_errors.ERROR, msg)
                encp.quit(1)

        Trace.message(4, "Opened control socket.")
        Trace.log(e_errors.INFO, "Opened control socket.")

        #Compare expected unique id with the returned unique id.
        if ticket.get('unique_id', None) != request['unique_id']:
            msg = "Unexpected unique_id received from %s.  Expected " \
                  "unique id %s, received %s instead.\n" % \
                  (mover_address, request['unique_id'],
                   ticket.get('unique_id', None))
            sys.stderr.write("%s\n" % msg)
            Trace.log(e_errors.ERROR, msg)
            #We are done with this mover.
            end_session(udp_socket, control_socket)
            #Doing a continue should force "get" to wait e.mover_timout
            # seconds in the open routing socket loop, before resubmitting.
            # This is a simple waiting mechanism, because we need for
            # library managers and movers to timeout before (successfully)
            # trying again.
            continue 
 
        #If this is a volume where the file information is not known and
        # the user did not specify the filenames...
        #if len(requests_per_vol[e.volume]) == 1 and \
        #   requests_per_vol[e.volume][0].get('bfid', None) == None:
        if request.get('bfid', None) == None and \
           len(requests_per_vol[e.volume]) == 1:
            #Initalize this.
            file_number = 1
            request['completion_status'] = EOD
            #Store these changes back into the master list.
            requests_per_vol[e.volume][index] = request
        else:
            file_number = None

        use_unique_id = request['unique_id']

        #Keep the udp socket queues clear.
        start_time = time.time()
        Trace.message(5, "Waiting for mover ready message.")
        Trace.log(e_errors.INFO, "Waiting for mover ready message.")
        while time.time() < start_time + e.mover_timeout:
            #Keep looping until the message arives.
            mover_ready = udp_socket.process_request()
            
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
                #We are done with this mover.
                end_session(udp_socket, control_socket)
                #Set completion status to failure.
                request['completion_status'] = FAILURE

                #Tell the calling process, this file failed.
                error_output(mover_ready)
                #Tell the calling process, of those files not attempted.
                untried_output(requests_per_vol[e.volume])
                #Perform any necessary file cleanup.
                encp.quit(0)
        Trace.message(5, "Received mover ready message.")
        Trace.log(e_errors.INFO, "Received mover ready message.")
                

        # Keep looping until the READ_EOD error occurs.
        while requests_outstanding(requests_per_vol[e.volume]):

            #Combine the ticket from the mover with the current information.
            # Remember the ealier dictionaries 'win' in setting values.
            # encp.combine_dict() is insufficent for this dictionary munge.
            # It must be done by hand because both tickets have correct
            # pieces of information that is old in the other ticket.
            request['mover'] = ticket['mover']
            request['encp_ip'] =  use_listen_socket.getsockname()[0]
            #Encp create_read_request() gives each file a new unique id.
            # The LM can't deal with multiple mover file requests from one
            # LM request.  Thus, we need to set this back to the last unique
            # id sent to the library manager.
            request['unique_id'] = use_unique_id
            #Store these changes back into the master list.
            requests_per_vol[e.volume][index] = request
            
            Trace.message(4, "Preparing to read %s." % request['outfile'])
            Trace.log(e_errors.INFO,
                      "Preparing to read %s." % request['outfile'])

            ################################################################
            #In this function call is where most of the work in transfering
            # a single file is done.
            done_ticket = get_single_file(request, tinfo, control_socket,
                                          udp_socket, e)
            ################################################################

            #Print out the final ticket.
            Trace.message(10, "DONE_TICKET (get_single_file):")
            Trace.message(10, pprint.pformat(done_ticket))

            #Combine the tickets:
            request = encp.combine_dict(done_ticket, request)
            #Store these changes back into the master list.
            requests_per_vol[e.volume][index] = request
            
            #Everything is fine.
            if e_errors.is_ok(done_ticket):

                #Tell the user what happend.
                Trace.message(e_errors.INFO,
                          "File %s copied successfully." % request['infile'])
                Trace.log(e_errors.INFO,
                          "File %s copied successfully." % request['infile'])

                #Don't delete.
                delete_at_exit.unregister(request['outfile']) 
                #Remember the completed transfer.
                files_transfered = files_transfered + 1

                #Set the metadata if it has not already been set.
                try:
                    p = pnfs.Pnfs(request['infile'])
                    p.get_bit_file_id()
                except (IOError, OSError, TypeError):
                    Trace.message(5, "Updating metadata for %s." %
                                  request['infile'])
                    set_metadata(request, e)
                
                if request.get('completion_status', None) == "EOD":
                    #The fields need to be updated for the next file
                    # on the tape to be read.  We should only get here if
                    # the metadata is unkown and --list was NOT used.
                    #Note: This will not work for the cern wrapper.  For this
                    # wrapper the header and trailer consume a 'file' on
                    # the tape.
                    file_number = file_number + 1
                    #next_request_update(request, file_number)
                else:
                    #Set completion status to successful.
                    request['completion_status'] = SUCCESS

                #Store these changes back into the master list.
                requests_per_vol[e.volume][index] = request
                #Get the next request before continueing.
                request, index = get_next_request(requests_per_vol[e.volume],
                                                  file_number)
                continue

            #The requested file does not exist on the tape.  (i.e. the
            # tape has only 6 files and the seventh file was requested.)
            elif done_ticket['status'] == (e_errors.READ_ERROR,
                                           e_errors.READ_EOD):
                #Tell the user what happend.
                Trace.message(1, "File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))
                Trace.log(e_errors.ERROR, "File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))

                #We are done with this mover.
                end_session(udp_socket, control_socket)
                #Set completion status to failure.
                request['completion_status'] = FAILURE

                #Tell the calling process, this file failed.
                error_output(done_ticket)
                #Tell the calling process, of those files not attempted.
                untried_output(requests_per_vol[e.volume])
                #Perform any necessary file cleanup.
                Trace.message(1, "Get exit status: %s" % (0,))
                Trace.log(e_errors.INFO, "Get exit status: %s" % (0,))
                encp.quit(0)
            #Give up.
            elif e_errors.is_non_retriable(done_ticket['status'][0]):
                #Tell the user what happend.
                Trace.message(1, "File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))
                Trace.log(e_errors.ERROR, "File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))

                #We are done with this mover.
                end_session(udp_socket, control_socket)
                #Set completion status to failure.
                request['completion_status'] = FAILURE

                #Tell the calling process, this file failed.
                error_output(done_ticket)
                #Tell the calling process, of those files not attempted.
                untried_output(requests_per_vol[e.volume])
                #Perform any necessary file cleanup.
                Trace.message(1, "Get exit status: %s" % (1,))
                Trace.log(e_errors.INFO, "Get exit status: %s" % (1,))
                encp.quit(1)
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
                #continue #The retry alread sent request to mover.

    #Perform any necessary file cleanup.
    Trace.message(1, "Get exit status: %s" % (0,))
    Trace.log(e_errors.INFO, "Get exit status: %s" % (0,))
    encp.quit(0)

def do_work(intf):
    delete_at_exit.setup_signal_handling()

    try:
        main(intf)
        encp.quit(0)
    except SystemExit:
        encp.quit(1)

if __name__ == '__main__':

    if len(sys.argv) < 4:
        print_usage()
        sys.exit(1)

    if not encp.is_volume(sys.argv[-3]):
        sys.stderr.write("First argument is not a volume name.\n")
        sys.exit(1)
        
    if not os.path.exists(sys.argv[-2]) or not os.path.isdir(sys.argv[-2]) \
       or not pnfs.is_pnfs_path(sys.argv[-2]):
        sys.stderr.write("Second argument is not an input directory.\n")
        sys.exit(1)

    if sys.argv[-1] == "/dev/null":
        pass  #If the output is /dev/null, this is okay.
    elif not os.path.exists(sys.argv[-1]) or not os.path.isdir(sys.argv[-1]):
        sys.stderr.write("Third argument is not an output directory.\n")
        sys.exit(1)

    intf = encp.EncpInterface(sys.argv[0:-3] + sys.argv[-2:], 0) #zero = admin
    intf.volume = sys.argv[-3] #Hackish
    
    #print encp.format_class_for_print(intf, "intf")
    
    do_work(intf)

