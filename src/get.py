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

SUCCESS = "SUCCESS"
FAILURE = "FAILURE"

def print_usage():
    print "Usage:", os.path.basename(sys.argv[0]), \
          "[--verbose <level>] <Volume> <Input Dir.> <Output Dir.>"

#def get_request_callback_addr(intf):
#
#    u = udp_client.UDPClient()
#    tsd = u.get_tsd()
#    
#    return (tsd.host, tsd.port), u
    
def transfer_file(in_fd, out_fd):

    bytes = 0L #Number of bytes transfered.
    crc = 0L

    while 1:

        r, w, x = select.select([in_fd], [], [], 15 * 60)

        if in_fd not in r:
            status = (e_errors.TIMEDOUT, "Read")
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}
        
        data = os.read(in_fd, 1048576)

        if len(data) == 0: #If read the EOF, return number of bytes transfered.
            status = (e_errors.OK, None)
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}

        r, w, x = select.select([], [out_fd], [], 15 * 60)

        if out_fd not in w:
            status = (e_errors.TIMEDOUT, "Write")
            return {'status' : status, 'bytes' : bytes, 'encp_crc' : crc}

        bytes = bytes + os.write(out_fd, data)

        #Calculate the checksum
        crc = checksum.adler32(crc, data, len(data))

    return {'status' : (e_errors.BROKEN, "Reached unreachable code."),
            'bytes' : bytes, 'encp_crc' : crc}

#Update the ticket so that next file can be read.
def next_request_update(work_ticket, file_number):
    lc = "0000_000000000_%07d" % file_number

    #Update the fields with the new location cookie.
    work_ticket['fc']['location_cookie'] = lc

    #Only update these fields if a filename is not known already.
    #This could be a problem if a file is the same name as its location cookie.
    if encp.is_location_cookie(work_ticket['infile']):
        # Update the tickets fields for the next file.
        work_ticket['infile'] = lc
        work_ticket['outfile'] = \
                    os.path.join(os.path.dirname(work_ticket['outfile']), lc)
        work_ticket['wrapper']['fullname'] = work_ticket['outfile']

    #Update the unique id for the LM.
    work_ticket['unique_id'] = encp.generate_unique_id()


def get_single_file(work_ticket, control_socket, udp_socket, e):

    #Loop around in case the file transfer needs to be retried.
    #while work_ticket.get('retry', 0) <= e.max_retry:

        Trace.message(5, "Opening local file.")

        # Open the local file.
        done_ticket = encp.open_local_file(work_ticket['outfile'], e)
        
        if not e_errors.is_ok(done_ticket):
            sys.stderr.write("Unable to open local file %s: %s\n",
                             (work_ticket['outfile'], done_ticket['status'],))
            encp.quit(1)
        else:
            out_fd = done_ticket['fd']

        #Get a port that will be used to send the requests to the mover.
        #This may not be necessary...
        #request_addr, request_socket = get_request_callback_addr(e)

        Trace.message(5, "Sending next file request to the mover.")

        # Send the request to the mover. (evil hacks)
        work_ticket['method'] = "read_next"
        request  = udp_socket.process_request()
        Trace.message(10, "MOVER_MESSAGE:")
        Trace.message(10, pprint.pformat(request))
        udp_socket.reply_to_caller(work_ticket)

        Trace.message(5, "Opening the data socket.")

        #Open the data socket.
        try:
            data_path_socket = encp.open_data_socket(
                work_ticket['mover']['callback_addr'],
                work_ticket['callback_addr'][0])

            if not data_path_socket:
                raise socket.error(errno.ENOTCONN,
                                   errno.errorcode[errno.ENOTCONN])
        except (encp.EncpError, socket.error), detail:
            sys.stderr.write("Unable to open data socket with mover: %s\n" %
                             (str(detail),))
            encp.quit(1)

        Trace.message(5, "Waiting for data")

        # Stall starting the count until the first byte is ready for reading.
        read_fd, write_fd, exc_fd = select.select([data_path_socket], [],
                                                  [data_path_socket], 15 * 60)

        if data_path_socket not in read_fd:
            return {'status':(e_errors.TIMEDOUT, "No data received")}

        Trace.message(5, "Reading data from tape.")
        
        # Read the file from the mover.
        done_ticket = transfer_file(data_path_socket.fileno(), out_fd)

        Trace.message(5, "Completed reading from tape.")
        Trace.message(10, "DONE_TICKET:")
        Trace.message(10, pprint.pformat(done_ticket))

        # Verify that everything went ok with the transfer.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                     done_ticket, None,
                                     None, None, e)

        if not e_errors.is_ok(result_dict):
            work_ticket = encp.combine_dict(result_dict,
                                            work_ticket)
            Trace.log(e_errors.ERROR, str(result_dict['status']))
            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd, data_path_socket)
            return work_ticket

        #Pretend we are the library manager.
        request = udp_socket.process_request()
        Trace.message(10, "LM MESSAGE:")
        Trace.message(10, pprint.pformat(request))

        #Combine these tickets.  Encp would have this already done, in
        # its transfer_file() function, but not gets transfer_file() function.
        work_ticket = encp.combine_dict({'exfer' : done_ticket},
                                        result_dict, work_ticket)

        #Get the final success/failure message from the mover.  If this side
        # has an error, don't wait for the mover in case the mover is waiting
        # for "Get" to do something.
        Trace.message(5, "Waiting for final dialog (1).")
        mover_done_ticket = encp.receive_final_dialog(control_socket)
        Trace.message(5, "Received final dialog (1).")
        Trace.message(10, "MOVER_DONE_TICKET:")
        Trace.message(10, pprint.pformat(mover_done_ticket))
        Trace.message(5, "Waiting for final dialog. (2)")
        #Keep the udp socket queues clear.
        mover_request = udp_socket.process_request()
        Trace.message(5, "Received final dialog. (2)")
        Trace.message(10, "MOVER_REQUEST:")
        Trace.message(10, pprint.pformat(mover_request))
        
        # Verify that everything went ok with the transfer.
        mover_result_dict = encp.handle_retries([work_ticket], work_ticket,
                                                mover_done_ticket, None,
                                                None, None, e)

        if not e_errors.is_ok(mover_result_dict):
            work_ticket = encp.combine_dict(mover_result_dict,
                                            work_ticket)
            Trace.log(e_errors.ERROR, str(mover_result_dict['status']))
            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(out_fd, data_path_socket)
            return work_ticket

        #Combine these tickets
        work_ticket = encp.combine_dict(mover_result_dict, work_ticket)
        
        #Check the crc.  Note: done_ticket has any error status set to it by
        # check_crc.
        encp.check_crc(work_ticket, e, out_fd)

        # Close these descriptors before they are forgotten about.
        encp.close_descriptors(out_fd, data_path_socket)

        # Verify that the crcs are correct and handle occordingly.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                          done_ticket, None,
                                          None, None, e)

        if not e_errors.is_ok(result_dict):
            work_ticket = encp.combine_dict(result_dict, work_ticket)
            Trace.log(e_errors.ERROR, str(result_dict['status']))
            return work_ticket
        
        #verify_file_size(done_ticket) #Verify size is the same.

        work_ticket = encp.combine_dict(result_dict, work_ticket)
        return work_ticket
        
def set_metadata(ticket, intf):
    pass
    #Create the pnfs file.
    #encp.create_zero_length_files(ticket['infile'])

    #Set the metadata for this new file.
    #encp.set_pnfs_settings(ticket, intf)

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

def requests_outstanding(request_list):

    files_left = 0

    for request in request_list:
        if request.get('completion_status', None) == None: 
            files_left = files_left + 1

    return files_left

#Return the next uncompleted transfer.
def get_next_request(request_list):

    for i in range(len(request_list)):
        if request_list[i].get('completion_status', None) == None:
            return request_list[i], i

    return None, 0
            
def main(e):

    t0 = time.time()
    tinfo = {'t0' : t0, 'encp_start_time' : t0}

    #Initialize the Trace module.
    Trace.init("GET")
    for x in xrange(6, e.verbose+1):
        Trace.do_print(x)
    for x in xrange(1, e.verbose+1):
        Trace.do_message(x)

    exit_status = 0 #For rembering failures.
    files_transfered = 0

    #Get a port to talk on and listen for connections
    callback_addr, listen_socket = encp.get_callback_addr(e)
    #Get an ip and port to listen for the mover address for
    # routing purposes.
    routing_addr, udp_socket = encp.get_routing_callback_addr(e)

    #Create all of the request dictionaries.
    requests_per_vol = encp.create_read_requests(callback_addr, routing_addr,
                                                 tinfo, e)
    #Set the max attempts that can be made on a transfer.
    check_lib = requests_per_vol.keys()
    encp.max_attempts(requests_per_vol[check_lib[0]][0]['vc']['library'], e)

    #This might be a problem when zero information is available...
    for request in requests_per_vol[e.volume]:
        #Make sure that we are not clobbering files.
        encp.outputfile_check(request['infile'], request['outfile'], e)

    for request in requests_per_vol[e.volume]:
        #Create the zero length file entry.
        encp.create_zero_length_files(request['outfile'])

    while requests_outstanding(requests_per_vol[e.volume]):

        #Get the next volume in the list to transfer.
        request, index = get_next_request(requests_per_vol[e.volume])

        #Only the first submition goes to the LM for volume reads.
        request['method'] = "read_tape_start" #evil hacks
        request['route_selection'] = 1 #On for Get..
        request['submitted'] = None #Failures won't be re-sent if not None.
        Trace.message(10, "LM SUBMITION TICKET:")
        Trace.message(10, pprint.pformat(request))
        submitted, reply_ticket = encp.submit_read_requests(
            [request], tinfo, e)

        if not e_errors.is_ok(reply_ticket):
            sys.stderr.write("Unable to read volume %s: %s\n",
                             (e.volume, reply_ticket['status']))
            encp.quit(1)

        Trace.message(4, "Read tape submition sent to LM.")

        #Open the routing socket.
        #config = host_config.get_config()
        use_listen_socket = listen_socket
        try:
            #There is no need to do this on a non-multihomed machine.
            #if config and config.get('interface', None):

            Trace.message(4, "Opening routing socket.")

            rticket, use_listen_socket = encp.open_routing_socket(
                udp_socket, [request['unique_id']], e)

            if not e_errors.is_ok(rticket):
                sys.stderr.write("Unable to handle routing: %s\n",
                                 (rticket['status'],))
                encp.quit(1)

            Trace.message(4, "Opened routing socket.")
        except (encp.EncpError,), detail:
            sys.stderr.write("Unable to handle routing: %s\n",
                             (str(detail),))
            encp.quit(1)

        #Open the control socket.
        Trace.message(4, "Opening control socket.")
        try:
            control_socket, mover_address, ticket = \
                            encp.open_control_socket(use_listen_socket, 15*60)

            if not e_errors.is_ok(ticket['status']):
                sys.stderr.write("Unable to open control socket with mover: %s\n",
                                 (ticket['status'],))
                encp.quit(1)
        except (encp.EncpError,), detail:
            sys.stderr.write("Unable to open control socket with mover: %s\n"
                             % (str(detail),))
            encp.quit(1)

        Trace.message(4, "Opened control socket.")


        #If this is a volume where the file information is not known and
        # the user did not specify the filenames...
        #if len(requests_per_vol[e.volume]) == 1 and \
        #   requests_per_vol[e.volume][0].get('bfid', None) == None:
        if request.get('bfid', None) == None:
            #Initalize this.
            file_number = 1

        # Keep looping until the READ_EOD error occurs.
        while requests_outstanding(requests_per_vol[e.volume]):

            #Combine the ticket from the mover with the current information.
            request = encp.combine_dict(ticket, request)
            #Store these changes back into the master list.
            requests_per_vol[e.volume][index] = request
            
            Trace.message(4, "Preparing to read %s." % request['outfile'])
            
            done_ticket = get_single_file(request, control_socket,
                                          udp_socket, e)

            #Everything is fine.
            if e_errors.is_ok(done_ticket):
                #Tell the user what happend.
                Trace.message(1,
                          "File %s copied successfully." % request['infile'])
                Trace.log(e_errors.ERROR,
                          "File %s copied successfully." % request['infile'])

                #Combine the tickets:
                request = encp.combine_dict(done_ticket, request)
                #Don't delete.
                delete_at_exit.unregister(request['outfile']) 
                #Remember the completed transfer.
                files_transfered = files_transfered + 1

                if request.get('bfid', None) == None:
                    #The fields need to be updated for the next file
                    # on the tape to be read.
                    #Note: This will not work for the cern wrapper.  For this
                    # wrapper the header and trailer consume a 'file' on
                    # the tape.
                    file_number = file_number + 1
                    next_request_update(request, file_number)

                    #Set the metadata.
                    set_metadata(request, e)
                else:
                    #Set completion status to successful.
                    request['completion_status'] = SUCCESS

                #Store these changes back into the master list.
                requests_per_vol[e.volume][index] = request
                #Get the next request before continueing.
                request, index = get_next_request(requests_per_vol[e.volume])
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

                encp.quit(1)
            #Give up.
            elif e_errors.is_non_retriable(done_ticket['status'][0]) or \
                 e_errors.is_non_retriable(done_ticket['status'][0]):
                #Tell the user what happend.
                Trace.message(1, "File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))
                Trace.log(e_errors.ERROR, "File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))

                #We are done with this mover.
                end_session(udp_socket, control_socket)
                #Set completion status to failure.
                request['completion_status'] = FAILURE

                encp.quit(1)
            #Keep trying.
            elif e_errors.is_retriable(done_ticket['status'][0]) or \
                 e_errors.is_retriable(done_ticket['status'][0]):
                #On retriable error go back and resubmit what is left
                # to the LM.

                #Record the intermidiate error.
                Trace.log(e_errors.ERROR, "File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))

                #We are done with this mover.
                end_session(udp_socket, control_socket)
                break

    encp.quit(0)

def do_work(intf):
    delete_at_exit.setup_signal_handling()

    try:
        main(intf)
        encp.quit(0)
    except SystemExit, msg:
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

    if not os.path.exists(sys.argv[-1]) or not os.path.isdir(sys.argv[-1]):
        sys.stderr.write("Third argument is not an output directory.\n")
        sys.exit(1)

    intf = encp.EncpInterface(sys.argv[0:-3] + sys.argv[-2:], 0) #zero = admin
    intf.volume = sys.argv[-3] #Hackish
    
    #print encp.format_class_for_print(intf, "intf")
    
    do_work(intf)

