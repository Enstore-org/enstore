
import sys
import os
import time
import pprint
import select
import socket

import encp
import pnfs
import e_errors
import delete_at_exit
import callback
import host_config
import Trace


def print_usage():
    print "Usage:", os.path.basename(sys.argv[0]), \
          "[--verbose <level>] <Volume> <Input Dir.> <Output Dir.>"

def transfer_file(in_fd, out_fd):

    bytes = 0L #Number of bytes transfered.

    while 1:

        r, w, x = select.select([in_fd], [], [], 15 * 60)

        if in_fd not in r:
            status = (e_errors.TIMEDOUT, "Read")
            return {'status' : status, 'bytes' : bytes}
        
        data = os.read(in_fd, 1048576)

        if len(data) == 0: #If read the EOF, return number of bytes transfered.
            status = (e_errors.OK, None)
            return {'status' : status, 'bytes' : bytes}

        r, w, x = select.select([], [out_fd], [], 15 * 60)

        if out_fd not in w:
            status = (e_errors.TIMEDOUT, "Write")
            return {'status' : status, 'bytes' : bytes}

        bytes += os.write(out_fd, data)
        
    return {'status' : (e_errors.BROKEN, "Reached unreachable code."),
            'bytes' : bytes}

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

def get_single_file(work_ticket, control_socket, e):

    #Loop around in case the file transfer needs to be retried.
    while work_ticket.get('retry', 0) <= e.max_retry:

        Trace.message(5, "Opening local file.")

        # Open the local file.
        done_ticket = encp.open_local_file(work_ticket['outfile'], e)
        
        if not e_errors.is_ok(done_ticket):
            sys.stderr.write("Unable to open local file %s: %s\n",
                             (work_ticket['outfile'], done_ticket['status'],))
            encp.quit(1)
        else:
            out_fd = done_ticket['fd']

        Trace.message(5, "Sending next file request to the mover.")

        # Send the request to the mover.
        work_ticket['method'] = "read_next" #evil hack
        try:
            done_ticket = callback.write_tcp_obj(control_socket, work_ticket)
        except e_errors.TCP_EXCEPTION:
            sys.stderr.write("Unable to communicate request to mover.\n",
                             (work_ticket['outfile'], done_ticket['status'],))
            encp.quit(1)

        Trace.message(5, "Opening the data socket.")

        #Open the data socket.
        try:
            data_path_socket = encp.open_data_socket(
                work_ticket['mover']['callback_addr'],
                work_ticket['callback_addr'][0])

            if not data_path_socket:
                print "2983333333333333"
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

        # Close these desriptors before they are forgotten about.
        encp.close_descriptors(out_fd, data_path_socket)
        
        # Verify that everything went ok with the transfer.
        result_dict = encp.handle_retries([work_ticket], work_ticket,
                                     done_ticket, None,
                                     None, None, e)

        #Everything is fine.
        if e_errors.is_ok(result_dict):
            work_ticket = encp.combine_dict(result_dict, work_ticket)
            delete_at_exit.unregister(work_ticket['outfile']) #don't delete
            return encp.combine_dict(result_dict, work_ticket)
        #The requested file does not exist on the tape.  (i.e. the tape has
        # only 6 files and the seventh file was requested.)
        if result_dict['status'] == (e_errors.READ_ERROR, e_errors.READ_EOD):
            return result_dict
        #Keep trying.
        elif e_errors.is_retriable(result_dict['status'][0]):
            continue
        #Give up.
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            return result_dict

    # Can we get here?
    return {'status' : (e_errors.TOO_MANY_RETRIES, None)}

def main(e):

    t0 = time.time()
    tinfo = {'t0' : t0, 'encp_start_time' : t0}

    #Initialize the Trace module.
    Trace.init("GET")
    for x in xrange(6, e.verbose+1):
        Trace.do_print(x)
    for x in xrange(1, e.verbose+1):
        Trace.do_message(x)

    # get a port to talk on and listen for connections
    callback_addr, listen_socket = encp.get_callback_addr(e)
    #Get an ip and port to listen for the mover address for routing purposes.
    routing_addr, udp_server = encp.get_routing_callback_addr(e)
    
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


    #Only the first submition goes to the LM for volume reads.
    requests_per_vol[e.volume][0]['method'] = "read_tape_start" #evil hack
    Trace.message(10, "LM SUBMITION TICKET:")
    Trace.message(10, pprint.pformat(requests_per_vol[e.volume][0]))
    submitted, reply_ticket = encp.submit_read_requests(
            [requests_per_vol[e.volume][0]], tinfo, e)

    if not e_errors.is_ok(reply_ticket):
        sys.stderr.write("Unable to read volume %s: %s\n",
                         (e.volume, reply_ticket['status']))
        encp.quit(1)

    Trace.message(4, "Read tape submition sent to LM.")

    #Open the routing socket.
    config = host_config.get_config()
    use_listen_socket = listen_socket
    try:
        #There is no need to do this on a non-multihomed machine.
        if config and config.get('interface', None):
            Trace.message(4, "Opening routing socket.")
            
            ticket, use_listen_socket = encp.open_routing_socket(
                udp_server,
                [requests_per_vol[e.volume][0]['unique_id']], #unique_id_list
                e)

            if not e_errors.is_ok(ticket):
                sys.stderr.write("Unable to handle routing: %s\n",
                         (reply_ticket['status'],))
                encp.quit(1)

            Trace.message(4, "Opened routing socket.")
    except (encp.EncpError,), detail:
        sys.stderr.write("Unable to handle routing: %s\n",
                         (reply_ticket['status'],))
        encp.quit(1)

    #Open the control socket.
    Trace.message(4, "Opening control socket.")
    try:
        control_socket, mover_address, ticket = \
                        encp.open_control_socket(use_listen_socket, 15 * 60)

        if not e_errors.is_ok(ticket['status']):
            sys.stderr.write("Unable to open control socket with mover: %s\n",
                             (ticket['status'],))
            encp.quit(1)
    except (encp.EncpError,), detail:
        sys.stderr.write("Unable to open control socket with mover: %s\n",
                         (reply_ticket['status'],))
        encp.quit(1)

    Trace.message(4, "Opened control socket.")

    # If this is a volume where the file information is not known...
    if requests_per_vol[e.volume][0].get('bfid', None) == None:
        #Initalize these.
        file_number = 1
        request = encp.combine_dict(ticket, request)

        # Keep looping until the READ_EOD error occurs.
        while 1:
            
            Trace.message(4, "Preparing to read %s." % request['outfile'])
            
            #Read from tape.
            done_ticket = get_single_file(request, control_socket, e)

            if e_errors.is_ok(done_ticket):
                Trace.message(1,
                           "File %s copied successfully." % request['infile'])
            elif done_ticket['status'] == (e_errors.READ_ERROR,
                                           e_errors.READ_EOD):
                #The last file was already read.  Exit.
                break
            else:
                Trace.message(1,"File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))

            # The following lines setup the next file to read.

            #The fields need to be updated for the next file on the tape
            # to be read.
            #Note: This will not work for the cern wrapper.  For this
            # wrapper the header and trailer consume a 'file' on the tape.
            file_number += 1
            next_request_update(request, file_number)
            #Create the local file.
            encp.create_zero_length_files(request['outfile'])
    
    # ... Or there is file information is already known.
    else:
        # Loop through the already known files list.
        for request in requests_per_vol[e.volume]:

            Trace.message(4, "Preparing to read %s." % request['outfile'])

            request = encp.combine_dict(ticket, request)
            done_ticket = get_single_file(request, control_socket, e)

            if e_errors.is_ok(done_ticket):
                Trace.message(1,
                           "File %s copied successfully." % request['infile'])
            else:
                Trace.message(1,"File %s read failed: %s" %
                              (request['infile'], done_ticket['status']))

            #print "DONE_TICKET:"
            #pprint.pprint(done_ticket)
            
            #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #check_crc(done_ticket, e, out_fd) #Check the CRC.
            #verify_file_size(done_ticket) #Verify size is the same.
            #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!        

    #We are done, tell the mover.
    nowork_ticket = {'work': "nowork", 'method' : "no_work"}
    try:
        done_ticket = callback.write_tcp_obj(control_socket, nowork_ticket)
    except e_errors.TCP_EXCEPTION:
        sys.stderr.write("Unable to terminate communication "
                         "with mover cleanly.\n")
        encp.quit(1)

    #Either success or failure; this can be closed.
    encp.close_descriptors(control_socket)
    
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

    intf = encp.EncpInterface(sys.argv[0:-3] + sys.argv[-2:], 1) # one = user
    intf.volume = sys.argv[-3] #Hackish

    #print encp.format_class_for_print(intf, "intf")
    
    do_work(intf)

