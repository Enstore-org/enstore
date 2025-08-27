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
#import select
#import socket
#import errno
import copy
#import types
#import stat

# enstore modules
import encp
import get
#import pnfs
import e_errors
import delete_at_exit
#import host_config
import Trace
#import checksum
import option
#import enstore_functions3


#Completion status field values.
SUCCESS = encp.SUCCESS    #"SUCCESS"
FAILURE = encp.FAILURE    #"FAILURE"

#Return values to know if put should stop or keep going.
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

def put_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "v0_0  CVS $Revision$ "
    get_file = globals().get('__file__', "")
    if get_file: version_string = version_string + get_file
    return version_string

#encp.encp_client_version = put_client_version

class PutInterface(encp.EncpInterface):
    #  define our specific parameters
    user_parameters = [
        "<source file> [source file [...]] <destination directory>",
        ]
    admin_parameters = user_parameters
    parameters = user_parameters #gets overridden in __init__().
    
    def __init__(self, args=sys.argv, user_mode=0):

        #Get a copy, so we don't modifiy encp's Interface class too.
        # This is an issue only if migration:
        # 1) uses get for reads and encp for writes
        # 2) uses encp for reads and put for writes
        # 3) uses get for reads and put for writes
        # If migration uses encp for read and writes there is not a conflict.
        self.encp_options = copy.deepcopy(self.encp_options)

        try:
            del self.encp_options[option.VOLUME]
        except KeyError:
            pass
        try:
            del self.encp_options[option.PUT_CACHE]
        except KeyError:
            pass
        try:
            del self.encp_options[option.OVERRIDE_RO_MOUNT]
        except KeyError:
            pass
        try:
            del self.encp_options[option.OVERRIDE_PATH]
        except KeyError:
            pass
        try:
            del self.encp_options[option.OVERRIDE_DELETED]
        except KeyError:
            pass
        try:
            del self.encp_options[option.GET_CACHE]
        except KeyError:
            pass
        try:
            del self.encp_options[option.GET_BFID]
        except KeyError:
            pass
        try:
            del self.encp_options[option.ECRC]
        except KeyError:
            pass
        try:
            del self.encp_options[option.COPY]
        except KeyError:
            pass

        encp.EncpInterface.__init__(self, args=args, user_mode=user_mode)
        
        self.put = 1

        if self.args[-1] == "/dev/null":
            pass  #If the output is /dev/null, this is okay.
        elif not os.path.exists(self.args[-1]) or not os.path.isdir(self.args[-1]):
            try:
                message = "Last argument is not an output directory.\n"
                sys.stderr.write(message)
                sys.stderr.flush()
            except IOError:
                pass
            sys.exit(1)


"""
def finish_request(done_ticket, request_list, index):
    #Everything is fine.
    if e_errors.is_ok(done_ticket):
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
                      (done_ticket['outfile'],)
            Trace.message(e_errors.INFO, message)
            Trace.log(e_errors.INFO, message)

            #Set completion status to successful.
            done_ticket['completion_status'] = SUCCESS
            done_ticket['exit_status'] = 0
            
            #Store these changes back into the master list.
            request_list[index] = done_ticket

        return CONTINUE

    #Give up.
    elif e_errors.is_non_retriable(done_ticket):
        if index == None:
            message = "Unknown transfer failed."
            try:
                sys.stderr.write(message + "\n")
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR,
                      message + "  " + str(done_ticket))

        else:
            #Tell the user what happend.
            message = "File %s write failed: %s" % \
                      (done_ticket['outfile'], done_ticket['status'])
            Trace.message(1, message)
            Trace.log(e_errors.ERROR, message)

            #Set completion status to failure.
            done_ticket['completion_status'] = FAILURE
            done_ticket['exit_status'] = 2

        #Tell the calling process, this file failed.
        get.error_output(done_ticket)
        #Tell the calling process, of those files not attempted.
        get.untried_output(request_list)

        return STOP

    #Keep trying.
    elif e_errors.is_retriable(done_ticket):
        #On retriable error go back and resubmit what is left
        # to the LM.

        #Record the intermidiate error.
        message = "File %s write failed: %s" % \
                  (done_ticket['outfile'], done_ticket['status'])
        Trace.log(e_errors.WARNING, message)

        return CONTINUE_FROM_BEGINNING
"""

def writetape_to_hsm(e, tinfo):

    Trace.trace(16,"writetape_to_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" % (e.input, e.output, e.verbose,
                                      e.chk_crc, tinfo['encp_start_time']))
    
    #This needs to be defined somewhere.
    #files_transfered = 0
    byte_sum = 0L #Sum of bytes transfered (when transfering multiple files).
    exit_status = 0 #Used to determine the final message text.
    number_of_files = 0 #Total number of files where a transfer was attempted.

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
    done_ticket, listen_socket, udp_serv, request_list = \
                 encp.prepare_write_to_hsm(tinfo, e)

    if e.check:
        return done_ticket
    if not e_errors.is_ok(done_ticket):
        #Tell the calling process, this file failed.
        get.error_output(done_ticket)
        #Tell the calling process, of those files not attempted.
        get.untried_output(request_list)
        
        done_ticket['exit_status'] = 2
        return done_ticket
    
    ######################################################################
    # Time to start writing some files.
    ######################################################################

    while encp.requests_outstanding(request_list):

        work_ticket, index, copy = encp.get_next_request(request_list)

        #Send the request to write the file to the library manager.
        work_ticket['method'] = "write_tape_start"
        done_ticket, lmc = encp.submit_write_request(work_ticket, e)

        work_ticket = encp.combine_dict(done_ticket, work_ticket)
        #handle_retries() is not required here since submit_write_request()
        # handles its own retrying when an error occurs.
        if not e_errors.is_ok(work_ticket):
            return work_ticket

        # Establish control socket connection with the mover.
        control_socket, ticket = get.mover_handshake(listen_socket,
                                                     udp_serv,
                                                     work_ticket, e)

        # Verify that everything went ok with the handshake.
        external_label = work_ticket.get('fc', {}).get('external_label', None)
        result_dict = encp.handle_retries([work_ticket], ticket,
                                          ticket, e,
                                          external_label = external_label)

        if not e_errors.is_ok(result_dict):
            # Close these descriptors before they are forgotten about.
            if control_socket != None:
                encp.close_descriptors(control_socket)
            #Don't loose the non-retriable error.
            if e_errors.is_non_retriable(result_dict):
                work_ticket = encp.combine_dict(result_dict, work_ticket)
                return work_ticket

            continue

        #maybe this isn't a good idea...
        work_ticket = encp.combine_dict(ticket, work_ticket)

        use_unique_id = work_ticket['unique_id']

        # Keep looping until the READ_EOD error occurs.
        while encp.requests_outstanding(request_list):

            #Flush out the standard output and error descriptors.  This
            # should help in some cases when they are redirected to a file
            # and the bytes get stuck in buffer(s).
            sys.stdout.flush()
            sys.stderr.flush()

            #Get the next file in the list to transfer.
            request, index = encp.get_next_request(request_list)

            #Grab a new clean udp_socket.
            ### Note: This should not be necessary after a bug in the
            ### mover was fixed long ago.
            #udp_callback_addr, unused = encp.get_udp_callback_addr(
            #    e, udp_serv)

            #Combine the ticket from the mover with the current
            # information.  Remember the ealier dictionaries 'win'
            # in setting values.  encp.combine_dict() is insufficent
            # for this dictionary munge.  It must be done by hand
            # because both tickets have correct pieces of information
            # that is old in the other ticket.
            work_ticket['mover'] = ticket['mover']
            work_ticket['callback_addr'] = listen_socket.getsockname()
            #The ticket item of 'routing_callback_addr' is a legacy name.
            work_ticket['routing_callback_addr'] = \
                                         udp_serv.get_server_address()
            #Encp create_read_request() gives each file a new unique id.
            # The LM can't deal with multiple mover file requests from one
            # LM request.  Thus, we need to set this back to the last
            # unique id sent to the library manager.
            work_ticket['unique_id'] = use_unique_id
            #Store these changes back into the master list.
            request_list[index] = work_ticket

            message = "Preparing to write %s." % work_ticket['outfile']
            Trace.message(TRANSFER_LEVEL, message)
            Trace.log(e_errors.INFO, message)

            data_path_socket, done_ticket = get.mover_handshake2(work_ticket,
                                                                 udp_serv, e)

            #Give up.
            if e_errors.is_non_retriable(done_ticket['status'][0]):
                #Tell the user what happend.
                message = "File %s write failed: %s" % \
                          (work_ticket['outfile'], done_ticket['status'])
                Trace.message(DONE_LEVEL, message)
                Trace.log(e_errors.ERROR, message)

                #We are done with this mover.
                get.end_session(udp_serv, control_socket)
                #Set completion status to failure.
                work_ticket['completion_status'] = FAILURE
                work_ticket['status'] = done_ticket['status']
                work_ticket['exit_status'] = 2

                #Tell the calling process, this file failed.
                get.error_output(work_ticket)
                #Tell the calling process, of those files not attempted.
                get.untried_output(request_list)

                #Perform any necessary file cleanup.
                return work_ticket

            #Keep trying.
            elif e_errors.is_retriable(done_ticket['status'][0]):
                #On retriable error go back and resubmit what is left
                # to the LM.

                #Record the intermidiate error.
                message = "File %s write failed: %s" % \
                              (work_ticket['outfile'], done_ticket['status'])
                Trace.log(e_errors.WARNING, message)

                #We are done with this mover.
                get.end_session(udp_serv, control_socket)
                break

            #############################################################
            #In this function call is where most of the work in transfering
            # a single file is done.
            done_ticket = encp.write_hsm_file(work_ticket, control_socket,
                                              data_path_socket,
                                              tinfo, e,
                                              udp_serv = udp_serv)
            #############################################################

            # Close these descriptors before they are forgotten about.
            encp.close_descriptors(data_path_socket)

            #Sum up the total amount of bytes transfered.
            exfer_ticket = done_ticket.get('exfer',
                                           {'bytes_transfered' : 0L})
            byte_sum = byte_sum + exfer_ticket.get('bytes_transfered', 0L)

            #Combine the tickets.
            work_ticket = encp.combine_dict(done_ticket, work_ticket)
            #Store these changes back into the master list.
            request_list[index] = work_ticket

            #The completion_status is modified in the request ticket.
            # what_to_do = 0 for stop
            #            = 1 for continue
            #            = 2 for continue after retry
            what_to_do = encp.finish_request(request, request_list,
                                             index, e)

            #If on non-success exit status was returned from
            # finish_request(), keep it around for later.
            if work_ticket['exit_status']:
                #We get here only on an error.  If the value is 1, then
                # the error should be transient.  If the value is 2, then
                # the error will likely require human intervention to
                # resolve.
                exit_status = work_ticket['exit_status']
            # Do what finish_request() says to do.
            if what_to_do == STOP:
                #We get here only on a non-retriable error.
                get.end_session(udp_serv, control_socket)
                return done_ticket
            elif what_to_do == CONTINUE_FROM_BEGINNING:
                #We get here only on a retriable error.
                get.end_session(udp_serv, control_socket)
                break

    else:
        #If we get here, then, we should have a success.
        get.end_session(udp_serv, control_socket)

    # we are done transferring - close out the listen socket
    encp.close_descriptors(listen_socket)

    #Print to screen the exit status.
    Trace.message(TO_GO_LEVEL, "EXIT STATUS: %d" % exit_status)

    #Finishing up with a few of these things.
    calc_ticket = encp.calculate_final_statistics(byte_sum, number_of_files,
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
    Trace.init("PUT")
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
        #data_access_ayer_requested.set()
    elif intf.verbose < 0:
        #Turn off all output to stdout and stderr.
        encp.data_access_layer_requested = -1
        intf.verbose = 0
        
    done_ticket = writetape_to_hsm(intf, tinfo)

    return encp.final_say(intf, done_ticket)
    
"""
def do_work(intf):
    delete_at_exit.setup_signal_handling()

    try:
        exit_status = main(intf)
	get.halt(exit_status)
    except (SystemExit, KeyboardInterrupt):
	get.halt(1)
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
        import traceback
        traceback.print_tb(tb)
        Trace.handle_error(exc, msg, tb)
        del tb #No cyclic references.
        #Remove any zero-length files left haning around.  Also, return
        # a non-zero exit status to the calling program/shell.
        get.halt(1)
"""

if __name__ == "__main__":   # pragma: no cover

    #setup_put_interface()

    #intf_of_put = PutInterface(sys.argv, 0)

    """
    if intf_of_put.volume and \
       ( not os.path.exists(sys.argv[-2]) or not os.path.isdir(sys.argv[-2]) \
         or not pnfs.is_pnfs_path(sys.argv[-2]) ):
        try:
            sys.stderr.write("First argument is not an input directory.\n")
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)
    """

    """
    if sys.argv[-1] == "/dev/null":
        pass  #If the output is /dev/null, this is okay.
    elif not os.path.exists(sys.argv[-1]) or not os.path.isdir(sys.argv[-1]):
        try:
            sys.stderr.write("Last argument is not an output directory.\n")
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)
    """

    #print encp.format_class_for_print(intf_of_put, "intf_of_put")

    #delete_at_exit.quit(do_work(intf_of_put))
    delete_at_exit.delete_and_quit(encp.start(0, encp.do_work, main, PutInterface))
