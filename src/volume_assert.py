#!/usr/bin/env python

###############################################################################
#
# src/$RCSfile$   $Revision$
#
###############################################################################

# system imports
import os
import sys
import string
import socket
import select
import pprint
import time
import errno
#import re
import types
try:
    import multiprocessing
    multiprocessing_available = True
except ImportError:
    multiprocessing_available = False
import threading
import thread

# enstore imports
import configuration_client
import library_manager_client
import volume_clerk_client
import option
import e_errors
import callback
import Trace
import hostaddr
import encp
import generic_client
import delete_at_exit
import enstore_functions3
import file_clerk_client

MY_NAME = "ASSERT"

#Hack for migration to report an error, instead of having to go to the log
# file for every error.
err_msg = {}
try:
    err_msg_lock = multiprocessing.Lock()
except NameError:
    err_msg_lock = threading.Lock()

############################################################################
############################################################################

def volume_assert_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "CVS $Revision$ "
    encp_file = globals().get('__file__', "")
    if encp_file: version_string = version_string + os.path.basename(encp_file)
    #If we end up longer than the current version length supported by the
    # accounting server; truncate the string.
    if len(version_string) > encp.MAX_VERSION_LENGTH:
	version_string = version_string[:encp.MAX_VERSION_LENGTH]
    return version_string


#Parse the file containing the volumes to be asserted.  It expects the first
# word on each line to be the volume.  Reamining text on a line are optionally
# location cookies to have their CRC verified.  Also, any line beginning with
# a "#" is a comment and ignored.
#
#The return type is a dictionary.  The keys are the volumes to check; the
# values are a list of location cookies.
def parse_file(filename):
    #paranoid check
    if type(filename) != types.StringType:
        return {}
    #Handle file access problems gracefully.
    try:
        fp = open(filename, "r")
    except OSError, msg:
        try:
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)
        
    data=map(string.strip, fp.readlines())
    tmp = {}
    for line in data:
	try:
            #The first element should be the volume.
            words = string.split(line)
            words[0].strip()
            if words[0] != "#":
                if not hasattr(tmp, words[0]): #Skip duplicates.
                    tmp[words[0]] = [] #First line for this volume.

                #The remaining elements should be location cookies.
                for lc in words[1:]:
                    if enstore_functions3.is_location_cookie(lc):
                        if lc not in tmp[words[0]]: #Skip duplicates.
                            tmp[words[0]].append(lc)
                    else:
                        sys.stderr.write("Not a location cookie: %s" % (lc,))
                        sys.stderr.flush()
	except IndexError:
	    continue #This happens for blank lines

    fp.close()
    return tmp

def parse_comma_list(comma_seperated_string):
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

def get_clerks_list():
    #Determine the entire valid list of configuration servers.
    csc = configuration_client.ConfigurationClient()
    config_server_addr_list = csc.get('known_config_servers', 10, 6)
    if not e_errors.is_ok(config_server_addr_list['status']):
        message = str(config_server_addr_list['status'])
        try:
            sys.stderr.write("%s\n" % (message,))
            sys.stderr.flush()
        except IOError:
            pass
        Trace.log(e_errors.ERROR, message)
        sys.exit(1)
    #Remove status.
    del config_server_addr_list['status']
    
    #Add this hosts current enstore system to the beginning of the list.
    csc_list = []
    csc_list.append(csc)
    vcc_list = []
    vcc_list.append(volume_clerk_client.VolumeClerkClient(csc,
                                                          rcv_timeout=5,
                                                          rcv_tries=2))
    fcc_list = []
    fcc_list.append(file_clerk_client.FileClient(csc,
                                                 rcv_timeout=5,
                                                 rcv_tries=2))
                    
    #For all systems that respond get the volume clerk and configuration
    # server clients.
    for config in config_server_addr_list.values():
        Trace.trace(4, "Locating volume clerk on %s." % (config,))
        _csc = configuration_client.ConfigurationClient(config)
        csc_list.append(_csc)
        vcc_list.append(volume_clerk_client.VolumeClerkClient(_csc,
                                                              rcv_timeout=5,
                                                              rcv_tries=2))
        fcc_list.append(file_clerk_client.FileClient(_csc,
                                                          rcv_timeout=5,
                                                          rcv_tries=2))

    return csc_list, vcc_list, fcc_list

def create_assert_list(check_requests, media_validate=None):
    #The list of volume clerks to check.
    csc_list, vcc_list, fcc_list = get_clerks_list()

    #Determine the calback address.
    callback_addr, listen_socket = encp.get_callback_addr()  #intf)

    #For each volume in the list, determine which system it belongs in by
    # asking each volume clerk until one responds positively.  When one does
    # build the ticket and add it to the list to assert.
    assert_list = []
    vol_list = check_requests.keys()
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
            #Internally used values.
            ticket['_csc'] = csc_list[i].server_address
            #Required items.
            ticket['unique_id'] = encp.generate_unique_id()
            ticket['callback_addr'] = callback_addr
            ticket['vc'] = vc
            ticket['vc']['address'] = vcc_list[i].server_address  #vcc instance
            ticket['vc']['storage_group'] = ''
            #Optional values based on command line switches.
            if check_requests[vol] == []:
                ticket['action'] = "crc_check"
                #ticket['parameters'] = [] #optional
            elif isinstance(check_requests[vol], (tuple, list)):
                ticket['action'] = "crc_check"
                ticket['parameters'] = check_requests[vol]
            if media_validate and vc['media_type'] == "T10000T2":
                ticket['action'] = media_validate
            #The following are for the inquisitor.
            ticket['vc']['file_family'] = ""
	    ticket['fc'] = {}  #Easier to do this than modify the mover.
            ticket['fc']['external_label'] = vc['external_label']
            ticket['fc']['location_cookie'] = "0000_000000000_0000000"
            ticket['fc']['address'] = fcc_list[i].server_address  #fcc instance
            ticket['fc']['pnfsid'] = ''
            ticket['fc']['pnfs_name0'] = ''
            ticket['fc']['deleted'] = ''
            ticket['fc']['bfid'] = ''
            ticket['fc']['sanity_cookie'] = (0,0)
            ticket['fc']['complete_crc'] = 0L
            ticket['fc']['size'] = 0L
            
	    ticket['times'] = {}
	    ticket['times']['t0'] = time.time()
	    ticket['encp'] = {}
	    ticket['encp']['adminpri'] = -1
	    ticket['encp']['basepri'] = 1
            ticket['encp']['curpri'] = 0  #For transfers, this is set by LM.
            ticket['encp']['delpri'] = 0
            ticket['encp']['agetime'] = 0
            ticket['encp']['delayed_dismount'] = 1
            ticket['infile'] = ""
            ticket['outfile'] = ""
            ticket['volume'] = vol
            #ticket['version'] = #LM will ignore if version isn't encp's.
            ticket['wrapper'] = encp.get_uinfo()
            ticket['wrapper']['size_bytes'] = 0
            ticket['wrapper']['machine'] = os.uname()
            ticket["wrapper"]["pnfsFilename"] = ""
            ticket["wrapper"]["fullname"] = ""
            ticket["wrapper"]['inode'] = 0
            ticket["wrapper"]['major'] = 0
            ticket["wrapper"]['minor'] = 0
            ticket["wrapper"]['sanity_size'] = 0
            ticket["wrapper"]['mode'] = 0
            ticket['override_ro_mount'] = 1
            
            ticket['file_size'] = 0
            ticket['version'] = ''

            #Add the assert work ticket to the list of volume asserts.
            assert_list.append(ticket)

            break  #When the correct vcc is found skip the rest.

        else:
            try:
                sys.stderr.write(e_msg)
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR, e_msg)

    return assert_list, listen_socket

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
        responce_ticket = lmc.volume_assert(ticket, 10, 6)

        if not e_errors.is_ok(responce_ticket['status']):
            message = "Submission for %s failed: %s\n" % \
                      (ticket['vc']['external_label'],
                       responce_ticket['status'])
            try:
                sys.stderr.write("%s\n" % (message,))
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR, message)
            continue

        unique_id_list.append(ticket['unique_id'])

    return unique_id_list  #return the list of unique ids to wait for.

def report_assert_results(done_ticket):
    global err_msg

    err_msg_lock.acquire()
    try:
        #For migration to report directly.
        err_msg[thread.get_ident()].append(done_ticket)
    except (KeyboardInterrupt, SystemExit):
        err_msg_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        try:
            Trace.handle_error()
        except (KeyboardInterrupt, SystemExit):
            err_msg_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass
    err_msg_lock.release()
            
    Trace.trace(10, "DONE TICKET")
    Trace.trace(10, pprint.pformat(done_ticket))

    #Print message if requested.  If an error occured print that to stderr.
    message = "Volume %s status is %s" % (
        done_ticket.get('volume', e_errors.UNKNOWN),
        done_ticket.get('status', (e_errors.UNKNOWN, None)))
    if e_errors.is_ok(done_ticket['status']):
        Trace.trace(1, message)
        exit_status = 0
    else:
        try:
            sys.stderr.write(message + "\n")
            sys.stderr.flush()
        except IOError:
            pass
        Trace.log(e_errors.ERROR, message)
        exit_status = 1

    #If CRC checks were requested, report the results.
    lc_keys = done_ticket.get('return_file_list', {}).keys()
    lc_keys.sort() #Sort them in order.
    for key in lc_keys:
        message = "file %s:%s status is %s" % (
            done_ticket.get('volume', e_errors.UNKNOWN), key,
            done_ticket['return_file_list'][key])
        if e_errors.is_ok(done_ticket['return_file_list'][key]):
            Trace.trace(1, message)
        else:
            try:
                sys.stderr.write(message + "\n")
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR, message) #log files only on error

    return exit_status
        
def stall_volume_assert(control_socket):
    while 1:
        try:
            read_control_fd, unused, unused = select.select([control_socket],
                                                            [], [], None)
            status_ticket = {'status' : (e_errors.OK, None)}
        except (socket.error, select.error), msg:
            if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
                #select() was interupted by a signal.
                continue
            
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         "%s: %s" % (str(msg),
                                               "No data read from mover."))}
            break
        except e_errors.TCP_EXCEPTION:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         e_errors.TCP_EXCEPTION)}
            break

        if control_socket in read_control_fd \
               and callback.get_socket_read_queue_length(control_socket):
            break

    return status_ticket

#Unique_id_list is a list of just the unique ids.  Assert_list is a list of
# the complete tickets.  Return false (0) for success, or true (1) if an
# error occured.
def handle_assert_requests(unique_id_list, assert_list, listen_socket, intf):

    error_id_list = []
    completed_id_list = []

    exit_status = 0  #set to 1 on error
    
    while len(error_id_list) + len(completed_id_list) < len(assert_list):
        
        try:
            #Obtain the control socket.
            socket, addr, callback_ticket = \
                    encp.open_control_socket(listen_socket, intf.mover_timeout)
        except KeyboardInterrupt:
            raise sys.exc_info()
        except:
            #Output a message.
            msg = sys.exc_info()[1]
            message = str(msg)
            try:
                sys.stderr.write("%s\n" % (message,))
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.WARNING, message)

            uncompleted_list = []
            for i in range(len(assert_list)):
                if assert_list[i]['unique_id'] not in error_id_list or \
                   assert_list[i]['unique_id'] not in completed_id_list:
                    #If an error occured, update the unique id.
                    if getattr(msg, "errno", None) != errno.ETIMEDOUT:
                        assert_list[i]['unique_id'] = encp.generate_unique_id()
                    uncompleted_list.append(assert_list[i])
            #Resend or resubmit the volume request.
            if len(uncompleted_list) > 0:
                submit_assert_requests([uncompleted_list[0]])
            continue

        Trace.trace(10, "CONTROL SOCKET CALLBACK TICKET")
        Trace.trace(10, pprint.pformat(callback_ticket))

        Trace.trace(1, "Asserting volume %s." % \
                    callback_ticket['vc']['external_label'])

        if not e_errors.is_ok(callback_ticket['status']):
            message = "Early error for %s: %s" % \
                      (callback_ticket['vc']['external_label'],
                       str(callback_ticket['status']))
            #Output a message.
            try:
                sys.stderr.write("%s\n" % (message,))
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.WARNING, message)
            #Do not retry from error.
            error_id_list.append(callback_ticket['unique_id'])
            continue

        #Handle erroneous callbacks.
        if callback_ticket['unique_id'] not in unique_id_list:
            socket.close()
            message = "Received unique id %s that is not expected." % \
                      (callback_ticket['unique_id'],)
            message2 = "Expected unique id list: %s\n" % (unique_id_list,)
            try:
                sys.stderr.write("%s\n" % (message,))
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR, message)
            Trace.log(e_errors.ERROR, message2)
            continue

        if intf.crc_check or intf.media_validate:
            stall_volume_assert(socket)
            

        try:
            #Obtain the results of the volume assert by the mover.
            done_ticket = encp.receive_final_dialog(socket)
        except KeyboardInterrupt:
            raise sys.exc_info()
        except:
            #Output a message.
            msg = sys.exc_info()[1]
            message = "Encountered final dialog error for %s: %s\n" % \
                      (callback_ticket['vc']['external_label'], str(msg))
            try:
                sys.stderr.write("%s\n" % (message,))
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR, message)
            #Do not retry from error.
            error_id_list.append(callback_ticket['unique_id'])
            continue

        #Report on the success and failure of the volume asserts and any
        # CRC checks on files that might have been done too.
        exit_status = exit_status + report_assert_results(done_ticket)

        #Perform some accounting so we know what is remaining.
        if done_ticket.get('unique_id', None) != None:
            completed_id_list.append(done_ticket['unique_id'])
        else:
            error_id_list.append(1) #Remember filler for counts.

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

    #There were errrors.
    if len(error_id_list) > 0:
        return 1
    if exit_status:
        return 1

    #Everything went fine.
    return 0

############################################################################

def log_volume_assert_start():
    global err_msg #hack for migration to report any error.

    #Clear the information that is used by migration to determine what
    # failed and what succeeded.
    err_msg_lock.acquire()
    try:
        err_msg[thread.get_ident()] = []
    except (KeyboardInterrupt, SystemExit):
        err_msg_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        try:
            Trace.handle_error()
        except (KeyboardInterrupt, SystemExit):
            err_msg_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass
    err_msg_lock.release()

    #log_volume_assert__start_time = time.time()

    message = 'Volume assert called with args: %s' % (sys.argv,)
    Trace.trace(3, message)
    Trace.log(e_errors.INFO, message)

############################################################################
############################################################################


class VolumeAssertInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        self.verbose = 0
        self.volume = ""
        self.mover_timeout = 60*60
        self.crc_check = None
        self.media_validate = None
        self.location_cookies = None
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.volume_assert_options)
    
    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

    parameters = ["volume_list_file"]
    
    volume_assert_options = {
        #option.CRC_CHECK:{option.HELP_STRING:"crc check specific file(s), " \
        #                  "seperate multiple location cookies with commas",
        #                  option.DEFAULT_NAME:'crc_check',
        #                  option.DEFAULT_TYPE:option.INTEGER,
        #                  option.VALUE_USAGE:option.OPTIONAL,
        #                  option.VALUE_TYPE:option.STRING,
        #                  option.VALUE_NAME:'location_cookies',
        #                  option.USER_LEVEL:option.USER,
        #                  option.FORCE_SET_DEFAULT:option.FORCE,},
        option.CRC_CHECK:{option.HELP_STRING:"crc check for volume(s)",
                          option.VALUE_TYPE:option.INTEGER,
                          option.VALUE_USAGE:option.IGNORED,
                          option.USER_LEVEL:option.ADMIN},
        option.MEDIA_VALIDATE:{option.HELP_STRING:"validate media (T10000 tapes only) [complete, standard]",
                               option.VALUE_TYPE:option.STRING,
                               option.VALUE_USAGE:option.REQUIRED,
                               option.USER_LEVEL:option.ADMIN},
        option.MOVER_TIMEOUT:{option.HELP_STRING:"set mover timeout period "\
                              " in seconds (default 1 hour)",
                              option.VALUE_USAGE:option.REQUIRED,
                              option.VALUE_TYPE:option.INTEGER,
                              option.USER_LEVEL:option.USER,},
        option.VERBOSE:{option.HELP_STRING:"print out information.",
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
    #Some globals are expected to exists for normal operation (i.e. a logger
    # client).  Create them.  (Ignore errors returned from clients.)
    encp.clients(intf)
    #Log that we have started the assert.
    log_volume_assert_start()
    validate = None
    #Read in the list of vols.
    if intf.args:  #read from file.
        check_requests = parse_file(intf.args[0])

        #If the user did not specify --crc-check, then convert all empty
        # location cookie lists to None.
        if not intf.crc_check:
            for vol in check_requests.keys():
                if check_requests[vol] == []:
                    check_requests[vol] = None
    elif intf.volume and intf.crc_check: #read from command line arguments.
        vol_list = parse_comma_list(intf.volume)
        lc_list  = parse_comma_list(intf.location_cookies)

        if len(vol_list) > 1 :
            message = "When specifying files to check, only one volume may" \
                      " be specified."
            try:
                sys.stderr.write("%s\n" % (message,))
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR, message)
            return 1

        check_requests = {vol_list[0] : lc_list}
    elif intf.volume and intf.media_validate: #read from command line arguments.
        if intf.media_validate not in ("complete", "standard"):
            intf.print_usage()
            return 1
        validate = "media_validate_%s"%(intf.media_validate,)
        vol_list = parse_comma_list(intf.volume)
        check_requests = {vol_list[0] : []}

    elif intf.volume: #read from command line argument.
        vol_list = parse_comma_list(intf.volume)
        lc_list = None

        check_requests = {}
        for vol in vol_list:
            check_requests[vol] = None #No CRC checks, just label check.
    else:
        message = "No volume labels given."
        try:
            sys.stderr.write("%s\n" % (message,))
            sys.stderr.flush()
        except IOError:
            pass
        Trace.log(e_errors.ERROR, message)
        return 1

    ### At this point check_requests is a dictionary with its keys being
    ### volume names and the values being a list of location cookies (possibly
    ### empty) and None.  Empty list implies check all CRCs, None implies
    ### just check the label.

    #Create the list of assert work requests.
    assert_list, listen_socket = create_assert_list(check_requests, media_validate=validate)

    if len(assert_list) == 0:
        message = "No volumes to assert?"
        try:
            sys.stderr.write("%s\n" % (message,))
            sys.stderr.flush()
        except IOError:
            pass
        Trace.log(e_errors.ERROR, message)
        return 1

    #Submit the first work request to the library manager.
    unique_id_list = submit_assert_requests([assert_list[0]])

    if len(unique_id_list) == 0:
        #None of the library sumbissions succeeded.
        message = "All assert request submissions failed."
        try:
            sys.stderr.write("%s\n" % (message,))
            sys.stderr.flush()
        except IOError:
            pass
        Trace.log(e_errors.ERROR, message)
        return 1

    #Wait for mover to call back with the volume assert status.
    exit_status = handle_assert_requests(unique_id_list, assert_list,
                                         listen_socket, intf)

    return exit_status
    
############################################################################
############################################################################

def do_work(intf):

    try:
        exit_status = main(intf)
    except (SystemExit, KeyboardInterrupt):
        exc, msg = sys.exc_info()[:2]
        Trace.log(e_errors.ERROR,
                  "encp aborted from: %s: %s" % (str(exc),str(msg)))
        
        exit_status = 1
    except:
        #Get the uncaught exception.
        exc, msg, tb = sys.exc_info()
        ticket = {'status' : (e_errors.UNCAUGHT_EXCEPTION,
                              "%s: %s" % (str(exc), str(msg)))}
        try:
            sys.stderr.write("%s\n" % (ticket['status'],))
            sys.stderr.flush()
        except IOError:
            pass
        #Send to the log server the traceback dump.  If unsuccessful,
        # print the traceback to standard error.
        Trace.handle_error(exc, msg, tb)
        del tb #No cyclic references.

        exit_status = 1

    return exit_status

############################################################################
############################################################################

if __name__ == "__main__":

    delete_at_exit.setup_signal_handling()

    intf = VolumeAssertInterface(user_mode=0)

    intf._mode = "admin"

    delete_at_exit.quit(do_work(intf))
