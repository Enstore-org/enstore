#!/usr/bin/env python
#
# $Id$
#

# system imports
import sys
import os
import stat
import time
import errno
import pprint
import pwd
import grp
import socket
import pdb
import string
import traceback
import select
import signal
import random
import pprint
import fcntl, FCNTL
import math

# enstore modules
import setpath 
import Trace
import pnfs
import callback
import log_client
import configuration_client
import udp_client
import EXfer
import interface
import e_errors
import hostaddr
import host_config
import atomic
import multiple_interface
import library_manager_client
import delete_at_exit
import runon
import enroute
import charset
import volume_family
import volume_clerk_client
import file_clerk_client

#############################################################################
# verbose: Roughly, five verbose levels are used.
# 0: Print nothing except fatal errors.
# 1: Print message for complete success.
# 2: Print (short) info about the read/write status.
# 3: Print (short) information on the number of files left to transfer.
# 4: Print (short) info about system config.
# 5: Print (long) info about everthing.
#############################################################################



#This is the global used by print_data_access_layer_format().  It uses it to
# determine whether standard out or error is used.
data_access_layer_requested = 0

#Initial seed for generate_unique_id().
_counter = 0

client = {} #Should not be here.

############################################################################

def signal_handler(sig, frame):
    try:
        sys.stderr.write("Caught signal %s, exiting\n" % (sig,))
        sys.stderr.flush()
    except:
        pass
    quit(1)

def encp_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "x2_6_a  CVS $Revision$ "
    file = globals().get('__file__', "")
    if file: version_string = version_string + file
    return version_string

def quit(exit_code=1):
    delete_at_exit.delete()
    os._exit(exit_code)

def print_error(errcode,errmsg):
    format = str(errcode)+" "+str(errmsg) + '\n'
    format = "ERROR: "+format
    sys.stderr.write(format)
    sys.stderr.flush()

def generate_unique_id():
    global _counter
    thishost = hostaddr.gethostinfo()[0]
    ret = "%s-%d-%d-%d" % (thishost, int(time.time()),_counter, os.getpid())
    _counter = _counter + 1
    return ret

def combine_dict(*dictionaries):
    new = {}
    for i in range(0, len(dictionaries)):
        if type(dictionaries[i]) != type({}):
            raise TypeError, "Dictionary required, not %s." % \
                  type(dictionaries[i])
        for key in dictionaries[i].keys():
            if not new.has_key(key):
                new[key] = dictionaries[i][key]

    return new
            
############################################################################

def print_data_access_layer_format(inputfile, outputfile, filesize, ticket):
    # check if all fields in ticket present
    fc_ticket = ticket.get('fc', {})
    external_label = fc_ticket.get('external_label', '')
    location_cookie = fc_ticket.get('location_cookie','')
    mover_ticket = ticket.get('mover', {})
    device = mover_ticket.get('device', '')
    device_sn = mover_ticket.get('serial_num','')
    time_ticket = ticket.get('times', {})
    transfer_time = time_ticket.get('transfer_time', 0)
    seek_time = time_ticket.get('seek_time', 0)
    mount_time = time_ticket.get('mount_time', 0)
    in_queue = time_ticket.get('in_queue', 0)
    now = time.time()
    #t0 = time_ticket.get('encp_start_time', now) ###XXX This makes no sense...
    total = now - time_ticket.get('encp_start_time', now)
    sts =  ticket.get('status', ('Unknown', None))
    status = sts[0]
    msg = sts[1:]
    if len(msg)==1:
        msg=msg[0]
        
    if not data_access_layer_requested and status != e_errors.OK:
        out=sys.stderr
    else:
        out=sys.stdout

    ###String used to print data access layer.
    data_access_layer_format = """
    INFILE=%s
    OUTFILE=%s
    FILESIZE=%s
    LABEL=%s
    LOCATION=%s
    DRIVE=%s
    DRIVE_SN=%s
    TRANSFER_TIME=%.02f
    SEEK_TIME=%.02f
    MOUNT_TIME=%.02f
    QWAIT_TIME=%.02f
    TIME2NOW=%.02f
    STATUS=%s\n"""  #TIME2NOW is TOTAL_TIME, QWAIT_TIME is QUEUE_WAIT_TIME.

    
    out.write(data_access_layer_format % (inputfile, outputfile, filesize,
                                          external_label,location_cookie,
                                          device, device_sn,
                                          transfer_time, seek_time,
                                          mount_time, in_queue,
                                          total, status))
    
    out.write('\n')
    out.flush()
    if msg:
        msg=str(msg)
        sys.stderr.write(msg+'\n')
        sys.stderr.flush()
    
    try:
        format = "INFILE=%s OUTFILE=%s FILESIZE=%d LABEL=%s LOCATION=%s " +\
                 "DRIVE=%s DRIVE_SN=%s TRANSFER_TIME=%.02f "+ \
                 "SEEK_TIME=%.02f MOUNT_TIME=%.02f QWAIT_TIME=%.02f " + \
                 "TIME2NOW=%.02f STATUS=%s"
        msg_type=e_errors.ERROR
        if status == e_errors.OK:
            msg_type = e_errors.INFO
        errmsg=format%(inputfile, outputfile, filesize, 
                       external_label, location_cookie,
                       device,device_sn,
                       transfer_time, seek_time, mount_time,
                       in_queue, total,
                       status)
        if msg:
            errmsg=errmsg+" "+msg
        Trace.log(msg_type, errmsg)
    except:
        exc,msg,tb=sys.exc_info()
        sys.stderr.write("cannot log error message %s\n"%(errmsg,))
        sys.stderr.write("internal error %s %s"%(exc,msg))

#######################################################################

# get the configuration client and udp client and logger client
# return some information about who we are so it can be used in the ticket

def clients(config_host,config_port):
    # get a configuration server
    csc = configuration_client.ConfigurationClient((config_host,config_port))

    # send out an alive request - if config not working, give up
    rcv_timeout = 20
    alive_retries = 10
    try:
        stati = csc.alive(configuration_client.MY_SERVER,
                          rcv_timeout, alive_retries)
    except:
        stati={}
        stati["status"] = (e_errors.CONFIGDEAD,"Config at %s port=%s"%
                           (config_host, config_port))
    if stati['status'][0] != e_errors.OK:
        print_data_access_layer_format("","",0, stati)
        quit()
    
    # get a udp client
    u = udp_client.UDPClient()

    # get a logger client
    global logc  #make it global so other functions in this module can use it.
    logc = log_client.LoggerClient(csc, 'ENCP', 'log_server')

    # convenient, but maybe not correct place, to hack in log message
    # that shows how encp was called
    Trace.log(e_errors.INFO,
                'encp version %s, command line: "%s"' %
                (encp_client_version(), string.join(sys.argv)))
        
    uinfo = {}
    uinfo['uid'] = os.getuid()
    uinfo['gid'] = os.getgid()
    try:
        uinfo['gname'] = grp.getgrgid(uinfo['gid'])[0]
    except:
        uinfo['gname'] = 'unknown'
    try:
        uinfo['uname'] = pwd.getpwuid(uinfo['uid'])[0]
    except:
        uinfo['uname'] = 'unknown'
    uinfo['machine'] = os.uname()
    uinfo['fullname'] = "" # will be filled in later for each transfer

    global client #should not do this
    client = {}
    client['csc']=csc
    client['u']=u
    client['uinfo']=uinfo
    
    return client

##############################################################################

# generate the full path name to the file

def fullpath(filename):
    if not filename:
        return None, None, None, None

    machine = hostaddr.gethostinfo()[0]

    #Expand the path to the complete absolute path.
    filename = os.path.expandvars(filename)
    filename = os.path.expanduser(filename)
    filename = os.path.abspath(filename)
    filename = os.path.normpath(filename)

    dirname, basename = os.path.split(filename)

    return machine, filename, dirname, basename

##############################################################################

# check the input file list for consistency

def inputfile_check(input_files, bytecount=None):
    # create internal list of input unix files even if just 1 file passed in
    if type(input_files)==type([]):
        inputlist=input_files
    else:
        inputlist = [input_files]

    #if bytecount != None and ninput != 1:
    #    print_data_access_layer_format(inputlist[0],'',0,{'status':(
    #        'EPROTO',"Cannot specify --bytes with multiple files")})
    #    quit()

    # we need to know how big each input file is
    file_size = []

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(0, len(inputlist)):

        # get fully qualified name
        machine, fullname, dirname, basename = fullpath(inputlist[i])
        inputlist[i] = fullname

        # input files must exist
        if not os.access(inputlist[i], os.R_OK):
            print_data_access_layer_format(inputlist[i], '', 0, {'status':(
                'USERERROR','Cannot read file %s'%(inputlist[i],))})
            quit()

        #Since, the file exists, we can get its stats.
        statinfo = os.stat(inputlist[i])

        # input files can't be directories
        if not stat.S_ISREG(statinfo[stat.ST_MODE]):
            print_data_access_layer_format(inputlist[i], '', 0, {'status':(
                'USERERROR', 'Not a regular file %s'%(inputlist[i],))})
            quit()

        # get the file size
        file_size.append(statinfo[stat.ST_SIZE])

        #if bytecount != None:
        #    file_size.append(bytecount)
        #else:
        #    file_size.append(statinfo[stat.ST_SIZE])

        # we cannot allow 2 input files to be the same
        # this will cause the 2nd to just overwrite the 1st
        try:
            match_index = inputlist[:i].index(inputlist[i])
            
            print_data_access_layer_format(inputlist[i], '', 0, {'status':(
                'USERERROR', 'Duplicated entry %s'%(inputlist[match_index],))})
            quit()
        except ValueError:
            pass  #There is no error.

    return (inputlist, file_size)


# check the output file list for consistency
# generate names based on input list if required
#"inputlist" is the list of input files.  "output" is a list with one element.

def outputfile_check(inputlist, output):
    # can only handle 1 input file copied to 1 output file
    # or multiple input files copied to 1 output directory or /dev/null
    if len(output)>1:
        print_data_access_layer_format('',output[0],0,{'status':(
            'USERERROR','Cannot have multiple output files')})
        quit()

    nfiles = len(inputlist)
    outputlist = []
    
    # Make sure we can open the files. If we can't, we bomb out to user
    # loop over all input files and generate full output file names
    for i in range(nfiles):
        outputlist.append(output[0])

        if outputlist[i] == '/dev/null':
            continue
        
        try:
            imachine, ifullname, idir, ibasename = fullpath(inputlist[i])
            omachine, ofullname, odir, obasename = fullpath(outputlist[i])

            #In this if...elif...else determine if the user entered in
            # valid file or directory names appropriately.
            
            if os.access(outputlist[i], os.W_OK):
                #File or directory exists with write permissions.
                if os.path.isdir(outputlist[i]):
                    outputlist[i] = os.path.join(outputlist[i], ibasename)
                else:
                    #Non-directory that exists with permission.
                    if len(inputlist) > 1:
                        #With mulitiple input files, the output is wrong.
                        raise e_errors.USERERROR, \
                              'Not a directory %s' % (output[0],)
                    else:
                        #It is a file that already exists.
                        raise e_errors.EEXIST, \
                              "File %s already exists" % (outputlist[i],)
            elif os.access(odir, os.W_OK) and len(inputlist) == 1:
                #Output is not a directory.  If one level up (odir) is a
                # valid directory and the number of input files is 1, then
                # the full name is the valid name of a one file transfer.
                outputlist[i] = ofullname
            else:
                #only error conditions left

                if os.access(outputlist[i], os.F_OK):
                    #Path exists, but without write permissions.
                    raise e_errors.USERERROR, "No write access to %s"%(odir,)
                elif not os.access(outputlist[i], os.F_OK):
                    raise e_errors.USERERROR, \
                          "Invalid file or directory %s" % (outputlist[i],)
                else:
                    raise e_errors.UNKNOWN, e_errors.UNKNOWN

            # we cannot allow 2 output files to be the same
            # this will cause the 2nd to just overwrite the 1st
            # In principle, this is already taken care of in the
            # inputfile_check, but do it again just to make sure in case
            # someone changes protocol
            try:
                match_index = inputlist[:i].index(inputlist[i])
                raise e_errors.USERERROR,  \
                      'Duplicated entry %s'%(inputlist[match_index],)

            except ValueError:
                pass  #There is no error.

        except (e_errors.USERERROR, e_errors.EEXIST, e_errors.UNKNOWN):
            exc, msg, tb = sys.exc_info()
            print_data_access_layer_format(ifullname, ofullname, 0,
                                           {'status':(exc, msg)})
            quit()

    if outputlist[0] == "/dev/null":
        return outputlist
    else:
        #now try to atomically create each file
        for f in outputlist:
            try:
                fd = atomic.open(f, mode=0666)
                if fd<0:
                    #The return code is the negitive return value.
                    error = int(math.fabs(fd))
                    raise OSError, (error, os.strerror(error))

                os.close(fd)

            except OSError:
                exc, msg, tb = sys.exc_info()
                print_data_access_layer_format('', f, 0,
                                               {'status':(exc, msg)})
                quit()

    return outputlist

#######################################################################

def get_server_info(client, library = None):
    csc=client['csc']
    
    fc_ticket = csc.get("file_clerk")
    if fc_ticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, fc_ticket)
        quit()

    vc_ticket = csc.get("volume_clerk")
    if vc_ticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, vc_ticket)
        quit()

    if library:
        lm_ticket = csc.get(library + '.library_manager')
        if lm_ticket['status'][0] != e_errors.OK:
            print_data_access_layer_format('', '', 0, lm_ticket)
            quit()
    else:
        lm_ticket = {}

    return fc_ticket, vc_ticket, lm_ticket

#######################################################################
# return pnfs information,

def pnfs_information(filelist,write=1):
    bfid = []
    pinfo = []
    library = []
    file_family = []
    width = []
    ff_wrapper = []
    storage_group = []
    if write: details = 1
    else: details = 0

    for file in filelist:
        p = pnfs.Pnfs(file, get_details = details)
        if not write:
            ## validate pnfs info
            if p.bit_file_id == pnfs.UNKNOWN:
                return (e_errors.USERERROR, "no bit file id"), None
            bfid.append(p.bit_file_id)
            
        if write:
            ## validate additional pnfs info
            if p.library == pnfs.UNKNOWN:
                return (e_errors.USERERROR, "no library"), None
            library.append(p.library)          # get the library
            if p.file_family == pnfs.UNKNOWN:
                return (e_errors.USERERROR, "no file family"), None
            file_family.append(p.file_family)  # get the file family
            try:
                ff_wrapper.append(p.file_family_wrapper)  # get the file family wrapper
            except:
                ff_wrapper.append("cpio_odc")  # default
            if p.file_family_width == pnfs.ERROR:
                return (e_errors.USERERROR, "no file family width"), None
            elif p.file_family_width <= 0:
                return (e_errors.USERERROR, "invalid file family width %s"%(p.file_family_width)), None
            width.append(p.file_family_width)  # get the width
            try:
                storage_group.append(p.storage_group) # get the storage group
            except:
                storage_group.append('none')   # default
                
        # get some debugging info for the ticket
        pinf = {}
        for k in [ 'pnfsFilename','gid', 'gname','uid', 'uname',
                   'major','minor','rmajor','rminor',
                   'mode','pstat' ] :
            try:
                pinf[k]=getattr(p,k)
            except AttributeError:
                pinf[k]="None"
        pinf['inode'] = 0                  # cpio wrapper needs this also
        pinfo.append(pinf)

    Trace.trace(16,"pnfs_information bfid=%s library=%s file_family=%s \
    wrapper_type=%s width=%s storage_group=%s pinfo=%s p=%s" %
                (bfid, library, file_family, ff_wrapper, width, storage_group, pinfo, p))
    return (e_errors.OK, None), (bfid,library,file_family,ff_wrapper,width,storage_group,pinfo,p)

############################################################################

#Args:
# Takes a list of request tickets.
#Returns:
#None
#Verifies that various information in the tickets are correct, valid, spelled
# correctly, etc.
def verify_write_request_consistancy(request_list):

    for request in request_list:

        #Consistancy check for valid pnfs tag values.
        for key in request['pnfs'].keys():
            #check for values that contain letters, digits and _.
            if not charset.is_in_charset(str(request['pnfs'][key])):
                msg="Pnfs tag, %s, contains invalid characters." % (key,)
                request['status'] = (e_errors.USERERROR, msg)

                print_data_access_layer_format(request['infile'],
                                               request['outfile'],
                                               request['file_size'],
                                               request)
                quit() #Harsh, but necessary.

        #Verify that the file family width is in fact a non-
        # negitive integer.
        try:
            expression = int(request['pnfs']['file_family_width']) < 0
            if expression:
                raise ValueError,(e_errors.USERERROR,
                                  request['pnfs']['file_family_width'])
        except ValueError:
            msg="Pnfs tag, %s, requires a non-negitive integer value."\
                 % ("file_family_width",)
            request['status'] = (e_errors.USERERROR, msg)

            print_data_access_layer_format(request['infile'],
                                           request['outfile'],
                                           request['file_size'],
                                           request)
            quit() #Harsh, but necessary.

        #Consistancy check with respect to wrapper and driver.  If
        # they don't match everything gets confused and breaks.
        if request['vc']['wrapper'] != \
           request['pnfs']['file_family_wrapper']:
            msg = "Volume clerk and pnfs returned conflicting wrappers." \
                  " VC_W=%s  PNFS_W=%s"%\
                  (request['vc']['wrapper'],
                   request['pnfs']['file_family_wrapper'])
            request['status'] = (e_errors.USERERROR, msg)

            print_data_access_layer_format(request['infile'],
                                           request['outfile'],
                                           request['file_size'], request)
            quit() #Harsh, but necessary.



#Args:
# Takes in a dictionary of lists of transfer requests sorted by volume.
#Rerturns:
# None
#Verifies that various information in the tickets are correct, valid, spelled
# correctly, etc.
def verify_read_request_consistancy(requests_per_vol):
    vols = requests_per_vol.keys()
    vols.sort()
    for vol in vols:
        request_list = requests_per_vol[vol]

        for request in request_list:

            #Verify that file clerk and volume clerk returned the same
            # external label.
            if request['vc']['external_label'] != \
               request['fc']['external_label']:
                msg = "Volume and file clerks returned conflicting volumes." \
                      " VC_V=%s  FC_V=%s"%\
                      (request['vc']['external_label'],
                       request['fc']['external_label'])
                request['status'] = (e_errors.USERERROR, msg)

                print_data_access_layer_format(request['infile'],
                                               request['outfile'],
                                               request['file_size'], request)
                quit() #Harsh, but necessary.

        


############################################################################

def check_load_balance(mode, dest):
    #mode should be 0 or 1 for "read" or "write"
    config = host_config.get_config()
    if not config:
        return
    interface_dict = config.get('interface')
    if not interface_dict:
        return
    interfaces = interface_dict.keys()
    if not interfaces:
        return
    Trace.log(e_errors.INFO, "probing network to select interface")
    rate_dict = multiple_interface.rates(interfaces)
    Trace.log(e_errors.INFO, "interface rates: %s" % (rate_dict,))
    choose = []
    for interface in interfaces:
        weight = interface_dict[interface].get('weight', 1.0)
        recv_rate, send_rate = rate_dict[interface]
        recv_rate = recv_rate/weight
        send_rate = send_rate/weight
        if mode==1: #writing
            #If rates are equal on different interfaces, randomize!
            choose.append((send_rate, -weight, random.random(), interface))
        else:
            choose.append((recv_rate, -weight, random.random(), interface))
    choose.sort() 
    rate, junk1, junk2, interface = choose[0]
    Trace.log(e_errors.INFO, "chose interface %s, %s rate=%s" % (
        interface, {0:"recv",1:"send"}.get(mode,"?"), rate))
    interface_details = interface_dict[interface]
    cpu = interface_details.get('cpu')
    if cpu is not None:
        err = runon.runon(cpu)
        if err:
            Trace.log(e_errors.ERROR, "runon(%s): failed, err=%s" % (cpu, err))
        else:
            Trace.log(e_errors.INFO, "runon(%s)" % (cpu,))
            
    gw = interface_details.get('gw')
    if gw is not None:
        err=enroute.routeDel(dest)
        if err:
            Trace.log(e_errors.INFO, "enroute.routeDel(%s) returns %s" % (dest, err))
        else:
            Trace.log(e_errors.INFO, "enroute.routeDel(%s)" % (dest,))
        err=enroute.routeAdd(dest, gw)
        if err:
            Trace.log(e_errors.INFO, "enroute.routeAdd(%s,%s) returns %s" % (dest, gw, err))
        else:
            Trace.log(e_errors.INFO, "enroute.routeAdd(%s,%s)" % (dest, gw))

    return interface_details

############################################################################

def open_control_socket(listen_socket, mover_timeout, verbose):
    
    read_fds,write_fds,exc_fds=select.select([listen_socket], [],
                                             [listen_socket],
                                             mover_timeout)
    if not read_fds:
        #timed out!
        msg = "open_control_socket: timeout on mover callback"
        if verbose > 3:
            print msg
        Trace.log(e_errors.NET_ERROR, msg)
        raise e_errors.NET_ERROR, (e_errors.NET_ERROR, msg)

    control_socket, address = listen_socket.accept()

    if not hostaddr.allow(address):
        control_socket.close()
        msg = "open_control_socket: host %s not allowed" % address[0]
        if verbose > 3:
            print msg
        Trace.log(e_errors.NET_ERROR, msg)
        raise e_errors.NET_ERROR, (e_errors.NET_ERROR, msg)

    try:
        ticket = callback.read_tcp_obj(control_socket)
    except "TCP connection closed":
        msg = "open_control_socket: TCP connection closed"
        if verbose > 3:
            print msg
        Trace.log(e_errors.NET_ERROR, msg)
        raise e_errors.NET_ERROR, (e_errors.NET_ERROR, msg)

    return control_socket, address, ticket
    
##############################################################################

def open_data_socket(mover_addr):
    data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    flags = fcntl.fcntl(data_path_socket.fileno(), FCNTL.F_GETFL)
    fcntl.fcntl(data_path_socket.fileno(), FCNTL.F_SETFL,
                flags | FCNTL.O_NONBLOCK)

    #set up any special network load-balancing voodoo
    interface=check_load_balance(mode=0, dest=mover_addr[0])
    #load balencing...
    if interface:
        ip = interface.get('ip')
        if ip:
            try:
                data_path_socket.bind((ip, 0))
                Trace.log(e_errors.INFO, "bind %s" % (ip,))
            except socket.error, msg:
                Trace.log(e_errors.ERROR, "bind: %s %s" % (ip, msg))

    try:
        data_path_socket.connect(mover_addr)
        error = 0
    except socket.error, msg:
        #We have seen that on IRIX, when the connection succeds, we
        # get an ISCONN error.
        if hasattr(errno, 'ISCONN') and msg[0] == errno.ISCONN:
            pass
        #The TCP handshake is in progress.
        elif msg[0] == errno.EINPROGRESS:
            pass
        #A real or fatal error has occured.  Handle accordingly.
        else:
            raise socket.error, msg

    #Check if the socket is open for reading and/or writing.
    r, w, ex = select.select([data_path_socket], [data_path_socket],[],10)

    if r or w:
        #Get the socket error condition...
        rtn = data_path_socket.getsockopt(socket.SOL_SOCKET,
                                          socket.SO_ERROR)
        error = 0
    #If the select didn't return sockets ready for read or write, then the
    # connection timed out.
    else:
        raise socket.error, (errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))

    #If the return value is zero then success, otherwise it failed.
    if rtn != 0:
        raise socket.error, (rtn, os.strerror(rtn))

    #Restore flag values to blocking mode.
    fcntl.fcntl(data_path_socket.fileno(), FCNTL.F_SETFL, flags)

    return data_path_socket

############################################################################

#Initiats the handshake with the mover.
#Args:
# listen_socket - The listen socket returned from a callback.get_callback().
# work_tickets - List of dictionaries, where each dictionary represents
#                a transfer.
# mover_timeout - number of seconds to wait for mover to make connection.
# max_retry - the allowable number of times to retry a specific request
#             before giving up.
# verbose - the verbose level of output.
#Returns:
# (control_socket, 

def mover_handshake(listen_socket, work_tickets, mover_timeout, max_retry,
                    verbose):
    ##19990723:  resubmit request after 15minute timeout.  Since the
    # unique_id is unchanged the library manager should not get
    # confused by duplicate requests.
    timedout=0
    while 1:  ###not (timedout or reply_read):
        
        #Attempt to get the control socket connected with the mover.
        try:
            control_socket, mover_address, ticket = open_control_socket(
                listen_socket, mover_timeout, verbose)
        except (socket.error, e_errors.NET_ERROR), detail:
            exc, msg, tb = sys.exc_info()
            ticket = {'status':(exc, msg)}

            #Since an error occured, just return it.
            return None, None, ticket

        if verbose > 4:
            print "MOVER HANDSHAKE"
            pprint.pprint(ticket)

        #verify that the id is one that we are excpeting and not one that got
        # lost in the ether.
        for i in range(0, len(work_tickets)):
            if work_tickets[i]['unique_id'] == ticket['unique_id']:
                break #Success, control socket opened!
            
        else: #Didn't find matching id.
            try:
                control_socket.close()
            except socket.error:
                pass

            list_of_ids = []
            for j in range(0, len(work_tickets)):
                list_of_ids.append(work_tickets[j]['unique_id'])

            Trace.log(e_errors.INFO,
                      "mover handshake: mover impostor called back\n"
                      "   mover address: %s   got id: %s   expected: %s\n"
                      "   ticket=%s" %
                      (mover_address,ticket['unique_id'], list_of_ids, ticket))
            
            continue

        # ok, we've been called back with a matched id - how's the status?
        if ticket['status'] != (e_errors.OK, None):
            return None, None, ticket

        mover_addr = ticket['mover']['callback_addr']
        
        #Attempt to get the data socket connected with the mover.
        try:
            data_path_socket = open_data_socket(mover_addr)

            if not data_path_socket:
                raise socket.error,(errno.ENOTCONN,os.strerror(errno.ENOTCONN))

        except (socket.error, e_errors.NET_ERROR), detail:
            exc, msg, tb = sys.exc_info()
            ticket['status'] = (exc, msg)

            #Since an error occured, just return it.
            return None, None, ticket

        #If we got here then the status is OK.
        ticket['status'] = (e_errors.OK, None)
        #Include new info from mover.
        work_tickets[i] = ticket
        
        return control_socket, data_path_socket, ticket
    
############################################################################

def submit_one_request(ticket, verbose):
    ##start of resubmit block
    Trace.trace(7,"write_to_hsm q'ing: %s"%(ticket,))

    if ticket['retry']:
        if verbose > 1: print "RETRY_CNT=", ticket['retry']

    #Send work ticket to LM
    #Get the library manager info information.
    lmc = library_manager_client.LibraryManagerClient(
        client['csc'], ticket['vc']['library'] + ".library_manager")
    responce_ticket = lmc.read_from_hsm(ticket)

    if responce_ticket['status'][0] != e_errors.OK :
        Trace.log(e_errors.NET_ERROR,
                  "submit_write_request: Write submit failed for %s"
                  " - retrying" % ticket['infile'])
    
    return responce_ticket
    
############################################################################

def recieve_final_dialog(control_socket, work_ticket, max_retry):
    # File has been sent - wait for final dialog with mover. 
    # We know the file has hit some sort of media.... 
    # when this occurs. Create a file in pnfs namespace with
    #information about transfer.
    
    try:
        done_ticket = callback.read_tcp_obj(control_socket)
    except:
        exc, msg, tb = sys.exc_info()
        done_ticket = {'status':(exc, msg)}
        
        #try:
        #    control_socket.close()
        #except socket.error:
        #    pass

    return done_ticket

############################################################################

def check_crc(done_ticket, chk_crc, my_crc):
    
    
    # Check the CRC
    if chk_crc:
        mover_crc = done_ticket['fc'].get('complete_crc', None)
        if mover_crc is None:
            msg =   "warning: mover did not return CRC; skipping CRC check"
            Trace.alarm(e_errors.WARNING, msg, {
                'infile':done_ticket['infile'],
                'outfile':done_ticket['outfile']})
            sys.stderr.write(msg+'\n')
            done_ticket['status'] = (e_errors.WARNING, msg)
            
        elif mover_crc != my_crc :
            msg = "CRC mismatch: %d != %d" % (mover_crc, my_crc)
            done_ticket['status'] = (e_errors.CRC_ERROR, msg)

            print_data_access_layer_format(done_ticket['infile'],
                                           done_ticket['outfile'],
                                           done_ticket['file_size'],
                                           done_ticket)

            Trace.alarm(e_errors.WARNING, e_errors.CRC_ERROR, {
                'infile':done_ticket['infile'],
                'outfile':done_ticket['outfile']})
            
############################################################################

def set_outfile_permissions(ticket):
    #Attempt to get the input files permissions and set the output file to
    # match them.
    if ticket['outfile'] != "/dev/null":
        try:
            perms = os.stat(ticket['infile'])[stat.ST_MODE]
            os.chmod(ticket['outfile'], perms)
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            Trace.log(e_errors.INFO, "chmod %s failed: %s" % \
                      (ticket['outfile'], msg))
            ticket['status'] = (e_errors.USERERROR,
                                "Unable to set permissions.")

############################################################################

def verify_file_size(ticket):
    if ticket['outfile'] == '/dev/null':
        filename = ticket['infile']
    else:
        filename = ticket['outfile']

    try:
        #Get the stat info to 
        statinfo = os.stat(filename)

        file_size = statinfo[stat.ST_SIZE]

        if file_size != ticket['file_size']:
            msg = "Expected file size (%s) not equal to actuall file size " \
                  "(%s) for file %s." % \
                  (ticket['file_size'], file_size, filename)
            Trace.log(e_errors.INFO, msg)

            ticket['file_size'] = file_size
            ticket['status'] = (e_errors.EPROTO,
                                "Input/Ouput file size mismatch.")
    except OSError, msg:
        Trace.log(e_errors.INFO, "Retrieving %s info. failed: %s" % \
                  (filename, msg))
        ticket['status'] = (e_errors.UNKNOWN, "Unable to verify file size.")

    return file_size #return used in read.

############################################################################

def handle_retries(request_list, request_dictionary, error_dictionary,
                   max_retries):
    #request_dictionary must have 'retry' as an element.
    #error_dictionary must have 'status':(e_errors.XXX, "explanation").

    #This is here to help track down a hard to track error.  Leave this hear
    # until the error is fixed.
    try:
        infile = request_dictionary['infile']
    except AttributeError, detail:
        print "request_dictionary:", type(request_dictionary), detail
        pprint.pprint(request_dictionary)

    infile = request_dictionary.get('infile', '')
    outfile = request_dictionary.get('outfile', '')
    file_size = request_dictionary.get('file_size', 0)
    retry = request_dictionary.get('retry', 0)
    
    status = error_dictionary.get('status', (e_errors.OK, None))

    #If there is no error, then don't do anything
    if status == (e_errors.OK, None):
        result_dict = {'status':(e_errors.OK, None), 'retry':retry,
                       'queue_size':len(request_list)}
        return result_dict

    #If the transfer has failed to many times, remove it from the queue.
    # Since TOO_MANY_RETRIES is non-retriable, set this here.
    if retry >= max_retries:
        status = (e_errors.TOO_MANY_RETRIES, status)
        
    #If the error is not retriable, remove it from the request queue.
    if not e_errors.is_retriable(status[0]):
        # print error to stdout in data_access_layer format
        print_data_access_layer_format(infile, outfile, file_size,
                                       error_dictionary)
        try:
            #Try to delete the request.  In the event that the connection
            # didn't let us determine which request failed, don't worry.
            del request_list[request_list.index(request_dictionary)]
        except KeyError:
            pass
        
        result_dict = {'status':status, 'retry':retry,
                       'queue_size':len(request_list)} #one less than before
        return result_dict

    #Keep retrying this file.
    try:
        request_dictionary['retry'] = retry + 1
    except KeyError:
        pass

    #Log the intermidiate error as a warning instead as a full error.
    Trace.log(e_errors.WARNING, status)

    #Change the unique id so the library manager won't remove the retry
    # request when it removes to old one.
    request_dictionary['unique_id'] = generate_unique_id()

    try:
        #Since a retriable error occured, resubmit the ticket.
        submit_one_request(request_dictionary, verbose=3)
    except KeyError:
        #If we get here, then the error occured while waiting for any (valid)
        # mover to call back.  Since, there was no information then the
        # submitting operation failed and we should go back to the top and
        # wait for the other transfers to commence.
        pass
    
    result_dict = {'status':(e_errors.RETRY, None),
                   'retry':request_dictionary['retry'],
                   'queue_size':len(request_list)}
    return result_dict

############################################################################

def calculate_rate(done_ticket, tinfo, verbose):
    # calculate some kind of rate - time from beginning to wait for
    # mover to respond until now. This doesn't include the overheads
    # before this, so it isn't a correct rate. I'm assuming that the
    # overheads I've neglected are small so the quoted rate is close
    # to the right one.  In any event, I calculate an overall rate at
    # the end of all transfers
    
    #calculate MB relatated stats
    bytes_per_MB = 1024 * 1024
    MB_transfered = float(done_ticket['file_size']) / float(bytes_per_MB)

    #For readablilty...
    id = done_ticket['unique_id']

    #Make these variables easier to use.
    transfer_time = tinfo['%s_elapsed_time' % (id,)]
    complete_time = tinfo['%s_elapsed_finished' % (id,)]

    if done_ticket['work'] == "read_from_hsm":
        preposition = "from"
    else: #write "to"
        preposition = "to"
    
    if done_ticket['status'][0] == e_errors.OK:

        if complete_time != 0:
            tinfo['rate_%s'%(id,)] = MB_transfered / complete_time
        else:
            tinfo['rate_%s'%(id,)] = 0.0

        if transfer_time != 0:
            tinfo['transrate_%s'%(id,)] = MB_transfered / transfer_time
        else:
            tinfo['transrate_%s'%(id,)] = 0.0

        print_format = "Transfer %s -> %s:\n" \
                 "\t%d bytes copied %s %s at %.3g MB/S (%.3g MB/S)\n" \
                 "\tdrive_id=%s drive_sn=%s drive_vendor=%s\n" \
                 "\tmover=%s media_changer=%s   elapsed= %.02f"
        
        log_format = "  %s %s -> %s: %d bytes copied %s %s at %.3g MB/S " \
                     "(%.3g MB/S) mover=%s drive_id=%s drive_sn=%s "\
                     "drive_venor=%s elapsed= %s.02f {'media_changer' : '%s',"\
                     "'mover_interface' : '%s', 'driver' : '%s'}"

        print_values = (done_ticket['infile'],
                        done_ticket['outfile'],
                        done_ticket['file_size'],
                        preposition,
                        done_ticket["fc"]["external_label"],
                        tinfo["rate_%s"%(id,)],
                        tinfo["transrate_%s"%(id,)],
                        done_ticket["mover"]["product_id"],
                        done_ticket["mover"]["serial_num"],
                        done_ticket["mover"]["vendor_id"],
                        done_ticket["mover"]["name"],
                        done_ticket["mover"]["media_changer"],
                        time.time() - tinfo["encp_start_time"])

        log_values = (done_ticket['work'],
                      done_ticket['infile'],
                      done_ticket['outfile'],
                      done_ticket['file_size'],
                      preposition,
                      done_ticket["fc"]["external_label"],
                      tinfo["rate_%s"%(id,)],
                      tinfo["transrate_%s"%(id,)],
                      done_ticket["mover"]["name"],
                      done_ticket["mover"]["product_id"],
                      done_ticket["mover"]["serial_num"],
                      done_ticket["mover"]["vendor_id"],
                      time.time() - tinfo["encp_start_time"],
                      done_ticket["mover"]["media_changer"],
                      socket.gethostbyaddr(done_ticket["mover"]["hostip"])[0],
                      done_ticket["mover"]["driver"])
        
        if verbose:
            print print_format % print_values

        Trace.log(e_errors.INFO, log_format % log_values, Trace.MSG_ENCP_XFER )
        
############################################################################

def calculate_final_statistics(bytes, number_of_files, exit_status, tinfo):
    # Calculate an overall rate: all bytes, all time

    #Calculate total running time from the begining.
    now = time.time()
    tinfo['total'] = now - tinfo['encp_start_time']

    #calculate MB relatated stats
    bytes_per_MB = 1024 * 1024
    MB_transfered = float(bytes) / float(bytes_per_MB)

    done_ticket = {}
    done_ticket["tinfo"] = tinfo

    if tinfo['total']: #protect against division by zero.
        done_ticket['MB_per_S'] = MB_transfered / (tinfo['total'])
    else:
        done_ticket['MB_per_S'] = 0.0
    
    msg = "%s transferring %s bytes in %s files in %s sec.\n" \
          "\tOverall rate = %s MB/sec."
    
    if exit_status:
        msg = msg % ("Error after", bytes, number_of_files,
                     done_ticket['tinfo']['total'], done_ticket["MB_per_S"])
    else:
        msg = msg % ("Completed", bytes, number_of_files,
                     done_ticket['tinfo']['total'], done_ticket["MB_per_S"])

    #set the final status values
    done_ticket['exit_status'] = exit_status
    done_ticket['status'] = (e_errors.OK, msg)
    return done_ticket

############################################################################
############################################################################

def create_write_request(input_file, output_file,
                       file_size, library, file_family,
                       ff_wrapper, width, storage_group,
                       pinfo, pnfs, client, file_clerk_address,
                       volume_clerk_address, callback_addr, e, tinfo):

    # make the part of the ticket that encp knows about (there's 
    # more later)
    encp = {}
    encp["basepri"] = e.priority
    encp["adminpri"] = e.admpri
    encp["delpri"] = e.delpri
    encp["agetime"] = e.age_time
    encp["delayed_dismount"] = e.delayed_dismount

    times = {'t0':tinfo['encp_start_time']}
    
    #If the environmental variable exists, send it to the lm.
    try:
        encp_daq = os.environ['ENCP_DAQ']
    except KeyError:
        encp_daq = None

    # create the wrapper subticket - copy all the user info 
    # into for starters
    wrapper = {}
    wrapper["fullname"] = input_file
    wrapper["type"] = ff_wrapper
    #file permissions from PNFS are junk, replace them
    #with the real mode
    wrapper['mode']=os.stat(input_file)[stat.ST_MODE]
    wrapper["sanity_size"] = 65536
    wrapper["size_bytes"] = file_size
    wrapper["mtime"] = int(time.time())

    # store the pnfs information info into the wrapper
    for key in pinfo.keys():
        wrapper[key] = pinfo[key]

    # the user key takes precedence over the pnfs key
    for key in client['uinfo'].keys():
        wrapper[key] = client['uinfo'][key]

    pnfs.get_file_family()
    pnfs.get_file_family_width()
    pnfs.get_file_family_wrapper()
    pnfs.get_library()
    pnfs.get_storage_group()

    volume_clerk = {"library"            : library,
                    "file_family"        : file_family,
                    # technically width does not belong here,
                    # but it associated with the volume
                    "file_family_width"  : width,
                    "wrapper"            : ff_wrapper,
                    "storage_group"      : storage_group,
                    "address"            : volume_clerk_address}
    file_clerk = {"address": file_clerk_address}

    work_ticket = {}
    work_ticket['callback_addr'] = callback_addr
    work_ticket['client_crc'] = e.chk_crc
    work_ticket['encp'] = encp
    work_ticket['encp_daq'] = encp_daq
    work_ticket['fc'] = file_clerk
    work_ticket['file_size'] = file_size
    work_ticket['infile'] = input_file
    work_ticket['outfile'] = output_file
    work_ticket['pnfs'] = {'file_family':pnfs.file_family,
                           'file_family_width':pnfs.file_family_width,
                           'file_family_wrapper':pnfs.file_family_wrapper,
                           'library':pnfs.library,
                           'storage_group':pnfs.storage_group}
    work_ticket['retry'] = 0 #retry,
    work_ticket['times'] = times
    work_ticket['unique_id'] = generate_unique_id()
    work_ticket['vc'] = volume_clerk
    work_ticket['version'] = encp_client_version()
    work_ticket['work'] = "write_to_hsm"
    work_ticket['wrapper'] = wrapper
    
    return work_ticket

############################################################################

def submit_write_request(work_ticket, client, max_retry, verbose):

    if verbose > 1:
        print "Submitting %s write request.   time=%s" % \
              (work_ticket['outfile'], time.time())

    # send the work ticket to the library manager
    while work_ticket['retry'] < max_retry:
        
        ##start of resubmit block
        Trace.trace(7,"write_to_hsm q'ing: %s"%(work_ticket,))

        ticket = submit_one_request(work_ticket, verbose)

        if verbose > 4:
            print "LIBRARY MANAGER"
            pprint.pprint(ticket)

        result_dict = handle_retries([work_ticket], work_ticket, ticket,
                                     max_retry)
        if result_dict['status'] == e_errors.RETRY or \
           result_dict['status'] in e_errors.non_retriable_errors:
            continue
        else:
            ticket['status'] = (e_errors.OK, ticket['status'])
            return ticket
            
    ticket['status'] = (e_errors.TOO_MANY_RETRIES, ticket['status'])
    return ticket

############################################################################

def set_pnfs_settings(ticket, client, verbose):
    
    # create a new pnfs object pointing to current output file
    Trace.trace(10,"write_to_hsm adding to pnfs "+ ticket['outfile'])
    p=pnfs.Pnfs(ticket['outfile'])

    # save the bfid and set the file size
    p.set_bit_file_id(ticket["fc"]["bfid"], ticket['file_size'])

    # create volume map and store cross reference data
    mover_ticket = ticket.get('mover', {})
    drive = "%s:%s" % (mover_ticket.get('device', 'Unknown'),
                       mover_ticket.get('serial_num','Unknown'))
    try:
        p.set_xreference(ticket["fc"]["external_label"],
                         ticket["fc"]["location_cookie"],
                         ticket["fc"]["size"],
                         drive)
        
        # add the pnfs ids and filenames to the file clerk ticket and store it
        fc_ticket = ticket.copy() #Make a copy so "work" isn't overridden.
        fc_ticket["fc"]["pnfsid"] = p.id
        fc_ticket["fc"]["pnfsvid"] = p.volume_fileP.id
        fc_ticket["fc"]["pnfs_name0"] = p.pnfsFilename
        fc_ticket["fc"]["pnfs_mapname"] = p.mapfile
        fc_ticket["fc"]["drive"] = drive

        fcc = file_clerk_client.FileClient(client['csc'], ticket["fc"]["bfid"])
        fc_reply = fcc.set_pnfsid(fc_ticket)
    
        if fc_reply['status'][0] != e_errors.OK:
            print_data_access_layer_format('', '', 0, fc_reply)
            quit()
        
        if verbose > 3:
            print "PNFS SET"
            pprint.pprint(fc_reply)

        ticket['status'] = fc_reply['status']
    except:
        exc,msg,tb=sys.exc_info()
        Trace.log(e_errors.INFO, "Trouble with pnfs.set_xreference %s %s, "
                  "continuing" % (exc, msg))
        
############################################################################

def write_hsm_file(listen_socket, work_ticket, client, tinfo, e):

    #Loop around in case the file transfer needs to be retried.
    while work_ticket.get('retry', 0) < e.max_retry:
        
        encp_crc = 0 #In case there is a problem, make sure this exists.

        if e.verbose > 1:
            print "Waiting for mover to call back.   elapsed=%s" % \
                  (time.time() - tinfo['encp_start_time'],)

        #Open the control and mover sockets.
        control_socket, data_path_socket, ticket = mover_handshake(
            listen_socket, [work_ticket], e.mover_timeout, e.max_retry,
            e.verbose)

        #Handle any possible errors occured so far.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     ticket, e.max_retry)
        if result_dict['status'] == e_errors.RETRY:
            continue
        elif result_dict['status'] in e_errors.non_retriable_errors:
            return ticket

        #This should be redundant error check.
        if not control_socket or not data_path_socket:
            ticket['status'] = (e_errors.NET_ERROR, "No socket")
            return ticket #This file failed.

        if e.verbose > 1:
            t2 = time.time() - tinfo['encp_start_time']
            print "Mover called back.  elapsed=", t2
        if e.verbose > 4:
            print "WORK TICKET:"
            pprint.pprint(ticket)

        work_ticket = combine_dict(ticket, work_ticket)

        try:
            in_file = open(work_ticket['infile'], "r")
        except IOError, detail:
            #Handle any possible errors occured so far
            status_ticket = (e_errors.IOERROR, detail)
            result_dict = handle_retries([work_ticket], work_ticket,
                                         status_ticket, e.max_retry)
            if result_dict['status'] == e_errors.RETRY:
                continue
            elif result_dict['status'] in e_errors.non_retriable_errors:
                return combine_dict(result_dict, work_ticket)

        if e.verbose > 1:
            print "Input file %s opened.   elapsed=%s" % \
                  (work_ticket['infile'], time.time()-tinfo['encp_start_time'])

        lap_time = time.time() #------------------------------------------Start

        #Now that the control and data sockets are established, move the data.
        try:
            if e.chk_crc:
                crc_flag = 1
            else:
                crc_flag = 0

            encp_crc = EXfer.fd_xfer(in_file.fileno(),
                                     data_path_socket.fileno(), 
                                     work_ticket['file_size'],
                                     e.bufsize, crc_flag, 0)
        except EXfer.error, msg:
            Trace.log(e_errors.ERROR, "write_to_hsm EXfer error: %s" % (msg,))

            try:
                done_ticket = callback.read_tcp_obj(control_socket)
            except ("TCP connection closed", socket.error), detail:
                done_ticket = {'status':(msg.args[1], msg.args[0])}
                #done_ticket = {'status':(e_errors.EPROTO,
                #                         "Network problem or mover reset")}

                #Handle any possible errors occured so far
                status_ticket = (e_errors.IOERROR, detail)
            
                result_dict = handle_retries([work_ticket], work_ticket,
                                             status_ticket, e.max_retry)
                if result_dict['status'] == e_errors.RETRY:
                    continue
                elif result_dict['status'] in e_errors.non_retriable_errors:
                    return combine_dict(result_dict, work_ticket)

        tstring = '%s_elapsed_time' % work_ticket['unique_id']
        tinfo[tstring] = time.time() - lap_time #--------------------------End

        if e.verbose > 1:
            print "Verifying %s transfer.  elapsed=%s" % \
                  (work_ticket['outfile'],time.time()-tinfo['encp_start_time'])

        # File has been sent - wait for final dialog with mover.
        
        done_ticket = recieve_final_dialog(control_socket, work_ticket,
                                           e.max_retry)
        try:
            control_socket.close()
            data_path_socket.close()
            in_file.close()
        except (socket.error, OSError):
            print "Error closeing something"
            pass

        #Verify that everything is ok.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e.max_retry)
        if result_dict['status'] == e_errors.RETRY:
            continue
        elif result_dict['status'] in e_errors.non_retriable_errors:
            return combine_dict(result_dict, work_ticket)

        if e.verbose > 1:
            print "File %s transfered.  elapsed=%s" % \
                  (done_ticket['outfile'],time.time()-tinfo['encp_start_time'])
        if e.verbose > 4:
            print "FINAL DIALOG"
            pprint.pprint(done_ticket)
        
        #Combine the work_ticket and done_ticket into for simplicity.
        #The done_ticket returned from the mover via recieve_final_dialog()
        # contains new ['fc'] fields.
        done_ticket = combine_dict(done_ticket, work_ticket)

        delete_at_exit.unregister(done_ticket['outfile']) #localname

        #We know the file has hit some sort of media. When this occurs
        # create a file in pnfs namespace with information about transfer.
        #These four functions write errors/warnings to the log file and put an
        # error status in the ticket.
        check_crc(done_ticket, e.chk_crc, encp_crc) #Check the CRC.
        set_outfile_permissions(done_ticket) #Writes errors to log file.
        set_pnfs_settings(done_ticket, client, e.verbose) #Tell pnfs file stats
        verify_file_size(done_ticket) #make sure file size is same.

        if e.verbose > 1:
            print "File status after verification: %s   elapsed=%s" % \
                  (done_ticket['status'], time.time()-tinfo['encp_start_time'])

        return done_ticket

    #If we get out of the while loop, then return error.
    msg = "Failed to write file %s." % work_ticket['outfile']
    done_ticket = {'status':(e_errors.TOO_MANY_RETRIES, msg)}

############################################################################

def write_to_hsm(e, client, tinfo):

    Trace.trace(6,"write_to_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" %
                (e.input, e.output, e.verbose, e.chk_crc,
                 tinfo['encp_start_time']))

    # initialize
    #max_retry = 3
    bytecount=None #Test moe only
    storage_info=None #DCache only
    unique_id = []
    
    # check the input unix files. if files don't exits, 
    # we bomb out to the user
    if e.dcache: #XXX
        input_files = [input_files]
        
    inputlist, file_size = inputfile_check(e.input, bytecount)
    ninput = len(inputlist)

    if e.verbose > 4:
        print "ninput", ninput
        print "inputlist=", inputlist
        print "file_size=", file_size
    
    if (len(inputlist)>1) and (e.delayed_dismount == 0):
        e.delayed_dismount = 1
    #else:
        #e.delayed_dismount = 0

#    if dcache:
#        output = [pnfs_hack.filename_from_id(output)]
        
    # check (and generate) the output pnfs files(s) names
    # bomb out if they exist already
    outputlist = outputfile_check(e.input, e.output)

    if e.verbose > 4:
        print "outputlist=",outputlist

    #Snag the pnfs_information and verify that everything matches.
    status, info = pnfs_information(outputlist,write=1)
    if status[0] != e_errors.OK:
        print_data_access_layer_format('','',0,{'status':status})
        print_error(status[0], status[1])
        quit()
    junk,library,file_family,ff_wrapper,width,storage_group,pinfo,p=info

    if e.verbose > 4:
        print "library=",library
        print "file_family=",file_family
        print "wrapper type=",ff_wrapper
        print "width=",width
        print "storage_group",storage_group
        print "pinfo=",pinfo
        
    if e.output_file_family != "":
        for i in range(0,len(outputlist)):
            file_family[i] = e.output_file_family
            width[i] = 1

    # note: Since multiple input files all go to 1 directory:
    #       all libraries are the same
    #       all file families are the same
    #       all widths are the same
    # be cautious and check to make sure this is indeed correct
    for i in range(1,len(outputlist)):
        if (library[i]!=library[0] or 
            file_family[i]!=file_family[0] or 
            width[i]!=width[0] or 
            ff_wrapper[i] != ff_wrapper[0]):
            print "library=",library
            print "file_family=",file_family
            print "wrapper type=",ff_wrapper
            print "width=",width
            print "storage_group",storage_group
            msg =  "library, file_family, width not all the same"
            print_data_access_layer_format('', '', 0,
                                           {'status':(e_errors.USERERROR,msg)})
            quit()

    
    # get a port to talk on and listen for connections
    Trace.trace(10,'write_to_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback(verbose=e.verbose)
    callback_addr = (host, port)
    listen_socket.listen(4)
    Trace.trace(10,'write_to_hsm got callback host=%s port=%s listen_socket=%s'
                % (host,port,listen_socket))
    
    if e.verbose > 3:
        print "Waiting for mover(s) to call back on (%s, %s)." % (host, port)

    #Get the information needed to contact the file clerk, volume clerk and
    # the library manager.
    fc_ticket, vc_ticket, lm_ticket = get_server_info(client)
    
    file_clerk_address = (fc_ticket["hostip"], fc_ticket["port"])
    volume_clerk_address = (vc_ticket["hostip"], vc_ticket["port"])

    if e.verbose > 3:
        print "File clerk address:", file_clerk_address
        print "Volume clerk address:", volume_clerk_address
    
    file_fam = None
    # loop on all input files sequentially
    #ninput = len(input_files)
    #files_left = ninput
    bytes = 0L
    exit_status = 0 #Used to determine the final message text.
    
    for i in range(0,ninput):
        lap_start = time.time() #------------------------------------Lap Start

        if e.verbose > 2:
            print "FILES LEFT:", ninput - i

        if file_fam: ###???
            rq_file_family = file_fam
        else:
            rq_file_family = file_family[i]


        #Build the dictionary, work_ticket, that will be sent to the
        # library manager.
        work_ticket = create_write_request(inputlist[i], outputlist[i],
                           file_size[i], library[i], rq_file_family,
                           ff_wrapper[i], width[i], storage_group[i],
                           pinfo[i], p, client, file_clerk_address,
                           volume_clerk_address, callback_addr, e, tinfo)

        if e.verbose > 4:
            print "WORK_TICKET"
            pprint.pprint(work_ticket)

        #This will halt the program if everything isn't consistant.
        verify_write_request_consistancy([work_ticket])
        
        if e.verbose > 1:
            print "Sending ticket to %s.library manager,  elapsed=%s" % \
                  (library[i], time.time() - tinfo['encp_start_time'])

        #Send the request to write the file to the library manager.
        done_ticket = submit_write_request(work_ticket, client,
                                           e.max_retry, e.verbose)

        if e.verbose > 4:
            print "DONE SUBMITTING TICKET"
            pprint.pprint(done_ticket)
        if e.verbose > 1:
            print "File queued: %s library: %s family: %s bytes: %d elapsed=%s"\
                  % (inputlist[i], library[i], rq_file_family, file_size[i],
                     time.time() - tinfo['encp_start_time'])

        #handle_retries() is not required here since submit_write_request()
        # handles its own retrying when an error occurs.
        if done_ticket['status'][0] != e_errors.OK:
            exit_status = 1
            continue

        #Send (write) the file to the mover.
        done_ticket = write_hsm_file(listen_socket, work_ticket, client,
                                     tinfo, e)

        if e.verbose > 4:
            print "DONE WRITTING TICKET"
            pprint.pprint(done_ticket)

        #handle_retries() is not required here since write_hsm_file()
        # handles its own retrying when an error occurs.
        if done_ticket['status'][0] != e_errors.OK:
            exit_status = 1
            print "exit_status", exit_status
            continue

        bytes = bytes + done_ticket['file_size']

        if (i == 0 and rq_file_family == "ephemeral"):
            file_fam = string.split(done_ticket["vc"]["file_family"], ".")[0]
            if e.verbose:
                print "New file family %s has been created for --ephemeral RQ"\
                      % (file_fam,)

        tstring = '%s_elapsed_finished' % done_ticket['unique_id']
        tinfo[tstring] = time.time() - lap_start #-------------------------End

        # calculate some kind of rate - time from beginning 
        # to wait for mover to respond until now. This doesn't 
        # include the overheads before this, so it isn't a 
        # correct rate. I'm assuming that the overheads I've 
        # neglected are small so the quoted rate is close
        # to the right one.  In any event, I calculate an 
        # overall rate at the end of all transfers
        calculate_rate(done_ticket, tinfo, e.verbose)
        
    # we are done transferring - close out the listen socket
    try:
        listen_socket.close()
    except socket.error:
        pass

    #Finishing up with a few of these things.
    done_ticket = calculate_final_statistics(bytes, ninput, exit_status, tinfo)

    if e.verbose:
        #If applicable print new file family.
        if file_family[0] == "ephemeral":
            ff = string.split(done_ticket["vc"]["file_family"], ".")
            print "New File Family Created:", ff[0]
    
    if e.verbose > 4:
        print "DONE TICKET"
        pprint.pprint(done_ticket)
    elif e.verbose:
        print done_ticket['status'][1]

    return done_ticket

#######################################################################
#######################################################################
#######################################################################

def get_clerks_info(bfid, client):

    #Get the file clerk information.
    fcc = file_clerk_client.FileClient(client['csc'], bfid)
    fc_ticket = fcc.bfid_info()
    
    if fc_ticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, fc_ticket)
        quit()
        
    #Get the volume clerk information.
    vcc = volume_clerk_client.VolumeClerkClient(client['csc'])
    vc_ticket = vcc.inquire_vol(fc_ticket['fc']['external_label'])

    if vc_ticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, vc_ticket)
        quit()

    Trace.trace(7,"read_from_hsm on volume=%s"%
                (fc_ticket['fc']['external_label'],))
    
    inhibit = vc_ticket['system_inhibit'][0]

    if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
        if inhibit==e_errors.NOACCESS:
            msg="Volume is marked NOACCESS"
        else:
            msg="Volume is marked NOTALLOWED"
        raise inhibit, msg
        
    inhibit = vc_ticket['user_inhibit'][0]
    if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
        if inhibit==e_errors.NOACCESS:
            msg="Volume is marked NOACCESS"
        else:
            msg="Volume is marked NOTALLOWED"
        raise inhibit, msg
        
    if fc_ticket["fc"]["deleted"] == "yes":
    #if fc_ticket["deleted"] == "yes":
        raise (e_errors.DELETED, "File has been deleted")

    #Verify that the external labels named by the file clerk and volume
    # clerk are the same.
    if vc_ticket['external_label'] != fc_ticket['fc']['external_label']:
    #if vc_ticket['external_label'] != fc_ticket['external_label']:
        msg = "External labels retrieved from file and volume clerks " \
              "are not the same.\n" \
              "From file clerk: %s\n" \
              "From volume clerk: %s\n" % \
              (fc_ticket['fc']['external_label'],
               #(fc_ticket['external_label'],
               vc_ticket['external_label'])
        raise (e_errors.BROKEN, msg)
        #print_data_access_layer_format(inputlist[i], outputlist[i], 0,
        #                               {'status':(e_errors.BROKEN, msg)})
        #quit()

    return fc_ticket, vc_ticket
    
    
#######################################################################

def create_read_requests(inputlist, outputlist, file_size,
                         client, tinfo, e, bfid, pinfo,
                         file_clerk_address, volume_clerk_address):

    nfiles = 0
    requests_per_vol = {}
    #Create the list of file requests that will be sent to the library manager.
    for i in range(0, len(inputlist)):
        
        Trace.trace(7,"read_from_hsm calling file clerk for bfid=%s"%(bfid[i],))

        #Get the information from the file and volume clerks.
        try:
            fc_reply = {}
            vc_reply = {}
            fc_reply, vc_reply = get_clerks_info(bfid[i], client)

            if fc_reply['status'][0] != e_errors.OK:
                raise fc_reply['status']
            if vc_reply['status'][0] != e_errors.OK:
                raise vc_reply['status']

        except (e_errors.NOACCESS, e_errors.NOTALLOWED, e_errors.DELETED):
            exc, msg, tb = sys.exc_info()
            print_data_access_layer_format(inputlist[i], outputlist[i], 0,
                                           {'status':(exc, msg)})
            continue

        if e.verbose > 4:
            print "FILE CLERK:"
            pprint.pprint(fc_reply)
            
        if e.verbose > 4:
            print "VOLUME CLERK:"
            pprint.pprint(vc_reply)
        
        # make the part of the ticket that encp knows about.
        # (there's more later)
        encp_el = {}
        ## quick fix to check HiPri functionality
        #admpri = -1
        #if pri < 0:
        #    pri = -pri
        #    admpri = pri
        encp_el['basepri'] = e.priority
        encp_el['adminpri'] = e.admpri
        encp_el['delpri'] = e.delpri
        encp_el['agetime'] = e.age_time
        encp_el['delayed_dismount'] = e.delayed_dismount

        #check for this environmental variable.
        try:
            encp_daq = os.environ["ENCP_DAQ"]
        except KeyError:
            encp_daq = None

        # create the time subticket
        times = {}
        times['t0'] = tinfo['encp_start_time']

        label = fc_reply['external_label']
        vf = fc_reply['vc']['volume_family']
        vc_reply['address'] = volume_clerk_address
        vc_reply['storage_group']=volume_family.extract_storage_group(vf)
        vc_reply['file_family'] = volume_family.extract_file_family(vf)
        vc_reply['wrapper'] = volume_family.extract_wrapper(vf)
        fc_reply['address'] = file_clerk_address
        try:
            del fc_reply['fc'] #Speed up debugging by removing these.
            del fc_reply['vc']
        except:
            pass
        
        #pnfs.get_file_family()
        #pnfs.get_file_family_width()
        #pnfs.get_file_family_wrapper()
        #pnfs.get_library()
        #pnfs.get_storage_group()
        
        request = {}
        request['bfid'] = bfid[i]
        request['callback_addr'] = client['callback_addr']
        request['client_crc'] = e.chk_crc
        request['encp'] = encp_el
        request['encp_daq'] = encp_daq
        request['fc'] = fc_reply
        request['file_size'] = file_size[i]
        request['infile'] = inputlist[i]
        request['outfile'] = outputlist[i]
        #request['pnfs'] = {'file_family':pnfs.file_family,
        #                   'file_family_width':pnfs.file_family_width,
        #                   'file_family_wrapper':pnfs.file_family_wrapper,
        #                   'library':pnfs.library,
        #                   'storage_group':pnfs.storage_group}
        request['retry'] = 0
        request['times'] = times
        request['unique_id'] = generate_unique_id()
        request['vc'] = vc_reply
        request['version'] = encp_client_version()
        request['volume'] = label
        request['work'] = 'read_from_hsm'
        request['wrapper'] = {}
        request['wrapper']['fullname'] = request['outfile']
        request['wrapper']['sanity_size'] = 65536
        request['wrapper']['size_bytes'] = request['file_size']

        # store the pnfs information info into the wrapper
        for key in pinfo[i].keys(): #request['pinfo'].keys():
            request['wrapper'][key] = pinfo[i][key] #request['pinfo'][key]

        # the user key takes precedence over the pnfs key
        for key in client['uinfo'].keys():
            request['wrapper'][key] = client['uinfo'][key]

        requests_per_vol[label] = requests_per_vol.get(label,[]) + [request]
        nfiles = nfiles+1

    return requests_per_vol

#######################################################################

# submit read_from_hsm requests
def submit_read_requests(requests, client, tinfo, verbose):

    submitted = 0
    requests_to_submit = requests[:]
    max_retry = 2

    # submit requests
    while requests_to_submit:
        for req in requests_to_submit:
            if req['retry']:
                if verbose > 1: print "RETRY_CNT=", req['retry']

            if verbose > 1:
                print "Submitting %s read request.   time=%s" % (req['infile'],
                                                                 time.time())

            Trace.trace(8,"submit_read_requests queueing:%s"%(req,))

            ticket = submit_one_request(req, verbose)

            if verbose > 4:
                print "LIBRARY MANAGER"
                pprint.pprint(ticket)

            result_dict = handle_retries(requests_to_submit, req, ticket,
                                         max_retry)
            if result_dict['status'] == e_errors.RETRY or \
               result_dict['status'] in e_errors.non_retriable_errors:
                continue
            
            del requests_to_submit[requests_to_submit.index(req)]
            submitted = submitted+1

    return submitted

#############################################################################
# read hsm files in the loop after read requests have been submitted

#Args:
# listen_socket - The socket to listen on returned from callback.get_callback()
# submittted - The number of elements of the list requests that were
#  successfully transfered.
# requests - A list of dictionaries.  Each dictionary represents on transfer.
# tinfo - Dictionary of timing info.
# chk_crc - Ether 0 or 1.  1 means check crc, 0 means skip the test.
# max_retry - The allowable number of times to retry any single transfer.
# verbose - The verbose level.  (0 <= verbose <= 100)
#Rerturns:
# (requests, bytes) - requests returned only contains those requests that
#   did not succed.  bytes is the total running sum of bytes transfered
#   for this encp.

def read_hsm_files(listen_socket, submitted, requests, tinfo, e):
#chk_crc, max_retry, verbose):
    for rq in requests: 
        Trace.trace(7,"read_hsm_files: %s"%(rq['infile'],))

    files_left = submitted
    bytes = 0L
    encp_crc = 0
    mover_timeout_retries = 0 #Number of times listen/select/accept has failed.

    #for waiting in range(submitted):
    while files_left:
        if e.verbose > 2:
            print "FILES LEFT:", files_left
        if e.verbose > 1:
            t2 = time.time() - tinfo['encp_start_time']
            print "Waiting for mover to call back.  elapsed=", t2

        # listen for a mover - see if id corresponds to one of the tickets
        #   we submitted for the volume
        control_socket, data_path_socket, request = mover_handshake(
            listen_socket,
            requests,
            e.mover_timeout,
            e.max_retry,
            e.verbose)

        if request.get('retry', 0):
                if e.verbose > 2: print "RETRY COUNT:", request['retry']

        result_dict = handle_retries(requests, request, request, e.max_retry)
        if result_dict['status'] == e_errors.RETRY:
            continue
        elif result_dict['status'] in e_errors.non_retriable_errors:
            files_left = result_dict['queue_size']
            continue

        #This is a redundant check.
        if not control_socket or not data_path_socket:
            mover_timeout_retries = mover_timeout_retries + 1
            if mover_timeout_retries > e.max_retry:
                #The mover has not called back in max_retry * mover_timeout
                # number of seconds. Assume it has died.
                files_left = 0
            continue
        else:
            mover_timeout_retries = 0 #reset this

        if e.verbose > 1:
            t2 = time.time() - tinfo['encp_start_time']
            print "Mover called back.  elapsed=", t2
        if e.verbose > 4:
            print "REQUEST:"
            pprint.pprint(request)

        try:
            #localname = request.get('outfile', "")
            j = requests.index(request) ###This should be changed.
        except (KeyError, ValueError), detail:
            continue

        #Try to open the local output file for write.
        try:
            if request['outfile'] == "/dev/null":
                out_fd = os.open("/dev/null", os.O_RDWR)
                out_fd_closed = 0
            else:
                out_fd = os.open(request['outfile'], os.O_CREAT|os.O_RDWR, 0)
                out_fd_closed = 0
        except OSError, detail:
            #USERERROR is on the list of non-retriable errors.  Because of
            # this the return from handle_retries will remove this request
            # from the list.  Thus avoiding issues with the continue and
            # range(submitted).
            done_ticket = {'status':(e_errors.USERERROR, detail)}
            result_dict = handle_retries(requests, requests[j],
                                        done_ticket, e.max_retry)
            if result_dict['status'] in e_errors.non_retriable_errors:
                files_left = result_dict['queue_size']
            control_socket.close()
            data_path_socket.close()
            continue

        if e.verbose > 1:
            t2 = time.time() - tinfo['encp_start_time']
            print "Output file", request['outfile'], "opened.  elapsed=", t2

        lap_start = time.time() #----------------------------------------Start

        #Read in the data from the mover and write it out to file.  Also,
        # total up the crc value for comparison with what was sent from
        # the mover.
        try:
            if e.chk_crc != 0:
                crc_flag = 1
            else:
                crc_flag = 0

            encp_crc = EXfer.fd_xfer(data_path_socket.fileno(), out_fd,
                                  requests[j]['file_size'], e.bufsize,
                                  crc_flag, 0)

        except EXfer.error, msg:
            #Regardless of the type of error, make sure it gets logged.
            Trace.log(e_errors.ERROR,"read_from_hsm EXfer error: %s" %
                      (msg,))

            #Unlink the file since it didn't tranfer in tact.
            try:
                if request['outfile'] != "/dev/null":
                    os.unlink(request['outfile'])
                    delete_at_exit.unregister(request['outfile'])
            except:
                pass

            #Set this now, so at least the local error will be displayed
            # if the mover side is not available.
            done_ticket = {'status':(msg.args[1], msg.args[0])}

            try:
                if msg.args[1] != errno.ENOSPC:
                    done_ticket = callback.read_tcp_obj(control_socket)
                else:
                    done_ticket = (e_errors.USERERROR, msg.args[0])
            except:
                pass #use local error.
                #done_ticket = {'status':(e_errors.EPROTO,
                #                         "Network problem or mover reset")}
                
            result_dict = handle_retries(requests, requests[j],
                                        done_ticket, e.max_retry)

            if result_dict['status'] in e_errors.non_retriable_errors:
                files_left = result_dict['queue_size']
            
            try:
                control_socket.close()
                data_path_socket.close()
                os.close(out_fd)
            except socket.error:
                pass

            continue

        tstring = "%s_elapsed_time" % requests[j]['unique_id']
        tinfo[tstring] = time.time() - lap_start #-------------------------End

        if e.verbose > 1:
            t2 = time.time() - tinfo['encp_start_time']
            print "Verifying", requests[j]['infile'], "transfer.  elapsed=", t2

        # File has been read - wait for final dialog with mover.
        Trace.trace(8,"read_hsm_files waiting for final mover dialog on %s" %
                    (control_socket,))

        # File has been sent - wait for final dialog with mover. 
        # We know the file has hit some sort of media.... 
        # when this occurs. Create a file in pnfs namespace with
        #information about transfer.
        done_ticket = recieve_final_dialog(control_socket, requests[j],
                                           e.max_retry)

        if e.verbose > 1:
            t2 = time.time() - tinfo['encp_start_time']
            print "File", requests[j]['infile'], "transfered.  elapsed=", t2
        if e.verbose > 4:
            print "DONE READING TICKET"
            pprint.pprint(done_ticket)
        
        try:
            control_socket.close()
            data_path_socket.close()
            os.close(out_fd)
        except (OSError, socket.error):
            print "Error closeing file descriptor."
            pass

        #Verfy that the final responce from the mover is that everything is ok.
        result_dict = handle_retries(requests, requests[j],
                                     done_ticket, e.max_retry)
        if result_dict['status'] == e_errors.RETRY:
            continue
        elif result_dict['status'] in e_errors.non_retriable_errors:
            files_left = result_dict['queue_size']
            continue

        if e.verbose > 1:
            print "File %s transfered.  elapsed=%s" % \
                  (done_ticket['infile'],time.time()-tinfo['encp_start_time'])
        if e.verbose > 4:
            print "FINAL DIALOG"
            pprint.pprint(done_ticket)

        #Combine the request and done_ticket into one ticket for simplicity.
        done_ticket = combine_dict(done_ticket, requests[j])
        
        delete_at_exit.unregister(done_ticket['outfile']) #localname

        #These four functions write errors/warnings to the log file and put an
        # error status in the ticket.
        check_crc(done_ticket, e.chk_crc, encp_crc) #Check the CRC.
        set_outfile_permissions(done_ticket) #Writes errors to log file.
        bytes = bytes + verify_file_size(done_ticket) #Verify size is the same.
    
        # remove file requests if transfer completed succesfuly.
        del(requests[j])
        if files_left > 0:
            files_left = files_left - 1

        tstring = "%s_elapsed_finished" % done_ticket['unique_id']
        tinfo[tstring] = time.time() - lap_start #-------------------------End

        if e.verbose > 1:
            print "File status after verification: %s   elapsed=%s" % \
                  (done_ticket["status"], t2)
        
        # calculate some kind of rate - time from beginning to wait for
        # mover to respond until now. This doesn't include the overheads
        # before this, so it isn't a correct rate. I'm assuming that the
        # overheads I've neglected are small so the quoted rate is close
        # to the right one.  In any event, I calculate an overall rate at
        # the end of all transfers
        calculate_rate(done_ticket, tinfo, e.verbose)

        try:
            control_socket.close()
            data_path_socket.close()
            os.close(out_fd)
        except (OSError, socket.error):
            pass

    if e.verbose > 4:
            print "DONE TICKET"
            pprint.pprint(done_ticket)
    
    return requests, bytes

#######################################################################

def read_from_hsm(e, client, tinfo):

    Trace.trace(6,"read_from_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" % (e.input, e.output, e.verbose,
                                      e.chk_crc, tinfo['encp_start_time']))

    requests_per_vol = {}
    
    #check the input unix files. if files don't exits, we bomb out to the user
#    if dcache: #XXX
#        input_files = [pnfs_hack.filename_from_id(e.input)]
#        output = [output]

    #Check the input unix files. if files don't exits, we bomb out to the user.
    (inputlist, file_size) = inputfile_check(e.input)
    ninput = len(inputlist)
    
    if e.verbose > 4:
        print "ninput=",ninput
        print "inputlist=",inputlist
        print "file_size=",file_size

    #Get the pnfs information.
    #Both status and info are tuples.
    status, info = pnfs_information(inputlist,write=0)
    if status[0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, {'status':status})
        print_error(status[0], status[1])
        quit()
    (bfid,junk,junk,junk,junk,junk,pinfo,p)= info

    if e.verbose > 4:
        print "bfid=",bfid
        print "pinfo=",pinfo
        print "p=",p

    # check (and generate) the output files(s)
    # bomb out if they exist already
    outputlist = outputfile_check(e.input, e.output)

    if e.verbose > 4:
        print "outputlist=",outputlist

    # get a port to talk on and listen for connections
    Trace.trace(10,'read_from_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback(verbose = e.verbose)
    client['callback_addr'] = (host, port)
    listen_socket.listen(4)

    if e.verbose > 3:
        print "Waiting for mover(s) to call back on (%s, %s)." % (host, port)

    Trace.trace(10,'read_from_hsm got callback host=%s port=%s listen_socket=%s'%
                (host,port,listen_socket))

    #Contact the configuration server for the file clerk, volume clerk and
    # library manager addresses.
    fc_ticket, vc_ticket, lm_ticket = get_server_info(client)

    file_clerk_address = (fc_ticket["hostip"], fc_ticket["port"])
    volume_clerk_address = (vc_ticket["hostip"], vc_ticket["port"])

    if e.verbose > 3:
        print "File clerk address:", file_clerk_address
        print "Volume clerk address:", volume_clerk_address

    #Create all of the request dictionaries.
    requests_per_vol = create_read_requests(inputlist, outputlist, file_size,
                                            client, tinfo, e, bfid, pinfo,
                                            file_clerk_address,
                                            volume_clerk_address)
                                            #pinfo, p, e.verbose, e.priority,
                                            #e.delpri, e.age_time,
                                            #e.delayed_dismount,)

    #This will halt the program if everything isn't consistant.
    verify_read_request_consistancy(requests_per_vol)
    
    if (len(requests_per_vol) == 0):
        quit()

    # loop over all volumes that are needed and submit all requests for
    # that volume. Read files from each volume before submitting requests
    # for different volumes.
    bytes = 0L
    exit_status = 0
    
    vols = requests_per_vol.keys()
    vols.sort()
    for vol in vols:
        request_list = requests_per_vol[vol]
        number_of_files = len(request_list)

        #The return value is the number of files successfully submitted.
        # This value may be different from len(request_list).  The value
        # of request_list is not changed by this function.
        submitted = submit_read_requests(request_list, client, tinfo, e.verbose)

        if e.verbose > 2:
            print "SUBMITED: ", submitted
        if e.verbose > 4:
            pprint.pprint(request_list)

        if e.verbose > 1:
            print "Files queued.   elapsed=%s" % \
                  (time.time() - tinfo['encp_start_time'])

        if submitted != 0:
            #Since request_list contains all of the entires, submitted must
            # also be passed so read_hsm_files knows how many elements of
            # request_list are valid.
            requests_failed, brcvd = read_hsm_files(listen_socket,
                                                    submitted,
                                                    request_list,
                                                    tinfo, e)
            #.chk_crc,
            #                                        e.max_retry,
            #                                        e.verbose)
            if e.verbose > 1:
                print "Files read for volume %s   elapsed=%s" % \
                      (vol, time.time() - tinfo['encp_start_time'])
            
            if len(requests_failed) > 0:
                exit_status = 1 #Error, when quit() called, this is passed in.
                if e.verbose > 2:
                    print "TRANSFERS FAILED:", len(requests_failed)
                if e.verbose > 4:
                    pprint.pprint(requests_failed)

            #Sum up the total amount of bytes transfered.
            bytes = bytes + brcvd

    if e.verbose > 1:
        print "Files read for all volumes   elapsed=%s" % \
              (time.time() - tinfo['encp_start_time'])

    # we are done transferring - close out the listen socket
    try:
        listen_socket.close()
    except socket.error:
        pass

    #Finishing up with a few of these things.
    done_ticket = calculate_final_statistics(bytes, ninput, exit_status, tinfo)
    
    if e.verbose > 4:
        print "DONE TICKET"
        pprint.pprint(done_ticket)
    elif e.verbose:
        print done_ticket['status'][1]

    return done_ticket

##############################################################################
##############################################################################

class encp(interface.Interface):

    deprecated_options = ['--crc']
    
    def __init__(self):
        self.chk_crc = 1           # we will check the crc unless told not to
        self.priority = 1          # lowest priority
        self.delpri = 0            # priority doesn't change
        self.admpri = -1           # quick fix to check HiPri functionality
        self.age_time = 0          # priority doesn't age
        self.data_access_layer = 0 # no special listings
        self.verbose = 0
        self.bufsize = 65536*4     #XXX CGW Investigate this
        self.delayed_dismount = 0
        self.max_retry = 2         # number of times to try again
        self.mover_timeout = 15*60 # seconds to wait for mover to call back,
                                   # before resubmitting req. to lib. mgr.
                                   # 15 minutes
        self.output_file_family = '' # initial set for use with --ephemeral or
                                     # or --file-family
                                     
        self.bytes = None

        self.dcache = 0 #Special options for operation with a disk cache layer.
        self.put_cache = self.get_cache = 0

        self.storage_info = None # Ditto
        self.test_mode = 0
        
        interface.Interface.__init__(self)
        # parse the options

        self.parse_options()

    ##########################################################################
    # define the command line options that are valid
    def options(self):
        return self.config_options()+[
            "verbose=","no-crc","priority=","delpri=","age-time=",
            "delayed-dismount=", "file-family=", "ephemeral",
#            "get-cache", "put-cache", "storage-info=",
            "data-access-layer"] + self.help_options()

    
    ##########################################################################
    #  define our specific help
    def help_line(self):
        prefix = self.help_prefix()
        return "%s%s\n or\n %s%s%s" % (
            prefix, self.parameters1(),
            prefix, self.parameters2(),
            self.format_options(self.options(), "\n\t\t"))

    ##########################################################################
    #  define our specific parameters
    def parameters1(self):
        return "inputfilename outputfilename"

    def parameters2(self):
        return "inputfilename1 ... inputfilenameN outputdirectory"

    def parameters(self):
        return "[["+self.parameters1()+"] or ["+self.parameters2()+"]]"

    ##########################################################################
    # parse the options from the command line
    def parse_options(self):
        # normal parsing of options
        interface.Interface.parse_options(self)

        # bomb out if we don't have an input and an output
        self.arglen = len(self.args)
        if self.arglen < 2 :
            print_error(e_errors.USERERROR, "not enough arguments specified")
            self.print_help()
            sys.exit(1)

        #if not client: 
        #    clients(self.config_host, self.config_port)
            
        # get fullpaths to the files
        p = []
        for i in range(0,self.arglen):
            (machine, fullname, dir, basename) = fullpath(self.args[i])
            self.args[i] = os.path.join(dir,basename)
            p.append(string.find(dir,"/pnfs"))
        # all files on the hsm system have /pnfs/ as 1st part of their name
        # scan input files for /pnfs - all have to be the same
        p1 = p[0]
        p2 = p[self.arglen-1]
        self.input = [self.args[0]]
        self.output = [self.args[self.arglen-1]]
        for i in range(1,len(self.args)-1):
            if p[i]!=p1:
                msg = "Not all input_files are %s files"
                if p1:
                    print_error(e_errors.USERERROR, msg % "/pnfs/...")
                else:
                    print_error(e_errors.USERERROR, msg % "unix")
                quit()
            else:
                self.input.append(self.args[i])

        if p1 == 0:
            self.intype="hsmfile"
        else:
            self.intype="unixfile"
        if p2 == 0:
            self.outtype="hsmfile"
        else:
            self.outtype="unixfile"


##############################################################################

def main():
    encp_start_time = time.time()
    tinfo = {'encp_start_time':encp_start_time}
    
    Trace.init("ENCP")
    Trace.trace( 6, 'encp called at %s: %s'%(encp_start_time, sys.argv) )

    for opt in encp.deprecated_options:
        if opt in sys.argv:
            print "WARNING: option %s is deprecated, ignoring" % (opt,)
            sys.argv.remove(opt)
    
    # use class to get standard way of parsing options
    e = encp()
    if e.test_mode:
        print "WARNING: running in test mode"

    for x in xrange(6, e.verbose+1):
        Trace.do_print(x)

    #Dictionary with configuration server, clean udp and other info.
    client = clients(e.config_host, e.config_port)
    if e.verbose > 3:
        print "csc=", client['csc']
        print "u=", client['u']
        print "logc=", logc

    if e.data_access_layer:
        global data_access_layer_requested
        data_access_layer_requested = e.data_access_layer
        #data_access_layer_requested.set()

    #Special handling for use with dcache - not yet enabled
    if e.get_cache:
        pnfs_id = sys.argv[-2]
        local_file = sys.argv[-1]
        done_ticket = read_from_hsm(pnfs_id, local_file, client,
                                    e.verbose, e.chk_crc,
                                    e.priority, e.delpri,
                                    e.age_time,
                                    e.delayed_dismount, encp_start_time,
                                    dcache=1)
        
    elif e.put_cache:
        pnfs_id = sys.argv[-2]
        local_file = sys.argv[-1]
        done_ticket = write_to_hsm(local_file, pnfs_id, client,
                                   e.output_file_family,
                                   e.verbose, e.chk_crc,
                                   e.priority, e.delpri, e.age_time,
                                   e.delayed_dismount, encp_start_time,
                                   e.bytes, dcache=1,
                                   storage_info=e.storage_info)
        
    ## have we been called "encp unixfile hsmfile" ?
    elif e.intype=="unixfile" and e.outtype=="hsmfile" :
        done_ticket = write_to_hsm(e, client, tinfo)
        

    ## have we been called "encp hsmfile unixfile" ?
    elif e.intype=="hsmfile" and e.outtype=="unixfile" :
        done_ticket = read_from_hsm(e, client, tinfo)


    ## have we been called "encp unixfile unixfile" ?
    elif e.intype=="unixfile" and e.outtype=="unixfile" :
        emsg="encp copies to/from tape. It is not involved in copying %s to %s" % (e.intype, e.outtype)
        print_error('USERERROR', emsg)
        if e.data_access_layer:
            print_data_access_layer_format(e.input, e.output, 0, {'status':("USERERROR",emsg)})
        quit()

    ## have we been called "encp hsmfile hsmfile?
    elif e.intype=="hsmfile" and e.outtype=="hsmfile" :
        emsg=  "encp tape to tape is not implemented. Copy file to local disk and them back to tape"
        print_error('USERERROR', emsg)
        if e.data_access_layer:
            print_data_access_layer_format(e.input, e.output, 0, {'status':("USERERROR",emsg)})
        quit()

    else:
        emsg = "ERROR: Can not process arguments %s"%(e.args,)
        Trace.trace(6,emsg)
        print_data_access_layer_format("","",0,{'status':("USERERROR",emsg)})
        quit()

    try:
        if e.data_access_layer or done_ticket['status'][0] != e_errors.OK:
            print_data_access_layer_format(e.input, e.output, 0, done_ticket)
    except ValueError:
        pass

    Trace.trace(10,"encp finished at %s"%(time.time(),))

if __name__ == '__main__':

    for sig in range(signal.NSIG):
        if sig not in (signal.SIGTSTP, signal.SIGCONT,
                       signal.SIGCHLD, signal.SIGWINCH):
            try:
                signal.signal(sig, signal_handler)
            except:
                pass
    
    try:
        main()
        quit(0)
    except SystemExit, msg:
        quit(1)
    #except:
        #exc, msg, tb = sys.exc_info()
        #sys.stderr.write("%s\n" % (tb,))
        #sys.stderr.write("%s %s\n" % (exc, msg))
        #quit(1)
        
        
