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
import enstore_constants

ONE_G = 1048576 * 1024
TWO_G = ONE_G - 1 + ONE_G               # actually, 2G - 1
MAX_FILE_SIZE = ONE_G - 2048 + ONE_G    # don't get overflow

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

# int32(v) -- if v > 2^31-1, make it long
#
# a quick fix for those 64 bit machine that consider int is 64 bit ...

def int32(v):
    if v > TWO_G:
        return long(v)
    else:
        return v

############################################################################

def signal_handler(sig, frame):
    try:
        sys.stderr.write("Caught signal %s, exiting\n" % (sig,))
        sys.stderr.flush()
    except:
        pass
    
    if sig != signal.SIGTERM: #If they kill, don't do anything.
        quit(1)

def encp_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "v2_14  CVS $Revision$ "
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
            #If both items in the dictionary are themselves dictionaries, then
            # do this recursivly.
            if new.get(key, None) and \
               type(dictionaries[i][key]) == type({}) and \
               type(new[key]) == type({}):
                         new[key] = combine_dict(new[key],
                                                 dictionaries[i][key])
                         
            #Just set the value if not a dictionary.
            elif not new.has_key(key):
                new[key] = dictionaries[i][key]

    return new

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

def close_descriptors(*fds):
    #For each file descriptor that is passed in, close it.  The fds can
    # contain itegers or class instances with a "close" function attribute.
    for fd in fds:
        if hasattr(fd, "close"):
            apply(getattr(fd, "close"))
        else:
            try:
                os.close(fd)
            except OSError:
                sys.stderr.write("Unable to close fd %s.\n" % fd)
            
def max_attempts(library, encp_intf):
    #Determine how many times a transfer can be retried from failures.
    #Also, determine how many times encp resends the request to the lm
    # and the mover fails to call back.
    if library[-17:] == ".library_manager":
        lib = library
    else:
        lib = library + ".library_manager"

    # get a configuration server
    config_host = interface.default_host()
    config_port = interface.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    lm = csc.get(lib)

    if encp_intf.max_retry == None:
        encp_intf.max_retry = lm.get('max_encp_retries',
                                     enstore_constants.DEFAULT_ENCP_RETRIES)
    if encp_intf.max_resubmit == None:
        encp_intf.max_resubmit = lm.get('max_encp_resubmits',
                                enstore_constants.DEFAULT_ENCP_RESUBMITIONS)
        

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
    total = now - time_ticket.get('encp_start_time', now)
    sts =  ticket.get('status', ('Unknown', None))
    status = sts[0]
    msg = sts[1:]
    if len(msg)==1:
        msg=msg[0]
        
    if type(outputfile) == type([]) and len(outputfile) == 1:
        outputfile = outputfile[0]
    if type(inputfile) == type([]) and len(inputfile) == 1:
        inputfile = inputfile[0]
        
    if not data_access_layer_requested and status != e_errors.OK:
        out=sys.stderr
    else:
        out=sys.stdout

    ###String used to print data access layer.
    data_access_layer_format = """INFILE=%s
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
            #Attach the data access layer info to the msg, but first remove
            # the newline and tab used for readability on the terminal.
            errmsg = errmsg + " " + string.replace(msg, "\n\t", "  ")
        Trace.log(msg_type, errmsg)
    except OSError:
        exc,msg,tb=sys.exc_info()
        sys.stderr.write("cannot log error message %s\n"%(errmsg,))
        sys.stderr.write("internal error %s %s\n"%(exc,msg))

#######################################################################

# get the configuration client and udp client and logger client
# return some information about who we are so it can be used in the ticket

def clients(config_host,config_port):
    # get a configuration serverg432
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
    
    # get a logger client
    logc = log_client.LoggerClient(csc, 'ENCP', 'log_server')
   
    #global client #should not do this
    client = {}
    client['csc']=csc
    client['logc']=logc
    
    return client

##############################################################################

# for automounted pnfs

pnfs_is_automounted = 0

# access_check(path, mode) -- a wrapper for os.access() that retries for
#                             automatically mounted file system

def access_check(path, mode):
    # if pnfs is not auto mounted, simply call os.access
    if not pnfs_is_automounted:
        return os.access(path, mode)

    # automaticall retry 6 times, one second delay each
    for i in range(5):
        if os.access(path, mode):
            return 1
        time.sleep(1)
    return os.access(path, mode)

#Make sure that the filename is valid.
def filename_check(filename):
    #pnfs (v3.1.7) only supports filepaths segments that are
    # less than 200 characters.
    for directory in filename.split("/"):
        if len(directory) > 200:
            status = ('USERERROR',
                      'Filepath segment %s exceeds 200 characters with %s' %
                      (directory, len(directory)))
            return status

    #limit the usable characters for a filename.
    if not charset.is_in_filenamecharset(filename):
        st = ""
        for ch in filename: #grab all illegal characters.
            if ch not in charset.filenamecharset:
                st = st + ch
        status = ('USERERROR',
                  'Filepath uses non-printable characters: %s' % (st,))
        return status

    return (e_errors.OK, None)
    
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

        #check to make sure that the filename string doesn't have any
        # wackiness to it.
        status = filename_check(inputlist[i])
        if status != (e_errors.OK, None):
            print_data_access_layer_format(inputlist[i], '', 0,
                                           {'status':status})
            quit()

        # input files must exist
        if not access_check(inputlist[i], os.R_OK):
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

        # input files can't be larger than 2G
        #if statinfo[stat.ST_SIZE] > MAX_FILE_SIZE:
        #    print_data_access_layer_format(inputlist[i], '', 0, {'status':(
        #        'USERERROR', 'file %s exceeds file size limit of %d bytes'%(inputlist[i],MAX_FILE_SIZE))})
        #    quit()

        # get the file size
	p = pnfs.Pnfs(inputlist[i])
	p.get_file_size()
	file_size.append(p.file_size)
	#This would work if pnfs supported NFS version 3.  Untill it does and
	# all the files have their pnfs layer 2s cleared out, this can not
	# be used.
        #file_size.append(long(statinfo[stat.ST_SIZE]))

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

def outputfile_check(inputlist, output, dcache):
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

            #check to make sure that the filename string doesn't have any
            # wackiness to it.
            status = filename_check(outputlist[i])
            if status != (e_errors.OK, None):
                print_data_access_layer_format('', outputlist[i], 0,
                                               {'status':status})
                quit()

            #In this if...elif...else determine if the user entered in
            # valid file or directory names appropriately.
            if access_check(outputlist[i], os.W_OK):
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
            elif access_check(odir, os.W_OK) and len(inputlist) == 1:
                #Output is not a directory.  If one level up (odir) is a
                # valid directory and the number of input files is 1, then
                # the full name is the valid name of a one file transfer.
                if not os.path.isdir(ofullname):
                    outputlist[i] = ofullname
                else:
                    outputlist[i] = os.path.join(outputlist[i], ibasename)
            else:
                #only error conditions left

                if access_check(outputlist[i], os.F_OK):
                    #Path exists, but without write permissions.
                    raise e_errors.USERERROR, "No write access to %s"%(odir,)
                elif not access_check(outputlist[i], os.F_OK):
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

        except (e_errors.USERERROR, e_errors.UNKNOWN):
            exc, msg, tb = sys.exc_info()
            print_data_access_layer_format(ifullname, ofullname, 0,
                                           {'status':(exc, msg)})
            quit()
        except (e_errors.EEXIST,), detail:
            if not dcache:
                exc, msg, tb = sys.exc_info()
                print_data_access_layer_format(ifullname, ofullname, 0,
                                               {'status':(exc, msg)})
                quit()
            else: #dache
                #Since, the file exists, we can get its file size.
                statinfo = os.stat(outputlist[i])
                if statinfo[stat.ST_SIZE]:
                    msg = "disk cache requires zero length file"
                    print_data_access_layer_format(ifullname,
                                                   ofullname,
                                                   statinfo[stat.ST_SIZE],
                                                   {'status':(e_errors.EEXIST,
                                                              msg)})
                    quit()
                else:
                    return outputlist

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

def file_check(e):
    # check the output pnfs files(s) names
    # bomb out if they exist already
    inputlist, file_size = inputfile_check(e.input)
    n_input = len(inputlist)

    Trace.message(5, "file count=" + str(n_input))
    Trace.message(5, "inputlist=" + str(inputlist))
    Trace.message(5, "file_size=" + str(file_size))

    # check (and generate) the output pnfs files(s) names
    # bomb out if they exist already
    outputlist = outputfile_check(e.input, e.output, e.put_cache)

    Trace.message(5, "outputlist=" + str(outputlist))

    return n_input, file_size, inputlist, outputlist

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
                                             [listen_socket], mover_timeout)

    if not read_fds:
        #timed out!
        msg = os.strerror(errno.ETIMEDOUT)
        Trace.message(4, msg)
        raise socket.error, (errno.ETIMEDOUT, msg)
    
    control_socket, address = listen_socket.accept()

    if not hostaddr.allow(address):
        control_socket.close()
        msg = "host %s not allowed" % address[0]
        Trace.message(4, msg)
        raise socket.error, (errno.EACCES, msg)

    try:
        ticket = callback.read_tcp_obj(control_socket)
    except e_errors.TCP_EXCEPTION:
        msg = e_errors.TCP_EXCEPTION
        Trace.message(4, msg)
        raise socket.error, (errno.NET_ERROR, msg)

    #try:
    #    a = ticket['mover']['callback_addr']
    #except KeyError:
    #    msg = "mover did not return mover info.  " + \
    #          str(ticket.get("status", None))
    #    Trace.message(4, msg)
    #    raise socket.error, (errno.EPROTO, msg )

    return control_socket, address, ticket
    
##############################################################################

def open_data_socket(mover_addr, mode=0):
    data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    flags = fcntl.fcntl(data_path_socket.fileno(), FCNTL.F_GETFL)
    fcntl.fcntl(data_path_socket.fileno(), FCNTL.F_SETFL,
                flags | FCNTL.O_NONBLOCK)

    #set up any special network load-balancing voodoo
    interface=host_config.check_load_balance(mode=0, dest=mover_addr[0])
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
    r, w, ex = select.select([data_path_socket], [data_path_socket], [], 30)

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
        except (socket.error,), detail:
            exc, msg, tb = sys.exc_info()
            if detail.args[0] == errno.ETIMEDOUT:
                ticket = {'status':(e_errors.RESUBMITTING, None)}
            else:
                ticket = {'status':(exc, msg)}
                
            #Trace.log(e_errors.INFO, e_errors.RESUBMITTING)

            #Since an error occured, just return it.
            return None, None, ticket

        Trace.message(5, "MOVER HANDSHAKE")
        Trace.message(5, pprint.pformat(ticket))

        #verify that the id is one that we are excpeting and not one that got
        # lost in the ether.
        for i in range(0, len(work_tickets)):
            if work_tickets[i]['unique_id'] == ticket['unique_id']:
                break #Success, control socket opened!
            
        else: #Didn't find matching id.
            close_descriptors(control_socket)

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

        try:
            mover_addr = ticket['mover']['callback_addr']
        except KeyError:
            exc, msg, tb = sys.exc_info()
            sys.stderr.write("Sub ticket 'mover' not found.\n")
            sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
            sys.stderr.write(pprint.pformat(ticket)+"\n")
            if ticket.get('status', (None, None))[0] == e_errors.OK:
                ticket['status'] = (str(exc), str(msg))
            return None, None, ticket

        #Determine if reading or writing.  This only has importance on
        # mulithomed machines were an interface needs to be choosen based
        # on reading and writing usages/rates of the interfaces.
        if ticket['outfile'][:5] == "/pnfs":
            mode = 1
        else:
            mode = 0

        #Attempt to get the data socket connected with the mover.
        try:
            data_path_socket = open_data_socket(mover_addr, mode)

            if not data_path_socket:
                raise socket.error,(errno.ENOTCONN,os.strerror(errno.ENOTCONN))

        except (socket.error,), detail:
            exc, msg, tb = sys.exc_info()
            ticket = {'status':(e_errors.NET_ERROR, str(msg))}
            #Trace.log(e_errors.INFO, str(msg))
            
            #Since an error occured, just return it.
            return None, None, ticket

        #If we got here then the status is OK.
        ticket['status'] = (e_errors.OK, None)
        #Include new info from mover.
        work_tickets[i] = ticket

        #The following three lines are usefull for testing error handling.
        #control_socket.close()
        #data_path_socket.close()
        #ticket['status'] = (e_errors.NET_ERROR, "because I closed them")

        return control_socket, data_path_socket, ticket

############################################################################

def submit_one_request(ticket, verbose):
    ##start of resubmit block
    Trace.trace(7,"write_to_hsm q'ing: %s"%(ticket,))

    if ticket['retry']:
        Trace.message(2, "RETRY COUNT:" + str(ticket['retry']))

    # get a configuration server
    config_host = interface.default_host()
    config_port = interface.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))

    #Send work ticket to LM
    #Get the library manager info information.
    lmc = library_manager_client.LibraryManagerClient(
        csc, ticket['vc']['library'] + ".library_manager")
    if ticket['infile'][:5] == "/pnfs":
        responce_ticket = lmc.read_from_hsm(ticket)
    else:
        responce_ticket = lmc.write_to_hsm(ticket)

    if responce_ticket['status'][0] != e_errors.OK :
        Trace.message(3, "Submition to LM failed: " + \
                      str(responce_ticket['status']))
        Trace.log(e_errors.NET_ERROR,
                  "submit_one_request: Ticket submit failed for %s"
                  " - retrying" % ticket['infile'])

    return responce_ticket

############################################################################

#mode should only contain two values, "read", "write".
def open_local_file(filename, mode):
    #Determine the os.open() flags to use.
    if mode == "write":
            flags = os.O_RDONLY
    elif mode == "read":
        if filename == "/dev/null":
            flags = os.O_WRONLY
        else:
            flags = os.O_WRONLY | os.O_CREAT
    else:
        done_ticket = {'status':(e_errors.UNKNOWN,
                                 "Unable to open local file.")}
        return done_ticket

    #Try to open the local file for read/write.
    try:
        #if filename == "/dev/null":
        #    local_fd = os.open("/dev/null", flags)
        #else:
        local_fd = os.open(filename, flags, 0)
    except OSError, detail:
        #USERERROR is on the list of non-retriable errors.  Because of
        # this the return from handle_retries will remove this request
        # from the list.  Thus avoiding issues with the continue and
        # range(submitted).
        done_ticket = {'status':(e_errors.USERERROR, detail)}
        return done_ticket

    done_ticket = {'status':(e_errors.OK, None), 'fd':local_fd}
    return done_ticket

############################################################################

def receive_final_dialog(control_socket, work_ticket):
    # File has been sent - wait for final dialog with mover. 
    # We know the file has hit some sort of media.... 
    # when this occurs. Create a file in pnfs namespace with
    #information about transfer.
    
    try:
        done_ticket = callback.read_tcp_obj(control_socket)
    except e_errors.TCP_EXCEPTION, msg:
        done_ticket = {'status':(e_errors.TCP_EXCEPTION,
                                 msg)}
        
    return done_ticket
        
############################################################################

#Returns two-tuple.  First is dictionary with 'status' element.  The next
# is an integer of the crc value.  On error returns 0.
def transfer_file(input_fd, output_fd, control_socket, request, e):

    encp_crc = 0
    
    #Read/Write in/out the data to/from the mover and write/read it out to
    # file.  Also, total up the crc value for comparison with what was
    # sent from the mover.
    try:
        if e.chk_crc != 0:
            crc_flag = 1
        else:
            crc_flag = 0

        encp_crc = EXfer.fd_xfer(input_fd, output_fd, request['file_size'],
                                 e.bufsize, crc_flag, 0)
        EXfer_ticket = {'status':(e_errors.OK, None)}
    except EXfer.error, msg:
        EXfer_ticket = {'status':(e_errors.IOERROR, msg)}
        Trace.log(e_errors.WARNING, "transfer file EXfer error: %s" % (msg,))

    # File has been read - wait for final dialog with mover.
    Trace.trace(8,"read_hsm_files waiting for final mover dialog on %s" %
                (control_socket,))

    #Even though the functionality is there for this to be done in
    # handle requests, this should be recieved outside since there must
    # be one... not only receiving one on error.
    done_ticket = receive_final_dialog(control_socket, request)

    if not e_errors.is_retriable(done_ticket['status'][0]):
        return done_ticket, 0
    elif not e_errors.is_retriable(EXfer_ticket['status'][0]):
        return EXfer_ticket, 0
    elif done_ticket['status'][0] != (e_errors.OK):
        return done_ticket, 0
    elif EXfer_ticket['status'][0] != (e_errors.OK):
        return EXfer_ticket, 0
    else:
        return done_ticket, encp_crc #Success.

############################################################################

def check_crc(done_ticket, chk_crc, my_crc):

    # Check the CRC
    if chk_crc:
        mover_crc = done_ticket['fc'].get('complete_crc', None)
        if mover_crc == None:
            msg =   "warning: mover did not return CRC; skipping CRC check\n"
            sys.stderr.write(msg)
            #done_ticket['status'] = (e_errors.NO_CRC_RETURNED, msg)

        elif mover_crc != my_crc :
            msg = "CRC mismatch: %d != %d" % (mover_crc, my_crc)
            done_ticket['status'] = (e_errors.CRC_ERROR, msg)

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

def handle_retries(request_list, request_dictionary, error_dictionary,
                   control_socket, encp_intf):
    #Extract for readability.
    max_retries = encp_intf.max_retry
    max_submits = encp_intf.max_resubmit
    verbose = encp_intf.verbose

    #request_dictionary must have 'retry' as an element.
    #error_dictionary must have 'status':(e_errors.XXX, "explanation").

    #These fields need to be retrieved in this fashion.  If the transfer
    # failed before encp could determine which transfer failed (aka failed
    # opening/reading the/from contol socket) then only the 'status' field
    # of both the request_dictionary and error_dictionary are guarenteed to
    # exist (although some situations will add others).
    infile = request_dictionary.get('infile', '')
    outfile = request_dictionary.get('outfile', '')
    file_size = request_dictionary.get('file_size', 0)
    retry = request_dictionary.get('retry', 0)
    
    resubmits = request_list[0].get('resubmits', 0)
    
    dict_status = error_dictionary.get('status', (e_errors.OK, None))

    #Get volume info from the volume clerk.
    #Need to check if the volume has been marked NOACCESS since it
    # was checked last.  This should only apply to reads.
    if request_dictionary.get('fc', {}).has_key('external_label'):
        # get a configuration server
        config_host = interface.default_host()
        config_port = interface.default_port()
        csc=configuration_client.ConfigurationClient((config_host,config_port))
        # get the volume clerk responce.
        vcc = volume_clerk_client.VolumeClerkClient(csc)
        vc_reply = vcc.inquire_vol(request_dictionary['fc']['external_label'])
        vc_status = vc_reply['status']
    else:
        vc_status = (e_errors.OK, None)

    #If there is a control socket open and there is data to read, then read it.
    socket_status = (e_errors.OK, None)
    socket_dict = {'status':socket_status}
    if control_socket:
        #Determine if the control socket has some errror to report.
        read_fd, write_fd, exc_fd = select.select([control_socket],
                                                  [], [], 15)
        #check control socket for error.
        if read_fd:
            socket_dict = receive_final_dialog(control_socket,
                                               request_dictionary)
            socket_status = socket_dict.get('status', (e_errors.OK , None))
            request_dictionary = combine_dict(socket_dict, request_dictionary)

    #The volume clerk set the volume NOACCESS.
    if vc_status[0] != e_errors.OK:
        status = vc_status
    #Set status if there was an error recieved from control socket.
    elif socket_status[0] != e_errors.OK:
        status = socket_status
    #Use the ticket status.
    else:
        status = dict_status

        
    #If there is no error, then don't do anything
    if status == (e_errors.OK, None):
        result_dict = {'status':(e_errors.OK, None), 'retry':retry,
                       'resubmits':resubmits, 'queue_size':len(request_list)}
        result_dict = combine_dict(result_dict, socket_dict)
        return result_dict

    #If the mover doesn't call back after max_submits number of times, give up.
    if max_submits != None and resubmits >= max_submits:
        Trace.message(3,"To many resubmitions for %s -> %s."%(infile,outfile))
        status = (e_errors.TOO_MANY_RESUBMITS, status)

    #If the transfer has failed to many times, remove it from the queue.
    # Since TOO_MANY_RETRIES is non-retriable, set this here.
    if max_retries != None and retry >= max_retries:
        Trace.message(3,"To many retries for %s -> %s."%(infile,outfile))
        status = (e_errors.TOO_MANY_RETRIES, status)

    #If the error is not retriable, remove it from the request queue.  There
    # are two types of non retriable errors.  Those that cause the transfer
    # to be aborted, and those that in addition to abborting the transfer
    # tell the operator that someone needs to investigate what happend.
    if not e_errors.is_retriable(status[0]):
        #Print error to stdout in data_access_layer format. However, only
        # do so if the dictionary is full (aka. the error occured after
        # the control socket was successfully opened).  Control socket
        # errors are printed elsewere (for reads only).
        error_dictionary['status'] = status
        #By checking those that aren't of significant length, we only
        # print these out on writes.
        if len(request_dictionary) > 3:
            print_data_access_layer_format(infile, outfile, file_size,
                                           error_dictionary)

        if status[0] in e_errors.raise_alarm_errors:
            Trace.alarm(e_errors.WARNING, status[0], {
                'infile':infile, 'outfile':outfile, 'status':status[1]})

        try:
            #Try to delete the request.  In the event that the connection
            # didn't let us determine which request failed, don't worry.
            del request_list[request_list.index(request_dictionary)]
            queue_size = len(request_list)
        except (KeyError, ValueError):
            queue_size = len(request_list) - 1

        #This is needed on reads to send back to the calling function
        # that ths error means that there should be none left in the queue.
        # On writes, if it gets this far it should already be 0 and doesn't
        # effect anything.
        if status == e_errors.TOO_MANY_RESUBMITS:
            queue_size = 0
        
        result_dict = {'status':status, 'retry':retry,
                       'resubmits':resubmits,
                       'queue_size':queue_size}

    #When nothing was recieved from the mover and the 15min has passed,
    # resubmit all entries in the queue.  Leave the unique id the same.
    # Even though for writes there is only one entry in the active request
    # list at a time, submitting like this will still work.
    elif status[0] == e_errors.RESUBMITTING:
        request_dictionary['resubmits'] = resubmits + 1

        for req in request_list:
            try:
                #Increase the resubmit count.
                req['resubmits'] = req.get('resubmits', 0) + 1

                #Before resubmitting, there are some fields that the library
                # manager and mover don't expect to receive from encp,
                # these should be removed.
                for item in ("mover", ):
                    try:
                        del req[item]
                    except KeyError:
                        pass

                #Since a retriable error occured, resubmit the ticket.
                lm_responce = submit_one_request(req, verbose)
                if lm_responce['status'][0] != e_errors.OK:
                    sys.stderr.write("Error resubmitting.\n")
                    sys.stderr.write(pprint.pformat(lm_responce))
            except:
                exc, msg, tb = sys.exc_info()
                sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
            
            #Log the intermidiate error as a warning instead as a full error.
            status = (status[0], req.get('unique_id', None))
            Trace.log(e_errors.WARNING, status)

            result_dict = {'status':(e_errors.RESUBMITTING, None),
                           'retry':request_dictionary.get('retry', 0),
                           'resubmits':request_dictionary.get('resubmits', 0),
                           'queue_size':len(request_list)}

    #Change the unique id so the library manager won't remove the retry
    # request when it removes the old one.  Do this only when there was an
    # actuall error, not just a timeout.  Also, increase the retry count by 1.
    else:
        request_dictionary['unique_id'] = generate_unique_id()
        #Keep retrying this file.
        try:
            #Increase the retry count.
            request_dictionary['retry'] = retry + 1
            
            #Before resending, there are some fields that the library
            # manager and mover don't expect to receive from encp,
            # these should be removed.
            for item in ("mover", ):
                try:
                    del request_dictionary[item]
                except KeyError:
                    pass
                
            #Since a retriable error occured, resubmit the ticket.
            lm_responce = submit_one_request(request_dictionary, verbose)
            if lm_responce['status'][0] != e_errors.OK:
                sys.stderr.write("Error resubmitting.\n")
                sys.stderr.write(pprint.pformat(lm_responce))
                status = lm_responce['status']
        except KeyError:
            sys.stderr.write("Error processing resubmition of %s.\n" %
                             (request_dictionary['unique_id']))
            sys.stderr.write(pprint.pformat(request_dictionary))

        #Log the intermidiate error as a warning instead as a full error.
        Trace.log(e_errors.WARNING, status)
        Trace.log(e_errors.WARNING, (e_errors.RETRY,
                                     request_dictionary['unique_id']))

        result_dict = {'status':(e_errors.RETRY,
                                 request_dictionary['unique_id']),
                       'retry':request_dictionary.get('retry', 0),
                       'resubmits':request_dictionary.get('resubmits', 0),
                       'queue_size':len(request_list)}

    #If we get here, then some type of error occured.  This means one of
    # three things happend.
    #1) The transfer was abborted.
    #    a) A non-retriable error occured once.
    #    b) A retriable error occured too many times.
    #2) The request(s) was/were resubmited.
    #    a) No mover called back before the listen socket timed out.
    #3) The request(s) was/were retried.
    #    a) A retriable error occured.
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
    network_time = tinfo.get('%s_elapsed_time' % (id,), 0)
    complete_time = tinfo.get('%s_elapsed_finished' % (id,), 0)
    drive_time = done_ticket['times'].get('transfer_time', 0)

    if done_ticket['work'] == "read_from_hsm":
        preposition = "from"
    else: #write "to"
        preposition = "to"
    
    if done_ticket['status'][0] == e_errors.OK:

        if complete_time != 0:
            tinfo['overall_rate_%s'%(id,)] = MB_transfered / complete_time
        else:
            tinfo['overall_rate_%s'%(id,)] = 0.0
        if network_time != 0:
            tinfo['network_rate_%s'%(id,)] = MB_transfered / network_time
        else:
            tinfo['network_rate_%s'%(id,)] = 0.0            
        if drive_time != 0:
            tinfo['drive_rate_%s'%(id,)] = MB_transfered / drive_time
        else:
            tinfo['drive_rate_%s'%(id,)] = 0.0
            
        print_format = "Transfer %s -> %s:\n" \
                 "\t%d bytes copied %s %s at %.3g MB/S\n " \
                 "\t(%.3g MB/S network) (%.3g MB/S drive)\n " \
                 "\tdrive_id=%s drive_sn=%s drive_vendor=%s\n" \
                 "\tmover=%s media_changer=%s   elapsed=%.02f"
        
        log_format = "  %s %s -> %s: "\
                     "%d bytes copied %s %s at %.3g MB/S " \
                     "(%.3g MB/S network) (%.3g MB/S drive) " \
                     "mover=%s " \
                     "drive_id=%s drive_sn=%s drive_venor=%s elapsed=%.05g "\
                     "{'media_changer' : '%s', 'mover_interface' : '%s', " \
                     "'driver' : '%s'}"

        print_values = (done_ticket['infile'],
                        done_ticket['outfile'],
                        done_ticket['file_size'],
                        preposition,
                        done_ticket["fc"]["external_label"],
                        tinfo["overall_rate_%s"%(id,)],
                        tinfo['network_rate_%s'%(id,)],
                        tinfo["drive_rate_%s"%(id,)],
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
                      tinfo["overall_rate_%s"%(id,)],
                      tinfo["network_rate_%s"%(id,)],
                      tinfo['drive_rate_%s'%(id,)],
                      done_ticket["mover"]["name"],
                      done_ticket["mover"]["product_id"],
                      done_ticket["mover"]["serial_num"],
                      done_ticket["mover"]["vendor_id"],
                      time.time() - tinfo["encp_start_time"],
                      done_ticket["mover"]["media_changer"],
                      socket.gethostbyaddr(done_ticket["mover"]["hostip"])[0],
                      done_ticket["mover"]["driver"])
        
        Trace.message(1, print_format % print_values)

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

    if tinfo['total']: #protect against division by zero.
        tinfo['MB_per_S_total'] = MB_transfered / tinfo['total']
    else:
        tinfo['MB_per_S_total'] = 0.0

    #get all the drive rates from the dictionary.
    drive_rate  = 0L
    count = 0
    for value in tinfo.keys():
        if string.find(value, "drive_rate") != -1:
            count = count + 1
            drive_rate  = drive_rate  + tinfo[value]
    if count:
        tinfo['MB_per_S_drive'] = drive_rate / count
    else:
        tinfo['MB_per_S_drive'] = 0.0

    #get all the drive rates from the dictionary.
    network_rate  = 0L
    count = 0
    for value in tinfo.keys():
        if string.find(value, "network_rate") != -1:
            count = count + 1
            network_rate  = network_rate  + tinfo[value]
    if count:
        tinfo['MB_per_S_network'] = network_rate / count
    else:
        tinfo['MB_per_S_network'] = 0.0
    
    msg = "%s transferring %s bytes in %s files in %s sec.\n" \
          "\tOverall rate = %.3g MB/sec.  Drive rate = %.3g MB/sec.\n" \
          "\tNetwork rate = %.3g MB/sec.  Exit status = %s."
    
    if exit_status:
        msg = msg % ("Error after", bytes, number_of_files,
                     tinfo['total'], tinfo["MB_per_S_total"],
                     tinfo['MB_per_S_drive'], tinfo['MB_per_S_network'],
                     exit_status)
    else:
        msg = msg % ("Completed", bytes, number_of_files,
                     tinfo['total'], tinfo["MB_per_S_total"],
                     tinfo['MB_per_S_drive'], tinfo['MB_per_S_network'],
                     exit_status)

    done_ticket = {}
    done_ticket['times'] = tinfo
    #set the final status values
    done_ticket['exit_status'] = exit_status
    done_ticket['status'] = (e_errors.OK, msg)
    return done_ticket

############################################################################
#Support functions for writes.
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
        
############################################################################

def set_pnfs_settings(ticket, client, verbose):
    # create a new pnfs object pointing to current output file
    Trace.trace(10,"write_to_hsm adding to pnfs "+ ticket['outfile'])
    p=pnfs.Pnfs(ticket['outfile'])

    try:
        # save the bfid and set the file size
        p.set_bit_file_id(ticket["fc"]["bfid"])
    except KeyboardInterrupt:
        exc, msg, tb = sys.exc_info()
        raise exc, msg, tb
    except:
        exc, msg, tb = sys.exc_info()
        Trace.log(e_errors.INFO, "Trouble with pnfs: %s %s."
                  % (str(exc), str(msg)))
        ticket['status'] = (str(exc), str(msg))
        return
        
    # create volume map and store cross reference data
    mover_ticket = ticket.get('mover', {})
    drive = "%s:%s" % (mover_ticket.get('device', 'Unknown'),
                       mover_ticket.get('serial_num','Unknown'))
    try:
        p.set_xreference(ticket["fc"]["external_label"],
                         ticket["fc"]["location_cookie"],
                         ticket["fc"]["size"],
                         drive)
    except KeyboardInterrupt:
        exc, msg, tb = sys.exc_info()
        raise exc, msg, tb
    except:
        exc,msg,tb=sys.exc_info()
        Trace.log(e_errors.INFO, "Trouble with pnfs.set_xreference %s %s."
                  % (str(exc), str(msg)))
        ticket['status'] = (str(exc), str(msg))
        return

    try:
        # add the pnfs ids and filenames to the file clerk ticket and store it
        fc_ticket = ticket.copy() #Make a copy so "work" isn't overridden.
        fc_ticket["fc"]["pnfsid"] = p.id
        fc_ticket["fc"]["pnfsvid"] = p.volume_fileP.id
        fc_ticket["fc"]["pnfs_name0"] = p.pnfsFilename
        fc_ticket["fc"]["pnfs_mapname"] = p.mapfile
        fc_ticket["fc"]["drive"] = drive

        fcc = file_clerk_client.FileClient(client['csc'], ticket["fc"]["bfid"])
        fc_reply = fcc.set_pnfsid(fc_ticket)

        #if fc_reply['status'][0] != e_errors.OK:
        #    print_data_access_layer_format('', '', 0, fc_reply)
        #    quit()

        Trace.message(4, "PNFS SET")
        Trace.message(4, pprint.pformat(fc_reply))

        ticket['status'] = fc_reply['status']
    except KeyboardInterrupt:
        exc, msg, tb = sys.exc_info()
        raise exc, msg, tb
    except:
        exc,msg,tb=sys.exc_info()
        Trace.log(e_errors.INFO, "Unable to send info. to file clerk. %s %s."
                  % (str(exc), str(msg)))
        ticket['status'] = (str(exc), str(msg))

    # file size needs to be the LAST metadata to be recorded
    try:
        # set the file size
        p.set_file_size(ticket['file_size'])
    except KeyboardInterrupt:
        exc, msg, tb = sys.exc_info()
        raise exc, msg, tb
    except:
        exc, msg, tb = sys.exc_info()
	ticket['status'] = (str(exc), str(msg))
        
############################################################################
#Functions for writes.
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

    # create the wrapper subticket - copy all the user info 
    # into for starters
    wrapper = {}

    # store the pnfs information info into the wrapper
    for key in pinfo.keys():
        wrapper[key] = pinfo[key]

    # the user key takes precedence over the pnfs key
    for key in uinfo.keys():
        wrapper[key] = uinfo[key]

    wrapper["fullname"] = input_file
    wrapper["type"] = ff_wrapper
    #file permissions from PNFS are junk, replace them
    #with the real mode
    wrapper['mode']=os.stat(input_file)[stat.ST_MODE]
    wrapper["sanity_size"] = 65536
    wrapper["size_bytes"] = file_size
    wrapper["mtime"] = int(time.time())

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

def submit_write_request(work_ticket, client, encp_intf):

    if encp_intf.verbose > 1:
        print "Submitting %s write request.   time=%s" % \
              (work_ticket['outfile'], time.time())

    # send the work ticket to the library manager
    while work_ticket['retry'] <= encp_intf.max_retry:

        ##start of resubmit block
        Trace.trace(7,"write_to_hsm q'ing: %s"%(work_ticket,))

        ticket = submit_one_request(work_ticket, encp_intf.verbose)
        
        Trace.message(5, "LIBRARY MANAGER\n%s\n." % pprint.pformat(ticket))

        if encp_intf.verbose > 4:
            print "LIBRARY MANAGER"
            pprint.pprint(ticket)

        result_dict = handle_retries([work_ticket], work_ticket, ticket,
                                     None, encp_intf)
	if result_dict['status'][0] == e_errors.OK:
	    ticket['status'] = result_dict['status']
            return ticket
        elif result_dict['status'][0] == e_errors.RETRY or \
           e_errors.is_retriable(result_dict['status'][0]):
            continue
        else:
            ticket['status'] = result_dict['status']
            return ticket
	
    ticket['status'] = (e_errors.TOO_MANY_RETRIES, ticket['status'])
    return ticket

############################################################################


def write_hsm_file(listen_socket, work_ticket, client, tinfo, e):

    #Loop around in case the file transfer needs to be retried.
    while work_ticket.get('retry', 0) <= e.max_retry:
        
        encp_crc = 0 #In case there is a problem, make sure this exists.

        Trace.message(2, "Waiting for mover to call back.   elapsed=%s" % \
                      (time.time() - tinfo['encp_start_time'],))

        #Open the control and mover sockets.
        control_socket, data_path_socket, ticket = mover_handshake(
            listen_socket, [work_ticket], e.mover_timeout, e.max_retry,
            e.verbose)

        #Handle any possible errors occured so far.
        result_dict = handle_retries([work_ticket], work_ticket, ticket,
                                     None, e)

        if result_dict['status'][0] == e_errors.RETRY or \
           result_dict['status'][0] == e_errors.RESUBMITTING:
            continue
        elif not e_errors.is_retriable(result_dict['status'][0]):
            ticket = combine_dict(result_dict, ticket)
            return ticket

        #This should be redundant error check.
        if not control_socket or not data_path_socket:
            ticket['status'] = (e_errors.NET_ERROR, "No socket")
            return ticket #This file failed.

        Trace.message(2, "Mover called back.  elapsed=%s" %
                      (time.time() - tinfo['encp_start_time'],))
        
        Trace.message(5, "WORK TICKET:")
        Trace.message(5, pprint.pformat(ticket))

        #maybe this isn't a good idea...
        work_ticket = combine_dict(ticket, work_ticket)

        done_ticket = open_local_file(work_ticket['infile'], "write")

        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, None, e)
        
        if result_dict['status'][0] == e_errors.RETRY:
            close_descriptors(control_socket, data_path_socket)
            continue
        elif not e_errors.is_retriable(result_dict['status'][0]):
            close_descriptors(control_socket, data_path_socket)
            return combine_dict(result_dict, work_ticket)
        else:
            in_fd = done_ticket['fd']

        Trace.message(2, "Input file %s opened.   elapsed=%s" % 
                      (work_ticket['infile'],
                       time.time()-tinfo['encp_start_time']))


        #Stall starting the count until the first byte is ready for reading.
        read_fd, write_fd, exc_fd = select.select([], [data_path_socket],
                                                  [data_path_socket], 15 * 60)

        if not write_fd:
            status_ticket = {'status':(e_errors.UNKNOWN,
                                       "No data written to mover.")}
            result_dict = handle_retries([work_ticket], work_ticket,
                                         status_ticket, control_socket, e)
            
            close_descriptors(control_socket, data_path_socket, in_fd)
            
            if result_dict['status'][0] == e_errors.RETRY:
                continue
            elif not e_errors.is_retriable(result_dict['status'][0]):
                return combine_dict(result_dict, work_ticket)
            
        lap_time = time.time() #------------------------------------------Start

        done_ticket, encp_crc = transfer_file(in_fd, data_path_socket.fileno(),
                                              control_socket, work_ticket, e)

        tstring = '%s_elapsed_time' % work_ticket['unique_id']
        tinfo[tstring] = time.time() - lap_time #--------------------------End

        Trace.message(2, "Verifying %s transfer.  elapsed=%s" %
                      (work_ticket['outfile'],
                       time.time()-tinfo['encp_start_time']))

        close_descriptors(control_socket, data_path_socket, in_fd)

        #Verify that everything is ok on the mover side of the transfer.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, None, e)

        if result_dict['status'][0] == e_errors.RETRY:
            continue
        elif not e_errors.is_retriable(result_dict['status'][0]):
            return done_ticket

        Trace.message(2, "File %s transfered.  elapsed=%s" %
                      (done_ticket['outfile'],
                       time.time()-tinfo['encp_start_time']))

        Trace.message(5, "FINAL DIALOG")
        Trace.message(5, pprint.pformat(done_ticket))

        #This function writes errors/warnings to the log file and puts an
        # error status in the ticket.
        check_crc(done_ticket, e.chk_crc, encp_crc) #Check the CRC.

        #Verify that the file transfered in tacted.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, None, e)
        if result_dict['status'][0] == e_errors.RETRY:
            continue
        elif not e_errors.is_retriable(result_dict['status'][0]):
            return combine_dict(result_dict, work_ticket)

        #Set the UNIX file permissions.
        #Writes errors to log file.
        set_outfile_permissions(done_ticket)

        ###What kind of check should be done here?
        #This error should result in the file being left where it is, but it
        # is still considered a failed transfer (aka. exit code = 1 and
        # data access layer is still printed).
        if done_ticket.get('status', (e_errors.OK,None)) != (e_errors.OK,None):
            print_data_access_layer_format(done_ticket['infile'],
                                           done_ticket['outfile'],
                                           done_ticket['file_size'],
                                           done_ticket)
            
        #We know the file has hit some sort of media. When this occurs
        # create a file in pnfs namespace with information about transfer.
        set_pnfs_settings(done_ticket, client, e.verbose)

        #Verify that the pnfs info was set correctly.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, None, e)
        if result_dict['status'][0] == e_errors.RETRY:
            continue
        elif not e_errors.is_retriable(result_dict['status'][0]):
            return combine_dict(result_dict, work_ticket)
        

        #Remove the new file from the list of those to be deleted should
        # encp stop suddenly.  (ie. crash or control-C).
        delete_at_exit.unregister(done_ticket['outfile']) #localname

        Trace.message(2, "File status after verification: %s   elapsed=%s" %
                      (done_ticket['status'],
                       time.time()-tinfo['encp_start_time']))

        return done_ticket

    #If we get out of the while loop, then return error.
    msg = "Failed to write file %s." % work_ticket['outfile']
    done_ticket = {'status':(e_errors.TOO_MANY_RETRIES, msg)}
    return done_ticket

############################################################################

def write_to_hsm(e, client, tinfo):

    Trace.trace(6,"write_to_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" %
                (e.input, e.output, e.verbose, e.chk_crc,
                 tinfo['encp_start_time']))

    # initialize
    bytecount=None #Test moe only
    storage_info=None #DCache only
    unique_id = []

    ninput, file_size, inputlist, outputlist = file_check(e)

    #Snag the pnfs_information and verify that everything matches.
    status, info = pnfs_information(outputlist,write=1)
    if status[0] != e_errors.OK:
        print_data_access_layer_format('','',0,{'status':status})
        print_error(status[0], status[1])
        quit()
    junk,library,file_family,ff_wrapper,width,storage_group,pinfo,p=info

    Trace.message(5, "library=" + str(library))
    Trace.message(5, "file_family=" + str(file_family))
    Trace.message(5, "wrapper type=" + str(ff_wrapper))
    Trace.message(5, "width=" + str(width))
    Trace.message(5, "storage_group=" + str(storage_group))
    Trace.message(5, "pinfo=" + str(pinfo))
    
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

    #Set the max attempts that can be made on a transfer.
    max_attempts(library[0], e)
    
    # get a port to talk on and listen for connections
    Trace.trace(10,'write_to_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback(verbose=e.verbose)
    callback_addr = (host, port)
    listen_socket.listen(4)
    Trace.trace(10,'write_to_hsm got callback host=%s port=%s listen_socket=%s'
                % (host,port,listen_socket))

    Trace.message(4, "Waiting for mover(s) to call back on (%s, %s)." %
                  (host, port))

    #Get the information needed to contact the file clerk, volume clerk and
    # the library manager.
    fc_ticket, vc_ticket, lm_ticket = get_server_info(client)
    
    file_clerk_address = (fc_ticket["hostip"], fc_ticket["port"])
    volume_clerk_address = (vc_ticket["hostip"], vc_ticket["port"])

    Trace.message(4, "File clerk address: " + str(file_clerk_address))
    Trace.message(4, "Volume clerk address: " + str(volume_clerk_address))
    
    file_fam = None
    #ninput = len(input_files)
    #files_left = ninput
    bytes = 0L
    exit_status = 0 #Used to determine the final message text.

    # loop on all input files sequentially
    for i in range(0,ninput):
        lap_start = time.time() #------------------------------------Lap Start

        Trace.message(3, "FILES LEFT: %s" % str(ninput - i))

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

        Trace.message(5, "WORK_TICKET")
        Trace.message(5, pprint.pformat(work_ticket))

        #This will halt the program if everything isn't consistant.
        verify_write_request_consistancy([work_ticket])

        Trace.message(2, "Sending ticket to %s.library manager,  elapsed=%s" %
                      (library[i], time.time() - tinfo['encp_start_time']))

        #Send the request to write the file to the library manager.
        done_ticket = submit_write_request(work_ticket, client, e)

        Trace.message(5, "LM RESPONCE TICKET")
        Trace.message(5, pprint.pformat(done_ticket))

        Trace.message(2,
              "File queued: %s library: %s family: %s bytes: %d elapsed=%s" %
                      (inputlist[i], library[i], rq_file_family, file_size[i],
                       time.time() - tinfo['encp_start_time']))

        #handle_retries() is not required here since submit_write_request()
        # handles its own retrying when an error occurs.
        if done_ticket['status'][0] != e_errors.OK:
            exit_status = 1
            continue

        #Send (write) the file to the mover.
        done_ticket = write_hsm_file(listen_socket, work_ticket, client,
                                     tinfo, e)

        Trace.message(5, "DONE WRITTING TICKET")
        Trace.message(5, pprint.pformat(done_ticket))

        #handle_retries() is not required here since write_hsm_file()
        # handles its own retrying when an error occurs.
        if done_ticket['status'][0] != e_errors.OK:
            exit_status = 1
            continue

        bytes = bytes + done_ticket['file_size']

        if (i == 0 and rq_file_family == "ephemeral"):
            file_fam = string.split(done_ticket["vc"]["file_family"], ".")[0]
            Trace.message(1, "New file family %s has been created for "
                          "--ephemeral RQ" % (file_fam,))

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
    close_descriptors(listen_socket)

    #Finishing up with a few of these things.
    calc_ticket = calculate_final_statistics(bytes, ninput, exit_status, tinfo)

    #If applicable print new file family.
    if file_family[0] == "ephemeral":
        ff = string.split(done_ticket["vc"]["file_family"], ".")
        Trace.message(1, "New File Family Created: %s" % ff[0])

    Trace.message(5, "DONE TICKET")
    Trace.message(5, pprint.pformat(done_ticket))

    done_ticket = combine_dict(calc_ticket, done_ticket)

    return done_ticket

#######################################################################
#Support function for reads.
#######################################################################

#######################################################################

# same_cookie(c1, c2) -- to see if c1 and c2 are the same

def same_cookie(c1, c2):
    try: # just to be paranoid
        return string.split(c1, '_')[-1] == string.split(c2, '_')[-1]
    except:
        return 0


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

            #A serious bug was found in the file clerk.  It could give
            # two different files the same bfid.  This is a check to make
            # sure that the bfid points to the correct file.
            p = pnfs.Pnfs(request['wrapper']['pnfsFilename'])
            p.get_xreference()
            if p.volume == pnfs.UNKNOWN or p.location_cookie == pnfs.UNKNOWN \
               or p.size == pnfs.UNKNOWN:
                rest = {'infile':request['infile'],
                        'bfid':request['bfid'],
                        'pnfs_volume':p.volume,
                        'pnfs_location_cookie':p.location_cookie,
                        'pnfs_size':p.size,
                        'status':"Missing data in pnfs layer 4."}
                sys.stderr.write(rest['status'] + "  Continuing.\n")
                Trace.alarm(e_errors.ERROR, e_errors.UNKNOWN, rest)
            
            elif request['fc']['external_label'] != p.volume or \
               not same_cookie(request['fc']['location_cookie'], p.location_cookie) or \
               long(request['fc']['size']) != long(p.size):

                rest = {'infile':request['infile'],
                        'outfile':request['outfile'],
                        'bfid':request['bfid'],
                        'db_volume':request['fc']['external_label'],
                        'pnfs_volume':p.volume,
                        'db_location_cookie':request['fc']['location_cookie'],
                        'pnfs_location_cookie':p.location_cookie,
                        'db_size':long(request['fc']['size']),
                        'pnfs_size':long(p.size),
                        'status':"Probable database conflict with pnfs."}
                Trace.alarm(e_errors.ERROR, e_errors.CONFLICT, rest)
                request['status'] = (e_errors.CONFLICT, rest['status'])
                print_data_access_layer_format(request['infile'],
                                               request['outfile'],
                                               request['file_size'], request)
                quit() #Harsh, but necessary.


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
    vc_ticket = vcc.inquire_vol(fc_ticket['external_label'])

    if vc_ticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, vc_ticket)
        quit()

    Trace.trace(7,"read_from_hsm on volume=%s"%
                (fc_ticket['external_label'],))
    
    inhibit = vc_ticket['system_inhibit'][0]
    if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
        msg = "volume is marked " + inhibit
        vc_ticket['status'] = inhibit, msg
        #if inhibit==e_errors.NOACCESS:
        #    msg="Volume is marked NOACCESS"
        #else:
        #    msg="Volume is marked NOTALLOWED"
        #raise inhibit, msg
        
    inhibit = vc_ticket['user_inhibit'][0]
    if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
        msg = "volume is marked " + inhibit
        vc_ticket['status'] = inhibit, msg
        #if inhibit==e_errors.NOACCESS:
        #    msg="Volume is marked NOACCESS"
        #else:
        #    msg="Volume is marked NOTALLOWED"
        #raise inhibit, msg
        
    if fc_ticket["deleted"] == "yes":
        #raise (e_errors.DELETED, "File has been deleted")
        fc_ticket['status'] = (e_errors.DELETED, "File has been deleted")

    #Verify that the external labels named by the file clerk and volume
    # clerk are the same.
    if vc_ticket['external_label'] != fc_ticket['external_label']:
        msg = "External labels retrieved from file and volume clerks " \
              "are not the same.\n" \
              "From file clerk: %s\n" \
              "From volume clerk: %s\n" % \
              (fc_ticket['external_label'],
               vc_ticket['external_label'])
        #raise (e_errors.BROKEN, msg)
        fc_ticket['status'] = vc_ticket['status'] = (e_errors.BROKEN, msg)

    return fc_ticket, vc_ticket

############################################################################

def verify_file_size(ticket):
    if ticket['outfile'] == '/dev/null':
        filename = ticket['infile']
    else:
        filename = ticket['outfile']
    file_size = 0 #quick hack
    try:
        #Get the stat info to 
        statinfo = os.stat(filename)

        file_size = statinfo[stat.ST_SIZE]

        if file_size != ticket['file_size']:
            msg = "Expected file size (%s) not equal to actuall file size " \
                  "(%s) for file %s." % \
                  (ticket['file_size'], file_size, filename)

            ticket['file_size'] = file_size
            ticket['status'] = (e_errors.EPROTO, msg)

    except OSError, msg:
        Trace.log(e_errors.INFO, "Retrieving %s info. failed: %s" % \
                  (filename, msg))
        ticket['status'] = (e_errors.UNKNOWN, "Unable to verify file size.")

    return file_size #return used in read.

#######################################################################
#Functions for reads.
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
                error_ticket = {'fc':fc_reply, 'vc':vc_reply}
                error_ticket['status'] = fc_reply['status']
                print_data_access_layer_format(inputlist[i], outputlist[i],
                                              error_ticket['fc'].get('size',0),
                                               error_ticket)
                raise fc_reply['status'][0]

            elif vc_reply['status'][0] != e_errors.OK:
                error_ticket = {'fc':fc_reply, 'vc':vc_reply}
                error_ticket['status'] = vc_reply['status']
                print_data_access_layer_format(inputlist[i], outputlist[i],
                                              error_ticket['fc'].get('size',0),
                                               error_ticket)
                raise vc_reply['status'][0]

        except (e_errors.NOACCESS, e_errors.NOTALLOWED, e_errors.DELETED,
                e_errors.BROKEN):
            continue

        Trace.message(5, "FILE CLERK:")
        Trace.message(5, pprint.pformat(fc_reply))
        Trace.message(5, "VOLUME CLERK:")
        Trace.message(5, pprint.pformat(vc_reply))

        try:
            # comment this out not to confuse the users
            # if fc_reply.has_key("fc") or fc_reply.has_key("vc"):
            #    sys.stderr.write("Old file clerk format detected.\n")
            del fc_reply['fc'] #Speed up debugging by removing these.
            del fc_reply['vc']
        except:
            pass

        # make the part of the ticket that encp knows about.
        # (there's more later)
        encp_el = {}
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
        vf = vc_reply['volume_family']
        vc_reply['address'] = volume_clerk_address
        vc_reply['storage_group']=volume_family.extract_storage_group(vf)
        vc_reply['file_family'] = volume_family.extract_file_family(vf)
        vc_reply['wrapper'] = volume_family.extract_wrapper(vf)
        fc_reply['address'] = file_clerk_address

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
        #uinfo['fullname'] = "" # will be filled in later for each transfer
        
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

        # store the pnfs information info into the wrapper
        for key in pinfo[i].keys(): #request['pinfo'].keys():
            request['wrapper'][key] = pinfo[i][key] #request['pinfo'][key]

        # the user key takes precedence over the pnfs key
        for key in uinfo.keys():
            request['wrapper'][key] = uinfo[key]

        request['wrapper']['fullname'] = request['outfile']
        request['wrapper']['sanity_size'] = 65536
        request['wrapper']['size_bytes'] = request['file_size']

        requests_per_vol[label] = requests_per_vol.get(label,[]) + [request]
        nfiles = nfiles+1

    return requests_per_vol

#######################################################################

# submit read_from_hsm requests
def submit_read_requests(requests, client, tinfo, encp_intf):

    submitted = 0
    requests_to_submit = requests[:]

    # submit requests
    while requests_to_submit:
        for req in requests_to_submit:

            if encp_intf.verbose > 1:
                print "Submitting %s read request.   time=%s" % (req['infile'],
                                                                 time.time())

            Trace.trace(8,"submit_read_requests queueing:%s"%(req,))

            ticket = submit_one_request(req, encp_intf.verbose)

            Trace.message(5, "LIBRARY MANAGER\n%s\n." % pprint.pformat(ticket))
                         
            #if encp_intf.verbose > 4:
            #    print "LIBRARY MANAGER"
            #    pprint.pprint(ticket)

            result_dict = handle_retries(requests_to_submit, req, ticket,
                                         None, encp_intf)
            if result_dict['status'][0] == e_errors.RETRY or \
               not e_errors.is_retriable(result_dict['status'][0]):
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
# e - class instance of the interface.
#Rerturns:
# (requests, bytes) - requests returned only contains those requests that
#   did not succed.  bytes is the total running sum of bytes transfered
#   for this encp.

def read_hsm_files(listen_socket, submitted, request_list, tinfo, e):

    for rq in request_list: 
        Trace.trace(7,"read_hsm_files: %s"%(rq['infile'],))

    files_left = submitted
    bytes = 0L
    encp_crc = 0
    failed_requests = []
    succeded_requests = []

    #for waiting in range(submitted):
    while files_left:
        Trace.message(3, "FILES LEFT: %s" % files_left)
        Trace.message(1, "Waiting for mover to call back.  elapsed=%s" %
                      (time.time() - tinfo['encp_start_time'],))
            
        # listen for a mover - see if id corresponds to one of the tickets
        #   we submitted for the volume
        control_socket, data_path_socket, request_ticket = mover_handshake(
            listen_socket,
            request_list,
            e.mover_timeout,
            e.max_retry,
            e.verbose)

        done_ticket = request_ticket #Make sure this exists by this point.
        result_dict = handle_retries(request_list, request_ticket,
                                     request_ticket, None, e)
        if result_dict['status'][0]== e_errors.RETRY or \
           result_dict['status'][0]== e_errors.RESUBMITTING:
            continue
        elif result_dict['status'][0]== e_errors.TOO_MANY_RESUBMITS:
            for n in range(files_left):
                failed_requests.append(request_ticket)
            files_left = 0
            continue
        elif not e_errors.is_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)
            continue

        Trace.message(2, "Mover called back.  elapsed=%s" %
                      (time.time() - tinfo['encp_start_time'],))
        Trace.message(5, "REQUEST:")
        Trace.message(5, pprint.pformat(request_ticket))

        #Open the output file.
        done_ticket = open_local_file(request_ticket['outfile'], "read")

        result_dict = handle_retries(request_list, request_ticket,
                                     done_ticket, None, e)
        if not e_errors.is_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)

            close_descriptors(control_socket, data_path_socket)
            continue
        else:
            out_fd = done_ticket['fd']

        Trace.message(2, "Output file %s opened.  elapsed=%s" %
                  (request_ticket['outfile'],
                   time.time() - tinfo['encp_start_time']))

        #Stall starting the count until the first byte is ready for reading.
        read_fd, write_fd, exc_fd = select.select([data_path_socket], [],
                                                  [data_path_socket], 15 * 60)

        
        lap_start = time.time() #----------------------------------------Start

        done_ticket, encp_crc = transfer_file(data_path_socket.fileno(),out_fd,
                                              control_socket, request_ticket,e)

        lap_end = time.time()  #-----------------------------------------End
        tstring = "%s_elapsed_time" % request_ticket['unique_id']
        tinfo[tstring] = lap_end - lap_start


        Trace.message(2, "Verifying %s transfer.  elapsed=%s" %
                      (request_ticket['infile'],
                       time.time() - tinfo['encp_start_time']))

        close_descriptors(control_socket, data_path_socket, out_fd)

        #Verify that everything went ok with the transfer.
        result_dict = handle_retries(request_list, request_ticket,
                                     done_ticket, None, e)
        
        if result_dict['status'][0] == e_errors.RETRY:
            continue
        elif not e_errors.is_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)
            continue

        #For simplicity combine everything together.
        #done_ticket = combine_dict(result_dict, done_ticket)
        
        Trace.message(2, "File %s transfered.  elapsed=%s" %
                      (request_ticket['infile'],
                       time.time() - tinfo['encp_start_time']))
        Trace.message(5, "FINAL DIALOG")
        Trace.message(5, pprint.pformat(done_ticket))

        #These functions write errors/warnings to the log file and put an
        # error status in the ticket.
        bytes = bytes + verify_file_size(done_ticket) #Verify size is the same.
        check_crc(done_ticket, e.chk_crc, encp_crc) #Check the CRC.

        #Verfy that the file transfered in tacted.
        result_dict = handle_retries(request_list, request_ticket,
                                     done_ticket, None, e)
        if result_dict['status'][0] == e_errors.RETRY:
            continue
        elif not e_errors.is_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)
            continue

        #This function writes errors/warnings to the log file and puts an
        # error status in the ticket.
        set_outfile_permissions(done_ticket) #Writes errors to log file.
        ###What kind of check should be done here?
        #This error should result in the file being left where it is, but it
        # is still considered a failed transfer (aka. exit code = 1 and
        # data access layer is still printed).
        if done_ticket.get('status', (e_errors.OK,None)) != (e_errors.OK,None):
            print_data_access_layer_format(done_ticket['infile'],
                                           done_ticket['outfile'],
                                           done_ticket['file_size'],
                                           done_ticket)

        #Remove the new file from the list of those to be deleted should
        # encp stop suddenly.  (ie. crash or control-C).
        delete_at_exit.unregister(done_ticket['outfile']) #localname

        # remove file requests if transfer completed succesfuly.
        del(request_ticket)
        if files_left > 0:
            files_left = files_left - 1

        tstring = "%s_elapsed_finished" % done_ticket['unique_id']
        tinfo[tstring] = time.time() - lap_start #-------------------------End

        Trace.message(2, "File status after verification: %s   elapsed=%s" %
                      (done_ticket["status"],
                       time.time() - tinfo['encp_start_time']))
        
        # calculate some kind of rate - time from beginning to wait for
        # mover to respond until now. This doesn't include the overheads
        # before this, so it isn't a correct rate. I'm assuming that the
        # overheads I've neglected are small so the quoted rate is close
        # to the right one.  In any event, I calculate an overall rate at
        # the end of all transfers
        calculate_rate(done_ticket, tinfo, e.verbose)

        #With the transfer a success, we can now add the ticket to the list
        # of succeses.
        succeded_requests.append(done_ticket)

    Trace.message(5, "DONE TICKET")
    Trace.message(5, pprint.pformat(done_ticket))

    #Pull out the failed transfers that occured while trying to open the
    # control socket.
    unknown_failed_transfers = []
    for transfer in failed_requests:
        if len(transfer) < 3:
            unknown_failed_transfers.append(transfer)

    #Extract the unique ids for the two lists.
    succeded_ids = []
    failed_ids = []
    for req in succeded_requests:
        try:
            succeded_ids.append(req['unique_id'])
        except KeyError:
            sys.stderr.write("Error obtaining unique id list of successes.\n")
            sys.stderr.write(pprint.pformat(req))
    for req in failed_requests:
        try:
            failed_ids.append(req['unique_id'])
        except KeyError:
            sys.stderr.write("Error obtaining unique id list of failures.\n")
            sys.stderr.write(pprint.pformat(req))

    #For each transfer that failed without even succeding to open a control
    # socket, print out their data access layer.
    for transfer in request_list:
        if transfer['unique_id'] not in succeded_ids and \
           transfer['unique_id'] not in failed_ids:
            try:
                transfer = combine_dict(unknown_failed_transfers[0], transfer)
                del unknown_failed_transfers[0] #shorten this list.

                print_data_access_layer_format(transfer['infile'],
                                               transfer['outfile'],
                                               transfer['file_size'],
                                               transfer)
            except IndexError:
                msg = "Unable to print data access layer.\n"
                sys.stderr.write(msg)

    return failed_requests, bytes, done_ticket

#######################################################################

def read_from_hsm(e, client, tinfo):

    Trace.trace(6,"read_from_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" % (e.input, e.output, e.verbose,
                                      e.chk_crc, tinfo['encp_start_time']))

    #requests_per_vol = {}

    ninput, file_size, inputlist, outputlist = file_check(e)

    #Get the pnfs information.
    #Both status and info are tuples.
    status, info = pnfs_information(inputlist,write=0)
    if status[0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, {'status':status})
        print_error(status[0], status[1])
        quit()
    (bfid,junk,junk,junk,junk,junk,pinfo,p)= info

    Trace.message(5, "bfid=%s" % bfid)
    Trace.message(5, "pinfo=%s" % pinfo)
    Trace.message(5, "p=%s" % p)

    # get a port to talk on and listen for connections
    Trace.trace(10,'read_from_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback(verbose = e.verbose)
    client['callback_addr'] = (host, port)
    listen_socket.listen(4)

    Trace.message(4, "Waiting for mover(s) to call back on (%s, %s)." %
                  (host, port))

    #Contact the configuration server for the file clerk, volume clerk and
    # library manager addresses.
    fc_ticket, vc_ticket, lm_ticket = get_server_info(client)

    file_clerk_address = (fc_ticket["hostip"], fc_ticket["port"])
    volume_clerk_address = (vc_ticket["hostip"], vc_ticket["port"])

    Trace.message(4, "File clerk address: %s" % str(file_clerk_address))
    Trace.message(4, "Volume clerk address: %s" % str(volume_clerk_address))

    #Create all of the request dictionaries.
    requests_per_vol = create_read_requests(inputlist, outputlist, file_size,
                                            client, tinfo, e, bfid, pinfo,
                                            file_clerk_address,
                                            volume_clerk_address)

    if (len(requests_per_vol) == 0):
        quit()

    #This will halt the program if everything isn't consistant.
    verify_read_request_consistancy(requests_per_vol)
    
    #Set the max attempts that can be made on a transfer.
    check_lib = requests_per_vol.keys()    
    max_attempts(requests_per_vol[check_lib[0]][0]['vc']['library'], e)

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
        submitted = submit_read_requests(request_list, client, tinfo, e)

        Trace.message(3, "SUBMITED: %s" % submitted)
        Trace.message(5, pprint.pformat(request_list))

        Trace.message(2, "Files queued.   elapsed=%s" %
                      (time.time() - tinfo['encp_start_time']))

        if submitted != 0:
            #Since request_list contains all of the entires, submitted must
            # also be passed so read_hsm_files knows how many elements of
            # request_list are valid.
            requests_failed, brcvd, data_access_layer_ticket = read_hsm_files(
                listen_socket, submitted, request_list, tinfo, e)

            Trace.message(2, "Files read for volume %s   elapsed=%s" %
                          (vol, time.time() - tinfo['encp_start_time']))

            if len(requests_failed) > 0 or \
               data_access_layer_ticket['status'][0] != e_errors.OK:
                exit_status = 1 #Error, when quit() called, this is passed in.
                Trace.message(3, "TRANSFERS FAILED: %s" % len(requests_failed))
                Trace.message(5, pprint.pformat(requests_failed))

            #Sum up the total amount of bytes transfered.
            bytes = bytes + brcvd

        else:
            #If all submits fail (i.e using an old encp), this avoids crashing.
            data_access_layer_ticket = {}

    Trace.message(2, "Files read for all volumes.   elapsed=%s" %
                  (time.time() - tinfo['encp_start_time'],))

    # we are done transferring - close out the listen socket
    close_descriptors(listen_socket)

    #Finishing up with a few of these things.
    calc_ticket = calculate_final_statistics(bytes, ninput, exit_status, tinfo)

    done_ticket = combine_dict(calc_ticket, data_access_layer_ticket)

    Trace.message(5, "DONE TICKET")
    Trace.message(5, pprint.pformat(done_ticket))

    return done_ticket

##############################################################################
##############################################################################

class encp(interface.Interface):

    deprecated_options = ['--crc']
    
    def __init__(self):
        global pnfs_is_automounted
        self.chk_crc = 1           # we will check the crc unless told not to
        self.priority = 1          # lowest priority
        self.delpri = 0            # priority doesn't change
        self.admpri = -1           # quick fix to check HiPri functionality
        self.age_time = 0          # priority doesn't age
        self.data_access_layer = 0 # no special listings
        self.verbose = 0
        self.bufsize = 65536*4     #XXX CGW Investigate this
        self.delayed_dismount = None
        self.max_retry = None      # number of times to try again
        self.max_resubmit = None   # number of times to try again
        self.mover_timeout = 15*60 # seconds to wait for mover to call back,
                                   # before resubmitting req. to lib. mgr.
                                   # 15 minutes
        self.output_file_family = '' # initial set for use with --ephemeral or
                                     # or --file-family
                                     
        self.bytes = None

        self.dcache = 0 #Special options for operation with a disk cache layer.
        self.put_cache = self.get_cache = 0
        self.pnfs_mount_point = ""

        self.storage_info = None # Ditto
        self.test_mode = 0
        self.pnfs_is_automounted = 0

        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()
        pnfs_is_automounted = self.pnfs_is_automounted

    ##########################################################################
    # define the command line options that are valid
    def options(self):
        return self.config_options()+[
            "verbose=","no-crc","priority=","delpri=","age-time=",
            "delayed-dismount=", "file-family=", "ephemeral",
            "get-cache", "put-cache", "storage-info=", "pnfs-mount-point=",
            "data-access-layer", "max-retry=", "max-resubmit=",
            "pnfs-is-automounted"] + \
            self.help_options()

    
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

        if self.get_cache:
            pnfs_id = sys.argv[-2]
            local_file = sys.argv[-1]
            #remote_file=os.popen("enstore pnfs --path " + pnfs_id).readlines()
            p = pnfs.Pnfs(pnfs_id=pnfs_id, mount_point=self.pnfs_mount_point)
            p.get_path()
            remote_file = p.path
            #self.args[0:2] = [remote_file[0][:-1], local_file]
            self.args[0:2] = [remote_file, local_file]
        if self.put_cache:
            pnfs_id = sys.argv[-2]
            local_file = sys.argv[-1]
            #remote_file=os.popen("enstore pnfs --path " + pnfs_id).readlines()
            p = pnfs.Pnfs(pnfs_id=pnfs_id, mount_point=self.pnfs_mount_point)
            p.get_path()
            remote_file = p.path
            #self.args[0:2] = [local_file, remote_file[0][:-1]]
            self.args[0:2] = [local_file, remote_file]

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
    for x in xrange(1, e.verbose+1):
        Trace.do_message(x)

    #Dictionary with configuration server, clean udp and other info.
    client = clients(e.config_host, e.config_port)
    Trace.message(4, "csc=" + str(client['csc']))
    Trace.message(4, "logc=" + str(client['logc']))

    # convenient, but maybe not correct place, to hack in log message
    # that shows how encp was called
    Trace.log(e_errors.INFO,
                'encp version %s, command line: "%s"' %
                (encp_client_version(), string.join(sys.argv)))

    if e.data_access_layer:
        global data_access_layer_requested
        data_access_layer_requested = e.data_access_layer
        #data_access_layer_requested.set()

    #Special handling for use with dcache - not yet enabled
    if e.get_cache:
        #pnfs_id = sys.argv[-2]
        #local_file = sys.argv[-1]
        #print "pnfsid", pnfs_id
        #print "local file", local_file
        done_ticket = read_from_hsm(e, client, tinfo)

    #Special handling for use with dcache - not yet enabled
    elif e.put_cache:
        #pnfs_id = sys.argv[-2]
        #local_file = sys.argv[-1]
        done_ticket = write_to_hsm(e, client, tinfo)
        
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

    exit_status = done_ticket.get('exit_status', 1)    
    try:
        if e.data_access_layer and not exit_status:
            print_data_access_layer_format(e.input, e.output,
                                           done_ticket.get('file_size', 0),
                                           done_ticket)
        else:
            status = done_ticket.get('status', (e_errors.UNKNOWN,
                                                e_errors.UNKNOWN)[1])
            Trace.log(e_errors.INFO, string.replace(status[1], "\n\t", "  "))
            Trace.message(1, str(status[1]))

    except ValueError:
        exc, msg, tb = sys.exc_inf()
        sys.stderr.write("Error (main): %s: %s\n" % (str(exc), str(msg)))
        sys.stderr.write("Exit status: %s\n", exit_status)

    Trace.trace(10,"encp finished at %s"%(time.time(),))
    #Quit safely by Removing any zero length file for transfers that failed.
    quit(exit_status)

if __name__ == '__main__':

    for sig in range(1, signal.NSIG):
        if sig not in (signal.SIGTSTP, signal.SIGCONT,
                       signal.SIGCHLD, signal.SIGWINCH):
            try:
                signal.signal(sig, signal_handler)
            except:
                sys.stderr.write("Setting signal %s to %s failed.\n" %
                                 (sig, signal_handler))
    
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
        
        
