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

# enstore modules
import setpath 
import Trace
import pnfs
#import pnfs_hack #TEMPORARY
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


def signal_handler(sig, frame):
    try:
        sys.stderr.write("Caught signal %s, exiting\n" % (sig,))
        sys.stderr.flush()
    except:
        pass
    quit(1)

for sig in range(signal.NSIG):
    if sig not in (signal.SIGTSTP, signal.SIGCONT, signal.SIGCHLD, signal.SIGWINCH):
        try:
            signal.signal(sig, signal_handler)
        except:
            pass
    
def encp_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "x2_6_a  CVS $Revision$ "
    file = globals().get('__file__', "")
    if file: version_string = version_string + file
    return version_string

#seconds to wait for mover to call back, before resubmitting req. to lib. mgr.
mover_timeout = 15*60  #15 minutes

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
QUEUE_TIME=%.02f
TOTAL_TIME=%.02f
STATUS=%s\n"""

class Flag:
    def __init__(self):
        self.val=0
    def set(self):
        self.val=1
    def clear(self):
        self.val=0
    def test(self):
        return self.val
    def __nonzero__(self):
        return self.val
    
data_access_layer_requested=Flag()

def quit(exit_code=1):
    delete_at_exit.delete()
    #sys.stderr.write("Encp exiting with rc=%d\n"%exit_code)
    #sys.stderr.flush()
    os._exit(exit_code)

def print_error(errcode,errmsg):
    format = str(errcode)+" "+str(errmsg) + '\n'
    format = "ERROR: "+format
    sys.stderr.write(format)
    sys.stderr.flush()
    
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
    t0 = time_ticket.get('t0', now)
    total = now - t0
    sts =  ticket.get('status', ('Unknown', None))
    status = sts[0]
    msg = sts[1:]
    if len(msg)==1:
        msg=msg[0]
        
    if not data_access_layer_requested and status != e_errors.OK:
        out=sys.stderr
    else:
        out=sys.stdout
        
    out.write(data_access_layer_format % (inputfile, outputfile, filesize,
                                          external_label,location_cookie,
                                          device, device_sn,
                                          transfer_time, seek_time, mount_time, in_queue,
                                          total, status))
    out.write('\n')
    out.flush()
    if msg:
        msg=str(msg)
        sys.stderr.write(msg+'\n')
        sys.stderr.flush()
    
    try:
        format = "INFILE=%s OUTFILE=%s FILESIZE=%d LABEL=%s LOCATION=%s DRIVE=%s DRIVE_SN=%s TRANSFER_TIME=%.02f "+\
                 "SEEK_TIME=%.02f MOUNT_TIME=%.02f QUEUE_TIME=%.02f TOTAL_TIME=%.02f STATUS=%s"
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

client={}
  
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
        stati = csc.alive(configuration_client.MY_SERVER, rcv_timeout, alive_retries)
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
    global logc  #needs to be global so other functions in this module can use it.
    logc = log_client.LoggerClient(csc, 'ENCP', 'log_server')

    # convenient, but maybe not correct place, to hack in log message that shows how encp was called
    
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
    client['csc']=csc
    client['u']=u
    client['uinfo']=uinfo

##############################################################################

# generate the full path name to the file

def fullpath(filename):
    machine = hostaddr.gethostinfo()[0]
    if not filename:
        return None, None, None, None
    filename = os.path.expandvars(filename)
    filename = os.path.expanduser(filename)
    if filename[0]!='/':
        filename = os.path.join(os.getcwd(), filename)
    filename = os.path.normpath(filename)
    dirname, file = os.path.split(filename)
    filename = os.path.join(dirname,file)
    return machine, filename, dirname, file

##############################################################################

# check the input file list for consistency

def inputfile_check(input_files, bytecount=None):
    # create internal list of input unix files even if just 1 file passed in
    if type(input_files)==type([]):
        inputlist=input_files
    else:
        inputlist = [input_files]
    ninput = len(inputlist)

    if bytecount != None and ninput != 1:
        print_data_access_layer_format(inputlist[0],'',0,{'status':(
            'EPROTO',"Cannot specify --bytes with multiple files")})
        quit()

    # we need to know how big each input file is
    file_size = []

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(0,ninput):

        # get fully qualified name
        machine, fullname, dir, basename = fullpath(inputlist[i])
        inputlist[i] = os.path.join(dir,basename)

        # input files must exist
        if not os.access(inputlist[i], os.R_OK):
            print_data_access_layer_format(inputlist[i], '',0,{'status':(
                'USERERROR','Cannot read file %s'%(inputlist[i],))})
            quit()

        # get the file size
        statinfo = os.stat(inputlist[i])

        if bytecount != None:
            file_size.append(bytecount)
        else:
            file_size.append(statinfo[stat.ST_SIZE])

        # input files can't be directories
        if not stat.S_ISREG(statinfo[stat.ST_MODE]) :
            print_data_access_layer_format(inputlist[i],'',0,{'status':(
                'USERERROR',
                'Not a regular file %s'%(inputlist[i],))})
            quit()

    # we cannot allow 2 input files to be the same
    # this will cause the 2nd to just overwrite the 1st
    for i in range(0,ninput):
        for j in range(i+1,ninput):
            if inputlist[i] == inputlist[j]:
                print_data_access_layer_format(inputlist[j],'',0,{'status':(
                    'USERERROR',
                    'Duplicated entry %s'%(inputlist[i],))})
                quit()

    return (inputlist, file_size)


# check the output file list for consistency
# generate names based on input list if required

def outputfile_check(inputlist,output):
    # can only handle 1 input file copied to 1 output file
    # or multiple input files copied to 1 output directory
    # this is just the current policy - nothing fundamental about it
    if len(output)>1:
        print_data_access_layer_format('',output[0],0,{'status':(
            'USERERROR','Cannot have multiple output files')})
        quit()

    # if user specified multiple input files, then output must be a directory or /dev/nulll
    outputlist = []
    if len(inputlist)!=1:
        if not os.path.exists(output[0]):
            print_data_access_layer_format('',output[0],0,{'status':(
                'USERERROR','No such directory %s'%(output[0],))})
            quit()

        if output[0]!='/dev/null' and not os.path.isdir(output[0]):
            print_data_access_layer_format('',output[0],0, {'status':(
                'USERERROR','Not a directory %s'%(output[0],))})  
            quit()

    outputlist = []

    # Make sure we can open the files. If we can't, we bomb out to user
    # loop over all input files and generate full output file names
    for i in range(0,len(inputlist)):
        outputlist.append(output[0])

        if outputlist[i] == '/dev/null':
            continue
            
        # see if output file exists as user specified
        itexists = os.path.exists(outputlist[i]) 
        
        if not itexists:
            omachine, ofullname, odir, obasename = fullpath(outputlist[i])
            if not os.path.exists(odir):
                # directory doesn't exist - error
                print_data_access_layer_format(inputlist[i], outputlist[i],0,{'status': (
                    'USERERROR', "No such directory %s"%(odir,))})
                quit()

        else:
            # if output file exists, then it must be a directory
            if os.path.isdir(outputlist[i]):
                omachine, ofullname, odir, obasename = fullpath(outputlist[i])
                imachine, ifullname, idir, ibasename = fullpath(inputlist[i])
                # take care of missing filenames (just directory or .)
                if obasename=='.' or len(obasename)==0:
                    outputlist[i] = os.path.join(odir,ibasename)
                else:
                    outputlist[i] = os.path.join(ofullname,ibasename)
                omachine, ofullname, odir, obasename = fullpath(outputlist[i])
                # need to make sure generated filename doesn't exist
                if os.path.exists(outputlist[i]):
                    # generated filename already exists - error
                    print_data_access_layer_format(inputlist[i], outputlist[i], 0, {'status': (
                        'EEXIST', "File %s already exists"%(outputlist[i],))})
                    quit()
            # filename already exists - error
            else:
                print_data_access_layer_format(inputlist[i], outputlist[i], 0, {'status':(
                    'EEXIST', "File %s already exists"%(outputlist[i],))})
                quit()

        # need to check that directory is writable
        # since all files go to one output directory, one check is enough
        if i==0 and outputlist[0]!='/dev/null':
            if not os.access(odir,os.W_OK):
                print_data_access_layer_format("",odir,0,{'status':(
                    'USERERROR',"No write access to %s"%(odir,))})
                quit()

    # we cannot allow 2 output files to be the same
    # this will cause the 2nd to just overwrite the 1st
    # In principle, this is already taken care of in the inputfile_check, but
    # do it again just to make sure in case someone changes protocol
    for i in range(0,len(outputlist)):
        for j in range(i+1,len(outputlist)):
            if outputlist[i] == outputlist[j] and outputlist[i]!='/dev/null':
                print_data_access_layer_format('',outputlist[j],0,{'status':(
                    'USERERROR',"Duplicated entry %s"%(outputlist[j],))})
                quit()

    #now try to atomically create each file
    for f in outputlist:
        if f=='/dev/null':
            continue
        err = 0
        try:
            fd = atomic.open(f, mode=0666)
            if fd<0:
                err = 1
            else:
                os.close(fd)
        except:
            err = 1
        if err:
            if os.path.exists(f):
                print_data_access_layer_format('', f, 0, {'status':(
                    'EEXIST', "File %s already exists"%(f,))})
            else:
                print_data_access_layer_format("",f,0,{'status':(
                    'USERERROR',"No write access to %s"%(f,))})
            quit()
    return outputlist

#######################################################################
# return pnfs information,
# and an open pnfs object so you can check if the system is enabled.

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
    
def write_to_hsm(input_files, output, output_file_family='',
                 verbose=0, chk_crc=1,
                 pri=1, delpri=0, agetime=0, delayed_dismount=0,
                 t0=0,
                 bytecount=None, #Test mode only
                 storage_info=None, #DCache only
                 dcache=0,
                 ):
    if t0==0:
        t0 = time.time()

    Trace.trace(6,"write_to_hsm input_files=%s output=%s  verbose=%s  chk_crc=%s t0=%s" %
                (input_files,output,verbose,chk_crc, t0))

    tinfo = {}
    tinfo["abs_start"] = t0

    t1 = time.time() #-------------------------------------------Start

    # initialize - and get config, udp and log clients
    maxretry = 3
    unique_id = []


    # create the wrapper subticket - copy all the user info 
    # into for starters
    wrapper = {}
    uinfo=client['uinfo']
    csc=client['csc']
    u=client['u']
    for key in uinfo.keys():
        wrapper[key] = uinfo[key]

    # make the part of the ticket that encp knows about (there's 
    # more later)
    encp = {}

    ## quick fix to check HiPri functionality
    admpri = -1
    #if pri < 0:
    #    pri = -pri
    #    admpri = pri
    encp["basepri"] = pri
    encp["adminpri"] = admpri
    encp["delpri"] = delpri
    encp["agetime"] = agetime

    pid = os.getpid()
    thishost = hostaddr.gethostinfo()[0]
    # create the time subticket
    times = {}
    times["t0"] = tinfo["abs_start"]

    if verbose>3:
        print "csc=",csc
        print "u=",u
        print "logc=",logc
        print "uinfo=",uinfo

    if verbose>2:
        print "Checking input unix files:",input_files, "   elapsed=", time.time()-t0
    t1 =  time.time() #------------------------------------------Start

    # check the input unix files. if files don't exits, 
    # we bomb out to the user
    if dcache: #XXX
        input_files = [input_files]
        
    inputlist, file_size = inputfile_check(input_files, bytecount)
    if (len(inputlist)>1) and (delayed_dismount == 0):
        delayed_dismount = 1
    #else:
        #delayed_dismount = 0

    tinfo["filecheck"] = time.time() - t1 #-------------------------End
    if verbose>2:
        print "  dt:",tinfo["filecheck"], "   elapsed=",time.time()-t0
    if verbose>3:
        print "inputlist=",inputlist
        print "file_size=",file_size
        print "delayed_dismount=",delayed_dismount

    if verbose>2:
        print "Checking output pnfs files:",output, "   elapsed=", time.time()-t0
    t1 = time.time() #--------------------------------------------Start

#    if dcache:
#        output = [pnfs_hack.filename_from_id(output)]
        
    # check (and generate) the output pnfs files(s) names
    # bomb out if they exist already
    outputlist = outputfile_check(inputlist,output)
    status, info = pnfs_information(outputlist,write=1)
    if status[0] != e_errors.OK:
        print_data_access_layer_format('','',0,{'status':status})
        print_error(status[0], status[1])
        quit()
        
    junk,library,file_family,ff_wrapper,width,storage_group,pinfo,p=info
    

    if output_file_family != "":
        for i in range(0,len(outputlist)):
            file_family[i] = output_file_family
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
            print_data_access_layer_format('','',0,{'status':(e_errors.USERERROR, msg)})
            quit()


    tinfo["pnfscheck"] = time.time() - t1 #------------------------End
    if verbose>2:
        print "  dt:",tinfo["pnfscheck"], "   elapsed=",time.time()-t0
    if verbose>3:
        print "outputlist=",outputlist
        print "library=",library
        print "file_family=",file_family
        print "wrapper type=",ff_wrapper
        print "width=",width
        print "storage_group",storage_group
        print "pinfo=",pinfo

    t1 = time.time() #-------------------------------------------Start
    if verbose>1:
        print "Requesting callback ports", "   elapsed=",time.time()-t0

    # get a port to talk on and listen for connections
    Trace.trace(10,'write_to_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback(verbose=verbose)
    callback_addr = (host, port)
    listen_socket.listen(4)
    Trace.trace(10,'write_to_hsm got callback host=%s port=%s listen_socket=%s'%
                (host,port,listen_socket))

    tinfo["get_callback"] = time.time() - t1 #----------------------End
    if verbose>1:
        print " ",host,port
        print "  dt:",tinfo["get_callback"], "   elapsed=",time.time()-t0

    if verbose>1:
        print "Calling Config Server to find file clerk  elapsed=",time.time()-t0
    t1 = time.time() #--------------------------------------------Start

    # ask configuration server what port the file clerk is using
    Trace.trace(10,"write_to_hsm calling config server to find "\
                "file clerk")
    fticket = csc.get("file_clerk")
    if fticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, fticket)
        quit()

    file_clerk_address = (fticket["hostip"],fticket["port"])
    Trace.trace(10,"write_to_hsm file clerk at host=%s port=%s"%
                (fticket["hostip"],fticket["port"]))

    tinfo["get_fileclerk"] = time.time() - t1 #---------------------End
    if verbose>1:
        print " ",fticket["hostip"],fticket["port"]
        print "  dt:", tinfo["get_fileclerk"], "   elapsed=",time.time()-t0

    # ask configuration server what port the volume clerk is using
    Trace.trace(10,"write_to_hsm calling config server to find volume clerk")
    vcticket = csc.get("volume_clerk")
    if vcticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, vcticket)
        quit()
        
    volume_clerk_address = (vcticket["hostip"],vcticket["port"])
    Trace.trace(10,"write_to_hsm volume clerk at host=%s port=%s"%
                (vcticket["hostip"],vcticket["port"]))

    tinfo["get_volumeclerk"] = time.time() - t1 #---------------------End
    if verbose>1:
        print " ",vcticket["hostip"],vcticket["port"]
        print "  dt:", tinfo["get_volumeclerk"], "   elapsed=",time.time()-t0

    if verbose>1:
        print "Calling Config Server to find %s.library_manager  elapsed=%s"%(library[0],time.time()-t0)


    t1 = time.time() #-------------------------------------------Start


    # ask configuration server what port library manager is using
    # note again:libraries have are identical since there is 
    # 1 output directory
    lib_mgr = library[0]+'.library_manager'
    Trace.trace(10,"write_to_hsm calling config server to find %s"%(lib_mgr,))
    ### XXX rename this to something sane
    vticket = csc.get(lib_mgr)
    if vticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, vticket)
        quit()
        
    Trace.trace(10,"write_to_hsm %s at host=%s port=%s"%
                (lib_mgr,vticket['hostip'],vticket['port']))


    tinfo["get_libman"] = time.time() - t1 #-----------------------End
    if verbose>1:
        print "  ",vticket["hostip"],vticket["port"]
        print "  dt:",tinfo["get_libman"], "   elapsed=",time.time()-t0

    file_fam = None
    # loop on all input files sequentially
    ninput = len(input_files)
    for i in range(0,ninput):
        unique_id.append(0) # will be set later when submitted

        # delete old tickets in case of a retry
        work_ticket=None

        # allow some retries if mover fails
        retry = maxretry
        while retry>0:  # note that real rates are not correct in retries
            if verbose:
                print "Sending ticket to %s.library manager,  elapsed=%s"%(library[i],time.time()-t0)

            t1 = time.time() #-------------------------------Lap Start

            # store timing info for each transfer in pnfs, not for all
            tinfo1 = tinfo.copy()  ## was deepcopy

            #unique_id[i] = time.time()  # note that this is down to mS
            unique_id[i] = "%s-%f-%d" % (thishost, time.time(), pid)
            wrapper["fullname"] = inputlist[i]
            wrapper["type"] = ff_wrapper[i]
            # store the pnfs information info into the wrapper
            for key in pinfo[i].keys():
                if not uinfo.has_key(key) : 
                    # the user key takes #precedence over the 
                    # pnfs key
                    wrapper[key] = pinfo[i][key]
            #file permissions from PNFS are junk, replace them
            #with the real mode
            wrapper['mode']=os.stat(inputlist[i])[stat.ST_MODE]
            # if old ticket exists, that means we are retrying
            #    then just bump priority and change unique id
            if work_ticket is not None:
                oldpri = work_ticket["encp"]["basepri"] 
                work_ticket["encp"]["basepri"] = oldpri + 4
                unique_id[i] = "%s-%f-%d" % (thishost, time.time(), pid)
                work_ticket["unique_id"] = unique_id[i]
                work_ticket["retry"] = retry
            else:
                if file_fam: rq_file_family = file_fam
                else: rq_file_family = file_family[i]
                volume_clerk = {"library"            : library[i],
                                "file_family"        : rq_file_family,
                                # technically width does not belong here, but it associated with the volume
                                "file_family_width"  : width[i],
                                "wrapper"            : ff_wrapper[i],
                                "storage_group"      : storage_group[i],
                                "address"            : volume_clerk_address}
                file_clerk = {"address": file_clerk_address}

                wrapper["sanity_size"] = 65536
                wrapper["size_bytes"] = file_size[i]
                wrapper["mtime"] = int(time.time())
                encp["delayed_dismount"] = delayed_dismount
                work_ticket = {"work"               : "write_to_hsm",
                               "callback_addr"      : callback_addr,
                               "fc"                 : file_clerk,
                               "vc"                 : volume_clerk,
                               "wrapper"            : wrapper,
                               "encp"               : encp,
                               "retry"              : retry,
                               "times"              : times,
                               "unique_id"          : unique_id[i]
                               }
            # send the work ticket to the library manager
            tinfo1["tot_to_send_ticket%d"%(i,)] = t1 - t0
            
            reply_read=0
            while not reply_read:
                ##start of resubmit block
                Trace.trace(7,"write_to_hsm q'ing: %s"%(work_ticket,))
                ticket = u.send(work_ticket, (vticket['hostip'], vticket['port']))
                if verbose > 3:
                    print "ENCP: write_to_hsm LM returned"
                    pprint.pprint(ticket)
                if ticket['status'][0] != e_errors.OK :
                    print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], ticket)
                    quit()

                tinfo1["send_ticket%d"%(i,)] = time.time() - t1 #--Lap End
                if verbose:
                    print "  queued:",inputlist[i], library[i],\
                          "family:",rq_file_family,\
                          "bytes:", file_size[i],\
                          "dt:",tinfo1["send_ticket%d"%(i,)],\
                              "   elapsed=",time.time()-t0

                if verbose>1:
                    print "Waiting for mover to call back  elapsed=",time.time()-t0
                t1 = time.time() #--------------------------------Lap-Start
                tMBstart = t1

                # We have placed our work in the system and now we 
                # have to wait for resources. All we need to do 
                # is wait for the system to call us back, and make 
                # sure that is it calling _us_ back, and not some 
                # sort of old call-back to this very same port. 
                # It is dicey to time out, as it is probably 
                # legitimate to wait for hours....
                
                ##19990723:  resubmit request after 15minute timeout.  Since the unique_id is unchanged
                ##the library manager should not get confused by duplicate requests.
                timedout=0
                while not (timedout or reply_read):
                    Trace.trace(10,"write_to_hsm listening for callback")
                    read_fds,write_fds,exc_fds=select.select([listen_socket],[],[],
                                                             mover_timeout)

                    if not read_fds:
                        #timed out
                        if verbose:
                            print "write_to_hsm: timeout on mover callback"
                        Trace.log(e_errors.INFO, "mover callback timed out, resubmitting request")
                        timedout=1
                        break
                    control_socket, address = listen_socket.accept()
                    #XXX check hostaddr.allow(address)
                    ticket = callback.read_tcp_obj(control_socket)
                    if verbose > 3:
                        print "ENCP:write_to_hsm MV called back with"
                        pprint.pprint(ticket)
                    callback_id = ticket['unique_id']
                    # compare strings not floats (floats fail comparisons)
                    if str(unique_id[i])==str(callback_id):
                        Trace.trace(10,"write_to_hsm mover called back on control_socket=%s, address=%s"
                                    %(control_socket, address))
                        reply_read=1
                        break
                    else:
                        Trace.log(e_errors.INFO,
                                  "write_to_hsm mover impostor called\
                                  control_socket=%s address=%s, got id %s expected %s\nticket=%s" %
                                  (control_socket,address,
                                   callback_id, unique_id[i],ticket))
                        reply_read=0
                        control_socket.close()
                if timedout:
                    msg = "Timeout on mover callback, resubmitting request"
                    if verbose:
                        print msg
                    Trace.log(e_errors.INFO, msg)


            #END of timeout/resubmit loop
            # ok, we've been called back with a matched id - how's 
            # the status?
            if ticket["status"][0] != e_errors.OK :
                print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], ticket)
                quit()


            tinfo1["tot_to_mover_callback%d"%(i,)] = time.time() - t0 
            dt = time.time() - t1 #-----------------------------Lap-End
            if verbose>1:

                print " ",ticket["mover"]["callback_addr"],\
                      "cum:",tinfo1["tot_to_mover_callback%d"%(i,)]
                print "  dt:",dt,"   elapsed=",time.time()-t0

            if verbose:
                print "Sending data for file ", outputlist[i], "   elapsed=",time.time()-t0
            t1 = time.time() #-------------------------------Lap-Start

            fsize = file_size[i]

            mover_addr = ticket['mover']['callback_addr']
            #Set up any network load-balancing voodoo
            interface=check_load_balance(mode=1, dest=mover_addr[0])
            # Call back mover on mover's port and send file on that port
            data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
            except socket.error, msg:
                Trace.log(e_errors.ERROR, "connect: %s %s" % (mover_addr, msg))

                print_error('EPROTO',  "failed to transfer: socket error %s" %(msg,))

                retry = retry - 1
                if retry>0:
                    sys.stderr.write("Retrying\n")
                    control_socket.close()
                    continue
                else:
                    quit()

            in_file = open(inputlist[i], "r")
            mycrc = 0
            bufsize = 65536*4 #XXX CGW Investigate this

            Trace.trace(7,"write_to_hsm: sending data to EXfer file=%s, socket=%s bufsize=%s chk_crc=%s"
                        %(inputlist[i],data_path_socket,bufsize,chk_crc))

            statinfo = os.stat(inputlist[i])

            if not bytecount and statinfo[stat.ST_SIZE] != fsize:
                print_data_access_layer_format(
                    inputlist[i],'',fsize,{'status':('EPROTO','size changed')})
                quit()

            try:
                if chk_crc:
                    crc_flag = 1
                else:
                    crc_flag = 0
                mycrc = EXfer.fd_xfer( in_file.fileno(),
                                       data_path_socket.fileno(), 
                                       fsize, bufsize, crc_flag, 0)
            except EXfer.error, msg:
                Trace.trace(6,"write_to_hsm EXfer error: %s %s"%
                            (sys.argv, msg))

                # might as well close our end --- we will either exit or
                # loop back around
                data_path_socket.close()
                in_file.close()

                #if str(err_msg) =="(32, 'fd_xfer - write - Broken pipe')":
                if msg.args[1] == errno.EPIPE:
                    # could be network or could be mover closing socket...
                    # try to get done_ticket
                    try:
                        done_ticket = callback.read_tcp_obj(control_socket)
                    except:
                        # assume network error...
                        # no done_ticket!
                        #print_data_access_layer_format(inputlist[i], outputlist[i],
                        #                               file_size[i], done_ticket)
                        # exit here
                        print_data_access_layer_format(inputlist[i],outputlist[i],0,
                                                       {'status':(
                            'EPROTO', 'Network problem or mover reset')}) ##XXX RENAME
                        ## disconnected
                        quit()

                    control_socket.close()

                    print_data_access_layer_format( inputlist[i], outputlist[i], file_size[i], done_ticket )
                    if not e_errors.is_retriable(done_ticket["status"][0]):
                        # exit here
                        quit()
                    print_error('EPROTO', "failed to transfer: status=%s"%(ticket['status'],))
                    retry = retry - 1
                    if retry>0:
                        sys.stderr.write("Retrying\n")
                        continue
                    else:
                        quit()

                else:
                    #some other error that needs coding
                    traceback.print_exc()
                    exc,msg,tb=sys.exc_info()
                    raise exc,msg


                # close the data socket and the file, we've sent it 
                #to the mover
                data_path_socket.close()
                in_file.close()

                tinfo1["sent_bytes%d"%(i,)] = time.time()-t1 #-----Lap-End
                if verbose>1:
                    if tinfo1["sent_bytes%d"%(i,)]!=0:
                        wtrate = 1.*fsize/1024./1024./tinfo1["sent_bytes%d"%(i,)]
                    else:
                        wdrate = 0.0
                        print "  bytes:",fsize, " Socket Write Rate = ", wtrate," MB/S"
                        print "  dt:",tinfo1["sent_bytes%d"%(i,)]," elapsed=",time.time()-t0
                        pass

                    pass

                pass

            if verbose>1:
                print "Waiting for final mover dialog  elapsed=",time.time()-t0
                t1 = time.time() #----------------------------Lap-Start

            # File has been sent - wait for final dialog with mover. 
            # We know the file has hit some sort of media.... 
            # when this occurs. Create a file in pnfs namespace with
            #information about transfer.
            Trace.trace(10,"write_to_hsm waiting for final mover dialog on %s"%(control_socket,))
            try:
                done_ticket = callback.read_tcp_obj(control_socket)

            except:
                exc, msg, tb = sys.exc_info()
                Trace.log(e_errors.ERROR, "waiting for final mover dialog: %s %s" % (exc, msg))
                try:
                    control_socket.close()
                except:
                    pass
                retry = retry - 1
                if retry:
                    sys.stderr.write("Retrying\n")
                    continue
                else:
                    quit()

            control_socket.close()
            Trace.trace(10,"write_to_hsm final dialog recieved")

            # make sure mover thinks transfer went ok
            if done_ticket["status"][0] != e_errors.OK :
                print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], done_ticket)
                # exit here
                if not e_errors.is_retriable(done_ticket["status"][0]):
                    quit()

                print_error('EPROTO', ' failed to transfer: status=%s'%(done_ticket['status'],))
                retry = retry - 1
                if retry:
                    sys.stderr.write("Retrying\n")
                    continue
                else:
                    quit()

            # Check the CRC
            if chk_crc:
                mover_crc = done_ticket["fc"]["complete_crc"]
                if mover_crc is None:
                    msg =   "warning: mover did not return CRC; skipping CRC check"
                    Trace.alarm(e_errors.WARNING, msg, {
                        'infile':inputlist[i],
                        'outfile':outputlist[i]})
                    sys.stderr.write(msg+'\n')
                    
                elif mover_crc != mycrc :
                    msg = "CRC mismatch"
                    sys.stderr.write(msg+'\n')
                    Trace.alarm(e_errors.WARNING, e_errors.CRC_ERROR, {
                        'infile':inputlist[i],
                        'outfile':outputlist[i]})

                    done_ticket['status']=('EPROTO', "CRC mismatch")
                    print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], done_ticket)
                    quit()


            tinfo1["final_dialog"] = time.time()-t1 #----------Lap End
            if verbose>1:
                print "  dt:",tinfo1["final_dialog"], "   elapsed=",time.time()-t0

            if verbose>1:
                print "Adding file to pnfs", "   elapsed=",time.time()-t0
            t1 = time.time() #-------------------------------Lap Start

            # create a new pnfs object pointing to current output file
            Trace.trace(10,"write_to_hsm adding to pnfs "+
                        outputlist[i])
            p=pnfs.Pnfs(outputlist[i])
            # save the bfid and set the file size
            p.set_bit_file_id(done_ticket["fc"]["bfid"],file_size[i])
            delete_at_exit.unregister(outputlist[i])
            try:
                perms = os.stat(inputlist[i])[stat.ST_MODE]
                os.chmod(outputlist[i], perms)
            except:
                exc, msg, tb = sys.exc_info()
                Trace.log(e_errors.INFO, "chmod %s failed: %s %s" % (outputlist[i], exc, msg))
            
            # create volume map and store cross reference data
            mover_ticket = done_ticket.get('mover', {})
            drive = "%s:%s"%(mover_ticket.get('device', 'Unknown'),mover_ticket.get('serial_num','Unknown'))
            try:
                p.set_xreference(done_ticket["fc"]["external_label"],
                                 done_ticket["fc"]["location_cookie"],
                                 done_ticket["fc"]["size"],
                                 drive)
            except:
                exc,msg,tb=sys.exc_info()
                Trace.log(e_errors.INFO, "Trouble with pnfs.set_xreference %s %s, continuing" %(exc,msg))
            # add the pnfs ids and filenames to the file clerk ticket and store it
            done_ticket["fc"]["pnfsid"] = p.id
            done_ticket["fc"]["pnfsvid"] = p.volume_fileP.id
            done_ticket["fc"]["pnfs_name0"] = p.pnfsFilename
            done_ticket["fc"]["pnfs_mapname"] = p.mapfile
            done_ticket["fc"]["drive"] = drive
            done_ticket["work"] = "set_pnfsid"
            fc_reply = u.send(done_ticket, (fticket['hostip'], 
                                             fticket['port']))
            if verbose > 3:
                print "ENCP: write_to_hsm FC returned"
                pprint.pprint(fc_reply)
            if done_ticket['status'][0] != e_errors.OK :
                print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], done_ticket)
                quit()

                
            # store debugging info about transfer
            done_ticket["tinfo"] = tinfo1 # store as much as we can into pnfs
            done_formatted  = pprint.pformat(done_ticket)
            p.set_info(done_formatted)
            Trace.trace(10,"write_to_hsm done adding to pnfs")

            tinfo1["pnfsupdate%d"%(i,)] = time.time() - t1 #--Lap End
            if verbose>1:
                print "  dt:",tinfo1["pnfsupdate%d"%(i,)], "elapsed=",time.time()-t0


            # calculate some kind of rate - time from beginning 
            # to wait for mover to respond until now. This doesn't 
            # include the overheads before this, so it isn't a 
            # correct rate. I'm assuming that the overheads I've 
            # neglected are small so the quoted rate is close
            # to the right one.  In any event, I calculate an 
            # overall rate at the end of all transfers
            tnow = time.time()
            if (tnow-tMBstart)!=0:
                tinfo1['rate%d'%(i,)] = 1.*fsize/1024./1024./(tnow-tMBstart)
            else:
                tinfo1['rate%d'%(i,)] = 0.0
            if done_ticket["times"]["transfer_time"]!=0:
                tinfo1['transrate%d'%(i,)] = \
                                     1.*fsize/1024./1024./done_ticket["times"]["transfer_time"]
            else:
                tinfo1['rate%d'%(i,)] = 0.0
            format = "  %s %s -> %s: %d bytes copied to %s at %.3g MB/S (%.3g MB/S) mover=%s drive_id=%s drive_sn=%s drive_vendor=%s elapsed= %.02f"

            if verbose:
                print format %\
                      ("write_to_hsm",
                       inputlist[i], outputlist[i], fsize,
                       done_ticket["fc"]["external_label"],
                       tinfo1["rate%d"%(i,)],
                       tinfo1["transrate%d"%(i,)],
                       done_ticket["mover"]["name"],
                       done_ticket["mover"]["product_id"],
                       done_ticket["mover"]["serial_num"],
                       done_ticket["mover"]["vendor_id"],
                       time.time()-t0)
            if data_access_layer_requested:
                print_data_access_layer_format(  inputlist[i],
                                                 outputlist[i],
                                                 fsize,
                                                 done_ticket)

            Trace.log(e_errors.INFO, format%("write_to_hsm",
                                             inputlist[i], outputlist[i],
                                             fsize,
                                             done_ticket["fc"]["external_label"],
                                             tinfo1["rate%d"%(i,)], 
                                             tinfo1["transrate%d"%(i,)],
                                             done_ticket["mover"]["name"],
                                             done_ticket["mover"]["product_id"],
                                             done_ticket["mover"]["serial_num"],
                                             done_ticket["mover"]["vendor_id"],
                                             time.time()-t0),
                      Trace.MSG_ENCP_XFER )
            retry = 0
            if (i == 0 and rq_file_family == "ephemeral"):
                file_fam = string.split(done_ticket["vc"]["file_family"], ".")[0]
                if verbose:
                    print "New file family %s has been created for --ephemeral RQ"%(file_fam,)
                

    # we are done transferring - close out the listen socket
    listen_socket.close()

    # Calculate an overall rate: all bytes, all time
    tf=tinfo1["total"] = time.time()-t0
    done_ticket["tinfo"] = tinfo1
    total_bytes = 0
    for i in range(0,ninput):
        total_bytes = total_bytes+file_size[i]
    tf = time.time()
    if tf!=t0:
        done_ticket["MB_per_S"] = 1.*total_bytes/1024./1024./(tf-t0)
    else:
        done_ticket["MB_per_S"] = 0.0

    msg ="Complete: %s bytes in %s files in %s sec.  Overall rate = %s MB/sec" % (
        total_bytes,ninput,tf-t0,done_ticket["MB_per_S"])

    if verbose:
        if file_family[0] == "ephemeral":
            ff = string.split(done_ticket["vc"]["file_family"], ".")
            print "New File Family Created:", ff[0]

    if verbose:
        print msg

    # tell library manager we are done - this allows it to delete 
    # our unique id in
    # its dictionary - this keeps things cleaner and stops memory 
    # from growing
    # u.send_no_wait({"work":"done_cleanup"}, (vticket['hostip'], 
    # vticket['port']))

    if verbose > 3:
        print "DONE TICKET"
        pprint.pprint(done_ticket)

    Trace.trace(6,"write_to_hsm "+msg)

##############################################################################
# A call back for sort, highest file location should be first.
def compare_location(t1,t2):
    if t1["work_ticket"]["fc"]["external_label"] == t2["work_ticket"]["fc"]["external_label"]:
        if t1["work_ticket"]["fc"]["location_cookie"] > t2["work_ticket"]["fc"]["location_cookie"]:
            return 1
        if t1["work_ticket"]["fc"]["location_cookie"] < t2["work_ticket"]["fc"]["location_cookie"]:
            return -1
    return -1
  

    
#######################################################################
# submit read_from_hsm requests
def submit_read_requests(requests, client, tinfo, vols, verbose, retry_flag):

  t2 = time.time() #--------------------------------------------Lap-Start
  rq_list = []
  ninput = len(requests)
  for rq in requests: 
      msg="submit_read_requests: %s t2=%s"%(rq['infile'],t2)
      if verbose>1:
          print msg
  Qd=""
  current_library = ''
  submitted = 0
  
  for vol in vols:
    # create the time subticket

    times = {}
    times["t0"] = tinfo["abs_start"]
    pid = os.getpid()
    thishost = hostaddr.gethostinfo()[0]

    for i in range(0,ninput):
        if requests[i]['volume']==vol:
            if not retry_flag:
                id = "%s-%f-%d" % (thishost, time.time(), pid)

                requests[i]['unique_id'] = id  # note that this is down to mS
            
            requests[i]['wrapper']['fullname'] = requests[i]['outfile']
            requests[i]['wrapper']["sanity_size"] = 65536
            requests[i]['wrapper']["size_bytes"] = requests[i]['file_size']

            ##XXX CGW: how does the uinfo value get into the dictionary here?  This looks like a bug.
            # store the pnfs information info into the wrapper
            for key in requests[i]['pinfo'].keys():
                if not client['uinfo'].has_key(key) : # the user key takes precedence over the pnfs key
                    requests[i]['wrapper'][key] = requests[i]['pinfo'][key]

            if verbose > 1: print "RETRY_CNT=", requests[i]['retry']
            # generate the work ticket
            work_ticket = {"work"              : "read_from_hsm",
                           "wrapper"           : requests[i]['wrapper'],
                           "callback_addr"     : client['callback_addr'],
                           "fc"                : requests[i]['fc'],
                           "vc"                : requests[i]['vc'],
                           "encp"              : requests[i]['encp'],
                           "retry_cnt"         : requests[i]['retry'],
                           "times"             : times,
                           "unique_id"         : requests[i]['unique_id']
                           }


            # send tickets to library manger
            Trace.trace(8,"submit_read_requests q'ing: %s"%(work_ticket,))

            # get the library manager
            library = requests[i]['vc']['library']

            rq = {"work_ticket": work_ticket,
                  "infile"     : requests[i]['infile'],
                  "bfid"       : requests[i]['bfid'],
                  "library"    : requests[i]['vc']['library'],
                  "index"      : i
                  }
            rq_list.append(rq)

  # now when we have request list per volume lets sort it
  # according file location
  #print "BEFORE SORTING"
  #for j in range(0, len(rq_list)):
      #print rq_list[j]["work_ticket"]["fc"]["location_cookie"]

  rq_list.sort(compare_location)

  #print "AFTER SORTING"
  #for j in range(0, len(rq_list)):
      #print rq_list[j]["work_ticket"]["fc"]["location_cookie"]

  # submit requests
  for j in range(0, len(rq_list)):
      # send tickets to library manger
      Trace.trace(8,"submit_read_requests q'ing:%s"%(rq_list[j]["work_ticket"],))

      # get LM info from Config Server only if it is different
      if (current_library != rq_list[j]["library"]):
          current_library = rq_list[j]["library"]
          Trace.trace(8,"submit_read_requests calling config server to find"+
                      rq_list[j]["library"]+".library_manager")
      if verbose > 3:
          print "calling Config. Server to get LM info for", current_library
      lmticket = client['csc'].get(current_library+".library_manager")
      if lmticket["status"][0] != e_errors.OK:
          pprint.pprint(lmticket)
          print_data_access_layer_format(rq_list[j]["infile"], 
                                         rq_list[j]["work_ticket"]["wrapper"]["fullname"], 
                                         rq_list[j]["work_ticket"]["wrapper"]["size_bytes"],
                                         lmticket)
          print_error("EPROTO", "submit_read_requests. lmget failed %s"%(lmticket["status"],))
          continue

      Trace.trace(8,"submit_read_requests %s.library_manager at host=%s port=%s"
                  %(current_library,lmticket["hostip"],lmticket["port"]))
      # send to library manager and tell user
      ticket = client['u'].send(rq_list[j]["work_ticket"], 
                                (lmticket['hostip'], lmticket['port']))
      if verbose > 3:
          print "ENCP:read_from_hsm. LM read_from_hsm returned"
          pprint.pprint(ticket)
      if ticket['status'][0] != "ok" :
          print_data_access_layer_format(rq_list[j]["infile"], 
                                         rq_list[j]["work_ticket"]["wrapper"]["fullname"], 
                                         rq_list[j]["work_ticket"]["wrapper"]["size_bytes"],
                                         ticket)

          print_error('EPROTO',  'encp.read_from_hsm: from u.send to LM at %s:%s,  ticket["status"]=%s'
                      %(lmticket['hostip'],lmticket['port'],ticket["status"]))
          continue
      submitted = submitted+1

      tinfo["send_ticket%d"%(rq_list[j]["index"],)] = time.time() - t2 #------Lap-End
      if verbose :
          if len(Qd)==0:
              format = "  queued: %s %s bytes: %d on %s %s dt: %.02f   elapsed=%.02f"
              Qd = format %\
                   (rq_list[j]["work_ticket"]["wrapper"]["fullname"],
                    rq_list[j]["bfid"],
                    rq_list[j]["work_ticket"]["wrapper"]["size_bytes"],
                    rq_list[j]["work_ticket"]["fc"]["external_label"],
                    rq_list[j]["work_ticket"]["fc"]["location_cookie"],
                    tinfo["send_ticket%d"%(rq_list[j]["index"],)],
                    time.time()-tinfo['abs_start'])
          else:
              Qd = "%s\n  queued: %s %s bytes: %d on %s %s dt: %.02f   elapsed=%.02f" %\
                   (Qd,
                    rq_list[j]["work_ticket"]["wrapper"]["fullname"],
                    rq_list[j]["bfid"],
                    rq_list[j]["work_ticket"]["wrapper"]["size_bytes"],
                    rq_list[j]["work_ticket"]["fc"]["external_label"],
                    rq_list[j]["work_ticket"]["fc"]["location_cookie"],
                    tinfo["send_ticket%d"%(rq_list[j]["index"],)], 
                    time.time()-tinfo['abs_start'])
    
  return submitted, Qd

#############################################################################
# read hsm files in the loop after read requests have been submitted

def read_hsm_files(listen_socket, submitted, requests,  
                   tinfo, t0, chk_crc, maxretry, verbose):
    ninput = len(requests)
    for rq in requests: 
        Trace.trace(7,"read_hsm_files: %s"%(rq['infile'],))
    files_left = ninput
    bytes = 0
    control_socket_closed = 1
    data_path_socket_closed = 1
    out_fd = None
    out_fd_closed = 1
    error = 0
    
    for waiting in range(0,submitted):
        if verbose>1:
            print "Waiting for mover to call back  elapsed=",time.time()-t0
        t2 = time.time() #----------------------------------------Lap-Start
        tMBstart = t2

        # listen for a mover - see if id corresponds to one of the tickets
        #   we submitted for the volume
        while 1 :
            Trace.trace(8,"read_hsm_files listening for callback")
            read_fds,write_fds,exc_fds=select.select([listen_socket],[],[],
                                                     mover_timeout)
            if not read_fds:
                #timed out!
                msg="read_hsm_files: timeout on mover callback"
                if verbose:
                    print msg
                Trace.log(e_errors.INFO, msg)
                return files_left, bytes, error
                
            control_socket, address = listen_socket.accept()
            control_socket_closed = 0
            ticket = callback.read_tcp_obj(control_socket)
            if verbose > 3:
                print "ENCP:read_from_hsm MV called back with", ticket
            callback_id = ticket['unique_id']
            forus = 0
            for j in range(0,ninput):
                # compare strings not floats (floats fail comparisons)
                if str(requests[j]['unique_id'])==str(callback_id):
                    forus = 1
                    break

            if forus:
                Trace.trace(8,"read_hsm_files: mover called back on control_socket=%s address=%s"
                            %(control_socket, address))
                break
            else:
                Trace.log(e_errors.INFO, "read_hsm_files: mover impostor called back on \
 control_socket=%s address=%s"%(control_socket, address))
                #control_socket.close()
                break

        # ok, we've been called back with a matched id - how's the status?
        if ticket["status"][0] != e_errors.OK :
            error = ticket["status"][0]
            # print error to stdout in data_access_layer format
            print_data_access_layer_format(requests[j]['infile'], requests[j]['outfile'], 
                                           requests[j]['file_size'],
                                           ticket)
            if not e_errors.is_retriable(ticket["status"][0]):


                del(requests[j])
                if files_left > 0:
                    files_left = files_left - 1

                print_error ('EPROTO',  'failed to setup transfer, status=%s' %(ticket["status"],))
                continue

            print_error ('EPROTO', 'failed to setup transfer, status=%s' %(ticket["status"],))

            if ticket['retry_cnt'] >= maxretry:
                del(requests[j])
                if files_left > 0:
                    files_left = files_left - 1
            else:
                requests[j]['retry'] = requests[j]['retry']+1
            continue

        tinfo["tot_to_mover_callback%d"%(j,)] = time.time() - t0 
        dt = time.time() - t2 #-------------------------------------Lap-End
        if verbose>1:

            print " ",ticket["mover"]["callback_addr"],\
                  "cum:",tinfo["tot_to_mover_callback%d"%(j,)]
            print "  dt:",dt,"   elapsed=",time.time()-t0

        if verbose: print "Receiving data for file ", requests[j]['outfile'],\
           "   elapsed=",time.time()-t0

        localname = requests[j]['outfile']

        t2 = time.time() #----------------------------------------Lap-Start

        l = 0
        mycrc = 0
        bufsize = 65536*4 #XXX CGW Investigate this
        mover_addr = ticket['mover']['callback_addr']
        #set up any special network load-balancing voodoo
        interface=check_load_balance(mode=0, dest=mover_addr[0])
        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

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
            msg = "socket error connecting to %s: %s" % (mover_addr,msg,)
            Trace.log(e_errors.ERROR, msg)
            done_ticket = {'status':(e_errors.NET_ERROR, msg)}
            error = e_errors.NET_ERROR
        data_path_socket_closed = 0
        try:
            if localname == '/dev/null':
                out_fd = os.open(localname, os.O_RDWR)
                out_fd_closed = 0
            else:
                out_fd = os.open(localname, os.O_CREAT|os.O_RDWR, 0)
        except:
            error = e_errors.USERERROR
            done_ticket = {'status':(error,"Can't write %s"%(localname,))}


        Trace.trace(8,"read_hsm_files: reading data to file %s, socket=%s, bufsize=%s, chk_crc=%s"%
                    (localname,data_path_socket.getsockname(),bufsize,chk_crc))

        # read file, crc the data if user has request crc check
        if not error:
            try:
                if chk_crc != 0:
                    crc_flag = 1
                else:
                    crc_flag = 0
                mycrc = EXfer.fd_xfer( data_path_socket.fileno(),
                                       out_fd,
                                       requests[j]['file_size'], bufsize,
                                       crc_flag, 0)
            except EXfer.error, msg: 

                Trace.trace(6,"read_from_hsm EXfer error: %s %s"%
                            (sys.argv,msg))

                if verbose > 1: traceback.print_exc()

                if msg.args[1]==errno.ENOSPC:
                    try:
                        if localname!="/dev/null":
                            os.unlink(localname)
                    except:
                        pass
                    print_data_access_layer_format(
                        requests[j]['infile'], requests[j]['outfile'], requests[j]['file_size'],
                        {'status':("ENOSPC", "No space left on device")})
                    quit()
                ###XXX we shouldn't be matching literal strings here... this is really wrong 
                    error = 1
                    data_path_socket.close()
                    try:
                        done_ticket = callback.read_tcp_obj(control_socket)
                    except:
                        # assume network error...
                        # no done_ticket!
                        # exit here
                        print_data_access_layer_format(requests[j]['infile'],  
                                                       requests[j]['outfile'],
                                                       0,
                                                       {'status':("EPROTO",
                                                                  "Network problem or mover reset")})
                        try:
                            if localname!="/dev/null":
                                os.unlink(localname)
                        except:
                            pass
                        quit()

                    control_socket.close()
                    print_data_access_layer_format(requests[j]['infile'],  
                                                   requests[j]['outfile'], 
                                                   requests[j]['file_size'], 
                                                   done_ticket )
                    if not e_errors.is_retriable(done_ticket["status"][0]):
                        del(requests[j])
                        if files_left > 0: files_left = files_left - 1

                        print_error ('EPROTO',  'Failed to transfer, status=%s' %(done_ticket["status"],))
                        error=1
                        break
                    print_error ('EPROTO', 'failed to transfer, status=%s'%(done_ticket["status"],))

                if ticket['retry_cnt'] >= maxretry:
                    del(requests[j])
                    if files_left > 0: files_left = files_left - 1
                    pass
                else:
                    requests[j]['retry'] = requests[j]['retry']+1
                    pass
                continue

            # if no exceptions, fsize is file_size[j]
            fsize = requests[j]['file_size']
            # fd_xfer does not check for EOF after reading the specified
            # number of bytes.
            buf = data_path_socket.recv(bufsize)# there should not be any more
            fsize = fsize + len(buf)
            if not data_path_socket_closed:
                data_path_socket.close()
                if not out_fd_closed:
                    out_fd_closed=1
                    os.close(out_fd)
                data_path_socket_closed = 1 
                pass



        t2 = time.time() #----------------------------------------Lap-Start

        # File has been read - wait for final dialog with mover.
        Trace.trace(8,"read_hsm_files waiting for final mover dialog on %s"%(control_socket,))
        try:
            done_ticket = callback.read_tcp_obj(control_socket)
            control_socket.close()
            control_socket_closed = 1
            Trace.trace(8,"read_hsm_files final dialog recieved")
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "recv final dialog %s %s" %(exc, msg))
            done_ticket = {'status': ( e_errors.NET_ERROR, "%s %s" % (exc, msg))}
            
        # make sure the mover thinks the transfer went ok
        if done_ticket["status"][0] != e_errors.OK:
            error = 1
            # print error to stdout in data_access_layer format
            print_data_access_layer_format(requests[j]['infile'], requests[j]['outfile'], 
                                           requests[j]['file_size'], done_ticket)
            # exit here
            if not e_errors.is_retriable(done_ticket["status"][0]):

                del(requests[j])
                if files_left > 0:
                    files_left = files_left - 1

                print_error ('EPROTO', 'failed to transfer, status=%s' %(done_ticket["status"],))
                continue

            print_error ('EPROTO',  'failed to transfer, status=%s' %(done_ticket["status"],))

            if ticket['retry_cnt'] >= maxretry:
                del(requests[j])
                if files_left > 0:
                    files_left = files_left - 1
            else:
                requests[j]['retry'] = requests[j]['retry']+1
            continue


        # verify that the crc's match
        if chk_crc :
            mover_crc = done_ticket["fc"]["complete_crc"]
            if mover_crc is None:
                msg =   "warning: mover did not return CRC; skipping CRC check"
                Trace.alarm(e_errors.WARNING, msg, {
                    'infile':requests[j]['infile'],
                    'outfile':requests[j]['outfile']})
                sys.stderr.write(msg+'\n')
                
            elif mover_crc != mycrc :
                error = 1
                # print error to stdout in data_access_layer format
                Trace.alarm(e_errors.WARNING, e_errors.CRC_ERROR, {
                    'infile':requests[j]['infile'],
                    'outfile':requests[j]['outfile']})
                                                                   
                done_ticket['status'] = (e_errors.CRC_ERROR,"%s != %s" %(mycrc, mover_crc))
                print_data_access_layer_format(requests[j]['infile'], 
                                               requests[j]['outfile'], 
                                               requests[j]['file_size'],
                                               done_ticket)

                print_error('EPROTO',  "encp.read_from_hsm: CRC's mismatch: %s %s"%(mover_crc, mycrc))

                # no retry for this case
                bytes = bytes+requests[j]['file_size']
                requests.remove(requests[j])
                if files_left > 0:
                    files_left = files_left - 1

                continue
                

        tinfo["final_dialog%d"%(j,)] = time.time()-t2 #-----------Lap-End
        if verbose>1:
            print "  dt:",tinfo["final_dialog%d"%(j,)],"elapsed=",time.time()-t0


        # calculate some kind of rate - time from beginning to wait for
        # mover to respond until now. This doesn't include the overheads
        # before this, so it isn't a correct rate. I'm assuming that the
        # overheads I've neglected are small so the quoted rate is close
        # to the right one.  In any event, I calculate an overall rate at
        # the end of all transfers
        tnow = time.time()


        infile = requests[j]['infile']
        outfile = requests[j]['outfile']
        # remove file requests if transfer completed succesfuly
        if done_ticket["status"][0] == e_errors.OK:
            delete_at_exit.unregister(localname)
            if localname != '/dev/null':
                try:
                    perms = os.stat(requests[j]['infile'])[stat.ST_MODE]
                    os.chmod(localname, perms)
                except:
                    exc, msg, tb = sys.exc_info()
                    Trace.log(e_errors.INFO, "chmod %s failed: %s %s" % (localname, exc, msg))
                
            bytes = bytes+requests[j]['file_size']
            del(requests[j])
            if files_left > 0:
                files_left = files_left - 1

        #print a message in d0 format if an error occured in the final rename,
        ##or if it was asked for explicitly

        if done_ticket['status'][0]==e_errors.OK:
            if localname == '/dev/null':
                statinfo = os.stat(infile)
            else:
                statinfo = os.stat(outfile)
            fsize = statinfo[stat.ST_SIZE]
        else:
            fsize=0
        if data_access_layer_requested or done_ticket['status'][0] != e_errors.OK:
            print_data_access_layer_format(infile, outfile, fsize, done_ticket)

        if done_ticket['status'][0] == e_errors.OK:
           
            if (tnow-tMBstart)!=0:
                tinfo['rate%d'%(j,)] = 1.*fsize/1024./1024./(tnow-tMBstart)
            else:
                tinfo['rate%d'%(j,)] = 0.0
            if done_ticket["times"]["transfer_time"]!=0:
                tinfo['transrate%d'%(j,)] = \
                            1.*fsize/1024./1024./done_ticket["times"]["transfer_time"]
            else:
                tinfo['rate%d'%(j,)] = 0.0
            format = "  %s %s -> %s: %d bytes copied from %s at %.3g MB/S (%.3g MB/S) mover=%s drive_id=%s drive_sn=%s drive_vendor=%s elapsed= %.02f  {'media_changer' : '%s'}"

            if verbose:
                print format %(
                    done_ticket["work"],
                    infile, outfile, fsize,
                    done_ticket["fc"]["external_label"],
                    tinfo["rate%d"%(j,)],
                    tinfo["transrate%d"%(j,)],
                    done_ticket["mover"]["name"],
                    done_ticket["mover"]["product_id"],
                    done_ticket["mover"]["serial_num"],
                    done_ticket["mover"]["vendor_id"],
                    time.time()-t0,
                    done_ticket["mover"]["media_changer"])

            Trace.log(e_errors.INFO, format%(done_ticket["work"],
                                             infile,outfile,fsize,
                                             done_ticket["fc"]["external_label"],
                                             tinfo["rate%d"%(j,)], 
                                             tinfo["transrate%d"%(j,)],
                                             done_ticket["mover"]["name"],
                                             done_ticket["mover"]["product_id"],
                                             done_ticket["mover"]["serial_num"],
                                             done_ticket["mover"]["vendor_id"],
                                             time.time()-t0,
                                             done_ticket["mover"]["media_changer"]),
                      Trace.MSG_ENCP_XFER )


        if verbose > 3:
            print "Done"
            pprint.pprint(done_ticket)
    
    if not data_path_socket_closed:
        data_path_socket.close();
        data_path_socket_closed = 1
    if not out_fd_closed:
        os.close(out_fd)
        out_fd_closed=1
    if not control_socket_closed:
        control_socket.close()
        control_socket_closed=1
    return files_left, bytes, error

    

#######################################################################
def read_from_hsm(input_files, output,
                  verbose=0, chk_crc=1, 
                  pri=1, delpri=0, agetime=0,
                  delayed_dismount=None, t0=0, dcache=0):
    if t0==0:
        t0 = time.time()
    Trace.trace(6,"read_from_hsm input_files=%s output=%s verbose=%s  chk_crc=%s t0=%s"%
                (input_files,output,verbose,chk_crc,t0))

    tinfo = {}
    tinfo["abs_start"] = t0

    request_list = []

    vols_needed = {}
    maxretry = 2

    if verbose>2:
        print "Checking input pnfs files:",input_files, "   elapsed=",time.time()-t0
    t1 =  time.time() #---------------------------------------------------Start

    #check the input unix files. if files don't exits, we bomb out to the user
#    if dcache: #XXX
#        input_files = [pnfs_hack.filename_from_id(input_files)]
#        output = [output]
        
    (inputlist, file_size) = inputfile_check(input_files)
        
    ninput = len(inputlist)
    status, info = pnfs_information(inputlist,write=0)
    if status[0] != e_errors.OK:
        print_error(status[0], status[1])
        #XXX data_access_layer?
        quit()
        
    (bfid,junk,junk,junk,junk,junk,pinfo,p)= info

    tinfo["pnfscheck"] = time.time() - t1 #--------------------------------End
    if verbose>2:
        print "  dt:",tinfo["pnfscheck"], "   elapsed=",time.time()-t0
    if verbose>3:
        print "ninput=",ninput
        print "inputlist=",inputlist
        print "file_size=",file_size
        print "bfid=",bfid
        print "pinfo=",pinfo
        print "p=",p

    if verbose>2:
        print "Checking output unix files:",output, "   elapsed=",time.time()-t0
    t1 = time.time() #---------------------------------------------------Start

    # check (and generate) the output files(s)
    # bomb out if they exist already
    outputlist = outputfile_check(inputlist,output)

    tinfo["filecheck"] = time.time() - t1 #--------------------------------End
    if verbose>2:
        print "  dt:",tinfo["filecheck"], "   elapsed=",time.time()-t0
    if verbose>3:
        print "outputlist=",outputlist

    if verbose>2:
        print "Requesting callback ports", "   elapsed=",time.time()-t0
    t1 = time.time() #---------------------------------------------------Start

    # get a port to talk on and listen for connections
    Trace.trace(10,'read_from_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback(verbose=verbose)
    client['callback_addr'] = (host, port)
    listen_socket.listen(4)

    Trace.trace(10,'read_from_hsm got callback host=%s port=%s listen_socket=%s'%
                (host,port,listen_socket))

    tinfo["get_callback"] = time.time() - t1 #-----------------------------End
    if verbose>2:
        print " ",host,port
        print "  dt:",tinfo["get_callback"], "   elapsed=",time.time()-t0

    if verbose>1:
        print "Calling Config Server to find file clerk  elapsed=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # ask configuration server what port the file clerk is using
    Trace.trace(10,"read_from_hsm calling config server to find file clerk")
    fticket = client['csc'].get("file_clerk")
    if fticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, fticket)
        quit()

    file_clerk_address = (fticket["hostip"],fticket["port"])
    Trace.trace(10,"read_from_hsm file clerk at host=%s port=%s"
                %(fticket["hostip"],fticket["port"]))

    tinfo["get_fileclerk"] = time.time() - t1 #-----------------------------End
    if verbose>1:
        print " ",fticket["hostip"],fticket["port"]
        print "  dt:", tinfo["get_fileclerk"], "   elapsed=",time.time()-t0

    # ask configuration server what port the volume clerk is using
    Trace.trace(10,"read_from_hsm calling config server to find volume clerk")
    vticket = client['csc'].get("volume_clerk")
    if vticket['status'][0] != e_errors.OK:
        print_data_access_layer_format('', '', 0, vticket)
        quit()

    volume_clerk_address = (vticket["hostip"],vticket["port"])
    Trace.trace(10,"read_from_hsm volume clerk at host=%s port=%s"
                %(vticket["hostip"],vticket["port"]))

    tinfo["get_volumeclerk"] = time.time() - t1 #-----------------------------End
    if verbose>1:
        print " ",vticket["hostip"],vticket["port"]
        print "  dt:", tinfo["get_volumeclerk"], "   elapsed=",time.time()-t0

    if verbose>1:
        print "Calling file clerk for file info", "   elapsed=",time.time()-t0
    t1 = time.time() # ---------------------------------------------------Start

    nfiles = 0
    # call file clerk and get file info about each bfid
    for i in range(0,ninput):
        t2 = time.time() # -------------------------------------------Lap-Start
        Trace.trace(7,"read_from_hsm calling file clerk for bfid=%s"%(bfid[i],))

        ###XXX should use fcc method here
        fc_reply = client['u'].send({'work': 'bfid_info', 'bfid': bfid[i]},
                                 (fticket['hostip'],fticket['port']))

        #file clerk reply also contains vol information
        
        if fc_reply['status'][0]!=e_errors.OK:
            print_data_access_layer_format('', '', 0, fc_reply)
            quit()

        Trace.trace(7,"read_from_hsm on volume=%s"%
                    (fc_reply['fc']['external_label'],))
        inhibit = fc_reply['vc']['system_inhibit'][0]
        if len(outputlist)==len(inputlist):
            ofile=outputlist[i]
        else:
            ofile=outputlist[0]
        if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
            if inhibit==e_errors.NOACCESS:
                msg="Volume is marked NOACCESS"
            else:
                msg="Volume is marked NOTALLOWED"
            fc_reply['status'] = (inhibit, msg)
            print_data_access_layer_format(inputlist[i], ofile, 0, fc_reply)
            continue
        inhibit = fc_reply['vc']['user_inhibit'][0]
        if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
            if inhibit==e_errors.NOACCESS:
                msg="Volume is marked NOACCESS"
            else:
                msg="Volume is marked NOTALLOWED"
            fc_reply['status'] = (inhibit, msg)
            print_data_access_layer_format(inputlist[i], ofile, 0, fc_reply)
            continue
        if fc_reply["fc"]["deleted"] == "yes":
            fc_reply['status'] = (e_errors.DELETED, "File has been deleted")
            print_data_access_layer_format(inputlist[i], ofile, 0, fc_reply)
            continue


        request = {}
        fc_reply['vc']['address'] = volume_clerk_address
        fc_reply['fc']['address'] = file_clerk_address
        request['vc'] = fc_reply['vc']
        request['fc'] = fc_reply['fc']
        request['volume'] = fc_reply['fc']['external_label']
        request['bfid'] = bfid[i]
        request['infile'] = inputlist[i]
        request['outfile'] = outputlist[i]
        request['pinfo'] = pinfo[i]
        request['file_size'] = file_size[i]
        request['retry'] = 0
        request['unique_id'] = ''

        label = fc_reply['fc']['external_label']
        wr = {}
        for key in client['uinfo'].keys():
            wr[key] = client['uinfo'][key]

        # make the part of the ticket that encp knows about (there's more later)
        encp_el = {}

        ## quick fix to check HiPri functionality
        admpri = -1
        #if pri < 0:
        #    pri = -pri
        #    admpri = pri
        encp_el["basepri"] = pri
        encp_el["adminpri"] = admpri
        encp_el["delpri"] = delpri
        encp_el["agetime"] = agetime
        encp_el["delayed_dismount"] = delayed_dismount

        request['wrapper'] = wr
        request['encp'] = encp_el

        try:
            vols_needed[label] = vols_needed[label]+1
        except KeyError:
            vols_needed[label] = 1
        request_list.append(request)
        nfiles = nfiles+1
        tinfo['fc%d'%(i,)] = time.time() - t2 #------------------------Lap--End

    if (nfiles == 0):
        quit()
    tinfo['fc'] = time.time() - t1 #-------------------------------End
    if verbose>1:
        print "  dt:",tinfo["fc"], "   elapsed=",time.time()-t0

    if verbose:
        print "Submitting read requests", "   elapsed=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    total_bytes = 0

    # loop over all volumes that are needed and submit all requests for
    # that volume. Read files from each volume before submitting requests
    # for different volumes.

    files_left = nfiles
    retry_flag = 0
    bytes = 0
    while files_left:

        (submitted,Qd) = submit_read_requests(request_list,
                                              client, tinfo, 
                                              vols_needed.keys(),
                                              verbose, 
                                              retry_flag)


        tinfo["send_ticket"] = time.time() - t1 #---------------------------End
        if verbose:
            print Qd
        if verbose>1:
            print "  dt:",tinfo["send_ticket"], "   elapsed=",time.time()-t0

        # We have placed our work in the system and now we have to 
        # wait for resources. All we need to do is wait for the system
        # to call us back, and make sure that is it calling _us_ back,
        # and not some sort of old call-back to this very same port. 
        # It is dicey to time out, as it is probably legitimate to 
        # wait for hours....
        if submitted != 0:
            files_left, brcvd, error = read_hsm_files(listen_socket, submitted, request_list,
                                                      tinfo, t0, chk_crc, maxretry, verbose)
            bytes = bytes + brcvd
            if verbose: print "FILES_LEFT ", files_left
            if files_left > 0:
                retry_flag = 1
        else: 
            files_left = 0
            error = 1

    # we are done transferring - close out the listen socket
    try:
        listen_socket.close()
    except:
        if verbose:
            print "Error closing socket"


    # Calculate an overall rate: all bytes, all time
    tf=tinfo["total"] = time.time()-t0
    done_ticket = {}
    done_ticket["tinfo"] = tinfo
    total_bytes = total_bytes + bytes
    tf = time.time()
    if tf!=t0:
        done_ticket["MB_per_S"] = 1.*total_bytes/1024./1024./(tf-t0)
    else:
        done_ticket["MB_per_S"] = 0.0


    if error == 0:
        msg ="Complete: %s bytes in %s files in %s sec.  Overall rate = %s MB/sec" % (
            total_bytes,ninput,tf-t0,done_ticket["MB_per_S"])
    else:
        msg ="Error after transferring %s bytes in %s files in %s sec.  Overall rate = %s MB/sec" % (
            total_bytes,ninput,tf-t0,done_ticket["MB_per_S"])
        
    if verbose:
        print msg

    if verbose > 3:
        print "DONE TICKET"
        pprint.pprint(done_ticket)

    Trace.trace(6,"read_from_hsm "+msg)
    quit(error)

    # tell file clerk we are done - this allows it to delete our unique id in
    # its dictionary - this keeps things cleaner and stops memory from growing
    #u.send_no_wait({"work":"done_cleanup"}, (fticket['hostip'], fticket['port']))

    
##############################################################################

class encp(interface.Interface):

    deprecated_options = ['--crc']
    
    def __init__(self):
        self.chk_crc = 1           # we will check the crc unless told not to
        self.priority = 1          # lowest priority
        self.delpri = 0            # priority doesn't change
        self.age_time = 0          # priority doesn't age
        self.data_access_layer = 0 # no special listings
        self.verbose = 0 
        
        self.delayed_dismount = 0
        self.output_file_family = '' # initial set for use with --ephemeral or
                                     # or --file-family

        self.bytes = None
        self.test_mode = 0
        self.put_cache = self.get_cache = 0 #Special options for operation with a disk cache layer
        self.storage_info = None # Ditto
        
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
            print_error("USERERROR", "not enough arguments specified")
            self.print_help()
            sys.exit(1)

        if not client: 
            clients(self.config_host, self.config_port)
            
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
                if p1:
                    print_error("USERERROR", "Not all input files are /pnfs/... files")
                else:
                    print_error("USERERROR", "Not all input files are unix files")
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
    t0 = time.time()
    Trace.init("ENCP")
    Trace.trace( 6, 'encp called at %s: %s'%(t0,sys.argv) )

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

    if e.data_access_layer:
        data_access_layer_requested.set()

    #Special handling for use with dcache - not yet enabled
    if e.get_cache:
        pnfs_id = sys.argv[-2]
        local_file = sys.argv[-1]
        read_from_hsm(pnfs_id, local_file, 
                      e.verbose, e.chk_crc,
                      e.priority, e.delpri,
                      e.age_time,
                      e.delayed_dismount, t0,
                      dcache=1)
        
    elif e.put_cache:
        pnfs_id = sys.argv[-2]
        local_file = sys.argv[-1]
        write_to_hsm(local_file, pnfs_id, e.output_file_family,
                     e.verbose, e.chk_crc,
                     e.priority, e.delpri, e.age_time,
                     e.delayed_dismount, t0, e.bytes,
                     dcache=1, storage_info=e.storage_info)
        
    ## have we been called "encp unixfile hsmfile" ?
    elif e.intype=="unixfile" and e.outtype=="hsmfile" :
        write_to_hsm(e.input,  e.output, e.output_file_family,
                     e.verbose, e.chk_crc,
                     e.priority, e.delpri, e.age_time,
                     e.delayed_dismount, t0, e.bytes)

    ## have we been called "encp hsmfile unixfile" ?
    elif e.intype=="hsmfile" and e.outtype=="unixfile" :
        read_from_hsm(e.input, e.output,
                      e.verbose, e.chk_crc,
                      e.priority, e.delpri, e.age_time,
                      e.delayed_dismount, t0)

    ## have we been called "encp unixfile unixfile" ?
    elif e.intype=="unixfile" and e.outtype=="unixfile" :
        emsg="encp copies to/from tape. It is not involved in copying %s to %s" % (e.intype, e.outtype)
        print_error('USERERROR', emsg)
        if data_access_layer_requested:
            print_data_access_layer_format(e.input, e.output, 0, {'status':("USERERROR",emsg)})
        quit()

    ## have we been called "encp hsmfile hsmfile?
    elif e.intype=="hsmfile" and e.outtype=="hsmfile" :
        emsg=  "encp tape to tape is not implemented. Copy file to local disk and them back to tape"
        print_error('USERERROR', emsg)
        if data_access_layer_requested:
            print_data_access_layer_format(e.input, e.output, 0, {'status':("USERERROR",emsg)})
        quit()

    else:
        emsg = "ERROR: Can not process arguments %s"%(e.args,)
        Trace.trace(6,emsg)
        print_data_access_layer_format("","",0,{'status':("USERERROR",emsg)})
        quit()

    Trace.trace(10,"encp finished at %s"%(time.time(),))

if __name__ == '__main__':
    try:
        main()
        quit(0)
    except SystemExit, msg:
        quit(1)
    except:
        exc, msg, tb = sys.exc_info()
        sys.stderr.write("%s %s\n" % (exc, msg))
        quit(1)
        
        
