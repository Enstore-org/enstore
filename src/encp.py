#!/usr/bin/env python
######################################################################
# src/$RCSfile$   $Revision$
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

# enstore modules
import Trace_lite
Trace=Trace_lite
import pnfs
import callback
import log_client
import configuration_client
import udp_client
import EXfer
import interface
import e_errors
import hostaddr
import library_manager_client

def encp_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but DO NOT CHANGE THE SYNTAX
    version_string = "v1_7  CVS $Revision$ "
    file = globals().get('__file__', "")
    if file: version_string = version_string + file
    return version_string


#seconds to wait for mover to call back, before resubmitting req. to lib. mgr.
mover_timeout = 15*60  #15 minutes


data_access_layer_format = """INFILE=%s
OUTFILE=%s
FILESIZE=%s
LABEL=%s
DRIVE=%s
TRANSFER_TIME=%f
SEEK_TIME=%f
MOUNT_TIME=%f
QWAIT_TIME=%f
TIME2NOW=%f
STATUS=%s\n"""

##############################################################################
#simple stopwatch
start_time=None


##############################################################################

# log the error to the logger, print it to the console and exit

def jraise(errcode,errmsg,exit_code=1) :
    format = "Fatal error: %s %s  Exit code: %s" % (errcode, errmsg, exit_code)
    x=sys.stdout;sys.stdout=sys.stderr
    print format
    try:
        Trace.log(e_errors.ERROR, format)
    except:
        pass
    # this error used to be a 0
    Trace.trace(6,"encp.jraise and exiting with code=%s"%(exit_code,))
    sys.stdout=x
    sys.exit(exit_code)


##############################################################################

# log the error to the logger and print it to the stderr

def print_error(errcode,errmsg,fatal=0) :
    format = str(errcode)+" "+str(errmsg)
    if fatal:
        format = "Fatal error: "+format
    else:
        format = "Error: "+format
    x=sys.stdout;sys.stdout=sys.stderr
    print format
    try:
        Trace.log(e_errors.ERROR, format)
    except:
        pass
    sys.stdout=x



##############################################################################
# print statistics in data_access_layer format
def print_data_access_layer_format(inputfile, outputfile, filesize, ticket):
    # check if all fields in ticket present
    try:
	external_label = ticket["fc"]["external_label"]
    except:
	external_label = ''
    try:
	device = ticket["mover"]["device"]
    except:
	device = ''
    try:
	transfer_time = ticket["times"]["transfer_time"]
    except:
	transfer_time = 0
    try:
	seek_time = ticket["times"]["seek_time"]
    except:
	seek_time = 0
    try:
	mount_time = ticket["times"]["mount_time"]
    except:
	mount_time = 0
    try:
	in_queue = ticket["times"]["in_queue"]
    except:
	in_queue = 0
    try:
	total = time.time()-ticket["times"]["t0"]
    except:
	total = 0
    try:
	status = ticket["status"][0]
    except:
	status = 'Unknown'
    print data_access_layer_format % (inputfile, outputfile, filesize, external_label,
                                      device, transfer_time, seek_time, mount_time, in_queue,
                                      total, status)
    
    try:
	format = "INFILE=%s OUTFILE=%s FILESIZE=%d LABEL=%s DRIVE=%s TRANSFER_TIME=%f"+\
		 "SEEK_TIME=%f MOUNT_TIME=%f QWAIT_TIME=%f TIME2NOW=%f STATUS=%s"
        msg_type=e_errors.ERROR
        if status == e_errors.OK:
            msg_type = e_errors.INFO
        
        Trace.log(msg_type, format%(inputfile, outputfile, filesize, 
					  external_label, device,
					  transfer_time, seek_time, mount_time,
					  in_queue, total,
					  status) )
    except:
        pass

    
#######################################################################

# get the configuration client and udp client and logger client
# return some information about who we are so it can be used in the ticket

def clients(config_host,config_port,verbose):
    # get a configuration server
    csc = configuration_client.ConfigurationClient((config_host,config_port))

    # send out an alive request - if config not working, give up
    rcv_timeout = 20
    alive_retries = 10
    try:
        stati = csc.alive(configuration_client.MY_SERVER, rcv_timeout,
                          alive_retries)
    except:
        stati={}
        stati["status"] = (e_errors.CONFIGDEAD,"Config at %s port=%s"%
                           (config_host, config_port))
    if stati['status'][0] != e_errors.OK:
        print_data_access_layer_format("","",0, stati)
        jraise(stati['status'],"No response on alive to config",1)
    
    # get a udp client
    u = udp_client.UDPClient()

    # get a logger client
    global logc  #needs to be global so other functions in this module can use it.
    ## You know, encp.py would be a lot cleaner!
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

    return (csc,u,uinfo)


##############################################################################

# generate the full path name to the file

def fullpath(filename):
    machine = hostaddr.gethostinfo()[0]
    dirname, file = os.path.split(filename)

    # if the directory is empty - get the users current working directory
    if dirname == '' :
        dirname = os.getcwd()

    dirname=os.path.expandvars(dirname)
    dirname=os.path.expanduser(dirname)
    dirname=os.path.normpath(dirname)
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
        print_data_access_layer_format(inputlist[0],'',0,
                                       {'status':('EPROTO',"Cannot specify --bytes with multiple files")}
                                       )
        jraise(errno.errocode[errno.EPROTO],"encp.inputfile_check: "
               "Cannot specify --bytes with multiple files")

    # we need to know how big each input file is
    file_size = []

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(0,ninput):

        # get fully qualified name
        machine, fullname, dir, basename = fullpath(inputlist[i])
        inputlist[i] = os.path.join(dir,basename)

        # input files must exist
        if not os.access(inputlist[i],os.R_OK):
            print_data_access_layer_format(inputlist[i],'',0,{'status':('EACCES','No such file')})
            jraise(errno.errorcode[errno.EACCES],"encp.inputfile_check: %s, no read access to file" %
                   (inputlist[i],))

        # get the file size
        statinfo = os.stat(inputlist[i])

        if bytecount != None:
            file_size.append(bytecount)
        else:
            file_size.append(statinfo[stat.ST_SIZE])

        # input files can't be directories
        if not stat.S_ISREG(statinfo[stat.ST_MODE]) :
            print_data_access_layer_format(inputlist[i],'',0,{'status':('EACCES','Not a regular file')})

            jraise(errno.errorcode[errno.EACCES],"encp.inputfile_check: %s is not a regular file"%
                   (inputlist[i],))

    # we cannot allow 2 input files to be the same
    # this will cause the 2nd to just overwrite the 1st
    for i in range(0,ninput):
        for j in range(i+1,ninput):
            if inputlist[i] == inputlist[j]:
                print_data_access_layer_format(inputlist[j],'',0,{'status':('EPROTO','Duplicate entry')})
                jraise(errno.errorcode[errno.EPROTO],"encp.inputfile_check: "+
                       inputlist[i]+" is the duplicated - not allowed")

    return (ninput, inputlist, file_size)



##############################################################################

# check the output file list for consistency
# generate names based on input list if required

def outputfile_check(ninput,inputlist,output):
    # can only handle 1 input file  copied to 1 output file
    #  or      multiple input files copied to 1 output directory
    # this is just the current policy - nothing fundamental about it
    if len(output)>1:
        print_data_access_layer_format('',output[0],0,{'status':('EPROTO','Cannot have multiple output files')})  
        jraise(errno.errorcode[errno.EPROTO],
               "encp.outputfile_check: cannot handle multiple output files: %s"%(output,))


    # if user specified multiple input files, then output must be a directory
    outputlist = []
    if ninput!=1:
        if not os.path.exists(output[0]):
            print_data_access_layer_format('',output[0],0,{'status':('EPROTO','Cannot have multiple output files')})  
            jraise(errno.errorcode[errno.EPROTO],
                   "encp.outputfile_check: multiple input files cannot be copied to non-existent directory %s"%
                   (output[0],))
        if not os.path.isdir(output[0]):
            print_data_access_layer_format('',output[0],0,{'status':('ENOTDIR','Not a directory')})  
            jraise(errno.errorcode[errno.ENOTDIR],
                   "encp.outputfile_check: multiple input files must be copied to a directory, not  %s"%(output[0],))


    outputlist = []

    # Make sure we can open the files. If we can't, we bomb out to user
    # loop over all input files and generate full output file names
    for i in range(0,ninput):
        outputlist.append(output[0])

        if outputlist[i] == '/dev/null':
            continue
            
        # see if output file exists as user specified
        itexists = os.path.exists(outputlist[i]) 
        
        if not itexists:
            omachine, ofullname, odir, obasename = fullpath(outputlist[i])
            if not os.path.exists(odir):
                # directory doesn't exist - error
                print_data_access_layer_format(inputlist[i],
                                               outputlist[i],
                                               0,
                                               {'status':
                                                ('EEXIST', "No such directory"+odir)})
                jraise(errno.errorcode[errno.ENOENT],"encp.outputfile_check:"
                       " base directory doesn't exist for "+outputlist[i])

        # note: removed from itexist=1 try block to isolate errors
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
                    print_data_access_layer_format(inputlist[i], outputlist[i], 0, {'status':
                                                                          ('EEXIST', None)})
                    jraise(errno.errorcode[errno.EEXIST],
                           "encp.outputfile_check: "+outputlist[i]+
                           " already exists")

            # filename already exists - error
            else:
                print_data_access_layer_format(inputlist[i], outputlist[i], 0, {'status':
                                                                      ('EEXIST', None)})
                jraise(errno.errorcode[errno.EEXIST],"encp.outputfile_check: "+
                       outputlist[i]+" already exists")

        # need to check that directory is writable
        # since all files go to one output directory, one check is enough
        if i==0 and outputlist[0]!='/dev/null':
            if not os.access(odir,os.W_OK):
                print_data_access_layer_format("",odir,0,{'status':('EEXIST',None)})
                jraise(errno.errorcode[errno.EACCES],"encp.write_to_hsm: "+
                       " NO write access to directory "+odir)

    # we can not allow 2 output files to be the same
    # this will cause the 2nd to just overwrite the 1st
    # In principle, this is already taken care of in the inputfile_check, but
    #  do it again just to make sure in case someone changes protocol
    for i in range(0,ninput):
        for j in range(i+1,ninput):
            if outputlist[i] == outputlist[j]:
                print_data_access_layer_format('',outputlist[j],0,
                                               {'status':('EPROTO',"Duplicated entry")})
                jraise(errno.errorcode[errno.EPROTO],"encp.outputfile_check: %s is duplicated - not allowed"
                       %(outputlist[i],))

    return outputlist

##############################################################################
# check if the system is still running by checking the wormhole file

def system_enabled(p):                 # p is a  pnfs object
    running = p.check_pnfs_enabled()
    if running != pnfs.ENABLED :
        print_data_access_layer_format("","","",{'status':("EACCES", "Pnfs disabled")})
        jraise(errno.errorcode[errno.EACCES],"encp.system_enabled: "
               "system disabled "+running)
    Trace.trace(10,"system_enabled running= "+running)


    
#######################################################################
# return pnfs information,
# and an open pnfs object so you can check if  the system is enabled.

def pnfs_information(filelist,nfiles):
    bfid = []
    pinfo = []
    library = []
    file_family = []
    width = []
    ff_wrapper = []

    for i in range(0,nfiles):
        p = pnfs.Pnfs(filelist[i])         # get the pnfs object
        bfid.append(p.bit_file_id)         # get the bit file id
        library.append(p.library)          # get the library
        file_family.append(p.file_family)  # get the file family
	try:
	    ff_wrapper.append(p.file_family_wrapper)  # get the file family wrapper
	except:
	    ff_wrapper.append("cpio_odc")  # default
        width.append(p.file_family_width)  # get the width

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
 wrapper_type=%s width=%s pinfo=%s p=%s" %
                (bfid, library, file_family, ff_wrapper, width, pinfo, p))
    return (bfid,library,file_family,ff_wrapper,width,pinfo,p)

    
##############################################################################
    
def write_to_hsm(input_files, output, output_file_family='',
                 config_host=None, config_port=None,
                 verbose=0, data_access_layer=0, chk_crc=0, use_IPC=0,
                 pri=1, delpri=0, agetime=0, delayed_dismount=0,
                 t0=0, bytecount=None):
    if t0==0:
        t0 = time.time()

    Trace.trace(6,"write_to_hsm input_files=%s output=%s config_host=%s config_port=%s \
 verbose=%s data_access_layer=%s chk_crc=%s t0=%s" %
                (input_files,output,config_host,config_port,verbose,data_access_layer,
                 chk_crc, t0))

    tinfo = {}
    tinfo["abs_start"] = t0

    if verbose>2:
        print "Getting clients, storing/checking local info   cumt=", time.time()-t0
    t1 = time.time() #-------------------------------------------Start

    # initialize - and get config, udp and log clients
    maxretry = 2
    unique_id = []
    (csc,u,uinfo) = clients(config_host,config_port,verbose)

    # create the wrapper subticket - copy all the user info 
    # into for starters
    wrapper = {}
    for key in uinfo.keys():
        wrapper[key] = uinfo[key]

    # make the part of the ticket that encp knows about (there's 
    # more later)
    encp = {}
    encp["basepri"] = pri
    encp["adminpri"] = -1
    encp["delpri"] = delpri
    encp["agetime"] = agetime

    pid = os.getpid()
    thishost = hostaddr.gethostinfo()[0]
    # create the time subticket
    times = {}
    times["t0"] = tinfo["abs_start"]

    tinfo["clients"] = time.time() - t1 #---------------------------End
    if verbose>2:
        print "  dt:",tinfo["clients"], "   cumt=",time.time()-t0
    if verbose>3:
        print "csc=",csc
        print "u=",u
        print "logc=",logc
        print "uinfo=",uinfo

    if verbose>2:
        print "Checking input unix files:",input_files, "   cumt=", time.time()-t0
    t1 =  time.time() #------------------------------------------Start

    # check the input unix files. if files don't exits, 
    # we bomb out to the user
    ninput, inputlist, file_size = inputfile_check(input_files, bytecount)
    if (ninput>1) and (delayed_dismount == 0):
        delayed_dismount = 1
    #else:
        #delayed_dismount = 0

    tinfo["filecheck"] = time.time() - t1 #-------------------------End
    if verbose>2:
        print "  dt:",tinfo["filecheck"], "   cumt=",time.time()-t0
    if verbose>3:
        print "ninput=",ninput
        print "inputlist=",inputlist
        print "file_size=",file_size
        print "delayed_dismount=",delayed_dismount

    if verbose>2:
        print "Checking output pnfs files:",output, "   cumt=", time.time()-t0
    t1 = time.time() #--------------------------------------------Start

    # check (and generate) the output pnfs files(s) names
    # bomb out if they exist already
    outputlist = outputfile_check(ninput,inputlist,output)
    junk,library,file_family,ff_wrapper,width,pinfo,p=pnfs_information(outputlist,ninput)

    if output_file_family != "":
        for i in range(0,ninput):
            file_family[i] = output_file_family
            width[i] = 1

    # note: Since multiple input files all go to 1 directory:
    #       all libraries are the same
    #       all file families are the same
    #       all widths are the same
    # be cautious and check to make sure this is indeed correct
    for i in range(1,ninput):
        if (library[i]!=library[0] or 
            file_family[i]!=file_family[0] or 
            width[i]!=width[0] or 
            ff_wrapper[i] != ff_wrapper[0]):
            print "library=",library
            print "file_family=",file_family
	    print "wrapper type=",ff_wrapper
            print "width=",width
            msg =  "library, file_family, width not all the same"
            print_data_access_layer_format('','',0,{'status':(e_errors.USERERROR,msg)})
            jraise(errno.errorcode[errno.EPROTO],msg)


    tinfo["pnfscheck"] = time.time() - t1 #------------------------End
    if verbose>2:
        print "  dt:",tinfo["pnfscheck"], "   cumt=",time.time()-t0
    if verbose>3:
        print "outputlist=",outputlist
        print "library=",library
        print "file_family=",file_family
	print "wrapper type=",ff_wrapper
        print "width=",width
        print "pinfo=",pinfo
        print "p=",p

    t1 = time.time() #-------------------------------------------Start
    if verbose>1:
        print "Requesting callback ports", "   cumt=",time.time()-t0

    # get a port to talk on and listen for connections
    Trace.trace(10,'write_to_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback(use_multiple=0,verbose=verbose)
    callback_addr = (host, port)
    listen_socket.listen(4)
    Trace.trace(10,'write_to_hsm got callback host=%s port=%s listen_socket=%s'%
                (host,port,listen_socket))

    tinfo["get_callback"] = time.time() - t1 #----------------------End
    if verbose>1:
        print " ",host,port
        print "  dt:",tinfo["get_callback"], "   cumt=",time.time()-t0

    if verbose>1:
        print "Calling Config Server to find file clerk  cumt=",time.time()-t0
    t1 = time.time() #--------------------------------------------Start

    # ask configuration server what port the file clerk is using
    Trace.trace(10,"write_to_hsm calling config server to find "\
		"file clerk")
    fticket = csc.get("file_clerk")
    if fticket['status'][0] != e_errors.OK:
	print_data_access_layer_format('', '', 0, fticket)
	jraise(fticket['status'][0], " encp.write_to_hsm:  ticket[\"status\"]=%s"%
               (fticket["status"],))

    file_clerk_address = (fticket["hostip"],fticket["port"])
    Trace.trace(10,"write_to_hsm file clerk at host=%s port=%s"%
                (fticket["hostip"],fticket["port"]))

    tinfo["get_fileclerk"] = time.time() - t1 #---------------------End
    if verbose>1:
        print " ",fticket["hostip"],fticket["port"]
        print "  dt:", tinfo["get_fileclerk"], "   cumt=",time.time()-t0

    # ask configuration server what port the volume clerk is using
    Trace.trace(10,"write_to_hsm calling config server to find "\
		"volume clerk")
    vcticket = csc.get("volume_clerk")
    if vcticket['status'][0] != e_errors.OK:
	print_data_access_layer_format('', '', 0, vcticket)
	jraise(vcticket['status'][0], ' encp.write_to_hsm:  ticket["status"]=%s'%(vcticket["status"]),)
        
    volume_clerk_address = (vcticket["hostip"],vcticket["port"])
    Trace.trace(10,"write_to_hsm volume clerk at host=%s port=%s"%
                (vcticket["hostip"],vcticket["port"]))

    tinfo["get_volumeclerk"] = time.time() - t1 #---------------------End
    if verbose>1:
        print " ",vcticket["hostip"],vcticket["port"]
        print "  dt:", tinfo["get_volumeclerk"], "   cumt=",time.time()-t0

    if verbose>1:
        print "Calling Config Server to find %s.library_manager  cumt=%s"%(library[0],time.time()-t0)


    t1 = time.time() #-------------------------------------------Start


    # ask configuration server what port library manager is using
    # note again:libraries have are identical since there is 
    # 1 output directory
    lib_mgr = library[0]+'.library_manager'
    Trace.trace(10,"write_to_hsm calling config server to find %s"%(lib_mgr,))
    vticket = csc.get(lib_mgr)
    if vticket['status'][0] != e_errors.OK:
	print_data_access_layer_format('', '', 0, vticket)
	jraise(vticket['status'][0], ' encp.write_to_hsm: status=%s'%(vticket["status"],))
	
    Trace.trace(10,"write_to_hsm %s at host=%s port=%s"%
                (lib_mgr,vticket['hostip'],vticket['port']))


    tinfo["get_libman"] = time.time() - t1 #-----------------------End
    if verbose>1:
        print "  ",vticket["hostip"],vticket["port"]
        print "  dt:",tinfo["get_libman"], "   cumt=",time.time()-t0

    file_fam = None
    # loop on all input files sequentially
    for i in range(0,ninput):
        unique_id.append(0) # will be set later when submitted

        # delete old tickets in case of a retry
        work_ticket=None

        # allow some retries if mover fails
        retry = maxretry
        while retry>0:  # note that real rates are not correct in retries
            if verbose:
                print "Sending ticket to %s.library manager,  cumt=%s"%(library[i],time.time()-t0)

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
                system_enabled(p) # make sure system still enabled before submitting
                ##start of resubmit block
                Trace.trace(7,"write_to_hsm q'ing: %s"%(work_ticket,))
                ticket = u.send(work_ticket, (vticket['hostip'], 
                                              vticket['port']))
                if verbose > 3:
                    print "ENCP: write_to_hsm LM returned"
                    pprint.pprint(ticket)
                if ticket['status'][0] != e_errors.OK :
                    print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], ticket)

                    jraise(errno.errorcode[errno.EPROTO],
                           ' encp.write_to_hsm: from u.send to %s at %s:%s, ticket["status"]=%s'
                           %(library[i],vticket['hostip'],vticket['port'],ticket['status']))

                tinfo1["send_ticket%d"%(i,)] = time.time() - t1 #--Lap End
                if verbose:
                    print "  Q'd:",inputlist[i], library[i],\
                          "family:",rq_file_family,\
                          "bytes:", file_size[i],\
                          "dt:",tinfo1["send_ticket%d"%(i,)],\
                              "   cumt=",time.time()-t0

                if verbose>1:
                    print "Waiting for mover to call back  cumt=",time.time()-t0
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
                        print "encp write_to_hsm: imposter called us, trying again"
                        Trace.trace(10,"write_to_hsm mover imposter called us control_socket=%s address=%s"
                                    %(control_socket,address))
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

                jraise(errno.errorcode[errno.EPROTO],
		       ' encp.write_to_hsm: 1st (pre-file-send) mover callback on socket %s  failed to setup transfer: \
 ticket["status"]=%s' %  (address, ticket["status"]),
                       exit_code=2)                
		pass

            tinfo1["tot_to_mover_callback%d"%(i,)] = time.time() - t0 #-----Cum
            dt = time.time() - t1 #-----------------------------Lap-End
            if verbose>1:
		# NOTE: callback can be "None" if local_mover
                print " ",ticket["mover"]["callback_addr"],\
                      "cum:",tinfo1["tot_to_mover_callback%d"%(i,)]
                print "  dt:",dt,"   cumt=",time.time()-t0

            if verbose:
                print "Sending data for file ", outputlist[i], "   cumt=",time.time()-t0
            t1 = time.time() #-------------------------------Lap-Start

	    fsize = file_size[i]

	    if ticket['mover']['local_mover']:
                if bytecount:
                    print "Fatal error:  test_mode not supported with local mover"
                    sys.exit(-1)
		# option is not applicable -- make sure it is disabled
		chk_crc = 0
	    else:
		# Call back mover on mover's port and send file on that port
		data_path_socket = callback.mover_callback_socket(ticket, use_multiple=1,
                                                                  verbose=verbose)
		in_file = open(inputlist[i], "r")
		mycrc = 0
		bufsize = 65536*4

                Trace.trace(7,"write_to_hsm: sending data to EXfer file=%s, socket=%s bufsize=%s chk_crc=%s"
                            %(inputlist[i],data_path_socket,bufsize,chk_crc))

		statinfo = os.stat(inputlist[i])
                if not bytecount and statinfo[stat.ST_SIZE] != fsize:
                    print_data_access_layer_format(
                        inputlist[i],'',fsize,{'status':('EPROTO','size changed')})
		    jraise(errno.errorcode[errno.EPROTO],
			   " encp.write_to_hsm:  file size has changed: was %s and now is %s" %
                           (fsize,statinfo[stat.ST_SIZE]))
		    pass
		try:
                    if chk_crc: crc_flag = 1
		    else:       crc_flag = 0
		    if use_IPC: ipc_flag = 1
		    else:       ipc_flag = "no"
		    mycrc = EXfer.fd_xfer( in_file.fileno(),
					   data_path_socket.fileno(), 
					   fsize, bufsize, crc_flag, 0, ipc_flag )
		except EXfer.error, msg:
                    # this error used to be a 0
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
                                                           {'status':('EPROTO',
                                                                      'Network problem or mover crash')})
                            
			    jraise(errno.errorcode[errno.EPROTO],
				   " encp.write_to_hsm: network problem or mover crash  %s"%(msg,))
			    pass

			control_socket.close()

			print_data_access_layer_format( inputlist[i], outputlist[i], file_size[i], done_ticket )
			if not e_errors.is_retriable(done_ticket["status"][0]):
			    # exit here
			    jraise(errno.errorcode[errno.EPROTO],
				   ' encp.write_to_hsm: 2nd (post-file-send): mover callback on socket %s,\
 failed to transfer: done_ticket["status"]=%s'%(address,done_ticket['status']))

			    pass
			print_error('EPROTO',
'encp.write_to_hsm: 2nd (post-file-send): mover callback on socket %s,\
 failed to transfer: done_ticket["status"]=%s'%(address,done_ticket['status']),
                                    fatal=(retry<2))
			retry = retry - 1
                        if retry>0: sys.stderr.write("Retrying\n")
			continue

		    else:
		        #some other error that needs coding
			traceback.print_exc()
                        exc,msg,tb=sys.exc_info()
			raise exc,msg
		    pass

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
			print "  dt:",tinfo1["sent_bytes%d"%(i,)]," cumt=",time.time()-t0
			pass

		    pass

		pass

	    if verbose>1:
		print "Waiting for final mover dialog  cumt=",time.time()-t0
		t1 = time.time() #----------------------------Lap-Start

	    # File has been sent - wait for final dialog with mover. 
	    # We know the file has hit some sort of media.... 
	    # when this occurs. Create a file in pnfs namespace with
	    #information about transfer.
	    Trace.trace(10,"write_to_hsm waiting for final mover dialog on %s"%(control_socket,))
	    done_ticket = callback.read_tcp_obj(control_socket)
	    control_socket.close()
	    Trace.trace(10,"write_to_hsm final dialog recieved")

	    # make sure mover thinks transfer went ok
	    if done_ticket["status"][0] != e_errors.OK :
		print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], done_ticket)
		# exit here
		if not e_errors.is_retriable(done_ticket["status"][0]):
		    jraise(errno.errorcode[errno.EPROTO],
			   ' encp.write_to_hsm: 2nd (post-file-send) mover callback on socket %s, failed to transfer:\
 done_ticket["status"]=%s'%(address,done_ticket['status']))                           


		print_error('EPROTO',
                            'encp.write_to_hsm: 2nd (post-file-send) mover callback on socket %s, failed to transfer:\
 done_ticket["status"]=%s'%(address,done_ticket['status']),
                            fatal=(retry<2))
		retry = retry - 1
                if retry:
                    sys.stderr.write("Retrying\n")
		continue

	    # Check the CRC
            if chk_crc:
                mover_crc = done_ticket["fc"]["complete_crc"]
                if mover_crc is None:
                    sys.stderr.write(
                        "warning: mover did not return CRC; skipping CRC check\n")
                    
                elif mover_crc != mycrc :
                    done_ticket['status']=('EPROTO', "CRC mismatch")
		    print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], done_ticket)
                    jraise(errno.errorcode[errno.EPROTO],
                           " encp.write_to_hsm: CRC's mismatch: mover_crc=%s, my_crc=%s"%(mover_crc,mycrc))

	    tinfo1["final_dialog"] = time.time()-t1 #----------Lap End
	    if verbose>1:
		print "  dt:",tinfo1["final_dialog"], "   cumt=",time.time()-t0

	    if verbose>1:
		print "Adding file to pnfs", "   cumt=",time.time()-t0
	    t1 = time.time() #-------------------------------Lap Start

	    # create a new pnfs object pointing to current output file
	    Trace.trace(10,"write_to_hsm adding to pnfs "+
			outputlist[i])
	    p=pnfs.Pnfs(outputlist[i])
	    # save the bfid and set the file size
	    p.set_bit_file_id(done_ticket["fc"]["bfid"],file_size[i])
	    # create volume map and store cross reference data
            try:
                p.set_xreference(done_ticket["fc"]["external_label"],
                                 done_ticket["fc"]["location_cookie"],
                                 done_ticket["fc"]["size"])
            except:
                exc,msg,tb=sys.exc_info()
                print  "Trouble with pnfs.set_xreference",str(exc),str(msg), "continuing..."
	    # add the pnfs ids and filenames to the file clerk ticket and store it
	    done_ticket["fc"]["pnfsid"] = p.id
            done_ticket["fc"]["pnfsvid"] = p.volume_fileP.id
            done_ticket["fc"]["pnfs_name0"] = p.pnfsFilename
            done_ticket["fc"]["pnfs_mapname"] = p.mapfile
	    done_ticket["work"] = "set_pnfsid"
	    binfo  = u.send(done_ticket, (fticket['hostip'], 
					  fticket['port']))
	    if verbose > 3:
		print "ENCP: write_to_hsm FC returned"
		pprint.pprint(binfo)
	    if done_ticket['status'][0] != e_errors.OK :
		print_data_access_layer_format(inputlist[i], outputlist[i], file_size[i], done_ticket)

		jraise(errno.errorcode[errno.EPROTO],
		       ' encp.write_to_hsm: from u.send to FC at %s:%s, ticket["status"]=%s'
                       %(fticket['hostip'],fticket['port'],done_ticket["status"]))
                
	    # store debugging info about transfer
	    done_ticket["tinfo"] = tinfo1 # store as much as we can into pnfs
	    done_formatted  = pprint.pformat(done_ticket)
	    p.set_info(done_formatted)
	    Trace.trace(10,"write_to_hsm done adding to pnfs")

	    tinfo1["pnfsupdate%d"%(i,)] = time.time() - t1 #--Lap End
	    if verbose>1:
		print "  dt:",tinfo1["pnfsupdate%d"%(i,)], "cumt=",time.time()-t0


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
	    format = "  %s %s -> %s: %d bytes copied to %s at %.3g MB/S (%.3g MB/S) mover=%s drive_id=%s drive_sn=%s drive_vendor=%s cumt= %f"

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
	    if data_access_layer:
		print data_access_layer_format % \
		      (inputlist[i],
		       outputlist[i],
		       fsize,
		       done_ticket["fc"]["external_label"],
		       done_ticket["mover"]["device"],
		       done_ticket["times"]["transfer_time"],
		       done_ticket["times"]["seek_time"],
		       done_ticket["times"]["mount_time"],
		       done_ticket["times"]["in_queue"],
		       time.time()-done_ticket["times"]["t0"],
		       e_errors.OK)

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
def submit_read_requests(requests, client, tinfo, vols, ninput, verbose, 
			 retry_flag):


  t2 = time.time() #--------------------------------------------Lap-Start
  rq_list = []
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
		id = "%s-%f-%d" \
		     % (thishost, time.time(), pid)

		requests[i]['unique_id'] = id  # note that this is down to mS
	    
            requests[i]['wrapper']['fullname'] = requests[i]['outfile']
            requests[i]['wrapper']["sanity_size"] = 65536
            requests[i]['wrapper']["size_bytes"] = requests[i]['file_size']

            # store the pnfs information info into the wrapper
            for key in requests[i]['pinfo'].keys():
                if not client['uinfo'].has_key(key) : # the user key takes precedence over the pnfs key
                    requests[i]['wrapper'][key] = requests[i]['pinfo'][key]

	    if verbose > 1: print "RETRY_CNT=", requests[i]['retry']
            # generate the work ticket
            work_ticket = {"work"              : "read_from_hsm",
                           "wrapper"           : requests[i]['wrapper'],
                           "callback_addr"     : client['callback_addr'],
                           "fc"                : requests[i]['finfo'],
                           "vc"                : requests[i]['vinfo'],
                           "encp"              : requests[i]['encp'],
			   "retry_cnt"         : requests[i]['retry'],
                           "times"             : times,
                           "unique_id"         : requests[i]['unique_id']
                           }


            # send tickets to library manger
            Trace.trace(8,"submit_read_requests q'ing: %s"%(work_ticket,))

            # get the library manager
            library = requests[i]['vinfo']['library']

	    rq = {"work_ticket": work_ticket,
		  "infile"     : requests[i]['infile'],
		  "bfid"       : requests[i]['bfid'],
		  "library"    : requests[i]['vinfo']['library'],
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
	  print "calling Config. Server to get LM info for", \
		current_library
      lmticket = client['csc'].get(current_library+".library_manager")
      if lmticket["status"][0] != e_errors.OK:
	  pprint.pprint(lmticket)
	  # this error used to be a 0
	  Trace.trace(6,"submit_read_requests. lmget failed %s"%(lmticket["status"],))
	  print_data_access_layer_format(rq_list[j]["infile"], 
					 rq_list[j]["work_ticket"]["wrapper"]["fullname"], 
					 rq_list[j]["work_ticket"]["wrapper"]["size_bytes"],
					 lmticket)
	  print_error("EPROTO",
		      "submit_read_requests. lmget failed %s"%(lmticket["status"],),
                      fatal=0)
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

	  print_error('EPROTO',
		      'encp.read_from_hsm: from u.send to LM at %s:%s,  ticket["status"]=%s'
                      %(lmticket['hostip'],lmticket['port'],ticket["status"]),
		      fatal=0)          
	  continue
      submitted = submitted+1

      tinfo["send_ticket%d"%(rq_list[j]["index"],)] = time.time() - t2 #------Lap-End
      if verbose :
	  if len(Qd)==0:
	      format = "  Q'd: %s %s bytes: %d on %s %s dt: %f   cumt=%f"
	      Qd = format %\
		   (rq_list[j]["work_ticket"]["wrapper"]["fullname"],
		    rq_list[j]["bfid"],
		    rq_list[j]["work_ticket"]["wrapper"]["size_bytes"],
		    rq_list[j]["work_ticket"]["fc"]["external_label"],
		    rq_list[j]["work_ticket"]["fc"]["location_cookie"],
		    tinfo["send_ticket%d"%(rq_list[j]["index"],)],
		    time.time()-tinfo['abs_start'])
	  else:
	      Qd = "%s\n  Q'd: %s %s bytes: %d on %s %s dt: %f   cumt=%f" %\
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

def read_hsm_files(listen_socket, submitted, ninput,requests,  
		   tinfo, t0, chk_crc, use_IPC, data_access_layer, maxretry, verbose):
    for rq in requests: 
	Trace.trace(7,"read_hsm_files: %s"%(rq['infile'],))
    files_left = ninput
    bytes = 0
    control_socket_closed = 0
    data_path_socket_closed = 1
    error = 0
    
    for waiting in range(0,submitted):
        if verbose>1:
            print "Waiting for mover to call back  cumt=",time.time()-t0
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
                Trace.trace(8,"read_hsm_files: mover impostor called back on \
 control_socket=%s address=%s"
                            %(control_socket, address))
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

		print_error (errno.errorcode[errno.EPROTO],
                             'encp.read_from_hsm: 1st (pre-file-read)\
 mover callback on socket %s failed to setup transfer, ticket["status"]=%s' %(address, ticket["status"]),
                             fatal=0)

		continue

            print_error (errno.errorcode[errno.EPROTO],'encp.read_from_hsm: 1st (pre-file-read)\
 mover callback on socket %s failed to setup transfer, ticket["status"]=%s' %(address, ticket["status"]),
                             fatal=0)

	    if ticket['retry_cnt'] >= maxretry:
		del(requests[j])
		if files_left > 0:
		    files_left = files_left - 1
	    else:
		requests[j]['retry'] = requests[j]['retry']+1
	    continue

        tinfo["tot_to_mover_callback%d"%(j,)] = time.time() - t0 #-----Cum
        dt = time.time() - t2 #-------------------------------------Lap-End
        if verbose>1:
	    # NOTE: callback can be "None" if local_mover
            print " ",ticket["mover"]["callback_addr"],\
                  "cum:",tinfo["tot_to_mover_callback%d"%(j,)]
            print "  dt:",dt,"   cumt=",time.time()-t0

        if verbose: print "Receiving data for file ", requests[j]['outfile'],\
	   "   cumt=",time.time()-t0

	tempname = requests[j]['outfile']
        if tempname != '/dev/null':
            tempname = tempname+'.'+requests[j]['unique_id']
	if ticket['mover']['local_mover']:
	    # option is not applicable -- make sure it is disabled
	    chk_crc = 0
	else:
	    t2 = time.time() #----------------------------------------Lap-Start

	    l = 0
	    mycrc = 0
	    bufsize = 65536*4

	    data_path_socket = callback.mover_callback_socket(ticket, use_multiple=1,
                                                              verbose=verbose)
	    data_path_socket_closed = 0
            try:
                _f_ = open(tempname,"w")
            except:
                done_ticket  = {'status':(e_errors.NOACCESS,None)}
                error = e_errors.NOACCESS
                
	    Trace.trace(8,"read_hsm_files: reading data to file %s, socket=%s, bufsize=%s, chk_crc=%s"%
                        (tempname,data_path_socket.getsockname(),bufsize,chk_crc))

	    # read file, crc the data if user has request crc check
            if not error:
                try:
                    if chk_crc != 0: crc_flag = 1
                    else:            crc_flag = 0
		    if use_IPC: ipc_flag = 1
		    else:       ipc_flag = "no"
                    mycrc = EXfer.fd_xfer( data_path_socket.fileno(),
					   _f_.fileno(),
					   requests[j]['file_size'], bufsize,
					   crc_flag, 0, ipc_flag )
                except EXfer.error, msg: 

                    Trace.trace(6,"read_from_hsm EXfer error: %s %s"%
                                (sys.argv,msg))
                    
                    if verbose > 1: traceback.print_exc()

                    if msg.args[1]==errno.ENOSPC:
                        try:
                            os.unlink(tempname)
                        except:
                            pass
                        print_data_access_layer_format(
                            requests[j]['infile'], requests[j]['outfile'], requests[j]['file_size'],
                            {'status':("ENOSPC", "No space left on device")})
                        jraise(errno.errorcode[errno.ENOSPC], "no space left on device");
                    ###XXX we shouldn't be matching literal strings here... this is really wrong                        
                    if msg.args[0] == "fd_xfer - read EOF unexpected":
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
                                                                      "Network problem or mover crash")})
                            jraise(errno.errorcode[errno.EPROTO],
                                   "encp.read_from_hsm: network problem or mover crash %s"%(msg,))

                            pass
                        control_socket.close()
                        print_data_access_layer_format(  requests[j]['infile'],  
                                                         requests[j]['outfile'], 
                                                         requests[j]['file_size'], 
                                                         done_ticket )
                        if not e_errors.is_retriable(done_ticket["status"][0]):
                            del(requests[j])
                            if files_left > 0: files_left = files_left - 1

                            print_error ('EPROTO',
                                         'encp.read_from_hsm: 2nd (post-file-send) mover callback on socket %s\
 failed to transfer,  done_ticket["status"]=%s' %(address, done_ticket["status"]),
                                         fatal=1)

                            error=1
                            break
                        print_error ('EPROTO',
                                     'encp.read_from_hsm: 2nd (post-file-send) mover callback on socket %s\
 failed to transfer,  done_ticket["status"]=%s' %(address, done_ticket["status"]),
                                     fatal=1)
                        pass

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
                if not error: _f_.close()
		data_path_socket_closed = 1 
		pass

	    pass	       # done with not ticket['mover']['local_mover']:

        t2 = time.time() #----------------------------------------Lap-Start

        # File has been read - wait for final dialog with mover.
        Trace.trace(8,"read_hsm_files waiting for final mover dialog on %s"%(control_socket,))
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
	control_socket_closed = 1
        Trace.trace(8,"read_hsm_files final dialog recieved")

        # make sure the mover thinks the transfer went ok
        if done_ticket["status"][0] != e_errors.OK:
	    error = 1
            try: os.remove(tempname)
            except: pass #XXX
	    # print error to stdout in data_access_layer format
	    print_data_access_layer_format(requests[j]['infile'], requests[j]['outfile'], 
                                           requests[j]['file_size'], done_ticket)
	    # exit here
	    if not e_errors.is_retriable(done_ticket["status"][0]):

		del(requests[j])
		if files_left > 0:
		    files_left = files_left - 1

		print_error ('EPROTO',
                             'encp.read_from_hsm: 2nd (post-file-read) mover callback on socket %s\
 failed to transfer, done_ticket["status"]=%s' %(address, done_ticket["status"]),
                             fatal=1)

		continue



            print_error ('EPROTO', 'encp.read_from_hsm: 2nd (post-file-read) mover callback on socket %s\
 failed to transfer, done_ticket["status"]=%s' %(address, done_ticket["status"]),
                         fatal=1)

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
                sys.stderr.write(
                    "warning: mover did not return CRC; skipping CRC check\n")
                
            elif mover_crc != mycrc :
		error = 1
		# print error to stdout in data_access_layer format
		done_ticket['status'] = (e_errors.READ_COMPCRC,'crc mismatch')
		print_data_access_layer_format(requests[j]['infile'], 
                                               requests[j]['outfile'], 
                                               requests[j]['file_size'],
                                               done_ticket)

                print_error('EPROTO',
                       "encp.read_from_hsm: CRC's mismatch: %s %s"%
                            (mover_crc, mycrc),fatal=0)

		# no retry for this case
		bytes = bytes+requests[j]['file_size']
		requests.remove(requests[j])
                try: os.remove(tempname)
                except os.error:
                    print_error('EACCES',
                                "cannot remove temporary file %s"%(tempname,),fatal=0)
		if files_left > 0:
		    files_left = files_left - 1

		continue
		

        tinfo["final_dialog%d"%(j,)] = time.time()-t2 #-----------Lap-End
        if verbose>1:
            print "  dt:",tinfo["final_dialog%d"%(j,)],"cumt=",time.time()-t0


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
            if tempname != outfile:
                try:  #the directory or file may have been deleted!
                    os.rename(tempname, outfile)
                except:
                    try:
                        os.unlink(tempname)
                    except:
                        pass
                    error = e_errors.NOACCESS
                    done_ticket['status']= (error,None)
                
                
	    bytes = bytes+requests[j]['file_size']
	    del(requests[j])
	    if files_left > 0:
		files_left = files_left - 1

        #print a message in d0 format if an error occured in the final rename,
        ##or if it was asked for explicitly

        if done_ticket['status'][0]==e_errors.OK:
            if tempname == '/dev/null':
               statinfo = os.stat(infile)
            else:
                statinfo = os.stat(outfile)
            fsize = statinfo[stat.ST_SIZE]
        else:
            fsize=0
        if data_access_layer or done_ticket['status'][0] != e_errors.OK:
	    print_data_access_layer_format(infile, outfile, 
                                           fsize, done_ticket)

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
            format = "  %s %s -> %s: %d bytes copied from %s at %.3g MB/S (%.3g MB/S) mover=%s drive_id=%s drive_sn=%s drive_vendor=%s cumt= %f  {'media_changer' : '%s'}"

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
    
    if not data_path_socket_closed: data_path_socket.close(); _f_.close()
    if not control_socket_closed: control_socket.close()
    return files_left, bytes, error


#######################################################################
def read_from_hsm(input_files, output,
                  config_host, config_port,
                  verbose=0, data_access_layer=0, chk_crc=0, use_IPC=0,
                  pri=1, delpri=0, agetime=0, delayed_dismount=None,
                  t0=0):
    if t0==0:
        t0 = time.time()
    Trace.trace(6,"read_from_hsm input_files=%s output=%s config_host=%s config_port=%s\
 verbose=%s  data_access_layer=%s chk_crc=%s t0=%s"%
                (input_files,output,config_host,config_port,verbose,data_access_layer,chk_crc,t0))
    tinfo = {}
    tinfo["abs_start"] = t0

    if verbose>2:
        print "Getting clients, storing/checking local info   cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # initialize - and get config, udp and log clients

    request_list = []


    vols_needed = {}
    maxretry = 2

    client = {}
    client['csc'],client['u'],client['uinfo'] = clients(config_host, config_port,verbose)

    tinfo["clients"] = time.time() - t1 #---------------------------------End
    if verbose>2:
        print "  dt:",tinfo["clients"], "   cumt=",time.time()-t0
    if verbose>3:
        print "csc=",client['csc']
        print "u=",client['u']
        print "logc=",logc
        print "uinfo=",client['uinfo']

    if verbose>2:
        print "Checking input pnfs files:",input_files, "   cumt=",time.time()-t0
    t1 =  time.time() #---------------------------------------------------Start

    #check the input unix files. if files don't exits, we bomb out to the user
    (ninput, inputlist, file_size) = inputfile_check(input_files)
    (bfid,junk,junk,junk,junk,pinfo,p)=pnfs_information(inputlist,ninput)

    tinfo["pnfscheck"] = time.time() - t1 #--------------------------------End
    if verbose>2:
        print "  dt:",tinfo["pnfscheck"], "   cumt=",time.time()-t0
    if verbose>3:
        print "ninput=",ninput
        print "inputlist=",inputlist
        print "file_size=",file_size
        print "bfid=",bfid
        print "pinfo=",pinfo
        print "p=",p

    if verbose>2:
        print "Checking output unix files:",output, "   cumt=",time.time()-t0
    t1 = time.time() #---------------------------------------------------Start

    # check (and generate) the output files(s)
    # bomb out if they exist already
    outputlist = outputfile_check(ninput,inputlist,output)

    tinfo["filecheck"] = time.time() - t1 #--------------------------------End
    if verbose>2:
        print "  dt:",tinfo["filecheck"], "   cumt=",time.time()-t0
    if verbose>3:
        print "outputlist=",outputlist

    if verbose>2:
        print "Requesting callback ports", "   cumt=",time.time()-t0
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
        print "  dt:",tinfo["get_callback"], "   cumt=",time.time()-t0

    if verbose>1:
        print "Calling Config Server to find file clerk  cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # ask configuration server what port the file clerk is using
    Trace.trace(10,"read_from_hsm calling config server to find file clerk")
    fticket = client['csc'].get("file_clerk")
    if fticket['status'][0] != e_errors.OK:
	print_data_access_layer_format('', '', 0, fticket)
	jraise(fticket['status'][0], ' encp.read_from_hsm: ticket["status"]=%s' %(fticket['status'],))

    file_clerk_address = (fticket["hostip"],fticket["port"])
    Trace.trace(10,"read_from_hsm file clerk at host=%s port=%s"
                %(fticket["hostip"],fticket["port"]))

    tinfo["get_fileclerk"] = time.time() - t1 #-----------------------------End
    if verbose>1:
        print " ",fticket["hostip"],fticket["port"]
        print "  dt:", tinfo["get_fileclerk"], "   cumt=",time.time()-t0

    # ask configuration server what port the volume clerk is using
    Trace.trace(10,"read_from_hsm calling config server to find volume clerk")
    vticket = client['csc'].get("volume_clerk")
    if vticket['status'][0] != e_errors.OK:
	print_data_access_layer_format('', '', 0, vticket)
	jraise(vticket['status'][0], ' encp.read_from_hsm: ticket["status"]=%s' %
               (vticket['status'],))
    volume_clerk_address = (vticket["hostip"],vticket["port"])
    Trace.trace(10,"read_from_hsm volume clerk at host=%s port=%s"
                %(vticket["hostip"],vticket["port"]))

    tinfo["get_volumeclerk"] = time.time() - t1 #-----------------------------End
    if verbose>1:
        print " ",vticket["hostip"],vticket["port"]
        print "  dt:", tinfo["get_volumeclerk"], "   cumt=",time.time()-t0

    if verbose>1:
        print "Calling file clerk for file info", "   cumt=",time.time()-t0
    t1 = time.time() # ---------------------------------------------------Start

    nfiles = 0
    # call file clerk and get file info about each bfid
    for i in range(0,ninput):
        t2 = time.time() # -------------------------------------------Lap-Start
        Trace.trace(7,"read_from_hsm calling file clerk for bfid=%s"%(bfid[i],))

        binfo  = client['u'].send({'work': 'bfid_info', 'bfid': bfid[i]},
                        (fticket['hostip'],fticket['port']))
        if binfo['status'][0]!=e_errors.OK:
	    print_data_access_layer_format('', '', 0, binfo)
            jraise(errno.errorcode[errno.EPROTO],
                   "encp.read_from_hsm:  cannot get info on bfid %s"%(bfid[i],))
        Trace.trace(7,"read_from_hsm on volume=%s"%
                    (binfo['fc']['external_label'],))
        inhibit = binfo['vc']['system_inhibit'][0]
        if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
	    binfo['status'] = (inhibit, None)
	    print_data_access_layer_format('', '', 0, binfo)
	    continue
        inhibit = binfo['vc']['user_inhibit'][0]
        if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
	    binfo['status'] = (inhibit, None)
	    print_data_access_layer_format('', '', 0, binfo)
	    continue
	if binfo["fc"]["deleted"] == "yes":
	    binfo['status'] = (e_errors.DELETED, None)
	    print_data_access_layer_format('', '', 0, binfo)
	    continue


	request = {}
	binfo['vc']['address'] = volume_clerk_address
	binfo['fc']['address'] = file_clerk_address
	request['vinfo'] = binfo['vc']
	request['finfo'] = binfo['fc']
	request['volume'] = binfo['fc']['external_label']
	request['bfid'] = bfid[i]
	request['infile'] = inputlist[i]
	request['outfile'] = outputlist[i]
	request['pinfo'] = pinfo[i]
	request['file_size'] = file_size[i]
	request['retry'] = 0
	request['unique_id'] = ''

        label = binfo['fc']['external_label']
	wr = {}
	for key in client['uinfo'].keys():
	    wr[key] = client['uinfo'][key]

	# make the part of the ticket that encp knows about (there's more later)
	encp_el = {}
	encp_el["basepri"] = pri
	encp_el["adminpri"] = -1
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

    if (nfiles == 0): sys.exit(1)
    tinfo['fc'] =  time.time() - t1 #-------------------------------End
    if verbose>1:
        print "  dt:",tinfo["fc"], "   cumt=",time.time()-t0

    if verbose:
        print "Submitting read requests", "   cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    total_bytes = 0

    # loop over all volumes that are needed and submit all requests for
    # that volume. Read files from each volume before submitting requests
    # for different volumes.

    files_left = nfiles
    retry_flag = 0
    bytes = 0
    while files_left:

	system_enabled(p) # make sure system is still enabled before submitting
	(submitted,Qd) = submit_read_requests(request_list,
					      client, tinfo, 
					      vols_needed.keys(),
					      files_left,
					      verbose, 
					      retry_flag)


	tinfo["send_ticket"] = time.time() - t1 #---------------------------End
	if verbose:
	    print Qd
	if verbose>1:
	    print "  dt:",tinfo["send_ticket"], "   cumt=",time.time()-t0

	# We have placed our work in the system and now we have to 
	# wait for resources. All we need to do is wait for the system
	# to call us back, and make sure that is it calling _us_ back,
	# and not some sort of old call-back to this very same port. 
	# It is dicey to time out, as it is probably legitimate to 
	# wait for hours....
	if submitted != 0:
	    files_left, brcvd, error = read_hsm_files(listen_socket, submitted,
						      files_left, request_list,
						      tinfo, t0, chk_crc, use_IPC, 
						      data_access_layer, 
						      maxretry, verbose)
	    bytes = bytes + brcvd
	    if verbose: print "FILES_LEFT ", files_left
	    if files_left > 0:
		retry_flag = 1
	else: 
	    files_left = 0
	    error = 1

    # we are done transferring - close out the listen socket
    listen_socket.close()

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

    msg ="Complete: %s bytes in %s files in %s sec.  Overall rate = %s MB/sec" % (
        total_bytes,ninput,tf-t0,done_ticket["MB_per_S"])

    if verbose:
        print msg

    if verbose > 3:
        print "DONE TICKET"
        pprint.pprint(done_ticket)

    Trace.trace(6,"read_from_hsm "+msg)
    sys.exit(error)

    # tell file clerk we are done - this allows it to delete our unique id in
    # its dictionary - this keeps things cleaner and stops memory from growing
    #u.send_no_wait({"work":"done_cleanup"}, (fticket['hostip'], fticket['port']))

    
##############################################################################

class encp(interface.Interface):

    def __init__(self):
        self.chk_crc = 0           # we will not check the crc unless told to
        self.priority = 1          # lowest priority
        self.delpri = 0            # priority doesn't change
        self.age_time = 0          # priority doesn't age
        self.data_access_layer = 0 # no special listings
        self.verbose = 0           # no output yet

	self.delayed_dismount = 0  # delayed dismount time is set to 0
	self.use_IPC = 0
	self.output_file_family = '' # initial set for use with --ephemeral or
	                             # or --file_family

        self.bytes = None
        self.test_mode = 0
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()

    ##########################################################################
    # define the command line options that are valid
    def options(self):
        the_options = self.config_options()+[
                      "verbose=","crc","priority=","delpri=","age_time=",
                      "delayed_dismount=", "file_family=", "ephemeral",
                      "data_access_layer","bytes=", "test_mode", "use_IPC"
                      ] + self.help_options()

        return the_options

    ##########################################################################
    #  define our specific help
    def help_line(self):
        prefix = self.help_prefix()
        the_help = "%s%s\n or\n %s%s%s" % (
            prefix, self.parameters1(),
            prefix, self.parameters2(),
            self.format_options(self.options(), "\n\t\t")
            )
        return the_help

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
        arglen = len(self.args)
        if arglen < 2 :
            print "ERROR: not enough arguments specified"
            self.print_help()
            sys.exit(1)

        # get fullpaths to the files
        p = []
        for i in range(0,arglen):
            (machine, fullname, dir, basename) = fullpath(self.args[i])
            self.args[i] = os.path.join(dir,basename)
            p.append(string.find(dir,"/pnfs"))

        # all files on the hsm system have /pnfs/ as 1st part of their name
        # scan input files for /pnfs - all have to be the same
        p1 = p[0]
        p2 = p[arglen-1]
        self.input = [self.args[0]]
        self.output = [self.args[arglen-1]]
        for i in range(1,len(self.args)-1):
            if p[i]!=p1:
                if p1:
                    print "ERROR: Not all input files are /pnfs/... files"
                else:
                    print "ERROR: Not all input files are unix files"
                sys.exit(1)
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

        # tracing info
        dictlist = ""
        for key in self.__dict__.keys():
            dictlist = "%s %s:%s"%(dictlist,key,self.__dict__[key])
        Trace.trace(16,"encp.parse_options objectdict=%s"%(dictlist,))


##############################################################################

if __name__  ==  "__main__" :
    t0 = time.time()
    Trace.init("ENCP")
    Trace.trace( 6, 'encp called at %s: %s'%(t0,sys.argv) )

    # use class to get standard way of parsing options
    e = encp()
    if e.test_mode:
        print "WARNING: running in test mode"

    if e.verbose>5 and Trace is Trace_lite:
        def trace(*args):
            for arg in args[1:]:
                print arg,
            print
        Trace.trace = trace
    ## have we been called "encp unixfile hsmfile" ?
    if e.intype=="unixfile" and e.outtype=="hsmfile" :
        write_to_hsm(e.input,  e.output, e.output_file_family,
                     e.config_host, e.config_port,
                     e.verbose, e.data_access_layer,
                     e.chk_crc, e.use_IPC,
                     e.priority, e.delpri, e.age_time,
                     e.delayed_dismount, t0, e.bytes)

    ## have we been called "encp hsmfile unixfile" ?
    elif e.intype=="hsmfile" and e.outtype=="unixfile" :
        read_from_hsm(e.input, e.output,
                      e.config_host, e.config_port,
                      e.verbose, e.data_access_layer,
                      e.chk_crc, e.use_IPC,
                      e.priority, e.delpri, e.age_time,
                      e.delayed_dismount, t0)

    ## have we been called "encp unixfile unixfile" ?
    elif e.intype=="unixfile" and e.outtype=="unixfile" :
        print "encp copies to/from hsm. It is not involved in copying %s to %s" % (e.intype, e.outtype)

    ## have we been called "encp hsmfile hsmfile?
    elif e.intype=="hsmfile" and e.outtype=="hsmfile" :
        print "encp hsm to hsm is not functional. Copy hsmfile to local disk and them back to hsm"

    else:
        emsg = "ERROR: Can not process arguments %s"%(e.args,)
        Trace.trace(6,emsg)
        print_data_access_layer_format("","",0,{'status':("EPROTO","Cannot parse arguments")})
        jraise(errno.errorcode[errno.EPROTO],emsg)

    Trace.trace(10,"encp finished at %s"%(time.time(),))


