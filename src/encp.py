import sys
import os
import stat
import time
import errno
import pprint
import pwd
import grp
import socket
import binascii
import regsub

# enstore modules
import pnfs
import callback
import log_client
import configuration_client
import udp_client
import EXfer


##############################################################################

def write_to_hsm(input, output, u, csc, logc, list, chk_crc) :
    t0 = time.time()
    tinfo = {}
    tinfo["abs_start"] = t0

    unixfile = input[0]
    pnfsfile = output[0]

    # first check the unix file the user specified
    t1 = time.time()
    if list:
        print "Checking",unixfile
    (machine, fullname, dir, basename) = fullpath(unixfile)
    #in_file = open(unixfile, "r")<--------------------------------------------------moved later
    command="if test -r "+unixfile+"; then echo ok; else echo no; fi"
    readable = os.popen(command,'r').readlines()
    if "ok\012" != readable[0] :
        jraise(errno.errorcode[errno.EACCES]," encp.write_to__hsm: "\
               +unixfile+", NO read access to file")
    statinfo = os.stat(unixfile)
    fsize = statinfo[stat.ST_SIZE]
    if not stat.S_ISREG(statinfo[stat.ST_MODE]) :
        jraise(errno.errorcode[errno.EPERM]," encp.write_to_hsm: "\
               +unixfile+" is not a regular file")
    tinfo["filecheck"] = time.time() - t1
    if list:
        print "  dt:",tinfo["filecheck"], "   cum=",time.time()-t0

    # check the output pnfs file next
    t1 = time.time()
    if list:
        print "Checking",pnfsfile, "   cum=",time.time()-t0
    p = pnfs.pnfs(pnfsfile)
    if p.valid != pnfs.valid :
        jraise(errno.errorcode[errno.EINVAL]," encp.write_to_hsm: "\
               +pnfsfile+" is an invalid pnfs filename "\
               +" or maybe NO read access to file")
    if p.exists == pnfs.exists :
        jraise(errno.errorcode[errno.EEXIST]," encp.write_to_hsm: "\
               +pnfsfile+" already exists")
    if p.writable != pnfs.enabled :
        jraise(errno.errorcode[errno.EACCES]," encp.write_to_hsm: "\
               +pnfsfile+", NO write access to directory")
    running = p.check_pnfs_enabled()
    if running != pnfs.enabled :
        jraise(errno.errorcode[errno.EACCES]," encp.write_to_hsm: "\
               +"system disabled"+running)
    tinfo["pnfscheck"] = time.time() - t1
    if list:
        print "  dt:",tinfo["pnfscheck"], "   cum=",time.time()-t0

    # store some local information as part of ticket
    t1 = time.time()
    if list:
        print "Storing local info   cum=",time.time()-t0

    # make the pnfs dictionary that will be part of the ticket
    pinfo = {}
    for k in [ 'pnfsFilename','gid', 'gname','uid', 'uname',\
               'major','minor','rmajor','rminor',\
               'mode','pstat' ] :
        exec("pinfo["+repr(k)+"] = p."+k)

    # let's save who's talking to us
    uinfo = {}
    uinfo['uid'] = os.getuid()
    uinfo['euid'] = os.geteuid()
    uinfo['gid'] = os.getgid()
    uinfo['egid'] = os.getegid()
    uinfo['gname'] = grp.getgrgid(uinfo['gid'])[0]
    uinfo['uname'] = pwd.getpwuid(uinfo['uid'])[0]
    uinfo['machine'] = os.uname()
    uinfo['fullname'] = (machine,fullname)

    tinfo["localinfo"] = time.time() - t1
    if list:
        print "  dt:",tinfo["localinfo"], "   cum=",time.time()-t0

    # get a port to talk on and listen for connections
    t1 = time.time()
    if list:
        print "Requesting callback ports", "   cum=",time.time()-t0
    host, port, listen_socket = callback.get_callback()
    listen_socket.listen(4)
    tinfo["get_callback"] = time.time() - t1
    if list:
        print "  ",host,port,"dt:",tinfo["get_callback"],\
               "   cum=",time.time()-t0

    # generate the work ticket
    ticket = {"work"               : "write_to_hsm",
              "library"            : p.library,
              "file_family"        : p.file_family,
              "file_family_width"  : p.file_family_width,
              "orig_filename"      : unixfile,
              "pnfs_info"          : pinfo,
              "user_info"          : uinfo,
              "mtime"              : int(time.time()),
              "size_bytes"         : fsize,
              "sanity_size"        : 5000,
              "user_callback_port" : port,
              "user_callback_host" : host,
              "unique_id"          : time.time()
              }

    maxretry = 2
    retry = maxretry
    while retry:
        # ask configuration server what port the right library manager is using
        t1 = time.time()
        if list:
            print "Calling Config Server to find",p.library+".library_manager",\
                   "   cum=",time.time()-t0
        vticket = csc.get(p.library+".library_manager")
        tinfo["get_libman"] = time.time() - t1
        if list:
            print "  ",vticket["host"],vticket["port"],"dt:",tinfo["get_libman"],\
                  "   cum=",time.time()-t0

        # send the work ticket to the library manager
        t1 = time.time()
        if list:
            print "Sending ticket to",p.library+".library_manager", \
                  "   cum=",time.time()-t0
        tinfo["tot_to_send_ticket"] = t1 -t0
        ticket = u.send(ticket, (vticket['host'], vticket['port']))
        if ticket['status'] != "ok" :
            jraise(errno.errorcode[errno.EPROTO]," encp.write_to_hsm: from "\
                   +"u.send to " +p.library+" at "\
                   +vticket['host']+"/"+repr(vticket['port'])\
                   +", ticket[\"status\"]="+ticket["status"])
        tinfo["send_ticket"] = time.time() - t1
        if list:
            print "  Q'd:",unixfile, ticket["library"], \
                  "family:",ticket["file_family"],\
                  "bytes:", ticket["size_bytes"],\
                  "dt:",tinfo["send_ticket"], "   cum=",time.time()-t0

        # We have placed our work in the system and now we have to wait for
        # resources. All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        if list:
            print "Waiting for mover to call back", "   cum=",time.time()-t0
        while 1 :
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_socket(control_socket, "encp write_"+\
                                                  "to_hsm, mover call back")
            if ticket["unique_id"] == new_ticket["unique_id"] :
                listen_socket.close()
                break
            else:
                print ("encp write_to_hsm: imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"] != "ok" :
            jraise(errno.errorcode[errno.EPROTO]," encp.write_to_hsm: "\
                   +"1st (pre-file-send) mover callback on socket "\
               +repr(address)+", failed to setup transfer: "\
               +"ticket[\"status\"]="+ticket["status"],2)
        data_path_socket = callback.mover_callback_socket(ticket)

        # If the system has called us back with our own  unique id, call back
        # the mover on the mover's port and send the file on that port.
        t1 = time.time()
        tinfo["tot_to_mover_callback"] = t1 - t0
        if list:
            print "  ",ticket["mover_callback_host"],\
                  ticket["mover_callback_port"], \
                  "   cum=",time.time()-t0

        t1 = time.time()
        mycrc = 0
        in_file = open(unixfile, "r")
        if list:
            print "Sending data", "   cum=",time.time()-t0

        try:
            mycrc = EXfer.usrTo_( in_file, data_path_socket, binascii.crc_hqx,
                                  65536/2, chk_crc )
            retry = 0
        except:
            print "Error with encp EXfer - continuing";traceback.print_exc()
            ticket = {
              "work"               : "write_to_hsm",
              "priority"           : 5,
              "library"            : p.library,
              "file_family"        : p.file_family,
              "file_family_width"  : p.file_family_width,
              "orig_filename"      : unixfile,
              "pnfs_info"          : pinfo,
              "user_info"          : uinfo,
              "mtime"              : int(time.time()),
              "size_bytes"         : fsize,
              "sanity_size"        : 5000,
              "user_callback_port" : port,
              "user_callback_host" : host,
              "unique_id"          : time.time()
              }
            retry = retry - 1
            data_path_socket.close()
            in_file.close()
            done_ticket = callback.read_tcp_socket(control_socket,
                      "encp write_to_hsm, error dialog")
            control_socket.close()
            print done_ticket

    data_path_socket.close()
    in_file.close()
    t2 = time.time()
    tinfo["sent_bytes"] = t2-t1
    tinfo["tot_to_sent_bytes"] = t2-t0
    if list:
        if t1!=t2:
            sent_rate = 1.*fsize/1024./1024./(t2-t1)
        else:
            sent_rate = 0.0
        print "  bytes:",fsize,"dt:",tinfo["sent_bytes"],"=",sent_rate,"MB/S",\
              "   cum=",time.time()-t0

    # File has been sent - wait for final dialog with mover. We know the file
    # has hit some sort of media.... when this occurs. Create a file in pnfs
    # namespace with information about transfer.
    t1 = time.time()
    if list:
        print "Waiting for final mover dialog", "   cum=",time.time()-t0
    done_ticket = callback.read_tcp_socket(control_socket,
                      "encp write_to_hsm, mover final dialog")
    control_socket.close()
    tinfo["final_dialog"] = time.time()-t1
    if list:
        print "  dt:",tinfo["final_dialog"], "   cum=",time.time()-t0

    if done_ticket["status"] == "ok" :
        if chk_crc != 0:
            if done_ticket["complete_crc"] != mycrc :
                print "CRC error",complete_crc, mycrc
        t1 = time.time()
        if list:
            print "Adding file to pnfs", "   cum=",time.time()-t0
        p.set_bit_file_id(done_ticket["bfid"],done_ticket["size_bytes"])
        p.set_xreference(done_ticket["file_clerk"]["external_label"],
                         done_ticket["file_clerk"]["bof_space_cookie"])
        tinfo["pnfsupdate"] = time.time() - t1
        if list:
            print "  dt:",tinfo["pnfsupdate"], "   cum=",time.time()-t0

        tinfo["total"] = time.time()-t0
        done_ticket["tinfo"] = tinfo
        tf = time.time()
        if tf!=t0:
            done_ticket["MB_per_S"] = 1.*fsize/1024./1024./(tf-t0)
        else:
            done_ticket["MB_per_S"] = 0.0

        t1 = time.time()
        if list:
            print "Adding transaction log to pnfs", "   cum=",time.time()-t0
        done_formatted  = pprint.pformat(done_ticket)
        p.set_info(done_formatted)
        t2 = time.time() - t1
        if list:
            print "  dt:",t2, "   cum=",time.time()-t0

        if list:
            fticket=done_ticket["file_clerk"]
            print p.pnfsFilename, ":",p.file_size,"bytes",\
                  "copied to", done_ticket["external_label"], \
                  "in ",tinfo["total"],"seconds",\
                  "at",done_ticket["MB_per_S"],"MB/S", "   cum=",time.time()-t0
            #print done_formatted

        format = "%s -> %s : %d bytes copied to %s in  %f seconds "+\
                 "at %f MB/S requestor:%s   cum= %f seconds"
        logc.send(log_client.INFO, 2, format, uinfo["fullname"],
                              p.pnfsFilename, p.file_size,
                              done_ticket["external_label"], tinfo["total"],
                              done_ticket["MB_per_S"], uinfo["uname"],
                              time.time()-t0)

    else :
        jraise(errno.errorcode[errno.EPROTO]," encp.write_to_hsm: "\
               +"2nd (post-file-send) mover callback on socket "\
               +repr(address)+", failed to transfer: "\
               +"done_ticket[\"status\"]="+done_ticket["status"])

    # tell library manager we are done - this allows it to delete our unique id in
    # its dictionary - this keeps things cleaner and stops memory from growing
    #u.send_no_wait({"work":"done_cleanup"}, (vticket['host'], vticket['port']))

##############################################################################

def read_from_hsm(input, output, u, csc, logc, list, chk_crc) :
    t0 = time.time()
    tinfo = {}
    tinfo["abs_start"] = t0

    # create internal list of input files even if just 1 file passed in
    try:
        ninput = len(input)
        inputlist = input
    except TypeError:
        inputlist = [input]
        ninput = 1

    # can only handle 1 input file  copied to 1 output file
    #  or      multiple input files copied to 1 output directory
    # this is just the current policy - nothing fundamental about it
    try:
        noutput = len(output)
        jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
               "can not handle multiple output files: "+output)
    except TypeError:
        pass

    # if user specified multiple input files, then output must be a directory
    outputlist = []
    if ninput!=1:
        try:
            statinfo = os.stat(output[0])
        except os.error:
            jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
                   "multiple input files can not be copied to non-existant "\
                   +"directory "+output[0])
        if not stat.S_ISDIR(statinfo[stat.ST_MODE]) :
            jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
                   "multiple input files must be copied to a directory, not "\
                   +output[0])

    bfid = []
    file_size = []
    pinfo = []
    finfo = []
    vinfo = []
    volume = []

    # first check the input pnfs files and get all info from pnfs that is needed
    # if files don't exits, we bomb out to the user
    if list:
        print "Checking input files:",inputlist, "   cum=",time.time()-t0
    t1 =  time.time()
    for i in range(0,ninput):

        # on reads, the file must exist in pnfs
        p = pnfs.pnfs(input[i])
        if p.exists != pnfs.exists :
            jraise(errno.errorcode[errno.ENOENT]," encp.read_from_hsm: "\
                   +inputlist[i]+" does not exist")

        # input files can't be directories
        if not stat.S_ISREG(p.pstat[stat.ST_MODE]) :
            jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
                   +input[i]+" is not a regular file")

        # get the most important info and store into separate lists
        bfid.append(p.bit_file_id)
        file_size.append(p.file_size)

        # get all the required pnfs info for the ticket
        pinf = {}
        for k in [ 'pnfsFilename','gid', 'gname','uid', 'uname',\
                   'major','minor','rmajor','rminor',\
                   'mode','pstat' ] :
            exec("pinf["+repr(k)+"] = p."+k)
        pinfo.append(pinf)

        # we need to check (just ?) once that the system is running
        if i==0:
            running = p.check_pnfs_enabled()
            if running != pnfs.enabled :
                jraise(errno.errorcode[errno.EACCES]," encp.read_from_hsm: "\
                       +"system disabled"+running)

    tinfo["pnfscheck"] = time.time() - t1
    if list:
        print "  dt:",tinfo["pnfscheck"], "   cum=",time.time()-t0


    # Make sure we can open the unixfiles. If we can't, we bomb out to user
    if list:
        print "Checking outputput files:",output, "   cum=",time.time()-t0
    t1 = time.time()
    for i in range(0,ninput):
        outputlist.append(output[0])

        # see if output file exists
        try:
            statinfo = os.stat(outputlist[i])
            itexists = 1

        # if output doesn't exist, then at least directory must exist
        except os.error:
            itexists = 0
            (omachine, ofullname, odir, obasename) = fullpath(outputlist[i])
            try:
                statinfo = os.stat(odir)
            except os.error:
                jraise(errno.errorcode[errno.EEXIST]," encp.read_to_hsm: "\
                       "base directory doesn't exist for "+outputlist[i])

        # if output file exists, then it must be a directory
        # note: removed from try block itexist=1 try block to isolate errors
        if itexists:
            if stat.S_ISDIR(statinfo[stat.ST_MODE]) :
                (omachine, ofullname, odir, obasename) = fullpath(outputlist[i])
                (imachine, ifullname, idir, ibasename) = fullpath(inputlist[i])
                # take care of missing filenames (just directory)
                if obasename=='.' or len(obasename)==0:
                    outputlist[i] = odir+'/'+ibasename
                else:
                    outputlist[i] = ofullname+'/'+ibasename
                (omachine, ofullname, odir, obasename) = fullpath(outputlist[i])
            else:
                jraise(errno.errorcode[errno.EEXIST]," encp.read_to_hsm: "\
                       +outputlist[i]+" already exists")

        # need to check that directory is writable
        # since all files go to one output directory, one check is enough
        if i==0:
            command="if test -w "+odir+"; then echo ok; else echo no; fi"
            writable = os.popen(command,'r').readlines()
            if "ok\012" != writable[0] :
                jraise(errno.errorcode[errno.EACCES]," encp.read_from__hsm: "\
                       +" NO write access to directory"+odir)
    tinfo["filecheck"] = time.time() - t1
    if list:
        print " ",outputlist
        print "  dt:",tinfo["filecheck"], "   cum=",time.time()-t0

    # store some local information as part of ticket
    if list:
        print "Storing local info   cum=",time.time()-t0

    t1 = time.time()
    uinfo = {}
    uinfo['uid'] = os.getuid()
    uinfo['gid'] = os.getgid()
    uinfo['gname'] = grp.getgrgid(uinfo['gid'])[0]
    uinfo['uname'] = pwd.getpwuid(uinfo['uid'])[0]
    uinfo['machine'] = os.uname()
    uinfo['fullname'] = outputlist[0]

    tinfo["localinfo"] = time.time() - t1
    if list:
        print "  dt:",tinfo["localinfo"], "   cum=",time.time()-t0

    # get a port to talk on and listen for connections
    t1 = time.time()
    if list:
        print "Requesting callback ports", "   cum=",time.time()-t0
    host, port, listen_socket = callback.get_callback()
    listen_socket.listen(4)
    tinfo["get_callback"] = time.time() - t1
    if list:
        print " ",host,port
        print "  dt:",tinfo["get_callback"], "   cum=",time.time()-t0

    # generate the work ticket
    ticket = {"work"               : "read_from_hsm",
              "pnfs_info"          : pinfo[0],
              "user_info"          : uinfo,
              "bfid"               : bfid[0],
              "sanity_size"        : 5000,
              "user_callback_port" : port,
              "user_callback_host" : host,
              "unique_id"          : time.time()
              }

    # ask configuration server what port the file clerk is using
    t1 = time.time()
    if list:
        print "Calling Config Server to find file clerk", \
              "   cum=",time.time()-t0
    fticket = csc.get("file_clerk")
    tinfo["get_fileclerk"] = time.time() - t1
    if list:
        print "  ",fticket["host"],fticket["port"],"dt:",\
              tinfo["get_fileclerk"], "   cum=",time.time()-t0

    # call file clerk and get file info about each bfid
    if list:
        print "Calling file clerk for file info", "   cum=",time.time()-t0
    t1 = time.time()
    for i in range(0,ninput):
        binfo  = u.send({'work': 'bfid_info', 'bfid': bfid[i]},
                        (fticket['host'],fticket['port']))
        if binfo['status']!='ok':
            pprint.pprint(binfo)
            jraise(errno.errorcode[errno.EPROTO]," encp.read_from__hsm: "\
                   +" can not get info on bfid"+repr(bfid[i]))
        vinfo.append(binfo['volume_clerk'])
        finfo.append(binfo['file_clerk'])
        volume.append(binfo['file_clerk']['external_label'])
    tinfo['file_clerk'] =  time.time() - t1
    if list:
        print "  dt:",tinfo["file_clerk"], "   cum=",time.time()-t0

    # send work ticket to file clerk who sends it to right library manger
    if list:
        print "Sending ticket to file clerk", "   cum=",time.time()-t0
    t1 = time.time()
    ticket = u.send(ticket, (fticket['host'], fticket['port']))
    if ticket['status'] != "ok" :
        jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: from"\
               +"u.send to file_clerk at "+fticket['host']+"/"\
               +repr(fticket['port']) +", ticket[\"status\"]="\
               +ticket["status"])
    tinfo["send_ticket"] = time.time() - t1
    if list :
        print "  Q'd:",inputlist[0], bfid[0], \
              "bytes:",file_size[0], "on",\
              finfo[0]["external_label"],finfo[0]["bof_space_cookie"],\
              "dt:",tinfo["send_ticket"], "   cum=",time.time()-t0


    # We have placed our work in the system and now we have to wait for
    # resources. All we  need to do is wait for the system to call us back,
    # and make sure that is it calling _us_ back, and not some sort of old
    # call-back to this very same port. It is dicey to time out, as it
    # is probably legitimate to wait for hours....
    if list:
        print "Waiting for mover to call back", "   cum=",time.time()-t0
    while 1 :
        control_socket, address = listen_socket.accept()
        new_ticket = callback.read_tcp_socket(control_socket, "encp read_"+\
                                              "to_hsm, mover call back")
        if ticket["unique_id"] == new_ticket["unique_id"] :
            listen_socket.close()
            break
        else:
            print ("encp read_from_hsm: imposter called us back, trying again")
            control_socket.close()
    ticket = new_ticket
    if ticket["status"] != "ok" :
        jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
               +"1st (pre-file-read) mover callback on socket "\
               +repr(address)+", failed to setup transfer: "\
               +"ticket[\"status\"]="+ticket["status"])
    data_path_socket = callback.mover_callback_socket(ticket)

    # If the system has called us back with our own  unique id, call back
    # the mover on the mover's port and read the file on that port.
    t1 = time.time()
    tinfo["tot_to_mover_callback"] = t1 - t0
    if list:
        print "  ",ticket["mover_callback_host"],\
              ticket["mover_callback_port"], \
              "cum_time:",tinfo["tot_to_mover_callback"], \
              "   cum=",time.time()-t0

    t1 = time.time()
    if list:
        print "Receiving data", "   cum=",time.time()-t0
    l = 0
    mycrc = 0
    f = open(outputlist[0],"w")
    while 1:
        buf = data_path_socket.recv(65536*4)
        l = l + len(buf)
        if len(buf) == 0 : break
        if chk_crc != 0 :
            mycrc = binascii.crc_hqx(buf,mycrc)
        f.write(buf)
    data_path_socket.close()
    f.close()
    fsize = l
    t2 = time.time()
    tinfo["recvd_bytes"] = t2-t1
    if list:
        print "  bytes:",l,"dt:",tinfo["recvd_bytes"], \
              "   cum=",time.time()-t0

    # File has been read - wait for final dialog with mover.
    t1 = time.time()
    if list:
        print "Waiting for final mover dialog", "   cum=",time.time()-t0
    done_ticket = callback.read_tcp_socket(control_socket, "encp read_"+\
                                           "to_hsm, mover final dialog")
    control_socket.close()

    if done_ticket["status"] == "ok" :

        if chk_crc != 0 :
            if done_ticket["complete_crc"] != mycrc :
                print "CRC error",complete_crc, mycrc

        tinfo["final_dialog"] = time.time()-t1
        if list:
            print "  dt:",tinfo["final_dialog"], "   cum=",time.time()-t0

        t1 = time.time()
        if 0:
            if list:
                print "Updating pnfs last parked", "   cum=",time.time()-t0
            try:
                p.set_lastparked(repr(uinfo['fullname']))
            except:
                print "Failed to update last parked info"
            tinfo["last_parked"] = time.time()-t1
            if list:
                print "  dt:",tinfo["last_parked"], "   cum=",time.time()-t0

        tinfo["total"] = time.time()-t0
        done_ticket["tinfo"] = tinfo
        tf = time.time()
        if tf!=t0:
            done_ticket["MB_per_S"] = 1.*fsize/1024./1024./(tf-t0)
        else:
            done_ticket["MB_per_S"] = 0.0

        if list:
            print outputlist[0], ":",fsize,"bytes",\
                  "copied from", done_ticket["external_label"], \
                  "in ",tinfo["total"],"seconds",\
                  "at",done_ticket["MB_per_S"],"MB/S", \
                  "   cum=",time.time()-t0
            #done_formatted  = pprint.pformat(done_ticket)
            #print done_formatted

        format = "%s -> %s : %d bytes copied from %s in  %f seconds at "+\
                     "%s MB/S  requestor:%s     cum= %f"
        logc.send(log_client.INFO, 2, format, inputlist[0],
                              uinfo["fullname"], fsize,
                              done_ticket["external_label"], tinfo["total"],
                              done_ticket["MB_per_S"], uinfo["uname"],
                              time.time()-t0)

    else :
        jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
               +"2nd (post-file-read) mover callback on socket "\
               +repr(address)+", failed to transfer: "\
               +"done_ticket[\"status\"]="+done_ticket["status"])

    # tell file clerk we are done - this allows it to delete our unique id in
    # its dictionary - this keeps things cleaner and stops memory from growing
    #u.send_no_wait({"work":"done_cleanup"}, (fticket['host'], fticket['port']))

##############################################################################

def jraise(errcode,errmsg,exit_code=1) :
    format = "Fatal error:"+str(errcode)+str(errmsg)
    print format
    logc.send(log_client.ERROR, 1, format)
    sys.exit(exit_code)

##############################################################################

def fullpath(filename):
    machine = socket.gethostbyaddr(socket.gethostname())[0]
    dir, file = os.path.split(filename)
    if dir == '' :
        dir = os.getcwd()
    command="(cd "+dir+";pwd)"
    try:
        dir = regsub.sub("\012","",os.popen(command,'r').readlines()[0])
        filename = dir+"/"+file
    except:
        pass

    filename = regsub.sub("//","/",filename)
    dir = regsub.sub("//","/",dir)
    file = regsub.sub("//","/",file)

    return (machine, filename, dir, file)

##############################################################################

if __name__  ==  "__main__" :
    import getopt
    import string

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0
    list = 0
    chk_crc = 1

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list", \
               "nocrc","list","verbose","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--nocrc":
            chk_crc = 0
        elif opt == "--list" or opt == "--verbose":
            list = 1
        elif opt == "--help" :
            print "python", sys.argv[0], options, "inputfilename outputfilename"
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    # bomb out if we don't have an input and an output
    arglen = len(args)
    if arglen < 2 :
        print "python",sys.argv[0], options, "inputfilename outputfilename"
        print "-or-"
        print "python",sys.argv[0], options, "inputfilename1 ... inputfilenameN outputdirectory"
        print "   do not forget the '--' in front of each option"
        sys.exit(1)

    # get a configuration server
    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client.configuration_client(config_host,config_port)
    u = udp_client.UDPClient()

    # get a logger
    logc = log_client.LoggerClient(csc, 'ENCP', 'logserver', 0)

    # get fullpaths to the files
    p = []
    for i in range(0,arglen):
        (machine, fullname, dir, basename) = fullpath(args[i])
        args[i] = dir+'/'+basename
        p.append(string.find(dir,"/pnfs/"))

    # all files on the hsm system have /pnfs/ as the 1st part of their name
    # scan input files for /pnfs - all have to be the same
    p1 = p[0]
    p2 = p[arglen-1]
    input = [args[0]]
    output = [args[arglen-1]]
    for i in range(1,len(args)-1):
        if p[i]!=p1:
            if p1:
                print "ERROR: Not all input files are /pnfs/... files"
            else:
                print "ERROR: Not all input files are unix files"
            sys.exit(1)
        else:
            input.append(args[i])

    # have we been called "encp unixfile hsmfile" ?
    if p1==-1 and p2==0 :
        write_to_hsm(input, output, u, csc, logc, list, chk_crc)

    # have we been called "encp hsmfile unixfile" ?
    elif p1==0 and p2==-1 :
        read_from_hsm(input, output, u, csc, logc, list, chk_crc)

    # have we been called "encp unixfile unixfile" ?
    elif p1==-1 and p2==-1 :
        print "encp copies to/from hsm. It is not involved in copying "\
              +input," to ",output

    # have we been called "encp hsmfile hsmfile?
    elif p1==0 and p2==0 :
        print "encp hsm to hsm is not functional. "\
              +"copy hsmfile to local disk and them back to hsm"

    else:
        print "ERROR: Can not process arguments "+args

