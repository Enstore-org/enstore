###############################################################################
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
import binascii
import regsub
import copy
import pdb
import string
import traceback

# enstore modules
import pnfs
import callback
import log_client
import configuration_client
import udp_client
import EXfer
import interface
import e_errors
import Trace

d0sam_format = "INFILE=%s\n"+\
               "OUTFILE=%s\n"+\
               "FILESIZE=%d\n"+\
               "LABEL=%s\n"+\
               "DRIVE=%s\n"+\
               "TRANSFER_TIME=%f\n"+\
               "SEEK_TIME=%f\n"+\
               "MOUNT_TIME=%f\n"+\
               "QWAIT_TIME=%f\n"+\
               "STATUS=%s\n"

##############################################################################

def write_to_hsm(input, output,
                 config_host, config_port,
                 ilist=0, chk_crc=1,
                 pri=1, delpri=0, agetime=0,
                 t0=0):
    if t0==0:
        t0 = time.time()
    Trace.trace(6,"{write_to_hsm input="+repr(input)+\
                " output="+repr(output)+" config_host="+repr(config_host)+\
                " config_port="+repr(config_port)+" list="+repr(ilist)+\
                " chk_crc="+repr(chk_crc)+" t0="+repr(t0))
    tinfo = {}
    tinfo["abs_start"] = t0

    # check if there special d0sam printing requested. This is designated by
    # the having bit 2^12 set (4096)
    d0sam = (ilist & 0x1000) !=0
    list = ilist & 0x0fff

    if list>2:
        print "Getting clients, storing/checking local info   cumt=",\
              time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # initialize - and get config, udp and log clients
    maxretry = 2
    unique_id = []
    global logc # needs to be global so other defs can use it in this file
    (csc,u,uinfo) = clients(config_host,config_port,list)

    # create the wrapper subticket - copy all the user info into for starters
    wrapper = {}
    for key in uinfo.keys():
        wrapper[key] = uinfo[key]

    # make the part of the ticket that encp knows about (there's more later)
    encp = {}
    encp["basepri"] = pri
    encp["adminpri"] = -1
    encp["delpri"] = delpri
    encp["agetime"] = agetime

    # create the time subticket
    times = {}
    times["t0"] = tinfo["abs_start"]

    tinfo["clients"] = time.time() - t1 #-----------------------------------End
    if list>2:
        print "  dt:",tinfo["clients"], "   cumt=",time.time()-t0
    if list>3:
        print "csc=",csc
        print "u=",u
        print "logc=",logc
        print "uinfo=",uinfo

    if list>2:
        print "Checking input unix files:",input, "   cumt=",time.time()-t0
    t1 =  time.time() #---------------------------------------------------Start

    # check the input unix files. if files don't exits, we bomb out to the user
    (ninput, inputlist, file_size) = inputfile_check(input)
    if ninput>1:
        delayed_dismount = 1
    else:
        delayed_dismount = 0

    tinfo["filecheck"] = time.time() - t1 #---------------------------------End
    if list>2:
        print "  dt:",tinfo["filecheck"], "   cumt=",time.time()-t0
    if list>3:
        print "ninput=",ninput
        print "inputlist=",inputlist
        print "file_size=",file_size
        print "delayed_dismount=",delayed_dismount

    if list>2:
        print "Checking output pnfs files:",output, "   cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # check (and generate) the output pnfs files(s) names
    # bomb out if they exist already
    outputlist = outputfile_check(ninput,inputlist,output)
    (junk,library,file_family,width,pinfo,p)=pnfs_information(outputlist,ninput)

    # note: Since multiple input files all go to 1 directory:
    #       all libraries are the same
    #       all file families are the same
    #       all widths are the same
    # be cautious and check to make sure this is indeed correct
    for i in range(1,ninput):
        if library[i]!=library[0] or\
           file_family[i]!=file_family[0] or\
           width[i]!=width[0] :
            print "library=",library
            print "file_family=",file_family
            print "width=",width
            jraise(errno.errorcode[errno.EPROTO]," encp.write_to_hsm: TILT "\
                   " library, file_family, width not all the same")

    tinfo["pnfscheck"] = time.time() - t1 #---------------------------------End
    if list>2:
        print "  dt:",tinfo["pnfscheck"], "   cumt=",time.time()-t0
    if list>3:
        print "outputlist=",outputlist
        print "library=",library
        print "file_family=",file_family
        print "width=",width
        print "pinfo=",pinfo
        print "p=",p

    t1 = time.time() #----------------------------------------------------Start
    if list>1:
        print "Requesting callback ports", "   cumt=",time.time()-t0

    # get a port to talk on and listen for connections
    Trace.trace(10,'write_to_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback()
    callback_addr = (host, port)
    listen_socket.listen(4)
    Trace.trace(10,'write_to_hsm got callback host='+repr(host)+\
                ' port='+repr(port)+' listen_socket='+repr(listen_socket))

    tinfo["get_callback"] = time.time() - t1 #------------------------------End
    if list>1:
        print " ",host,port
        print "  dt:",tinfo["get_callback"], "   cumt=",time.time()-t0

    if list>1:
        print "Calling Config Server to find",library[0]+".library_manager",\
              "   cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # ask configuration server what port library manager is using
    # note again:libraries have are identical since there is 1 output directory
    Trace.trace(10,"write_to_hsm calling config server to find "+\
                library[0]+".library_manager")
    vticket = csc.get(library[0]+".library_manager")
    Trace.trace(10,"write_to_hsm."+ library[0]+".library_manager at host="+\
                repr(vticket["hostip"])+" port="+repr(vticket["port"]))

    tinfo["get_libman"] = time.time() - t1 #--------------------------------End
    if list>1:
        print "  ",vticket["hostip"],vticket["port"]
        print "  dt:",tinfo["get_libman"], "   cumt=",time.time()-t0

    # loop on all input files sequentially
    for i in range(0,ninput):
        unique_id.append(0) # will be set later when submitted

        # delete old tickets in case of a retry
        try:
            del work_ticket
        except NameError:
            pass

        # allow some retries if mover fails
        retry = maxretry
        while retry:  # note that real rates are not correct in retries

            if list:
                print "Sending ticket to",library[i]+".library_manager",\
                      "   cumt=",time.time()-t0
            t1 = time.time() #----------------------------------------Lap Start

            # store timing info for each transfer in pnfs, not for all
            tinfo1 = copy.deepcopy(tinfo)

            unique_id[i] = time.time()  # note that this is down to mS
            wrapper["fullname"] = outputlist[i]

            # store the pnfs information info into the wrapper
            for key in pinfo[i].keys():
                if not uinfo.has_key(key) : # the user key takes precedence over the pnfs key
                    wrapper[key] = pinfo[i][key]

            # if old ticket exists, that means we are retrying
            #    then just bump priority and change unique id
            try:
                oldtick = work_ticket["encp"]["curpri"] # get a name error if this is new ticket
                work_ticket["encp"]["basepri"] = work_ticket["encp"]["basepri"] + 4

            # if no ticket, then this is a not a retry
            except NameError:
                volume_clerk = {"library"            : library[i],\
                                "file_family"        : file_family[i],\
                                "file_family_width"  : width[i]} # technically width does not belong here, but it associated with the volume

                wrapper["sanity_size"] = 5000
                wrapper["size_bytes"] = file_size[i]
                wrapper["mtime"] = int(time.time())
                encp["delayed_dismount"] = delayed_dismount
                work_ticket = {"work"               : "write_to_hsm",
                               "callback_addr"      : callback_addr,
                               "vc"                 : volume_clerk,
                               "wrapper"            : wrapper,
                               "encp"               : encp,
                               "times"              : times,
                               "unique_id"          : unique_id[i]
                               }

            # send the work ticket to the library manager
            tinfo1["tot_to_send_ticket"+repr(i)] = t1 - t0
            system_enabled(p) # make sure system still enabled before submitting
            Trace.trace(7,"write_to_hsm q'ing:"+repr(work_ticket))
            ticket = u.send(work_ticket, (vticket['hostip'], vticket['port']))
            if list > 3:
                print "ENCP: write_to_hsm LM returned"
                pprint.pprint(ticket)
            if ticket['status'][0] != "ok" :
                jraise(errno.errorcode[errno.EPROTO]," encp.write_to_hsm: "\
                       "from u.send to " +library[i]+" at "\
                       +vticket['hostip']+"/"+repr(vticket['port'])\
                       +", ticket[\"status\"]="+ticket["status"])

            tinfo1["send_ticket"+repr(i)] = time.time() - t1 #-----------Lap End
            if list:
                print "  Q'd:",inputlist[i], library[i],\
                      "family:",file_family[i],\
                      "bytes:", file_size[i],\
                      "dt:",tinfo1["send_ticket"+repr(i)],\
                          "   cumt=",time.time()-t0

            if list>1:
                print "Waiting for mover to call back", \
                      "   cumt=",time.time()-t0
            t1 = time.time() #----------------------------------------Lap-Start
            tMBstart = t1

            # We have placed our work in the system and now we have to wait
            # for resources. All we need to do is wait for the system to call
            # us back, and make sure that is it calling _us_ back, and not
            # some sort of old call-back to this very same port. It is dicey
            # to time out, as it is probably legitimate to wait for hours....

            #sys.exit(1)

            while 1 :
                Trace.trace(10,"write_to_hsm listening for callback")
                control_socket, address = listen_socket.accept()
                ticket = callback.read_tcp_socket(control_socket,\
                             "encp write_to_hsm, mover call back")
                if list > 3:
                    print "ENCP:write_to_hsm MV called back with"
                    pprint.pprint(ticket)
                callback_id = ticket['unique_id']
                # compare strings not floats (floats fail comparisons)
                if str(unique_id[i])==str(callback_id):
                    Trace.trace(10,"write_to_hsm mover called back on "+\
                                "control_socket="+repr(control_socket)+\
                                " address="+repr(address))
                    break
                else:
                    print("encp write_to_hsm: imposter called us, trying again")
                    Trace.trace(10,"write_to_hsm mover imposter called us "+\
                                "control_socket="+repr(control_socket)+\
                                " address="+repr(address))
                    control_socket.close()

            # ok, we've been called back with a matched id - how's the status?
            if ticket["status"][0] != "ok" :
                jraise(errno.errorcode[errno.EPROTO]," encp.write_to_hsm: "\
                       +"1st (pre-file-send) mover callback on socket "\
                       +repr(address)+", failed to setup transfer: "\
                       +"ticket[\"status\"]="+ticket["status"],2)
            data_path_socket = callback.mover_callback_socket(ticket)

            tinfo1["tot_to_mover_callback"+repr(i)] = time.time() - t0 #-----Cum
            dt = time.time() - t1 #-------------------------------------Lap-End
            if list>1:
                print " ",ticket["mover"]["callback_addr"][0],\
                      ticket["mover"]["callback_addr"][1],\
                      "cum:",tinfo1["tot_to_mover_callback"+repr(i)]
                print "  dt:",dt,"   cumt=",time.time()-t0

            if list:
                print "Sending data for file ", outputlist[i],\
                      "   cumt=",time.time()-t0
            t1 = time.time() #----------------------------------------Lap-Start

            # Call back mover on mover's port and send file on that port
            in_file = open(inputlist[i], "r")
            mycrc = 0
            fsize = file_size[i]
            bufsize = 65536*4
            Trace.trace(7,"write_to_hsm: sending data to EXfer file="+\
                        inputlist[i]+" socket="+repr(data_path_socket)+\
                        " bufsize="+repr(bufsize)+" chk_crc="+repr(chk_crc))
            try:
                mycrc = EXfer.usrTo_(in_file,data_path_socket,binascii.crc_hqx,
                                     bufsize, chk_crc )
                retry = 0
            except:
                Trace.trace(0,"write_to_hsm EXfer error:"+str(sys.argv)+" "+\
                            str(sys.exc_info()[0])+" "+\
                            str(sys.exc_info()[1]))
                print "Error with encp EXfer - continuing"
                traceback.print_exc()
                retry = retry - 1
                data_path_socket.close()
                in_file.close()
                done_ticket = callback.read_tcp_socket(control_socket,
                                  "encp write_to_hsm, error dialog")
                control_socket.close()
                print done_ticket, "retrying"

        # close the data socket and the file, we've sent it to the mover
        data_path_socket.close()
        in_file.close()

        tinfo1["sent_bytes"+repr(i)] = time.time()-t1 #------------------Lap-End
        if list>1:
            if tinfo1["sent_bytes"+repr(i)]!=0:
                wtrate = 1.*fsize/1024./1024./tinfo1["sent_bytes"+repr(i)]
            else:
                wdrate = 0.0
            print "  bytes:",fsize, " Socket Write Rate = ",wtrate," MB/S"
            print "  dt:",tinfo1["sent_bytes"+repr(i)],\
                  "   cumt=",time.time()-t0
        if list>1:
            print "Waiting for final mover dialog",\
                  "   cumt=",time.time()-t0
        t1 = time.time() #--------------------------------------------Lap-Start

        # File has been sent - wait for final dialog with mover. We know
        # the file has hit some sort of media.... when this occurs. Create
        #  a file in pnfs namespace with information about transfer.
        Trace.trace(10,"write_to_hsm waiting for final mover dialog on"+\
                    repr(control_socket))
        done_ticket = callback.read_tcp_socket(control_socket,
                          "encp write_to_hsm, mover final dialog")
        control_socket.close()
        Trace.trace(10,"write_to_hsm final dialog recieved")

        # make sure mover thinks transfer went ok
        if done_ticket["status"][0] != "ok" :
            jraise(errno.errorcode[errno.EPROTO]," encp.write_to_hsm: "\
                   +"2nd (post-file-send) mover callback on socket "\
                   +repr(address)+", failed to transfer: "\
                   +"done_ticket[\"status\"]="+done_ticket["status"])

        # Check the CRC
            if chk_crc != 0:
                if done_ticket["fc"]["complete_crc"] != mycrc :
                    jraise(errno.errorcode[errno.EPROTO],\
                           " encp.write_to_hsm: CRC's mismatch: "\
                           +repr(complete_crc)+" "+repr(mycrc))

        tinfo1["final_dialog"] = time.time()-t1 #------------------------Lap End
        if list>1:
            print "  dt:",tinfo1["final_dialog"], "   cumt=",time.time()-t0

        if list>1:
            print "Adding file to pnfs", "   cumt=",time.time()-t0
        t1 = time.time() #--------------------------------------------Lap Start

        # create a new pnfs object pointing to current output file
        Trace.trace(10,"write_to_hsm adding to pnfs "+outputlist[i])
        p=pnfs.Pnfs(outputlist[i])
        # save the bfid and set the file size
        p.set_bit_file_id(done_ticket["fc"]["bfid"],file_size[i])
        # create volume map and store cross reference data
        p.set_xreference(done_ticket["fc"]["external_label"],
                         done_ticket["fc"]["bof_space_cookie"])
        # store debugging info about transfer
        done_ticket["tinfo"] = tinfo1 # store as much as we can into pnfs
        done_formatted  = pprint.pformat(done_ticket)
        p.set_info(done_formatted)
        Trace.trace(10,"write_to_hsm done adding to pnfs")

        tinfo1["pnfsupdate"+repr(i)] = time.time() - t1 #---------------Lap End
        if list>1:
            print "  dt:",tinfo1["pnfsupdate"+repr(i)],"   cumt=",time.time()-t0


        # calculate some kind of rate - time from beginning to wait for
        # mover to respond until now. This doesn't include the overheads
        # before this, so it isn't a correct rate. I'm assuming that the
        # overheads I've neglected are small so the quoted rate is close
        # to the right one.  In any event, I calculate an overall rate at
        # the end of all transfers
        tnow = time.time()
        if (tnow-tMBstart)!=0:
            tinfo1['rate'+repr(i)] = 1.*fsize/1024./1024./(tnow-tMBstart)
        else:
            tinfo1['rate'+repr(i)] = 0.0
        format = "  %s -> %s : %d bytes copied to %s at"+\
                 " %s MB/S  requestor:%s     cumt= %f"

        if list:
            print format %\
                  (inputlist[i], outputlist[i], fsize,
                   done_ticket["fc"]["external_label"],
                   tinfo1["rate"+repr(i)], wrapper["uname"],
                   time.time()-t0)
        if d0sam:
            print d0sam_format % \
                  (inputlist[i],
                   outputlist[i],
                   fsize,
                   done_ticket["fc"]["external_label"],
                   done_ticket["mover"]["device"],
                   done_ticket["times"]["transfer_time"],
                   done_ticket["times"]["seek_time"],
                   done_ticket["times"]["mount_time"],
                   done_ticket["times"]["lm_dequeued"]-done_ticket["times"]["t0"],
                   e_errors.OK)

        logc.send(log_client.INFO, 2, format,
                  inputlist[i], outputlist[i], fsize,
                  done_ticket["fc"]["external_label"],
                  tinfo1["rate"+repr(i)], wrapper["uname"],
                  time.time()-t0)


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

    msg ="Complete: "+repr(total_bytes)+" bytes in "+repr(ninput)+" files"+\
          " in "+repr(tf-t0)+"S.  Overall rate = "+\
          repr(done_ticket["MB_per_S"])+" MB/S"
    if list:
        print msg

    # tell library manager we are done - this allows it to delete our unique id in
    # its dictionary - this keeps things cleaner and stops memory from growing
    #u.send_no_wait({"work":"done_cleanup"}, (vticket['hostip'], vticket['port']))

    if list > 3:
        print "DONE TICKET"
        pprint.pprint(done_ticket)

    Trace.trace(6,"}write_to_hsm "+msg)

##############################################################################

def read_from_hsm(input, output,
                  config_host, config_port,
                  ilist=0, chk_crc=1,
                  pri=1, delpri=0, agetime=0,
                  t0=0):
    if t0==0:
        t0 = time.time()
    Trace.trace(6,"{read_from_hsm input="+repr(input)+\
                " output="+repr(output)+" config_host="+repr(config_host)+\
                " config_port="+repr(config_port)+" list="+repr(ilist)+\
                " chk_crc="+repr(chk_crc)+" t0="+repr(t0))
    tinfo = {}
    tinfo["abs_start"] = t0

    # check if there special d0sam printing requested. This is designated by
    # the having bit 2^12 set (4096)
    d0sam = (ilist & 0x1000) !=0
    list = ilist & 0x0fff

    if list>2:
        print "Getting clients, storing/checking local info   cumt=",\
              time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # initialize - and get config, udp and log clients
    finfo = []
    vinfo = []
    volume = []
    unique_id = []
    vols_needed = {}
    delayed_dismount = 0
    global logc
    (csc,u,uinfo) = clients(config_host,config_port,list)

    wrapper = {}
    for key in uinfo.keys():
        wrapper[key] = uinfo[key]

    # make the part of the ticket that encp knows about (there's more later)
    encp = {}
    encp["basepri"] = pri
    encp["adminpri"] = -1
    encp["delpri"] = delpri
    encp["agetime"] = agetime

    # create the time subticket
    times = {}
    times["t0"] = tinfo["abs_start"]

    tinfo["clients"] = time.time() - t1 #-----------------------------------End
    if list>2:
        print "  dt:",tinfo["clients"], "   cumt=",time.time()-t0
    if list>3:
        print "csc=",csc
        print "u=",u
        print "logc=",logc
        print "uinfo=",uinfo

    if list>2:
        print "Checking input pnfs files:",input, "   cumt=",time.time()-t0
    t1 =  time.time() #---------------------------------------------------Start

    # check the input unix files. if files don't exits, we bomb out to the user
    (ninput, inputlist, file_size) = inputfile_check(input)
    (bfid,junk,junk,junk,pinfo,p)=pnfs_information(inputlist,ninput)

    tinfo["pnfscheck"] = time.time() - t1 #---------------------------------End
    if list>2:
        print "  dt:",tinfo["pnfscheck"], "   cumt=",time.time()-t0
    if list>3:
        print "ninput=",ninput
        print "inputlist=",inputlist
        print "file_size=",file_size
        print "bfid=",bfid
        print "pinfo=",pinfo
        print "p=",p

    if list>2:
        print "Checking output unix files:",output, "   cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # check (and generate) the output files(s)
    # bomb out if they exist already
    outputlist = outputfile_check(ninput,inputlist,output)

    tinfo["filecheck"] = time.time() - t1 #---------------------------------End
    if list>2:
        print "  dt:",tinfo["filecheck"], "   cumt=",time.time()-t0
    if list>3:
        print "outputlist=",outputlist

    if list>2:
        print "Requesting callback ports", "   cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # get a port to talk on and listen for connections
    Trace.trace(10,'read_from_hsm calling callback.get_callback')
    host, port, listen_socket = callback.get_callback()
    callback_addr = (host, port)
    listen_socket.listen(4)
    Trace.trace(10,'read_from_hsm got callback host='+repr(host)+\
                ' port='+repr(port)+' listen_socket='+repr(listen_socket))

    tinfo["get_callback"] = time.time() - t1 #------------------------------End
    if list>2:
        print " ",host,port
        print "  dt:",tinfo["get_callback"], "   cumt=",time.time()-t0

    if list>1:
        print "Calling Config Server to find file clerk",\
              "   cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # ask configuration server what port the file clerk is using
    Trace.trace(10,"read_from_hsm calling config server to find file clerk")
    fticket = csc.get("file_clerk")
    Trace.trace(10,"read_from_hsm file clerk at host="+\
                repr(fticket["hostip"])+" port="+repr(fticket["port"]))

    tinfo["get_fileclerk"] = time.time() - t1 #-----------------------------End
    if list>1:
        print " ",fticket["hostip"],fticket["port"]
        print "  dt:", tinfo["get_fileclerk"], "   cumt=",time.time()-t0

    if list>1:
        print "Calling file clerk for file info", "   cumt=",time.time()-t0
    t1 = time.time() # ---------------------------------------------------Start

    # call file clerk and get file info about each bfid
    for i in range(0,ninput):
        t2 = time.time() # -------------------------------------------Lap-Start
        unique_id.append(0) # will be set later when submitted
        Trace.trace(7,"read_from_hsm calling file clerk for bfid="+\
                    repr(bfid[i]))
        binfo  = u.send({'work': 'bfid_info', 'bfid': bfid[i]},
                        (fticket['hostip'],fticket['port']))
        if binfo['status'][0]!='ok':
            pprint.pprint(binfo)
            jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
                   +" can not get info on bfid"+repr(bfid[i]))
        Trace.trace(7,"read_from_hsm on volume="+\
                    repr(binfo['fc']['external_label']))
        vinfo.append(binfo['vc'])
        finfo.append(binfo['fc'])
        label = binfo['fc']['external_label']
        volume.append(label)
        try:
            vols_needed[label] = vols_needed[label]+1
        except KeyError:
            vols_needed[label] = 1
        tinfo['fc'+repr(i)] = time.time() - t2 #------------------------Lap--End

    tinfo['fc'] =  time.time() - t1 #-------------------------------End
    if list>1:
        print "  dt:",tinfo["fc"], "   cumt=",time.time()-t0

    if list:
        print "Sending ticket to file clerk", "   cumt=",time.time()-t0
    t1 = time.time() #----------------------------------------------------Start

    # loop over all volumes that are needed and submit all requests for
    # that volume. Read files from each volume before submitting requests
    # for different volumes.

    for vol in vols_needed.keys():
        t2 = time.time() #--------------------------------------------Lap-Start
        submitted = 0
        Qd=""
        system_enabled(p) # make sure system is still enabled before submitting
        for i in range(0,ninput):
            if volume[i]==vol:
                unique_id[i] = time.time()  # note that this is down to mS
                wrapper["fullname"] = outputlist[i]
                wrapper["sanity_size"] = 5000
                wrapper["size_bytes"] = file_size[i]
                encp["delayed_dismount"] = delayed_dismount

                # store the pnfs information info into the wrapper
                for key in pinfo[i].keys():
                    if not uinfo.has_key(key) : # the user key takes precedence over the pnfs key
                        wrapper[key] = pinfo[i][key]

                # generate the work ticket
                file_clerk = {"bfid"               : bfid[i]}
                work_ticket = {"work"              : "read_from_hsm",
                               "wrapper"           : wrapper,
                               "callback_addr"     : callback_addr,
                               "fc"                : file_clerk,
                               "encp"              : encp,
                               "times"             : times,
                               "unique_id"         : unique_id[i]
                               }

                # send ticket to file clerk who sends it to right library manger
                Trace.trace(7,"read_from_hsm q'ing:"+repr(work_ticket))
                ticket = u.send(work_ticket, (fticket['hostip'], fticket['port']))
                if list > 3:
                    print "ENCP:read_from_hsm FC read_from_hsm returned"
                    pprint.pprint(ticket)
                if ticket['status'][0] != "ok" :
                    jraise(errno.errorcode[errno.EPROTO],\
                           " encp.read_from_hsm: from"\
                           +"u.send to file_clerk at "+fticket['hostip']+"/"\
                           +repr(fticket['port']) +", ticket[\"status\"]="\
                           +ticket["status"])
                submitted = submitted+1
                tinfo["send_ticket"+repr(i)] = time.time() - t2 #------Lap-End
                if list :
                    if len(Qd)==0:
                        format = "  Q'd: %s %s bytes: %d on %s %s "\
                                 "dt: %f   cumt=%f"
                        Qd = format %\
                             (inputlist[i],bfid[i],file_size[i],\
                              finfo[i]["external_label"],\
                              finfo[i]["bof_space_cookie"],\
                              tinfo["send_ticket"+repr(i)],time.time()-t0)
                    else:
                        Qd = "%s\n  Q'd: %s %s bytes: %d on %s %s "\
                             "dt: %f   cumt=%f" %\
                             (Qd,inputlist[i],bfid[i],file_size[i],\
                              finfo[i]["external_label"],\
                              finfo[i]["bof_space_cookie"],\
                              tinfo["send_ticket"+repr(i)],time.time()-t0)

        tinfo["send_ticket"] = time.time() - t1 #---------------------------End
        if list:
            print Qd
        if list>1:
            print "  dt:",tinfo["send_ticket"], "   cumt=",time.time()-t0

        # We have placed our work in the system and now we have to wait for
        # resources. All we need to do is wait for the system to call us
        # back, and make sure that is it calling _us_ back, and not some
        # sort of old call-back to this very same port. It is dicey to time
        # out, as it is probably legitimate to wait for hours....

        for waiting in range(0,submitted):
            if list>1:
                print "Waiting for mover to call back",\
                      "   cumt=",time.time()-t0
            t2 = time.time() #----------------------------------------Lap-Start
            tMBstart = t2

            # listen for a mover - see if id corresponds to one of the tickets
            #   we submitted for the volume
            while 1 :
                Trace.trace(10,"read_from_hsm listening for callback")
                control_socket, address = listen_socket.accept()
                ticket = callback.read_tcp_socket(control_socket,\
                             "encp read_from_hsm, mover call back")
                if list > 3:
                    print "ENCP:read_from_hsm MV called back with"
                    pprint.pprint(ticket)
                callback_id = ticket['unique_id']
                forus = 0
                for j in range(0,ninput):
                    # compare strings not floats (floats fail comparisons)
                    if str(unique_id[j])==str(callback_id):
                        forus = 1
                        break
                if forus==1:
                    Trace.trace(10,"read_from_hsm mover called back on "+\
                                "control_socket="+repr(control_socket)+\
                                " address="+repr(address))
                    break
                else:
                    print ("encp read_from_hsm: imposter called us back, "\
                           +"trying again")
                    Trace.trace(10,"write_to_hsm mover imposter called us "+\
                                "control_socket="+repr(control_socket)+\
                                " address="+repr(address))
                    control_socket.close()

            # ok, we've been called back with a matched id - how's the status?
            if ticket["status"][0] != "ok" :
                jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
                       +"1st (pre-file-read) mover callback on socket "\
                       +repr(address)+", failed to setup transfer: "\
                       +"ticket[\"status\"]="+ticket["status"])
            data_path_socket = callback.mover_callback_socket(ticket)

            tinfo["tot_to_mover_callback"+repr(j)] = time.time() - t0 #-----Cum
            dt = time.time() - t2 #-------------------------------------Lap-End
            if list>1:
                print " ",ticket["mover"]["callback_addr"][0],\
                      ticket["mover"]["callback_addr"][1],\
                      "cum:",tinfo["tot_to_mover_callback"+repr(j)]
                print "  dt:",dt,"   cumt=",time.time()-t0

            if list:
                print "Receiving data for file ", outputlist[j],\
                      "   cumt=",time.time()-t0
            t2 = time.time() #----------------------------------------Lap-Start

            # open file that corresponds to the mover call back and read file
            # crc the data if user has request crc check
            l = 0
            mycrc = 0
            bufsize = 65536*4
            f = open(outputlist[j],"w")
            Trace.trace(7,"read_from__hsm: reading data to  file="+\
                        inputlist[i]+" socket="+repr(data_path_socket)+\
                        " bufsize="+repr(bufsize)+" chk_crc="+repr(chk_crc))
            while 1:
                buf = data_path_socket.recv(bufsize)
                l = l + len(buf)
                if len(buf) == 0 : break
                if chk_crc != 0 :
                    mycrc = binascii.crc_hqx(buf,mycrc)
                f.write(buf)
            data_path_socket.close()
            f.close()
            fsize = l

            tinfo["recvd_bytes"+repr(j)] = time.time()-t2 #-------------Lap-End
            if list>1:
                if tinfo["recvd_bytes"+repr(j)]!=0:
                    rdrate = 1.*fsize/1024./1024./tinfo["recvd_bytes"+repr(j)]
                else:
                    rdrate = 0.0
                print "  bytes:",fsize, " Socket read Rate = ",rdrate," MB/S"
                print "  dt:",tinfo["recvd_bytes"+repr(j)],\
                      "   cumt=",time.time()-t0
            if list>1:
                print "Waiting for final mover dialog",\
                      "   cumt=",time.time()-t0
            t2 = time.time() #----------------------------------------Lap-Start

            # File has been read - wait for final dialog with mover.
            Trace.trace(10,"read_from_hsm waiting for final mover dialog on"+\
                        repr(control_socket))
            done_ticket = callback.read_tcp_socket(control_socket,\
                          "encp read_from_hsm, mover final dialog")
            control_socket.close()
            Trace.trace(10,"read_from_hsm final dialog recieved")

            # make sure the mover thinks the transfer went ok
            if done_ticket["status"][0] != "ok" :
                jraise(errno.errorcode[errno.EPROTO]," encp.read_from_hsm: "\
                       +"2nd (post-file-read) mover callback on socket "\
                       +repr(address)+", failed to transfer: "\
                       +"done_ticket[\"status\"]="+done_ticket["status"])

            # verify that the crc's match
            if chk_crc != 0 :
                if done_ticket["fc"]["complete_crc"] != mycrc :
                    jraise(errno.errorcode[errno.EPROTO],\
                           " encp.read_from_hsm: CRC's mismatch: "\
                           +repr(complete_crc)+" "+repr(mycrc))

            tinfo["final_dialog"+repr(j)] = time.time()-t2 #------------Lap-End
            if list>1:
                print "  dt:",tinfo["final_dialog"+repr(j)],\
                "   cumt=",time.time()-t0


            # update the last parked info if we have write access
            if 0:
                if list>1:
                    print "Updating pnfs last parked",\
                          "   cumt=",time.time()-t0
                try:
                    p.set_lastparked(repr(wrapper['fullname']))
                except:
                    print "Failed to update last parked info"
                if list>1:
                    print "  dt:",tinfo["last_parked"],\
                          "   cumt=",time.time()-t0

            # calculate some kind of rate - time from beginning to wait for
            # mover to respond until now. This doesn't include the overheads
            # before this, so it isn't a correct rate. I'm assuming that the
            # overheads I've neglected are small so the quoted rate is close
            # to the right one.  In any event, I calculate an overall rate at
            # the end of all transfers
            tnow = time.time()
            if (tnow-tMBstart)!=0:
                tinfo['rate'+repr(j)] = 1.*fsize/1024./1024./(tnow-tMBstart)
            else:
                tinfo['rate'+repr(j)] = 0.0
            format = "  %s -> %s : %d bytes copied from %s at"+\
                     " %s MB/S  requestor:%s     cumt= %f"

            if list:
                print format %\
                      (inputlist[j], outputlist[j], fsize,\
                       done_ticket["fc"]["external_label"],\
                       tinfo["rate"+repr(j)], wrapper["uname"],\
                       time.time()-t0)
            if d0sam:
                print d0sam_format % \
                      (inputlist[i],
                       outputlist[i],
                       fsize,
                       done_ticket["fc"]["external_label"],
                       done_ticket["mover"]["device"],
                       done_ticket["times"]["transfer_time"],
                       done_ticket["times"]["seek_time"],
                       done_ticket["times"]["mount_time"],
                       done_ticket["times"]["lm_dequeued"]-done_ticket["times"]["t0"],
                       e_errors.OK)

            logc.send(log_client.INFO, 2, format,
                      inputlist[j], outputlist[j], fsize,
                      done_ticket["fc"]["external_label"],
                      tinfo["rate"+repr(j)], wrapper["uname"],
                      time.time()-t0)


    # we are done transferring - close out the listen socket
    listen_socket.close()

    # Calculate an overall rate: all bytes, all time
    tf=tinfo["total"] = time.time()-t0
    done_ticket["tinfo"] = tinfo
    total_bytes = 0
    for i in range(0,ninput):
        total_bytes = total_bytes+file_size[i]
    tf = time.time()
    if tf!=t0:
        done_ticket["MB_per_S"] = 1.*total_bytes/1024./1024./(tf-t0)
    else:
        done_ticket["MB_per_S"] = 0.0

    msg = "Complete: "+repr(total_bytes)+" bytes in "+repr(ninput)+" files"+\
          " in"+repr(tf-t0)+"S.  Overall rate = "+\
          repr(done_ticket["MB_per_S"])+" MB/s"
    if list:
        print msg

    if list > 3:
        print "DONE TICKET"
        pprint.pprint(done_ticket)

    Trace.trace(6,"}read_from_hsm "+msg)

    # tell file clerk we are done - this allows it to delete our unique id in
    # its dictionary - this keeps things cleaner and stops memory from growing
    #u.send_no_wait({"work":"done_cleanup"}, (fticket['hostip'], fticket['port']))

##############################################################################

# log the error to the logger, print it to the console and exit

def jraise(errcode,errmsg,exit_code=1) :
    Trace.trace(0,"{encp.jraise errcode="+repr(errcode)+\
                " errmsg="+repr(errmsg)+" exit_code="+repr(exit_code))

    format = "Fatal error:"+str(errcode)+str(errmsg)
    print format
    try:
        global logc
        logc.send(log_client.ERROR, 1, format)
    except:
        pass
    Trace.trace(0,"}encp.jraise and exitting with code="+\
                repr(exit_code))
    sys.exit(exit_code)

##############################################################################

# get the configuration client and udp client and logger client
# return some information about who we are so it can be used in the ticket

def clients(config_host,config_port,list):
    Trace.trace(16,"{clients config_host="+repr(config_host)+\
                " port="+repr(config_port)+" list="+repr(list))

    # get a configuration server
    csc = configuration_client.ConfigurationClient(config_host,config_port,\
                                                    list)
    # get a udp client
    u = udp_client.UDPClient()

    # get a logger client
    global logc
    logc = log_client.LoggerClient(csc, 'ENCP', 'logserver')

    uinfo = {}
    uinfo['uid'] = os.getuid()
    uinfo['gid'] = os.getgid()
    uinfo['gname'] = grp.getgrgid(uinfo['gid'])[0]
    uinfo['uname'] = pwd.getpwuid(uinfo['uid'])[0]
    uinfo['machine'] = os.uname()
    uinfo['fullname'] = "" # will be filled in later for each transfer

    Trace.trace(16,"}clients csc="+repr(csc)+" u="+repr(u)+\
                " uinfo="+repr(uinfo))
    return (csc,u,uinfo)

##############################################################################

# check if the system is still running by checking the wormhole file

def system_enabled(p):                 # p is a  pnfs object
    Trace.trace(10,"{system_enabled p="+repr(p))

    running = p.check_pnfs_enabled()
    if running != pnfs.ENABLED :
        jraise(errno.errorcode[errno.EACCES]," encp.system_enabeld: "\
               +"system disabled"+running)
    Trace.trace(10,"}system_enabled running="+running)

##############################################################################

# return pnfs information,
# and an open pnfs object so you can check if  the system is enabled.

def pnfs_information(filelist,nfiles):
    Trace.trace(16,'{pnfs_information filelist='+repr(filelist)+\
                " nfiles="+repr(nfiles))
    bfid = []
    pinfo = []
    library = []
    file_family = []
    width = []

    for i in range(0,nfiles):
        p = pnfs.Pnfs(filelist[i])         # get the pnfs object
        bfid.append(p.bit_file_id)         # get the bit file id
        library.append(p.library)          # get the library
        file_family.append(p.file_family)  # get the file family
        width.append(p.file_family_width)  # get the width

        # get some debugging info for the ticket
        pinf = {}
        for k in [ 'pnfsFilename','gid', 'gname','uid', 'uname',\
                   'major','minor','rmajor','rminor',\
                   'mode','pstat' ] :
            exec("pinf["+repr(k)+"] = p."+k)
        pinf['inode'] = 0                  # cpio wrapper needs this also
        pinfo.append(pinf)

    Trace.trace(16,"}pnfs_information bfid="+repr(bfid)+\
                " library="+repr(library)+" file_family="+repr(file_family)+\
                " width="+repr(width)+" pinfo="+repr(pinfo)+" p="+repr(p))
    return (bfid,library,file_family,width,pinfo,p)

##############################################################################

# generate the full path name to the file

def fullpath(filename):
    Trace.trace(16,'{fullpath filename='+filename)

    machine = socket.gethostbyaddr(socket.gethostname())[0]
    dir, file = os.path.split(filename)

    # if the directory is empty - get the users current working directory
    if dir == '' :
        dir = os.getcwd()
    command="(cd "+dir+";pwd)"
    try:
        dir = regsub.sub("\012","",os.popen(command,'r').readlines()[0])
        filename = dir+"/"+file
    except:
        pass

    # take care of any inadvertant extra "/"
    # Note: as far as I know, this only happens when the user specifies the
    #       the filename as /
    filename = regsub.sub("//","/",filename)
    dir = regsub.sub("//","/",dir)
    file = regsub.sub("//","/",file)

    Trace.trace(16,"}fullpath machine="+machine+\
                " filename="+filename+" dir="+dir+" file="+file)
    return (machine, filename, dir, file)

##############################################################################

# check the input file list for consistency

def inputfile_check(input):
    Trace.trace(16,"{inputfile_check input="+repr(input))

    # create internal list of input unix files even if just 1 file passed in
    try:
        ninput = len(input)
        inputlist = input
    except TypeError:
        inputlist = [input]
        ninput = 1

    # we need to know how big each input file is
    file_size = []

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(0,ninput):

        # get fully qualified name
        (machine, fullname, dir, basename) = fullpath(inputlist[i])
        inputlist[i] = dir+'/'+basename

        # input files must exist
        command="if test -r "+inputlist[i]+"; then echo ok; else echo no; fi"
        readable = os.popen(command,'r').readlines()
        if "ok\012" != readable[0] :
            jraise(errno.errorcode[errno.EACCES]," encp.inputfile_check: "\
                   +inputlist[i]+", NO read access to file")

        # get the file size
        statinfo = os.stat(inputlist[i])
        file_size.append(statinfo[stat.ST_SIZE])

        # input files can't be directories
        if not stat.S_ISREG(statinfo[stat.ST_MODE]) :
            jraise(errno.errorcode[errno.EPROTO]," encp.inputfile_check: "\
                   +input[i]+" is not a regular file")

    # we can not allow 2 input files to be the same
    # this will cause the 2nd to just overwrite the 1st
    for i in range(0,ninput):
        for j in range(i+1,ninput):
            if inputlist[i] == inputlist[j]:
                jraise(errno.errorcode[errno.EPROTO]," encp.inputfile_check: "\
                       +inputlist[i]+" is the duplicated - not allowed")

    Trace.trace(16,"}inputfile_check ninput="+repr(ninput)+\
                " inputlist="+repr(inputlist)+" file_size="+repr(file_size))
    return (ninput, inputlist, file_size)

##############################################################################

# check the output file list for consistency
# generate names based on input list if required

def outputfile_check(ninput,inputlist,output):
    Trace.trace(16,"{outputfile_check ninput="+repr(ninput)+\
                " inputlist="+repr(inputlist)+" output="+repr(output))

    # can only handle 1 input file  copied to 1 output file
    #  or      multiple input files copied to 1 output directory
    # this is just the current policy - nothing fundamental about it
    try:
        noutput = len(output)
        jraise(errno.errorcode[errno.EPROTO]," encp.outputfile_check: "\
               +"can not handle multiple output files: "+output)
    except TypeError:
        pass

    # if user specified multiple input files, then output must be a directory
    outputlist = []
    if ninput!=1:
        try:
            statinfo = os.stat(output[0])
        except os.error:
            jraise(errno.errorcode[errno.EPROTO]," encp.outputfile_check: "\
                   "multiple input files can not be copied to non-existant "\
                   +"directory "+output[0])
        if not stat.S_ISDIR(statinfo[stat.ST_MODE]) :
            jraise(errno.errorcode[errno.EPROTO]," encp.outputfile_check: "\
                   "multiple input files must be copied to a directory, not "\
                   +output[0])

    outputlist = []

    # Make sure we can open the files. If we can't, we bomb out to user
    # loop over all input files and generate full output file names
    for i in range(0,ninput):
        outputlist.append(output[0])

        # see if output file exists as user specified
        try:
            statinfo = os.stat(outputlist[i])
            itexists = 1

        # if output doesn't exist, then at least directory must exist
        except os.error:
            itexists = 0
            (omachine, ofullname, odir, obasename) = fullpath(outputlist[i])
            try:
                statinfo = os.stat(odir)
            # directory doesn't exist - error
            except os.error:
                jraise(errno.errorcode[errno.EEXIST]," encp.outputfile_check:"\
                       " base directory doesn't exist for "+outputlist[i])

        # note: removed from itexist=1 try block to isolate errors
        if itexists:
            # if output file exists, then it must be a directory
            if stat.S_ISDIR(statinfo[stat.ST_MODE]) :
                (omachine, ofullname, odir, obasename) = fullpath(outputlist[i])
                (imachine, ifullname, idir, ibasename) = fullpath(inputlist[i])
                # take care of missing filenames (just directory or .)
                if obasename=='.' or len(obasename)==0:
                    outputlist[i] = odir+'/'+ibasename
                else:
                    outputlist[i] = ofullname+'/'+ibasename
                (omachine, ofullname, odir, obasename) = fullpath(outputlist[i])
                # need to make sure generated filename doesn't exist
                try:
                    statinfo = os.stat(outputlist[i])
                    # generated filename already exists - error
                    jraise(errno.errorcode[errno.EEXIST],\
                           " encp.outputfile_check: "+outputlist[i]+\
                           " already exists")
                except os.error:
                    pass # ok, generated name doesn't exist
            # filename already exists - error
            else:
                jraise(errno.errorcode[errno.EEXIST]," encp.outputfile_check: "\
                       +outputlist[i]+" already exists")

        # need to check that directory is writable
        # since all files go to one output directory, one check is enough
        if i==0:
            command="if test -w "+odir+"; then echo ok; else echo no; fi"
            writable = os.popen(command,'r').readlines()
            if "ok\012" != writable[0] :
                jraise(errno.errorcode[errno.EACCES]," encp.write_to_hsm: "\
                       +" NO write access to directory"+odir)

    # we can not allow 2 output files to be the same
    # this will cause the 2nd to just overwrite the 1st
    # In principal, this is already taken care of in the inputfile_check, but
    #  do it again just to make sure in case someone changes protocol
    for i in range(0,ninput):
        for j in range(i+1,ninput):
            if outputlist[i] == outputlist[j]:
                jraise(errno.errorcode[errno.EPROTO]," encp.outputfile_check: "\
                       +outputlist[i]+" is the duplicated - not allowed")

    Trace.trace(16,"}outputfile_check outputlist="+repr(outputlist))
    return outputlist

##############################################################################

class encp(interface.Interface):

    def __init__(self):
        Trace.trace(16,"{encp.__init__")

        self.chk_crc = 1 # we will check the crc unless told not to
        self.pri = 1     # lowest priority
        self.delpri = 0  # priority doesn't change
        self.agetime = 0 # priority doesn't age
        self.d0sam = 0   # no special sam listings

        host = 'localhost'
        port = 0
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()
        Trace.trace(16,"{encp.__init__")

    ##########################################################################
    # define the command line options that are valid
    def options(self):
        Trace.trace(16,"{encp.options")

        the_options = self.config_options()+\
                      self.list_options()+\
                      ["nocrc","pri=","delpri=","agetime=", "d0sam"] +\
                      self.help_options()

        Trace.trace(16,"}encp.options options="+repr(the_options))
        return the_options

    ##########################################################################
    #  define our specific help
    def help_line(self):
        Trace.trace(16,"{encp.help_line")

        the_help = interface.Interface.help_line(self)+\
                   " inputfilename outputfilename \n  or\n"+\
                   interface.Interface.help_line(self)+\
                   " inputfilename1 ... inputfilenameN outputdirectory"

        Trace.trace(16,"}encp.help_line help_line="+the_help)
        return the_help

    ##########################################################################
    # parse the options from the command line
    def parse_options(self):
        Trace.trace(16,"{encp.parse_options")

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
            self.args[i] = dir+'/'+basename
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
            dictlist = dictlist+" "+key+":"+repr(self.__dict__[key])
        Trace.trace(16,"}encp.parse_options objectdict="+dictlist)


##############################################################################

if __name__  ==  "__main__" :
    t0 = time.time()
    Trace.init("encp")
    Trace.trace(1,"encp called at "+repr(t0)+":"+repr(sys.argv))

    # use class to get standard way of parsing options
    e = encp()

    # have we been called "encp unixfile hsmfile" ?
    if e.intype=="unixfile" and e.outtype=="hsmfile" :
        write_to_hsm(e.input,  e.output,
                     e.config_host, e.config_port,
                     e.list, e.chk_crc,
                     e.pri, e.delpri, e.agetime, t0)

    # have we been called "encp hsmfile unixfile" ?
    elif e.intype=="hsmfile" and e.outtype=="unixfile" :
        read_from_hsm(e.input, e.output,
                      e.config_host, e.config_port,
                      e.list, e.chk_crc,
                      e.pri, e.delpri, e.agetime, t0)

    # have we been called "encp unixfile unixfile" ?
    elif e.intype=="unixfile" and e.outtype=="unixfile" :
        print "encp copies to/from hsm. It is not involved in copying "\
              +input," to ",output

    # have we been called "encp hsmfile hsmfile?
    elif e.intype=="hsmfile" and e.outtype=="hsmfile" :
        print "encp hsm to hsm is not functional. "\
              +"copy hsmfile to local disk and them back to hsm"

    else:
        emsg = "ERROR: Can not process arguments "+repr(e.args)
        Trace.trace(0,emgs)
        jraise(errno.errorcode[errno.EPROTO],emsg)

    Trace.trace(1,"encp finished at "+repr(time.time()))
