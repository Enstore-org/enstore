import os
import stat
import time
import errno
import pprint
import pwd
import grp
import pnfs
import callback
import binascii
from configuration_client import configuration_client
from udp_client import UDPClient, TRANSFER_MAX

# Import SOCKS module if it exists, else standard socket module socket
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

##############################################################################

def write_to_hsm(unixfile, pnfsfile, u, csc, list, chk_crc) :
    t0 = time.time()
    tinfo = {}
    tinfo["abs_start"] = t0

    # first check the unix file the user specified
    # Note that the unix file remains open
    t1 = time.time()
    if list:
        print "Checking",unixfile
    in_file = open(unixfile, "r")
    statinfo = os.stat(unixfile)
    major = 0
    minor = 0
    rmajor = 0
    rminor = 0
    fsize = statinfo[stat.ST_SIZE]
    if not stat.S_ISREG(statinfo[stat.ST_MODE]) :
        raise errno.errorcode[errno.EPERM],"encp.write_to_hsm: "\
              +unixfile+" is not a regular file"
    tinfo["filecheck"] = time.time() - t1
    if list:
        print "  dt:",tinfo["filecheck"], "   cum=",time.time()-t0

    # check the output pnfs file next
    t1 = time.time()
    if list:
        print "Checking",pnfsfile, "   cum=",time.time()-t0
    p = pnfs.pnfs(pnfsfile)
    if p.valid != pnfs.valid :
        raise errno.errorcode[errno.EINVAL],"encp.write_to_hsm: "\
              +pnfsfile+" is an invalid pnfs filename "\
              " or maybe NO read access to file"
    if p.exists == pnfs.exists :
        raise errno.errorcode[errno.EEXIST],"encp.write_to_hsm: "\
              +pnfsfile+" already exists"
    if p.writable != pnfs.enabled :
        raise errno.errorcode[errno.EACCES],"encp.write_to_hsm: "\
              +pnfsfile+", NO write access to directory"
    tinfo["pnfscheck"] = time.time() - t1
    if list:
        print "  dt:",tinfo["pnfscheck"], "   cum=",time.time()-t0

    # make the pnfs dictionary that will be part of the ticket
    pinfo = {}
    for k in [ 'pnfsFilename','gid', 'gname','uid', 'uname',\
               'major','minor','rmajor','rminor',\
               'mode','stat' ] :
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
    #uinfo['node'] = socket.gethostbyaddr(socket.gethostname())

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
              "pnfsfile_info"      : pinfo,
              "user_info"          : uinfo,
              "mtime"              : int(time.time()),
              "size_bytes"         : fsize,
              "sanity_size"        : 5000,
              "user_callback_port" : port,
              "user_callback_host" : host,
              "unique_id"          : time.time()
              }

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
        raise errno.errorcode[errno.EPROTO],"encp.write_to_hsm: from "+\
              "u.send to " +p.library+" at "\
              +vticket['host']+"/"+repr(vticket['port'])\
              +", ticket[\"status\"]="+ticket["status"]
    tinfo["send_ticket"] = time.time() - t1
    if list:
        print "  Q'd:",unixfile, ticket["library"], ticket["file_family"],\
              ticket["file_family_width"]," bytes:", ticket["size_bytes"],\
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
        raise errno.errorcode[errno.EPROTO],"encp.write_to_hsm: "\
              +"1st (pre-file-send) mover callback on socket "\
              +repr(address)+", failed to setup transfer: "\
              +"ticket[\"status\"]="+ticket["status"]
    data_path_socket = callback.mover_callback_socket(ticket)

    # If the system has called us back with our own  unique id, call back
    # the mover on the mover's port and send the file on that port.
    t1 = time.time()
    tinfo["tot_to_mover_callback"] = t1 - t0
    if list:
        print "  ",ticket["mover_callback_host"],\
              ticket["mover_callback_port"], \
              "cum_time:",tinfo["tot_to_mover_callback"],\
              "   cum=",time.time()-t0

    t1 = time.time()
    mycrc = 0
    if list:
        print "Sending data", "   cum=",time.time()-t0
    while 1:
        buf = in_file.read(min(fsize, 65536*4))
        l = len(buf)
        if len(buf) == 0 : break
        if chk_crc != 0 :
            mycrc = binascii.crc_hqx(buf,mycrc)
        badsock = data_path_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0 :
            print "encp write_to_hsm, sending data, pre-send error:", \
                  errno.errorcode[badsock]
        data_path_socket.send(buf)
        badsock = data_path_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0 :
            print "encp write_to_hsm, sending data, post-send error:", \
                  errno.errorcode[badsock]
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
    done_ticket = callback.read_tcp_socket(control_socket, "encp write_"+\
                                           "to_hsm, mover final dialog")
    control_socket.close()
    tinfo["final_dialog"] = time.time()-t1
    if list:
        print "  dt:",tinfo["final_dialog"], "   cum=",time.time()-t0

    if done_ticket["status"] == "ok" :
        if chk_crc != 0 and done_ticket["complete_crc"] != mycrc :
            print "CRC error",complete_crc, mycrc
        t1 = time.time()
        if list:
            print "Adding file to pnfs", "   cum=",time.time()-t0
        p.set_bit_file_id(done_ticket["bfid"],done_ticket["size_bytes"])
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
        p.writelayer(3,done_formatted)
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

    else :
        raise errno.errorcode[errno.EPROTO],"encp.write_to_hsm: "\
              +"2nd (post-file-send) mover callback on socket "\
              +repr(address)+", failed to transfer: "\
              +"ticket[\"status\"]="+ticket["status"]

##############################################################################

def read_from_hsm(pnfsfile, outfile, u, csc, list, chk_crc) :
    t0 = time.time()
    tinfo = {}
    tinfo["abs_start"] = t0

    # first check the input pnfs file - this will also provide the bfid
    t1 =  time.time()
    if list:
        print "Checking",pnfsfile, "   cum=",time.time()-t0
    p = pnfs.pnfs(pnfsfile)
    if p.exists != pnfs.exists :
        raise errno.errorcode[errno.ENOENT],"encp.read_from_hsm: "\
              +pnfsfile+" does not exist"
    tinfo["pnfscheck"] = time.time() - t1
    if list:
        print "  dt:",tinfo["pnfscheck"], "   cum=",time.time()-t0

    # Make sure we can open the unixfile. If we can't, we bomb out to user
    # Note that the unix file remains open
    t1 = time.time()
    if list:
        print "Checking",outfile, "   cum=",time.time()-t0
    dir,file = os.path.split(outfile)
    if dir == '' :
        dir = '.'
    command="if test -w "+dir+"; then echo ok; else echo no; fi"
    writable = os.popen(command,'r').readlines()
    if "ok\012" != writable[0] :
        raise errno.errorcode[errno.EACCES],"encp.read_from__hsm: "\
              +outfile+", NO write access to directory"
    f = open(outfile,"w")
    tinfo["filecheck"] = time.time() - t1
    if list:
        print "  dt:",tinfo["filecheck"], "   cum=",time.time()-t0

    # make the pnfs dictionary that will be part of the ticket
    pinfo = {}
    for k in [ 'pnfsFilename','gid', 'gname','uid', 'uname',\
               'major','minor','rmajor','rminor',\
               'mode','stat' ] :
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
    #uinfo['node'] = socket.gethostbyaddr(socket.gethostname())

    # get a port to talk on and listen for connections
    t1 = time.time()
    if list:
        print "Requesting callback ports", "   cum=",time.time()-t0
    host, port, listen_socket = callback.get_callback()
    listen_socket.listen(4)
    tinfo["get_callback"] = time.time() - t1
    if list:
        print "  ",host,port,"dt:",tinfo["get_callback"], \
              "   cum=",time.time()-t0

    # generate the work ticket
    ticket = {"work"               : "read_from_hsm",
              "pnfs_info"          : pinfo,
              "user_info"          : uinfo,
              "bfid"               : p.bit_file_id,
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

    # send work ticket to file clerk who sends it to right library manger
    t1 = time.time()
    if list:
        print "Sending ticket to file clerk", "   cum=",time.time()-t0
    ticket = u.send(ticket, (fticket['host'], fticket['port']))
    if ticket['status'] != "ok" :
        raise errno.errorcode[errno.EPROTO],"encp.read_from_hsm: from u.send"+\
              "to file_clerk at "+fticket['host']+"/"+repr(fticket['port'])\
              +", ticket[\"status\"]="+ticket["status"]
    tinfo["send_ticket"] = time.time() - t1
    if list :
        print "  Q'd:",p.pnfsFilename, ticket["bfid"], p.file_size\
              ,ticket["external_label"],ticket["bof_space_cookie"],\
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
        raise errno.errorcode[errno.EPROTO],"encp.read_from_hsm: "\
              +"1st (pre-file-read) mover callback on socket "\
              +repr(address)+", failed to setup transfer: "\
              +"ticket[\"status\"]="+ticket["status"]
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
    if chk_crc != 0 and done_ticket["complete_crc"] != mycrc :
        print "CRC error",complete_crc, mycrc
    tinfo["final_dialog"] = time.time()-t1
    if list:
        print "  dt:",tinfo["final_dialog"], "   cum=",time.time()-t0

    tinfo["total"] = time.time()-t0
    done_ticket["tinfo"] = tinfo
    tf = time.time()
    if tf!=t0:
        done_ticket["MB_per_S"] = 1.*fsize/1024./1024./(tf-t0)
    else:
        done_ticket["MB_per_S"] = 0.0

    if done_ticket["status"] != "ok" :
        raise errno.errorcode[errno.EPROTO],"encp.read_from_hsm: "\
              +"2nd (post-file-read) mover callback on socket "\
              +repr(address)+", failed to transfer: "\
              +"ticket[\"status\"]="+ticket["status"]
    if list:
        done_formatted  = pprint.pformat(done_ticket)
        print outfile, ":",fsize,"bytes",\
                  "copied from", done_ticket["external_label"], \
                  "in ",tinfo["total"],"seconds",\
                  "at",done_ticket["MB_per_S"],"MB/S", \
                  "   cum=",time.time()-t0
        #print done_formatted


##############################################################################

if __name__  ==  "__main__" :
    import sys
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
               "nocrc","list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--nocrc" :
            chkcrc = 0
        elif opt == "--list" :
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
    if len(args) < 2 :
        print "python",sys.argv[0], options, "inputfilename outputfilename"
        print "   do not forget the '--' in front of each option"
        sys.exit(1)

    # get a configuration server
    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)
    u = UDPClient()

    # all files on the hsm system have /pnfs/ as the 1st part of their name
    p1 = string.find(args[0],"/pnfs/")
    p2 = string.find(args[1],"/pnfs/")

    # have we been called "encp unixfile hsmfile" ?
    if p1==-1 and p2==0 :
        write_to_hsm(args[0], args[1], u, csc, list, chk_crc)
        if list > 1 :
            p=pnfs.pnfs(args[1],1)
            p.dump()

    # have we been called "encp hsmfile unixfile" ?
    elif p1==0 and p2==-1 :
        if list > 1 :
            p=pnfs.pnfs(args[0],1)
            p.dump()
        read_from_hsm(args[0], args[1], u, csc, list, chk_crc)

    # have we been called "encp unixfile unixfile" ?
    elif p1==-1 and p2==-1 :
        print "encp copies to/from hsm. It is not involved in copying "\
              +args[0]," to ",args[1]

    # have we been called "encp unixfile unixfile" ?
    elif p1==0 and p2==0 :
        print "encp hsm to hsm is not functional. "\
              +"copy hsmfile to local disk and them back to hsm"

    else:
        print "ERROR: Can not process arguments "\
              +args[0]," to ",args[1]
