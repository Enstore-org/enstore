import os
import string
from callback import *
from configuration_client import configuration_client
from dict_to_a import *
from udp_client import UDPClient
import pnfs
import stat
import time
from errno import *
import pprint

# Import SOCKS module if it exists, else standard socket module socket
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

##############################################################################

def write_to_hsm(unixfile, pnfsfile, u, csc, list) :

    # first check the unix file the user specified
    # Note that the unix file remains open
    in_file = open(unixfile, "r")
    statinfo = os.stat(unixfile)
    fsize = statinfo[stat.ST_SIZE]
    if not stat.S_ISREG(statinfo[stat.ST_MODE]) :
        raise errorcode[EPERM],"encp.write_to_hsm: "\
              +unixfile+" is not a regular file"

    # check the output pnfs file next
    p = pnfs.pnfs(pnfsfile)
    if p.valid != pnfs.valid :
        raise errorcode[EINVAL],"encp.write_to_hsm: "\
              +pnfsfile+" is an invalid pnfs filename"
    if p.exists == pnfs.exists :
        raise errorcode[EEXIST],"encp.write_to_hsm: "\
              +pnfsfile+" already exists"

    # get a port to talk on and listen for connections
    host, port, listen_socket = get_callback()
    listen_socket.listen(4)

    # generate the work ticket
    ticket = {"work"               : "write_to_hsm",
              "library"            : p.library,
              "file_family"        : p.file_family,
              "file_family_width"  : p.file_family_width,
              "uid"                : p.uid,
              "uname"              : p.uname,
              "gid"                : p.gid,
              "gname"              : p.uname,
              "protection"         : p.mode,
              "mtime"              : int(time.time()),
              "size_bytes"         : fsize,
              "user_callback_port" : port,
              "user_callback_host" : host,
              "unique_id"          : time.time()
              }

    # ask configuration server what port the right library manager is using
    vticket = csc.get(p.library + ".library_manager")

    # send the work ticket to the library manager
    ticket = u.send(ticket, (vticket['host'], vticket['port']))
    if not ticket['status'] == "ok" :
        raise errorcode[EPROTO],"encp.write_to_hsm: from u.send to "\
              +p.library+".library_manager at "\
              +vticket['host']+"/"+repr(vticket['port'])\
              +", ticket[\"status\"]="+ticket["status"]
    if list :
        print "Q'd:",unixfile, ticket["library"], ticket["file_family"]\
              ,ticket["file_family_width"],ticket["size_bytes"]

    # We have placed our work in the system and now we have to wait for
    # resources. All we  need to do is wait for the system to call us back,
    # and make sure that is it calling _us_ back, and not some sort of old
    # call-back to this very same port. It is dicey to time out, as it
    # is probably legitimate to wait for hours....
    while 1 :
        control_socket, address = listen_socket.accept()
        new_ticket = a_to_dict(control_socket.recv(10000))
        if ticket["unique_id"] == new_ticket["unique_id"] :
            listen_socket.close()
            break
        else:
            print ("encp write_to_hsm: imposter called us back, trying again")
            control_socket.close()
    ticket = new_ticket
    if not ticket["status"] == "ok" :
        raise errorcode[EPROTO],"encp.write_to_hsm: "\
              +"1st (pre-file-send) mover callback on socket at "\
              +repr(address)+", failed to setup transfer: "\
              +"ticket[\"status\"]="+ticket["status"]

    # If the system has called us back with our own  unique id, call back
    # the mover on the mover's port and send the file on that port.
    data_path_socket = mover_callback_socket(ticket)
    while 1:
        buf = in_file.read(min(fsize, 65536*4))
        l = len(buf)
        if len(buf) == 0 : break
        data_path_socket.send(buf)
    data_path_socket.close()
    in_file.close()

    # File has been sent - wait for final dialog with mover. We know the file
    # has hit some sort of media.... when this occurs. Create a file in pnfs
    # namespace with information about transfer.
    done_ticket = a_to_dict(control_socket.recv(10000))
    control_socket.close()
    if done_ticket["status"] == "ok" :
        p.set_bit_file_id(done_ticket["bfid"],done_ticket["size_bytes"]\
                          ,pprint.pformat(done_ticket))
        if list :
            print p.pnfsFilename, p.bit_file_id, p.file_size\
                  ,done_ticket["external_label"],done_ticket["bof_space_cookie"]
    else :
        raise errorcode[EPROTO],"encp.write_to_hsm: "\
              +"2nd (post-file-send) mover callback on socket at "\
              +repr(address)+", failed to transfer: "\
              +"ticket[\"status\"]="+ticket["status"]

##############################################################################

def read_from_hsm(pnfsfile, outfile, u, csc, list) :

    # first check the input pnfs file - this will also provide the bfid
    p = pnfs.pnfs(pnfsfile)
    if p.exists != pnfs.exists :
        raise errorcode[ENOENT],"encp.read_from_hsm: "\
              +pnfsfile+" does not exist"

    # Make sure we can open the unixfile. If we can't, we bomb out to user
    # Note that the unix file remains open
    f = open(outfile,"w")

    # get a port to talk on and listen for connections
    host, port, listen_socket = get_callback()
    listen_socket.listen(4)

    # generate the work ticket
    ticket = {"work"               : "read_from_hsm",
              "bfid"               : p.bit_file_id,
              "user_callback_port" : port,
              "user_callback_host" : host,
              "unique_id"          : time.time()
              }

    # ask configuration server what port the file clerk is using
    fticket = csc.get("file_clerk")

    # send work ticket to file clerk who sends it to right library manger
    ticket = u.send(ticket, (fticket['host'], fticket['port']))
    if not ticket['status'] == "ok" :
        raise errorcode[EPROTO],"encp.read_from_hsm: from u.send to "\
              +"file_clerk at "+fticket['host']+"/"+repr(fticket['port'])\
              +", ticket[\"status\"]="+ticket["status"]
    if list :
        print "Q'd:",p.pnfsFilename, ticket["bfid"], p.file_size\
              ,ticket["external_label"],ticket["bof_space_cookie"]

    # We have placed our work in the system and now we have to wait for
    # resources. All we  need to do is wait for the system to call us back,
    # and make sure that is it calling _us_ back, and not some sort of old
    # call-back to this very same port. It is dicey to time out, as it
    # is probably legitimate to wait for hours....
    while 1 :
        control_socket, address = listen_socket.accept()
        new_ticket = a_to_dict(control_socket.recv(10000))
        if ticket["unique_id"] == new_ticket["unique_id"] :
            listen_socket.close()
            break
        else:
            print ("encp read_from_hsm: imposter called us back, trying again")
            control_socket.close()
    ticket = new_ticket
    if not ticket["status"] == "ok" :
        raise errorcode[EPROTO],"encp.read_from_hsm: "\
              +"1st (pre-file-read) mover callback on socket at "\
              +repr(address)+", failed to setup transfer: "\
              +"ticket[\"status\"]="+ticket["status"]

    # If the system has called us back with our own  unique id, call back
    # the mover on the mover's port and read the file on that port.
    data_path_socket = mover_callback_socket(ticket)
    l = 0
    while 1:
        buf = data_path_socket.recv(65536*4)
        l = l + len(buf)
        if len(buf) == 0 : break
        f.write(buf)
    data_path_socket.close()
    f.close()

    # File has been read - wait for final dialog with mover.
    done_ticket = a_to_dict(control_socket.recv(10000))
    control_socket.close()
    if not done_ticket["status"] == "ok" :
        raise errorcode[EPROTO],"encp.read_from_hsm: "\
              +"2nd (post-file-read) mover callback on socket at "\
              +repr(address)+", failed to transfer: "\
              +"ticket[\"status\"]="+ticket["status"]
    if list :
        print outfile, p.file_size

##############################################################################

if __name__  ==  "__main__" :
    import getopt

    # defaults
    config_host = "localhost"
    #(config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0
    list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list", "list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
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
    if len(args) != 2 :
        print "python",sys.argv[0], options, "inputfilename outputfilename"
        print "   do not forget the '--' in front of each option"
        sys.exit(0)

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
        write_to_hsm(args[0], args[1], u, csc, list)
        if list > 1 :
            p=pnfs.pnfs(args[1],1)
            p.dump()

    # have we been called "encp hsmfile unixfile" ?
    elif p1==0 and p2==-1 :
        if list > 1 :
            p=pnfs.pnfs(args[0],1)
            p.dump()
        read_from_hsm(args[0], args[1], u, csc, list)

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
