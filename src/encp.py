#!/usr/bin/python

import os
import string
from callback import *
from configuration_server_client import configuration_server_client
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

def write_to_hsm(unixfile, pnfsfile, u, csc) :

    in_file = open(unixfile, "r")
    statinfo = os.stat(unixfile)
    fsize = statinfo[stat.ST_SIZE]
    if not stat.S_ISREG(statinfo[stat.ST_MODE]) :
        raise errorcode[EPERM],"encp.write_to_hsm: "+unixfile+" is not a regular file"

    p = pnfs.pnfs(pnfsfile)
    if p.valid != pnfs.valid :
        raise errorcode[EPERM],"encp.write_to_hsm: "+pnfsfile+" is an invalid pnfs filename"
    if p.exists == pnfs.exists :
        raise errorcode[EEXIST],"encp.write_to_hsm: "+pnfsfile+" already exists"

    host, port, listen_socket = get_callback()
    uqid = time.time()
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
              "unique_id"          : uqid
              }

    listen_socket.listen(4)
    vticket = csc.get(p.library + ".library_manager")
    ticket = u.send(ticket, (vticket['host'], vticket['port']))
    if not ticket['status'] == "ok" :
        raise errorcode[EPROTO],"encp.write_to_hsm: from u.send to "+p.library+\
              ".library_manager at "+\
              vticket['host']+"/"+repr(vticket['port'])+\
              ", ticket[\"status\"]="+ticket["status"]

    # so, we have placed our work in the system.
    # and now we have to wait for resources. All we
    # need to do is
    # wait for the system to call us back, and make
    # sure that is it calling _us_ back, and not some
    # sort of old call-back to this very same port.
    # It is dicey to time out, as it is probably legitimate
    # to wait for hours....
    while 1 :
        control_socket, address = listen_socket.accept()
        new_ticket = a_to_dict(control_socket.recv(10000))
        if ticket["unique_id"] == new_ticket["unique_id"] :
            listen_socket.close()
            break
        else:
            print ("imposter called us back, trying again")
            control_socket.close()

    # if the system has called us back with our own
    # unique id, call back the mover on the mover's port.
    # and send the file on that port.
    ticket = new_ticket
    if not ticket["status"] == "ok" :
        raise errorcode[EPROTO],"encp.write_to_hsm: from control socket at "+repr(address)+\
              ", ticket[\"status\"]="+ticket["status"]
    data_path_socket = mover_callback_socket(ticket)
    while 1:
        buf = in_file.read(min(fsize, 65536*4))
        l = len(buf)
        if len(buf) == 0 : break
        data_path_socket.send(buf)
    data_path_socket.close()

    # Final dialog with the mover. We know the file has
    # hit some sort of media....
    done_ticket = a_to_dict(control_socket.recv(10000))
    control_socket.close()
    if done_ticket["status"] == "ok" :
        p.set_bit_file_id(done_ticket["bfid"],done_ticket["size_bytes"],\
                          pprint.pformat(done_ticket))
        print unixfile, p.pnfsFilename, p.bit_file_id, p.file_size,\
              done_ticket["external_label"], done_ticket["bof_space_cookie"]
    else :
        raise errorcode[EPROTO],"encp.write_to_hsm: from control socket at "+repr(address)+\
              "  failed to transfer:  ticket[\"status\"]="+ticket["status"]



def read_from_hsm(pnfsfile, outfile, u, csc) :

    p = pnfs.pnfs(pnfsfile)
    if p.exists != pnfs.exists :
        raise errorcode[ENOENT],"encp.read_from_hsm: "+pnfsfile+" does not exist"

    f = open(outfile,"w")
    host, port, listen_socket = get_callback()
    uqid = time.time()

    ticket = {"work"               : "read_from_hsm",
              "bfid"               : p.bit_file_id,
              "user_callback_port" : port,
              "user_callback_host" : host,
              "unique_id"          : uqid
              }
    listen_socket.listen(4)

    fticket = csc.get("file_clerk")
    ticket = u.send(ticket, (fticket['host'], fticket['port']))
    if not ticket['status'] == "ok" :
        raise errorcode[EPROTO],"encp.read_from_hsm: from u.send to file_clerk at "+\
              fticket['host']+"/"+repr(fticket['port'])+\
              ", ticket[\"status\"]="+ticket["status"]

    # so, we have placed our work in the system.
    # and now we have to wait for resources. All we
    # need to do is
    # wait for the system to call us back, and make
    # sure that is it calling _us_ back, and not some
    # sort of old call-back to this very same port.
    # It is dicey to time out, as it is probably legitimate
    # to wait for hours....
    while 1 :
        control_socket, address = listen_socket.accept()
        new_ticket = a_to_dict(control_socket.recv(10000))
        if ticket["unique_id"] == new_ticket["unique_id"] :
            listen_socket.close()
            break
        else:
            print ("imposter called us back, trying again")
            control_socket.close()

    # if the system has called us back with our own
    # unique id, call back the mover on the mover's port.
    # and send the file on that port.
    ticket = new_ticket
    if not ticket["status"] == "ok" :
        raise ticket["status"]
    data_path_socket = mover_callback_socket(ticket)
    l = 0
    while 1:
        buf = data_path_socket.recv(65536*4)
        l = l + len(buf)
        if len(buf) == 0 : break
        f.write(buf)
    data_path_socket.close()


    # Final dialog with the mover. We know the file has
    # hit some sort of media....
    done_ticket = a_to_dict(control_socket.recv(10000))
    control_socket.close()
    if not done_ticket["status"] == "ok" :
        raise errorcode[EPROTO],"encp.read_from_hsm: from control socket at "+repr(address)+\
              "  failed to transfer:  ticket[\"status\"]="+ticket["status"]



if __name__  ==  "__main__" :
    csc = configuration_server_client()
    u = UDPClient()

    p1 = string.find(sys.argv[1],"/pnfs/")
    p2 = string.find(sys.argv[2],"/pnfs/")

    # have we been called "encp unixfile hsmfile" ?
    if p1==-1 and p2==0 :
        write_to_hsm(sys.argv[1], sys.argv[2], u, csc)

    # have we been called "encp hsmfile unixfile" ?
    elif p1==0 and p2==-1 :
        read_from_hsm(sys.argv[1], sys.argv[2], u, csc)

    # have we been called "encp unixfile unixfile" ?
    elif p1==-1 and p2==-1 :
        print "encp copies to/from hsm. It is not involved in copying  "+sys.argv[1]," to ",sys.argv[2]

    # have we been called "encp unixfile unixfile" ?
    elif p1==0 and p2==0 :
        print "encp hsm to hsm is not functional. copy hsmfile to local disk and them back to hsm"

    else:
        print "ERROR: Can not process arguments "+sys.argv[1]," to ",sys.argv[2]
