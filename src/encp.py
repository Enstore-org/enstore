#!/usr/bin/python

from pnfs_surrogate import *
import os
import string
from callback import *
from configuration_server_client import configuration_server_client
from dict_to_a import *
from udp_client import UDPClient
import stat
import time

# Import SOCKS module if it exists, else standard socket module socket
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

def  write_to_hsm(unixfile, u, csc) :

	in_file = open(unixfile, "r")
	statinfo = os.stat(unixfile)
	fsize = statinfo[stat.ST_SIZE]	
	if not stat.S_ISREG(statinfo[stat.ST_MODE]) :
		raise "can only handle regular files"

	host, port, listen_socket = get_callback()
	uqid = time.time()
	ticket = {"work" : "write_to_hsm",
		  "library" : pnfs_library, 
		  "file_family" : pnfs_file_family,
		  "file_family_width" : pnfs_file_family_width,
		  "uid" : pnfs_uid,
		  "uname" : pnfs_uname,
		  "gid" : pnfs_gid,
		  "gname" : pnfs_uname,
  		  "protection" : pnfs_file_mode,
		  "mtime" : int(time.time()), 
		  "size_bytes" : fsize,
		  "user_callback_port" : port,
		  "user_callback_host" : host,
		  "unique_id" : uqid
		  }
	listen_socket.listen(4)
	vticket = csc.get(pnfs_library + ".library_manager")
	ticket = u.send(ticket, (vticket['host'], vticket['port']))
	if not ticket['status'] == "ok" : 
		raise ticket["status"]
	#
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
	while 1:
		buf = in_file.read(min(fsize, 65536*4))
		l = len(buf)
		if len(buf) == 0 : break
		data_path_socket.send(buf)
	data_path_socket.close()
	#
	# Finaldialoog with the mover. WE know the file has
	# hit some sort of media....
	#
	done_ticket = a_to_dict(control_socket.recv(10000))
	control_socket.close()
	if done_ticket["status"] == "ok" :
		print unixfile, done_ticket["bfid"]
	else :
		raise "failed to transfer: " + ticket["status"]


def  read_from_hsm(bfid, outfile, u, csc) :

	f = open(outfile,"w")	
	host, port, listen_socket = get_callback()
	uqid = time.time()

	ticket = {"work" : "read_from_hsm",
		  "bfid" : bfid,
		  "user_callback_port" : port,
		  "user_callback_host" : host,
		  "unique_id" : uqid
		  }
	listen_socket.listen(4)

	fticket = csc.get("file_clerk")
	ticket = u.send(ticket, (fticket['host'], fticket['port']))
	if not ticket['status'] == "ok" : 
		raise ticket["status"]
	#
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
	#
	# Finaldialoog with the mover. WE know the file has
	# hit some sort of media....
	#
	done_ticket = a_to_dict(control_socket.recv(10000))
	control_socket.close()
	if not done_ticket["status"] == "ok" :
		raise "failed to transfer: " + ticket["status"]


if __name__  ==  "__main__" :
	csc = configuration_server_client()
	u = UDPClient()
	if sys.argv[1] == "w" :
		write_to_hsm(sys.argv[2], u, csc)
	elif sys.argv[1] == "r" :
		read_from_hsm(sys.argv[2], sys.argv[3], u, csc)
	else:
		print " w file | r bfid outfile"









