#!/usr/bin/python

from SocketServer import *
from lockfile import *
from time import *
from dict_to_a import *

def try_a_port(host, port) :
	try:
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind(host, port)
	except:
		sock.close()
		return (0 , sock)
	return 1 , sock

def get_callback() :
	host = 'localhost'
        
        #
        # get the hunt lock.  We have the exlusive right to hunt for
	# limited rights to connect to the system
	# Hunt lock will (I hope) properly serlialze the waiters.
	# so that they will be services in the orser of arrival.
	# system will properrly clean up, even on kill -9
	#
	lockf = open ("/var/lock/hsm/lockfile", "w")
	writelock(lockf)  #holding the write lock signifies the 
			  #right to hunt for a port.
        while  1:
		for port in range (7600, 7650) :
		# by the way range (7600, 7600) has 0 memebers...
			success, socket = try_a_port (host, port)  
			if success :
				unlock(lockf) 
				lockf.close()   
				return host, port, socket   ## Exit the while 
		sleep (1) # tried all ports, try later.
			# remeber, only the person with the hunt lock is
			# pounding so hard on this code....



def mover_callback_socket(ticket) :
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(ticket['mover_callback_host'], 
			ticket['mover_callback_port'])
	return sock

def user_callback_socket(ticket) :
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(ticket['user_callback_host'], 
			ticket['user_callback_port'])
	sock.send(dict_to_a(ticket)) 
	return sock

def send_to_user_callback(ticket) :
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(ticket['user_callback_host'], 
			ticket['user_callback_port'])
	sock.send(dict_to_a(ticket)) 
	sock.close()

if __name__ == "__main__" :
	print get_callback()

