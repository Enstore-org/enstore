#!/usr/bin/env python

"""
	The purpose of this module is to provide a clean datagram
	interface for Enstore. Data grams are send, and no errors
	are returned if the peer if not present. 

	Notice that the python socket is a primitive type, like
	file descriptors, and so is not available for inheritance.
	therefore we have to pedantically code all the "methods".

	Specific errors that are masked:

	1) Linux ipv4 -- returning an error on the next UDP send or recieve
	when an ICMP port unreachable measage is recieved. Retry self.retry_max
	times to make sure we have no recieved sevearl messages , say
	the last several messge. N.B. the ECONNREFUSED message is cleared
	after it is returned to the user.

"""
import socket
import Trace

class cleanUDP :

	retry_max = 10

	def __init__(self, protocol, kind) :
		if kind != socket.SOCK_DGRAM :
			throw ("mis-use of class cleanUDP")
		self.socket = socket.socket(protocol, kind)
		return
	
	def accept(self) : 
		return self.socket.accept()
	def bind(self, address) : 
		return self.socket.bind(address)
	def close(self) : 
		retval =  self.socket.close()
		self.socket = None
		return retval
	def connect(self, address) : 
		return self.socket.bind(address)
	def fileno(self) : 
		return self.socket.fileno()
	def getpeername(self) : 
		return self.socket.getpeername()
	def getsockname(self) : 
		return self.socket.getscokname()
	def getsockopt(self, level, optname) :
		return self.socket.getsockopt(level, optname)
	def listen(self, backlog) : 
		return self.socket.listen(backlog)
	def makefile(self) : 
		return self.socket.makefile()
	def recv(self, bufsize) : 
		return self.socket.recv(bufsize)

	# Mitigate case 1 -- ECONNREFUSED from previous sendto
	def recvfrom(self, bufsize) : 
		for n in range(0, self.retry_max - 1) :
			try:
				return self.socket.recvfrom(bufsize)
			except socket.error:
				pass
			etext = "CleanUDP recvfom retry %d: %s  %d" % (
				n, address, errno.errorcode[badsock])
			Trace.trace(0, etext)
		return self.socket.recvfrom(bufsize)


	def send(self, string) : 
		return self.socket.send(string)

	# Mitigate case 1 -- ECONNREFUSED from previous sendto
	def sendto(self, string, address) : 
		for n in range(0, self.retry_max - 1) :
			try:
				return self.socket.sendto(string, address)
			except socket.error:
				pass
			etext = "Clean UDP sento try %d failed: %s  d" % (
				n, address)
			print etext, socket.error, type(socket.error)
			#Trace.trace(0, etext)
		return self.socket.sendto(string, address)
		
	def setblocking(self, flag) :
		return self.socket.setblocking(flag)
	def setsockopt(self, level, optname, value) : 
		return self.socket.setsockopt(level, optname, value)
	def shutdown(self, how) :
		return self.socket.shutdown(how)


if __name__ == "__main__" :
	s = cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
	s.bind(('localhost',303030))
	# on linux, should see one retry from the followng.
	s.sendto("all dogs have fleas", ('localhost', 303031))	
	s.sendto("all dogs have fleas", ('localhost', 303031))	











