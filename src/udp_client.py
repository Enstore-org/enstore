#!/usr/bin/python

import socket
import time
import select
TRANSFER_MAX=1024

def try_a_port(host, port) :

	try:
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.bind(host, port)
	except:
		sock.close()
		return (0 , sock)
	return 1 , sock

def get_client() :
	host = 'localhost'
	#host = socket.gethostname()
        while  1:
		for port in range (7600, 7700) :
		# by the way range (7600, 7600) has 0 memebers...
			success, socket = try_a_port (host, port)  
			if success :
				return host, port, socket   ## Exit the while 
		sleep (10) # tried all ports, try later.


class UDPClient:	
	
	def __init__(self):
		self.number = 0
		self.host, self.port, self.socket = get_client()
		self.ident = "%s-%d-%d" % (self.host, 
					   self.port, 
					   long(time.time())
					   )
	def send(self, text, address) :
		self.number = self.number + 1
		message = `(self.ident, self.number, text)`
		if len(message) > TRANSFER_MAX :
			raise "message too big : %s" % message
		number = 0
		while not number == self.number:
			self.socket.sendto (message, address)
			f  = self.socket.fileno()
			r, w, x = select.select([f],[],[f],10)
			if x :
				raise "peer exited"
			if r : 
				reply , server = self.socket.recvfrom(
								TRANSFER_MAX)
				exec ("number,  out  = "  + reply)
		return out
			

if __name__ == "__main__" :
	u = UDPClient()
	print u.send("all dogs have fleas", ('localhost', 7550))
	print u.send("all dogs have fleas", ('localhost', 7550))







