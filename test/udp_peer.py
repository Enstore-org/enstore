#
#@(#)$Id$
#
#	Reliable UDP-based communication protocol implementation
#
#	Server example:
#	
#		from udp_peer import *
#		from socket import *
#		sock = socket(AF_INET, SOCK_DGRAM)
#		sock.bind('', 5000)
#		peer = UdpPeer(sock)
#
#		while 1:
#			msg, sender = peer.recvfrom(1024)
#			if msg == 'Ping':
#				peer.sendto('Pong', sender)
#
#	Client example:
#		from udp_peer import *
#		from socket import *
#		peer = UdpPeer()
#		while 1:
#			peer.sendto('Ping', ('127.0.0.1', 5000))
#			reply, sender = peer.recvfrom(1024)
#			print reply	# should be 'Pong'
#
#	Methods:
#		sts = peer.sendto(msg, (address, port)) sends msg to the 
#			specified peer, which must be another UdpPeer
#
#			Returns 1 on success or 0 if internal buffer
#			is full because the peer does not confirm
#			data receipt
#
#		data, sender = peer.recvfrom(max) receives message on
#			the socket
#			
#			Returns the message and (address,port) pair for
#			sender.
#		
#
#	Exceptions:
#		UdpPeer.sendto() and recvfrom() raise the following
#		exceptions:
#		UdpError		- low level socket error
#		UdpDisconnect, peer	- peer (apparently) disconnected
#		UdpTimeout		- data transfer request timed out
#

from socket import *
import string
import select
import time

UdpDisconnect = "Peer disconnected"
UdpTimeout = "Operation timed out"
UdpError = "Socket error"

def	RelUdpPack(type, seq=0, data=''):
	return '%s:%d:%s' % (type, seq, data)

def	RelUdpUnpack(pkt):
	i1 = string.find(pkt, ':')
	type = pkt[:i1]
	i2 = string.find(pkt, ':', i1+1)
	seq = string.atoi(pkt[i1+1:i2])
	data = pkt[i2+1:]
	return type, seq, data
	
class	UdpPeer:
	def	__init__(self, sock = -1):
		if sock != -1:
			self.Sock = sock
		else:
			self.Sock = socket(AF_INET, SOCK_DGRAM)
		self.Links = {}
		self.RetryTimeout = 1 
		self.FailTimeout = 10 
		self.CloseTimeout = 30 
		self.LastNakTime = time.time()

	def	idle(self):
		while self._waitForInput(0) != 0:
			self._processInput()
	
	def	_sendNaks(self):
		if time.time() < self.LastNakTime + self.RetryTimeout:
			return
		t = time.time()
		self.LastNakTime = t
		for peer in self.Links.keys():
			lnk = self.Links[peer]
			if t > lnk.LastSeen + self.CloseTimeout:
				del self.Links[peer]
				raise UdpDisconnect, peer
			else:
				lnk.sendNak()
		
	def	_waitForInput(self, timeout = -1):
		#print '_waitForInput(%d): ' % timeout,
		tmo = timeout
		rd, wr, ex = select.select([self.Sock], [], [self.Sock], tmo)
		if self.Sock in rd:
			#print 'got it'
			return 1 
		elif self.Sock in ex:
			raise	UdpError
		else:
			#print 'nothing'
			return 0

	def	_createLink(self, peer):
		link = Link(peer, self.Sock)
		self.Links[peer] = link
		link.LastSeen = time.time()
		return link

	def	_processInput(self):
		data, peer = self.Sock.recvfrom(100000)
		try:	link = self.Links[peer]
		except KeyError:
			type, seq, pkt = RelUdpUnpack(data)
			if type != 'NACK' or seq != 1:	return
			link = self._createLink(peer)
		link.dataIn(data)
		link.LastSeen = time.time()

	def	sendto(self, data, peer):
		#
		# First, see if something has arrived
		#
		self.idle()

		#
		# If new peer, create it
		#
		try:
			link = self.Links[peer]
		except KeyError:
			link = self._createLink(peer)

		if link.outBufferFull() == 0:
			link.dataOut(data)
			return 1

		#
		# Wait for room in output buffer
		#
		t0 = time.time()
		nextResend = t0 + self.RetryTimeout
		while time.time() < t0 + self.FailTimeout:
			while self._waitForInput(self.RetryTimeout) != 0:
				self._processInput()
				if link.outBufferFull():
					if time.time() > nextResend:
						link.resendData()
						nextResend = time.time() + \
							self.RetryTimeout
				else:
					link.dataOut(data)
					return 1
			if time.time() > nextResend:
				link.sendData()
				nextResend = time.time() + \
					self.RetryTimeout
		return 0 # timeout

	def	recvfrom(self, max):
		#
		# First, see if something has arrived
		#
		self.idle()

		#
		# Check on all peers if they have anything
		#
		for peer in self.Links.keys():
			lnk = self.Links[peer]
			data = lnk.getData()
			if data != 0:
				return data, peer

		t0 = time.time()
		while int(time.time()) - t0 < self.FailTimeout:
			if self._waitForInput(self.RetryTimeout) == 0:
				self._sendNaks()
				continue

			data, peer = self.Sock.recvfrom(max)
			try: lnk = self.Links[peer]
			except KeyError:
				# new peer
				type, seq, pkt = RelUdpUnpack(data)
				if type != 'NACK' or seq != 1: continue 
				lnk = self._createLink(peer)
			# existing peer, see what we can do...
			lnk.LastSeen = time.time()
			lnk.dataIn(data)
			data = lnk.getData()
			if data != 0: return data, lnk.Peer

		self._sendNaks()
		raise UdpTimeout
	


class	Link:
	def	__init__(self, peer = (), sock = -1):
		self.Peer = peer
		self.InRecvd = 0
		self.InNext = 1
		self.OutSent = 0
		self.OutAck = 0
		self.Sent = {}
		self.Received = {}    
		self.Window = 10
		self.Connected = 0

		if sock >= 0: 	self.Sock = sock
		else: 		self.Sock = socket(AF_INET, SOCK_DGRAM)
		if peer != (): 	self.sendNak(1)

	def	_isRecvd(self, seq):
		if seq > self.InRecvd: 	return 0
		try:
			pkt = self.Received[seq]
			gotit = 1
		except KeyError: gotit = 0
		return gotit

	def	_isNextRecvd(self):
		return self._isRecvd(self.InNext)

	def	getData(self):
		if self._isNextRecvd() == 0:	return 0
		pkt = self.Received[self.InNext]
		#print 'getData(): got it: <%s>' % pkt  
		seq = self.InNext
		del self.Received[seq]
		self.InNext = seq + 1
		return pkt  
		
	def	outBufferFull(self):	
		return self.OutSent - self.OutAck > self.Window

	def	dataOut(self, data):
		seq = self.OutSent + 1
		pkt = RelUdpPack('DATA', seq, data)
		self.Sock.sendto(pkt, self.Peer)
		#print 'Link %s: <DATA,%d> sent' % \
		#	(self.Peer, seq)
		self.OutSent = seq	
		self.Sent[seq] = pkt

	def	dataIn(self, pkt):
		type, seq, data = RelUdpUnpack(pkt)
		#print 'Link %s: <%s,%d> received' % \
		# 	(self.Peer, type, seq)
		#print 'dataIn(): got <%s %d> from %s  Ack/Sent = %d/%d' % \
		#		(type, seq, self.Peer, self.OutAck, self.OutSent)

		#
		# Process the packet
		#
		if type == 'ACKN':
			if seq <= self.OutSent and seq > self.OutAck:
				while self.OutAck < seq:
					self.OutAck = self.OutAck + 1
					del self.Sent[self.OutAck]
				#print 'ackn: out ack = %d' % \
				#	self.OutAck

		elif type == 'NACK':
			#print 'NACK %d: OutAck/Sent = %d/%d' % \
			#	(seq, self.OutAck, self.OutSent)
			self.sendData(seq)

		elif type == 'DATA':
			self.Received[seq] = data
			if seq > self.InRecvd: 	
				self.InRecvd = seq
			i = self.InNext
			sendAck = -1
			while i <= self.InRecvd and self._isRecvd(i):
				sendAck = i
				i = i + 1
			if sendAck > 0:
				self.sendAck(sendAck)

		else: # unknown packet -- ignore
			pass

	def	sendAck(self, seq):
		#print 'Sending ACK %d' % seq
		if self.Peer != ():
			pkt = RelUdpPack('ACKN', seq)
			self.Sock.sendto(pkt, self.Peer)
			#print 'Link %s: <ACKN,%d> sent' % \
			#	(self.Peer, seq)
		
	def	sendNak(self, seq = -1):
		if seq == -1:	seq = self.InNext
		#print 'Sending NACK %d' % seq
		if self.Peer != ():
			pkt = RelUdpPack('NACK', seq)
			self.Sock.sendto(pkt, self.Peer)
			#print 'Link %s: <NACK,%d> sent' % \
			#	(self.Peer, seq)
		
	def	sendData(self, seq = -1):
		if seq == -1:
			seq = self.OutAck + 1
		if seq <= self.OutSent and seq > self.OutAck:
			self.Sock.sendto(self.Sent[seq], self.Peer)
			#print 'Link %s: <DATA,%d> sent' % \
			#	(self.Peer, seq)

