#!/usr/bin/python

SEEK_SET = 0
ESPACE = 28
NoSpace = "no space for write"
DeviceError = "bad device"
MediaError = "Something wrong with the media"
import sys

STATE_UNLOAD = 0
STATE_LOAD = 1
STATE_OPEN_READ = 2
STATE_OPEN_WRITE = 3

class GenericDriver:

	def __init__(self, device, eod_cookie, remaining_bytes):
		# remember, this is just a guess
		self.remaining_bytes = remaining_bytes 
		self.device = device
		self.df = open(device, "a+")
		self.state = STATE_UNLOAD

	def load(self): pass
	
	def unload(self): pass
	
	# callers are to try to send buffers of iomax, excepting the last
	# buffer, which may be exact.
	def get_iomax(self) : return self.iomax

class  RawDiskDriver(GenericDriver) :

	""" I use this driver to enhance development -- I test using
	floppys, no gaurantee of good or general support....

	This driver implements the eod_cookie as an ascii number which
	represents the byte to seek to when appending to a disk file.

	This driver implements the file_cookie as the python ascification
	of a two-number list. -- The byte to seek to before doing a read,
	and a byte just past the last byte written when we are finished
	"""

	def __init__(self, device, eod_cookie, remaining_bytes):
		GenericDriver.__init__(self, device, eod_cookie, remaining_bytes)
		self.set_eod(eod_cookie)
		self.blocksize = 4096
		self.iomax = self.blocksize * 16

	def set_eod(self, eod_cookie) :
	#When a volume is ceated, the system sets EOD cookie to "none" 
		if eod_cookie == "none" :
			self.eod = 0
		else:
			self.eod = eval(eod_cookie)

	# read file -- use the "cookie" to
	# not walk off the end, sine we have no "file marks"
	# on a disk
	def open_file_read(self, file_location_cookie) :
		self.firstbyte, self.pastbyte = eval(file_location_cookie)
		self.df.seek(self.firstbyte, 0)
		self.left_to_read = self.pastbyte - self.firstbyte

	def close_file_read(self) :
		pass

	def read_block(self):
		# no file marks on a disk, so use the information
		# in the cookie to bound the file.
		n_to_read = min(self.iomax , self.left_to_read)
		if n_to_read == 0 : return ""
		buf = self.df.read(n_to_read)
		self.left_to_read = self.left_to_read - len(buf)
		if self.left_to_read < 0:
			raise "assert error"
		return buf

	#
	# New file routines.
	#
	def open_file_write(self):
		# we cannot auto sense a floppy, so we must trust the user
		self.df.seek(self.eod, 0)
		self.first_write_block = 1
	
	def close_file_write(self):
		first_byte = self.eod
		last_byte = self.df.tell()
		self.eod = last_byte
		self.eod = self.eod + (
			self.blocksize - (self.eod % self.blocksize))
		return `(first_byte, last_byte)`  #cookie describing the file

	def get_eod_cookie(self):
		return `self.eod`

	def get_eod_remaining_bytes(self):
		return self.remaining_bytes

	def write_block(self, data):
		try :
			if len(data) > self.remaining_bytes :
				raise NoSpace
			self.remaining_bytes = (self.remaining_bytes - 
				len(data))
			self.df.write(data)
			if self.first_write_block :
			     self.first_write_block = 0
			     self.eod = self.df.tell() - len(data)
			return
		except ESPACE :
			print sys.exc_type, sys.exc_value
			raise NoSpace
		except :
			print sys.exc_type, sys.exc_value
			raise DeviceError

if __name__ == "__main__" :
	rdd = RawDiskDriver ("/dev/fd0", "0", 760000)
	rdd.load()

	cookie = {}

	try: 
		rdd.open_file_write()
		rdd.write_block("0"*1)
		cookie[0] = rdd.close_file_write()

		rdd.open_file_write()
		rdd.write_block("1"*10)
		cookie[1] = rdd.close_file_write()

		rdd.open_file_write()
		rdd.write_block("2"*100)
		cookie[2] = rdd.close_file_write()

		rdd.open_file_write()
		rdd.write_block("3"*1000)
		cookie[3] = rdd.close_file_write()

		rdd.open_file_write()
		rdd.write_block("4"*10000)
		cookie[4] = rdd.close_file_write()

		rdd.open_file_write()
		rdd.write_block("5"*100000)
		cookie[5] = rdd.close_file_write()

		rdd.open_file_write()
		rdd.write_block("6"*1000000)
		cookie[6] = rdd.close_file_write()

		rdd.open_file_write()
		rdd.write_block("7"*1000000)
		cookie[7] = rdd.close_file_write()
	except:
		import sys
		print sys.exc_type, sys.exc_value
		pass

	print "EOD cookie: %s" % rdd.get_eod_cookie()
	print "Print lower bound on bytes available: %s " % \
					rdd.get_eod_remaining_bytes()

	for k in cookie.keys() :
		rdd.open_file_read(cookie[k])
		print  "cookie, %d READBACK, %s" % (k, rdd.read_block(1))
		rdd.close_file_read()

	rdd.unload()
