#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system import
import os
import sys
import string
import pprint
import pwd
import socket
import select
import errno

# enstore import
import generic_client
import option
import time
import callback
import hostaddr
import e_errors
import enstore_constants
import edb

MY_NAME = enstore_constants.INFO_CLIENT     #"info_client"
MY_SERVER = enstore_constants.INFO_SERVER   #"info_server"
RCV_TIMEOUT = 10
RCV_TRIES = 1


#turn byte count into a nicely formatted string
def capacity_str(x,mode="GB"):
	if mode == "GB":
		z = x/1024./1024./1024. # GB
		return "%7.2fGB"%(z,)

	x=1.0*x	## make x floating-point
	neg=x<0	## remember the sign of x
	x=abs(x)   ##  make x positive so that "<" comparisons work

	for suffix in ('B ', 'KB', 'MB', 'GB', 'TB', 'PB'):
		if x <= 1024:
			break
		x=x/1024
	if neg:	## if x was negative coming in, restore the - sign  
		x = -x
	return "%6.2f%s"%(x,suffix)

def show_volume_header():
	print "%-16s %9s   %-41s   %-16s %-36s %-12s"%(
		"label", "avail.", "system_inhibit", "library", "volume_family", "comment")

def show_volume(v):
	# pprint.pprint(v)
	si0t = ''
	si1t = ''
	si_time = (edb.timestamp2time(v['si_time_0']), edb.timestamp2time(v['si_time_1']))
	if si_time[0] > 0:
		si0t = time.strftime("%m%d-%H%M",
			time.localtime(si_time[0]))
	if si_time[1] > 0:
		si1t = time.strftime("%m%d-%H%M",
			time.localtime(si_time[1]))
	print "%-16s %9s   (%-10s %9s %-8s %9s)   %-16s %-36s"%(
		v['label'], capacity_str(v['remaining_bytes']),
		v['system_inhibit_0'], si0t,
		v['system_inhibit_1'], si1t,
		# v['user_inhibit_0'],v['user_inhibit_1'],
		v['library'],
		v['storage_group']+'.'+v['file_family']+'.'+v['wrapper']),
	if v['comment']:
		print v['comment']
	else:
		print

class infoClient(generic_client.GenericClient):
	def __init__(self, csc, logname='UNKNOWN', rcv_timeout = RCV_TIMEOUT,
		     rcv_tries = RCV_TRIES, flags=0, logc=None, alarmc=None,
		     server_address = None):

		self.logname = logname
		self.node = os.uname()[1]
		self.pid = os.getpid()
		generic_client.GenericClient.__init__(self, csc, MY_NAME,
						      server_address,
						      flags=flags, logc=logc,
						      alarmc=alarmc,
						      server_name = MY_SERVER)
		try:
			self.uid = pwd.getpwuid(os.getuid())[0]
		except:
			self.uid = "unknown"
		self.rcv_timeout = rcv_timeout
		self.rcv_tries = rcv_tries
		#self.server_address = self.get_server_address(MY_SERVER, self.rcv_timeout, self.rcv_tries)

	# send_no_wait
	def send2(self, ticket):
		if not self.server_address: return
		self.u.send_no_wait(ticket, self.server_address)

	# generic test
	def hello(self):
		if not self.server_address: return
		ticket = {'work': 'hello'}
		return self.send(ticket, 30, 1)

	# generic test for send_no_wait
	def hello2(self):
		if not self.server_address: return
		ticket = {'work': 'hello'}
		return self.send2(ticket)

	def debug(self, level = 0):
		ticket = {
			'work': 'debugging',
			'level': level}
		self.send2(ticket)

	def debug_on(self):
		self.debug(1)

	def debug_off(self):
		self.debug(0)

	def bfid_info(self, bfid):
		r = self.send({"work" : "bfid_info", "bfid" : bfid } )
		try:
			del r['work']
		except: # something is wrong
			msg = 'ticket = '+`r`
			r['status'] = (e_errors.ERROR, msg)
		return r

	def find_same_file(self, bfid):
		return self.send({"work": "find_same_file", "bfid": bfid})

	def inquire_vol(self, external_label, timeout=60, retry=10):
		ticket= { 'work': 'inquire_vol',
			'external_label' : external_label }
		return self.send(ticket,timeout,retry)

	# get a list of all volumes
	def get_vols(self, key=None,state=None, not_cond=None, print_list=1):
		# get a port to talk on and listen for connections
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		  : "get_vols2",
				  "callback_addr" : (host, port),
				  "key"		   : key,
				  "in_state"	  : state,
				  "not"		   : not_cond}

		# send the work ticket to the library manager
		ticket = self.send(ticket, 60, 1)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r,w,x = select.select([listen_socket], [], [], 60)
		if not r:
			raise errno.errorcode[errno.ETIMEDOUT], "timeout wiating for info clerk callback"
		
		control_socket, address = listen_socket.accept()
		
		if not hostaddr.allow(address):
			control_socket.close()
			listen_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)
		
		ticket = callback.read_tcp_obj(control_socket)

		listen_socket.close()

		if ticket["status"][0] != e_errors.OK:
			return ticket

		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
		ticket= callback.read_tcp_obj(data_path_socket)
		volumes = callback.read_tcp_obj_new(data_path_socket)
		data_path_socket.close()


		# Work has been read - wait for final dialog with volume clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket

		if volumes.has_key("header"):		# full info
			if print_list:
				show_volume_header()
				print
				for v in volumes["volumes"]:
					show_volume(v)
		else:
			vlist = ''
			for v in volumes.get("volumes",[]):
				vlist = vlist+v['label']+" "
			if print_list:
				print vlist
				
		ticket['volumes'] = volumes.get('volumes',[])
		return ticket

	def get_pvols(self):
		# get a port to talk on and listen for connections
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		: "get_pvols",
			  "callback_addr"	: (host, port)}

		# send the work ticket to the library manager
		ticket = self.send(ticket, 60, 1)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r,w,x = select.select([listen_socket], [], [], 60)
		if not r:
			raise errno.errorcode[errno.ETIMEDOUT], "timeout wiating for info clerk callback"
		
		control_socket, address = listen_socket.accept()
		
		if not hostaddr.allow(address):
			control_socket.close()
			listen_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)
		
		ticket = callback.read_tcp_obj(control_socket)

		listen_socket.close()

		if ticket["status"][0] != e_errors.OK:
			return ticket

		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
		ticket= callback.read_tcp_obj(data_path_socket)
		volumes = callback.read_tcp_obj_new(data_path_socket)
		data_path_socket.close()


		# Work has been read - wait for final dialog with volume clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket

		ticket['volumes'] = volumes.get('volumes',[])
		return ticket

	def get_sg_count(self, lib, sg, timeout=60, retry=10):
		ticket = {'work':'get_sg_count',
				  'library': lib,
				  'storage_group': sg}
		return(self.send(ticket,timeout,retry))

	# list all sg counts
	def list_sg_count(self):
		# get a port to talk on and listen for connections
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		  : "list_sg_count",
				  "callback_addr" : (host, port)}

		ticket = self.send(ticket,60,1)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r,w,x = select.select([listen_socket], [], [], 60)
		if not r:
			raise errno.errorcode[errno.ETIMEDOUT], "timeout wiating for volume clerk callback"
		
		control_socket, address = listen_socket.accept()
		
		if not hostaddr.allow(address):
			control_socket.close()
			listen_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)
		
		ticket = callback.read_tcp_obj(control_socket)

		listen_socket.close()

		if ticket["status"][0] != e_errors.OK:
			return ticket

		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
		ticket= callback.read_tcp_obj(data_path_socket)
		sgcnt = callback.read_tcp_obj_new(data_path_socket)
		data_path_socket.close()

		# Work has been read - wait for final dialog with volume clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket

		ticket['sgcnt'] = sgcnt
		return ticket

	# get a list of all volumes
	def get_vol_list(self):
		# get a port to talk on and listen for connections
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		  : "get_vol_list",
				  "callback_addr" : (host, port)}

		# send the work ticket to the library manager
		ticket = self.send(ticket,60,1)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r,w,x = select.select([listen_socket], [], [], 60)
		if not r:
			raise errno.errorcode[errno.ETIMEDOUT], "timeout wiating for volume clerk callback"
		
		control_socket, address = listen_socket.accept()
		
		if not hostaddr.allow(address):
			control_socket.close()
			listen_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)
		
		ticket = callback.read_tcp_obj(control_socket)

		listen_socket.close()

		if ticket["status"][0] != e_errors.OK:
			return ticket

		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
		ticket= callback.read_tcp_obj(data_path_socket)
		volumes = callback.read_tcp_obj_new(data_path_socket)
		data_path_socket.close()

		# Work has been read - wait for final dialog with volume clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket

		ticket['volumes'] = volumes
		return ticket

	def get_bfids(self,external_label):
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		  : "get_bfids",
				  "callback_addr" : (host, port),
				  "external_label": external_label}
		# send the work ticket to the file clerk
		ticket = self.send(ticket)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r, w, x = select.select([listen_socket], [], [], 60)
		if not r:
			listen_socket.close()
			raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
		control_socket, address = listen_socket.accept()
		if not hostaddr.allow(address):
			listen_socket.close()
			control_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

		ticket = callback.read_tcp_obj(control_socket)
		listen_socket.close()
		
		if ticket["status"][0] != e_errors.OK:
			return ticket
		
		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
  
		ticket= callback.read_tcp_obj(data_path_socket)
		list = callback.read_tcp_obj_new(data_path_socket)
		ticket['bfids'] = list
		data_path_socket.close()

		# Work has been read - wait for final dialog with file clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket

		return ticket

	def list_active(self,external_label):
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		  : "list_active",
				  "callback_addr" : (host, port),
				  "external_label": external_label}
		# send the work ticket to the file clerk
		ticket = self.send(ticket)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r, w, x = select.select([listen_socket], [], [], 60)
		if not r:
			listen_socket.close()
			raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
		control_socket, address = listen_socket.accept()
		if not hostaddr.allow(address):
			listen_socket.close()
			control_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

		ticket = callback.read_tcp_obj(control_socket)
		listen_socket.close()
		
		if ticket["status"][0] != e_errors.OK:
			return ticket
		
		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
  
		ticket= callback.read_tcp_obj(data_path_socket)
		list = callback.read_tcp_obj_new(data_path_socket)
		ticket['active_list'] = list
		data_path_socket.close()

		# Work has been read - wait for final dialog with file clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket

		return ticket

	def tape_list(self,external_label):
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		  : "tape_list2",
				  "callback_addr" : (host, port),
				  "external_label": external_label}
		# send the work ticket to the file clerk
		ticket = self.send(ticket)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r, w, x = select.select([listen_socket], [], [], 60)
		if not r:
			listen_socket.close()
			raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
		control_socket, address = listen_socket.accept()
		if not hostaddr.allow(address):
			listen_socket.close()
			control_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

		ticket = callback.read_tcp_obj(control_socket)
		listen_socket.close()
		
		if ticket["status"][0] != e_errors.OK:
			return ticket
		
		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
  
		ticket= callback.read_tcp_obj(data_path_socket)
		vol = callback.read_tcp_obj_new(data_path_socket)
		ticket['tape_list'] = vol
		data_path_socket.close()

		# Work has been read - wait for final dialog with file clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket

		return ticket

	# show_history
	def show_history(self, vol):
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		   : "history",
				  "external_label" : vol,
				  "callback_addr"  : (host, port)}

		# send the work ticket to volume clerk
		ticket = self.send(ticket, 10, 1)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r, w, x = select.select([listen_socket], [], [], 60)
		if not r:
			listen_socket.close()
			raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for volume clerk callback"

		control_socket, address = listen_socket.accept()
		if not hostaddr.allow(address):
			listen_socket.close()
			control_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

		ticket = callback.read_tcp_obj(control_socket)
		listen_socket.close()
		if ticket["status"][0] != e_errors.OK:
			return ticket

		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
		ticket= callback.read_tcp_obj(data_path_socket)
		res = callback.read_tcp_obj_new(data_path_socket)
		ticket['history'] = res

		data_path_socket.close()

		# Work has been read - wait for final dialog with volume clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket

		return ticket

	def write_protect_status(self, vol):
		ticket = {"work"			: "write_protect_status",
				  "external_label"  : vol}
		return self.send(ticket)

	def show_bad(self):
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		  : "show_bad",
				  "callback_addr" : (host, port)}
		# send the work ticket to the file clerk
		ticket = self.send(ticket)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r, w, x = select.select([listen_socket], [], [], 60)
		if not r:
			listen_socket.close()
			raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
		control_socket, address = listen_socket.accept()
		if not hostaddr.allow(address):
			listen_socket.close()
			control_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

		ticket = callback.read_tcp_obj(control_socket)
		listen_socket.close()
		
		if ticket["status"][0] != e_errors.OK:
			return ticket
		
		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
  
		ticket= callback.read_tcp_obj(data_path_socket)
		bad_files = callback.read_tcp_obj_new(data_path_socket)
		ticket['bad_files'] = bad_files
		data_path_socket.close()

		# Work has been read - wait for final dialog with file clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket
		return ticket

	def query_db(self, q):
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"	  : "query_db",
			  "query"         : q,
			  "callback_addr" : (host, port)}
		# send the work ticket to the file clerk
		ticket = self.send(ticket)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r, w, x = select.select([listen_socket], [], [], 60)
		if not r:
			listen_socket.close()
			raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
		control_socket, address = listen_socket.accept()
		if not hostaddr.allow(address):
			listen_socket.close()
			control_socket.close()
			raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

		ticket = callback.read_tcp_obj(control_socket)
		listen_socket.close()
		
		if ticket["status"][0] != e_errors.OK:
			return ticket
		
		data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		data_path_socket.connect(ticket['info_clerk_callback_addr'])
  
		ticket= callback.read_tcp_obj(data_path_socket)
		result = callback.read_tcp_obj_new(data_path_socket)
		ticket['result'] = result
		data_path_socket.close()

		# Work has been read - wait for final dialog with file clerk
		done_ticket = callback.read_tcp_obj(control_socket)
		control_socket.close()
		if done_ticket["status"][0] != e_errors.OK:
			return done_ticket
		return ticket

def show_query_result(result):
	width = []
	w = len(result['fields'])
	for i in range(w):
		width.append(len(result['fields'][i]))

	for r in result['result']:
		for i in range(w):
			l1 = len(str(r[i]))
			if l1 > width[i]:
				width[i] = l1

	format = []
	for i in range(w):
		format.append("%%%ds "%(width[i]))

	ll = 0
	for i in range(w):
		ll = ll + width[i]
	ll = ll + 2*(w - 1)

	for i in range(w):
		print format[i]%(result['fields'][i]),
	print
	print "-"*ll
	for r in result['result']:
		for i in range(w):
			print format[i]%(r[i]),
		print

class InfoClientInterface(generic_client.GenericClientInterface):

	def __init__(self, args=sys.argv, user_mode=1):
		self.list =None 
		self.bfid = 0
		self.bfids = None
		self.check = ""
		self.alive_rcv_timeout = 0
		self.alive_retries = 0
		self.ls_active = None
		self.vols = 0
		self.pvols = None
		self.gvol = None
		self.ls_sg_count = 0
		self.get_sg_count = None
		self.vol = None
		self.ls_sg_count = 0
		self.just = None
		self.labels = None
		self.history = None
		self.write_protect_status = None
		self.show_bad = 0
		self.query = ''
		self.find_same_file = None

		generic_client.GenericClientInterface.__init__(self, args=args,
													   user_mode=user_mode)


	def valid_dictionaries(self):
		return (self.alive_options, self.help_options, self.trace_options,
				self.info_options)

	info_options = {
		option.BFID:{option.HELP_STRING:"get info of a file",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.USER_LEVEL:option.USER},
		option.FIND_SAME_FILE:{option.HELP_STRING:"find a file of the same size and crc",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_LABEL: "bfid",
				option.VALUE_USAGE:option.REQUIRED,
				option.USER_LEVEL:option.ADMIN},
		option.BFIDS:{option.HELP_STRING:"list all bfids on a volume",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"volume_name",
				option.USER_LEVEL:option.ADMIN},
		option.CHECK:{option.HELP_STRING:"check a volume",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"volume_name",
				option.USER_LEVEL:option.ADMIN},
		option.LIST:{option.HELP_STRING:"list the files in a volume",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"volume_name",
				option.USER_LEVEL:option.USER},
		option.LS_ACTIVE:{option.HELP_STRING:"list active files in a volume",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"volume_name",
				option.USER_LEVEL:option.USER},
		option.SHOW_BAD:{option.HELP_STRING:"list all bad files",
				option.DEFAULT_VALUE:option.DEFAULT,
				option.DEFAULT_TYPE:option.INTEGER,
				option.VALUE_USAGE:option.IGNORED,
				option.USER_LEVEL:option.USER},
		option.GET_SG_COUNT:{
				option.HELP_STRING: 'check allocated count for lib,sg',
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"library",
				option.USER_LEVEL:option.ADMIN,
				option.EXTRA_VALUES:[{
					option.VALUE_NAME:"storage_group",
					option.VALUE_LABEL:"storage_group",
					option.VALUE_TYPE:option.STRING,
					option.VALUE_USAGE:option.REQUIRED}]},
		option.VOL:{option.HELP_STRING:"get info of a volume",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"volume_name",
				option.USER_LEVEL:option.USER},
		option.QUERY:{option.HELP_STRING:"query database",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"query",
				option.USER_LEVEL:option.ADMIN},
		option.LABELS:{
				option.HELP_STRING:"list all volume labels",
				option.DEFAULT_VALUE:option.DEFAULT,
				option.DEFAULT_TYPE:option.INTEGER,
				option.VALUE_USAGE:option.IGNORED,
				option.USER_LEVEL:option.ADMIN},
		option.GVOL:{option.HELP_STRING:"get info of a volume in human readable time format",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"volume_name",
				option.USER_LEVEL:option.USER},
		option.VOLS:{option.HELP_STRING:"list all volumes",
				option.DEFAULT_VALUE:option.DEFAULT,
				option.DEFAULT_TYPE:option.INTEGER,
				option.VALUE_USAGE:option.IGNORED,
				option.USER_LEVEL:option.USER},
		option.PVOLS:{option.HELP_STRING:"list all problem volumes",
				option.DEFAULT_VALUE:option.DEFAULT,
				option.DEFAULT_TYPE:option.INTEGER,
				option.VALUE_USAGE:option.IGNORED,
				option.USER_LEVEL:option.USER},
		option.HISTORY:{option.HELP_STRING:"show state change history of volume",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"volume_name",
				option.USER_LEVEL:option.ADMIN},
		option.WRITE_PROTECT_STATUS:{option.HELP_STRING:"show write protect status",
				option.VALUE_TYPE:option.STRING,
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_LABEL:"volume_name",
				option.USER_LEVEL:option.ADMIN},
		option.LIST_SG_COUNT:{
				option.HELP_STRING:"list all sg counts",
				option.DEFAULT_VALUE:option.DEFAULT,
				option.DEFAULT_TYPE:option.INTEGER,
				option.VALUE_USAGE:option.IGNORED,
				option.USER_LEVEL:option.USER},
		option.JUST:{option.HELP_STRING:"used with --pvols to list problem",
				option.DEFAULT_VALUE:option.DEFAULT,
				option.DEFAULT_TYPE:option.INTEGER,
				option.VALUE_USAGE:option.IGNORED,
				option.USER_LEVEL:option.USER},
		}

def do_work(intf):
	# now get a info client
	ifc = infoClient((intf.config_host, intf.config_port), None, intf.alive_rcv_timeout, intf.alive_retries)

	ticket = ifc.handle_generic_commands(MY_SERVER, intf)
	if ticket:
		pass

	elif intf.list:
		ticket = ifc.tape_list(intf.list)
		if ticket['status'][0] == e_errors.OK:
			format = "%%-%ds %%-16s %%10s %%-22s %%-7s %%s"%(len(intf.list))
			# print "%-8s %-16s %10s %-22s %-7s %s\n"%(
			#	"label", "bfid", "size", "location_cookie", "delflag", "original_name")
			print format%("label", "bfid", "size", "location_cookie", "delflag", "original_name")
			print
			tape = ticket['tape_list']
			for record in tape:
				if record['deleted'] == 'y':
					deleted = 'deleted'
				elif record['deleted'] == 'n':
					deleted = 'active'
				else:
					deleted = 'unknown'
				# print "%-8s %-16s %10i %-22s %-7s %s" % (intf.list,
				print format % (intf.list,
					record['bfid'], record['size'],
					record['location_cookie'], deleted,
					record['pnfs_path'])

	elif intf.ls_active:
		ticket = ifc.list_active(intf.ls_active)
		if ticket['status'][0] == e_errors.OK:
			for i in ticket['active_list']:
				print i
	elif intf.bfids:
		ticket  = ifc.get_bfids(intf.bfids)
		if ticket['status'][0] == e_errors.OK:
			for i in ticket['bfids']:
				print i
			# print `ticket['bfids']`
	elif intf.bfid:
		ticket = ifc.bfid_info(intf.bfid)
		if ticket['status'][0] ==  e_errors.OK:
			status = ticket['status']
			del ticket['status']
			pprint.pprint(ticket)
			ticket['status'] = status
	elif intf.find_same_file:
		ticket = ifc.find_same_file(intf.find_same_file)
		if ticket['status'][0] ==  e_errors.OK:
			
			print "%10s %20s %10s %22s %7s %s" % (
			"label", "bfid", "size", "location_cookie", "delflag", "original path")
			for record in ticket['files']:
				deleted = 'unknown'
				if record.has_key('deleted'):
					if record['deleted'] == 'yes':
						deleted = 'deleted'
					elif record['deleted'] == 'no':
						deleted = 'active'

				print "%10s %20s %10d %22s %7s %s" % (
					record['external_label'],
					record['bfid'], record['size'],
					record['location_cookie'], deleted,
					record['pnfs_name0'])

	elif intf.check:
		ticket = ifc.inquire_vol(intf.check)
		# guard against error
		if ticket['status'][0] == e_errors.OK:
			print "%-10s  %s %s %s" % (ticket['external_label'],
				capacity_str(ticket['remaining_bytes']),
				ticket['system_inhibit'],
				ticket['user_inhibit'])
	elif intf.history:
		ticket = ifc.show_history(intf.history)
		if ticket['status'][0] == e_errors.OK and len(ticket['history']):
			for state in ticket['history']:
				type = state['type']
				if state['type'] == 'system_inhibit_0':
					type = 'system_inhibit[0]'
				elif state['type'] == 'system_inhibit_1':
					type = 'system_inhibit[1]'
				elif state['type'] == 'user_inhibit_0':
					type = 'user_inhibit[0]'
				elif state['type'] == 'user_inhibit_1':
					type = 'user_inhibit[1]'
				print "%-28s %-20s %s"%(state['time'], type, state['value'])
	elif intf.write_protect_status:
		ticket = ifc.write_protect_status(intf.write_protect_status)
		if ticket['status'][0] == e_errors.OK:
			print intf.write_protect_status, "write-protect", ticket['status'][1]
	elif intf.vols:
		# optional argument
		nargs = len(intf.args)
		not_cond = None
		if nargs:
			if nargs == 3:
				key = intf.args[0]	 
				in_state=intf.args[1]
				not_cond = intf.args[2]
			elif nargs == 2:
				key = intf.args[0]	 
				in_state=intf.args[1]
			elif nargs == 1:
				key = None
				in_state=intf.args[0]
			else:
				print "Wrong number of arguments"
				print "usage: --vols"
				print "	   --vols state (will match system_inhibit)"
				print "	   --vols key state"
				print "	   --vols key state not (not in state)"
				return
		else:
			key = None
			in_state = None 
		ticket = ifc.get_vols(key, in_state, not_cond)
	elif intf.pvols:
		ticket = ifc.get_pvols()
		problem_vol = {}
		for i in ticket['volumes']:
			if i['system_inhibit_0'] != 'none':
				if problem_vol.has_key(i['system_inhibit_0']):
					problem_vol[i['system_inhibit_0']].append(i)
				else:
					problem_vol[i['system_inhibit_0']] = [i]
			if i['system_inhibit_1'] != 'none':
				if problem_vol.has_key(i['system_inhibit_1']):
					problem_vol[i['system_inhibit_1']].append(i)
				else:
					problem_vol[i['system_inhibit_1']] = [i]

		if intf.just:
			interested = intf.args
		else:
			interested = problem_vol.keys()
		for k in problem_vol.keys():
			if k in interested:
				print '====', k
				for v in problem_vol[k]:
					show_volume(v)
				print
	elif intf.labels:
		ticket = ifc.get_vol_list()
		if ticket['status'][0] == e_errors.OK:
			for i in ticket['volumes']:
				print i
	elif intf.show_bad:
		ticket = ifc.show_bad()
		if ticket['status'][0] == e_errors.OK:
			for f in ticket['bad_files']:
				print f['label'], f['bfid'], f['size'], f['path']
	elif intf.query:
		ticket = ifc.query_db(intf.query)
		if ticket['status'][0] == e_errors.OK:
			if ticket['result']['status'][0] != e_errors.OK:
				ticket['status'] = ticket['result']['status']
			else:
				show_query_result(ticket['result'])
	elif intf.ls_sg_count:
		ticket = ifc.list_sg_count()
		sgcnt = ticket['sgcnt']
		sk = sgcnt.keys()
		sk.sort()
		print "%12s %16s %10s"%('library', 'storage group', 'allocated')
		print '='*40
		for i in sk:
			lib, sg = string.split(i, ".")
			print "%12s %16s %10d"%(lib, sg, sgcnt[i])
	elif intf.get_sg_count:
		ticket = ifc.get_sg_count(intf.get_sg_count, intf.storage_group)
		print "%12s %16s %10d"%(ticket['library'], ticket['storage_group'], ticket['count'])
	elif intf.vol:
		ticket = ifc.inquire_vol(intf.vol)
		if ticket['status'][0] == e_errors.OK:
			status = ticket['status']
			del ticket['status']
			# do not show non_del_files
			del ticket['non_del_files']
			pprint.pprint(ticket)
			ticket['status'] = status
	elif intf.gvol:
		ticket = ifc.inquire_vol(intf.gvol)
		if ticket['status'][0] == e_errors.OK:
			status = ticket['status']
			del ticket['status']
			# do not show non_del_files
			del ticket['non_del_files']
			ticket['declared'] = time.ctime(ticket['declared'])
			ticket['first_access'] = time.ctime(ticket['first_access'])
			ticket['last_access'] = time.ctime(ticket['last_access'])
			if ticket.has_key('si_time'):
				ticket['si_time'] = (time.ctime(ticket['si_time'][0]),
									 time.ctime(ticket['si_time'][1]))
			pprint.pprint(ticket)
			ticket['status'] = status
	else:
		intf.print_help()
		sys.exit(0)

	ifc.check_ticket(ticket)

if __name__ == '__main__':
	intf = InfoClientInterface(user_mode=0)
	do_work(intf)

