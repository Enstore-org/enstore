#!/usr/bin/env python

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

MY_NAME = "info_client"
MY_SERVER = "info_server"
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

class infoClient(generic_client.GenericClient):
	def __init__(self, csc, logname='UNKNOWN', rcv_timeout = RCV_TIMEOUT,
		rcv_tries = RCV_TRIES):
		self.logname = logname
		self.node = os.uname()[1]
		self.pid = os.getpid()
		generic_client.GenericClient.__init__(self, csc, MY_NAME)
		try:
			self.uid = pwd.getpwuid(os.getuid())[0]
		except:
			self.uid = "unknown"
		self.rcv_timeout = rcv_timeout
		self.rcv_tries = rcv_tries
		self.server_address = self.get_server_address(MY_SERVER, self.rcv_timeout, self.rcv_tries)

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

	def inquire_vol(self, external_label, timeout=20, retry=5):
		ticket= { 'work': 'inquire_vol',
			'external_label' : external_label }
		return self.send(ticket,timeout,retry)

	# get a list of all volumes
	def get_vols(self, key=None,state=None, not_cond=None, print_list=1):
		# get a port to talk on and listen for connections
		host, port, listen_socket = callback.get_callback()
		listen_socket.listen(4)
		ticket = {"work"		  : "get_vols",
				  "callback_addr" : (host, port),
				  "key"		   : key,
				  "in_state"	  : state,
				  "not"		   : not_cond}

		# send the work ticket to the library manager
		ticket = self.send(ticket, 60, 1)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r,w,x = select.select([listen_socket], [], [], 15)
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
				print "%-10s	 %-8s		 %-17s		  %012s	 %-012s		  %-012s"%(
					"label","avail.", "system_inhibit",
					"library","	volume_family", "comment")
				for v in volumes["volumes"]:
					print "%-10s"%(v['volume'],),
					print capacity_str(v['remaining_bytes']),
					si0t = ''
					si1t = ''
					if v.has_key('si_time'):
						if v['si_time'][0] > 0:
							si0t = time.strftime("%m%d-%H%M",
								time.localtime(v['si_time'][0]))
						if v['si_time'][1] > 0:
							si1t = time.strftime("%m%d-%H%M",
								time.localtime(v['si_time'][1]))
					print " (%-010s %9s %08s %9s) %-012s %012s"%(
						v['system_inhibit'][0], si0t,
						v['system_inhibit'][1], si1t,
						# v['user_inhibit'][0],v['user_inhibit'][1],
						v['library'],v['volume_family']),
					if v.has_key('comment'):
						print v['comment']
					else:
						print
		else:
			vlist = ''
			for v in volumes.get("volumes",[]):
				vlist = vlist+v['volume']+" "
			if print_list:
				print vlist
				
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

		r,w,x = select.select([listen_socket], [], [], 15)
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

		r,w,x = select.select([listen_socket], [], [], 15)
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

		r, w, x = select.select([listen_socket], [], [], 15)
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

		r, w, x = select.select([listen_socket], [], [], 15)
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
		ticket = {"work"		  : "tape_list",
				  "callback_addr" : (host, port),
				  "external_label": external_label}
		# send the work ticket to the file clerk
		ticket = self.send(ticket)
		if ticket['status'][0] != e_errors.OK:
			return ticket

		r, w, x = select.select([listen_socket], [], [], 15)
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
			
			print "	 label		   bfid	   size		location_cookie delflag original_name\n"
			tape = ticket['tape_list']
			for record in tape:
				deleted = 'unknown'
				if record.has_key('deleted'):
					if record['deleted'] == 'yes':
						deleted = 'deleted'
					elif record['deleted'] == 'no':
						deleted = 'active'

				print "%10s %s %10i %22s %7s %s" % (intf.list,
					record['bfid'], record['size'],
					record['location_cookie'], deleted,
					record['pnfs_name0'])

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
	elif intf.check:
		ticket = ifc.inquire_vol(intf.check)
		# guard against error
		if ticket['status'][0] == e_errors.OK:
			print "%-10s  %s %s %s" % (ticket['external_label'],
				capacity_str(ticket['remaining_bytes']),
				ticket['system_inhibit'],
				ticket['user_inhibit'])
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
		ticket = ifc.get_vols(print_list=0)
		problem_vol = {}
		for i in ticket['volumes']:
			if i['volume'][-8:] != '.deleted':
				for j in [0, 1]:
					if i['system_inhibit'][j] != 'none':
						if problem_vol.has_key(i['system_inhibit'][j]):
							problem_vol[i['system_inhibit'][j]].append(i)
						else:
							problem_vol[i['system_inhibit'][j]] = [i]

		if intf.just:
			interested = intf.args
		else:
			interested = problem_vol.keys()
		for k in problem_vol.keys():
			if k in interested:
				output = []
				print '====', k
				for v in problem_vol[k]:
					si0t = '*'
					si1t = '*'
					if v.has_key('si_time'):
						if v['si_time'][0] > 0:
							si0t = time.strftime("%m%d-%H%M",
								   time.localtime(v['si_time'][0]))
						if v['si_time'][1] > 0:
							si1t = time.strftime("%m%d-%H%M",
								   time.localtime(v['si_time'][1]))
					output.append("%-10s %012s %9s %12s %9s"%(
							v['volume'],
							v['system_inhibit'][0], si0t,
								v['system_inhibit'][1], si1t,))
				output.sort()
				for i in output:
					print i
	elif intf.labels:
		ticket = ifc.get_vol_list()
		if ticket['status'][0] == e_errors.OK:
			for i in ticket['volumes']:
				print i
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
