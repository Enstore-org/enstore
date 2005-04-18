#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################

'''
Readonly access to file and volume database
'''

# system import
import os
import sys
import string
# import pprint

# enstore import
import dispatching_worker
import generic_server
import Trace
import e_errors
import enstore_constants
import monitored_server
import event_relay_messages
import time
import hostaddr
import callback
import socket
import select
import edb
import esgdb

MY_NAME = enstore_constants.INFO_SERVER   #"info_server"

default_host = 'stkensrv0.fnal.gov'

# err_msg(fucntion, ticket, exc, value) -- format error message from
# exceptions

def err_msg(function, ticket, exc, value, tb=None):
	return function+' '+`ticket`+' '+str(exc)+' '+str(value)+' '+str(tb)

class Interface(generic_server.GenericServerInterface):
	def __init__(self):
		generic_server.GenericServerInterface.__init__(self)

	def valid_dictionary(self):
		return (self.help_options)

class Server(dispatching_worker.DispatchingWorker, generic_server.GenericServer):
	def __init__(self, csc):
		self.debug = 0
		generic_server.GenericServer.__init__(self, csc, MY_NAME,
						function = self.handle_er_msg)
		Trace.init(self.log_name)
		self.keys = self.csc.get(MY_NAME)

		self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME,
                                                                  self.keys)

		att = self.csc.get(MY_NAME)
		self.hostip = att['hostip']
		dispatching_worker.DispatchingWorker.__init__(self,
			(att['hostip'], att['port']))

		self.file = edb.FileDB(host=att['dbhost'], auto_journal=0)
		self.db = self.file.db
		self.volume = edb.VolumeDB(host=att['dbhost'], auto_journal=0, rdb=self.db)
		self.sgdb = esgdb.SGDb(self.db)


		# setup the communications with the event relay task
		self.erc.start([event_relay_messages.NEWCONFIGFILE])
		# start our heartbeat to the event relay process
		self.erc.start_heartbeat(enstore_constants.INFO_SERVER, 
					 self.alive_interval)

		self.set_error_handler(self.info_error_handler)
		return

	def info_error_handler(self, exc, msg, tb):
		if exc == edb.pg.error or msg == "no connection to the server":
			self.reconnect(msg)
		self.reply_to_caller({'status':(str(exc),str(msg), 'error'),
			'exc_type':str(exc), 'exc_value':str(msg)} )


	# reconnect() -- re-establish connection to database
	def reconnect(self, msg="unknown reason"):
		Trace.alarm(e_errors.WARNING, "reconnect to database due to "+msg)
		self.file.reconnect()
		self.db = self.file.db
		self.volume.db = self.db
		self.sgdb.db = self.db

	# The following are local methods

	# close the database connection
	def close(self):
		self.db.close()
		return

	# turn on/off the debugging
	def debugging(self, ticket):
		self.debug = ticket.get('level', 0)
		print 'debug =', self.debug

	# These need confirmation
	def quit(self, ticket):
		self.db.close()
		dispatching_worker.DispatchingWorker.quit(self, ticket)
		# can't go this far
		# self.reply_to_caller({'status':(e_errors.OK, None)})
		# sys.exit(0)

	# return all info about a certain bfid - this does everything that the
	# read_from_hsm method does, except send the ticket to the library manager
	def bfid_info(self, ticket):
		try:
			bfid = ticket["bfid"]
		except KeyError, detail:
			msg = "File Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		# look up in our dictionary the request bit field id
		finfo = self.file[bfid] 
		if not finfo:
			ticket["status"] = (e_errors.NO_FILE,
				"Info Clerk: bfid %s not found"%(bfid,))
			Trace.log(e_errors.ERROR, "%s"%(ticket,))
			self.reply_to_caller(ticket)
			Trace.trace(10,"bfid_info %s"%(ticket["status"],))
			return

		#Copy all file information we have to user's ticket.  Copy the info
		# one key at a time to avoid cyclic dictionary references.
		for key in finfo.keys():
			ticket[key] = finfo[key]

		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)
		Trace.trace(10,"bfid_info bfid=%s"%(bfid,))
		return

	# find_file_by_path() -- find a file using pnfs_path
	def find_file_by_path(self, ticket):
		try:
			pnfs_path = ticket["pnfs_name0"]
		except KeyError, detail:
			msg = "Info Server: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		q = "select \
			bfid, crc, deleted, drive, \
			volume.label, location_cookie, pnfs_path, \
			pnfs_id, sanity_size, sanity_crc, size, \
			uid, gid \
			from file, volume \
			where \
				file.volume = volume.id and \
				pnfs_path = '%s';"%(pnfs_path)

		res = self.db.query(q).dictresult()
		if len(res) == 0:
			ticket["status"] = (e_errors.NO_FILE,
				"Info Server: path %s not found"%(pnfs_path))
			Trace.log(e_errors.ERROR, "%s"%(ticket,))
			self.reply_to_caller(ticket)
			return

		finfo = self.file.export_format(res[0])

		for key in finfo.keys():
			ticket[key] = finfo[key]
	
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)
		return
				
	# find_file_by_pnfsid() -- find a file using pnfs_path
	def find_file_by_pnfsid(self, ticket):
		try:
			pnfs_id = ticket["pnfsid"]
		except KeyError, detail:
			msg = "Info Server: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		q = "select \
			bfid, crc, deleted, drive, \
			volume.label, location_cookie, pnfs_path, \
			pnfs_id, sanity_size, sanity_crc, size, \
			uid, gid \
			from file, volume \
			where \
				file.volume = volume.id and \
				pnfs_id = '%s';"%(pnfs_id)

		res = self.db.query(q).dictresult()
		if len(res) == 0:
			ticket["status"] = (e_errors.NO_FILE,
				"Info Server: pnfsid %s not found"%(pnfs_id))
			Trace.log(e_errors.ERROR, "%s"%(ticket,))
			self.reply_to_caller(ticket)
			return

		finfo = self.file.export_format(res[0])

		for key in finfo.keys():
			ticket[key] = finfo[key]
	
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)
		return
				
	# find_file_by_location() -- find a file using pnfs_path
	def find_file_by_location(self, ticket):
		try:
			label = ticket['external_label']
			location_cookie = ticket['location_cookie']
		except KeyError, detail:
			msg = "Info Server: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		q = "select \
			bfid, crc, deleted, drive, \
			volume.label, location_cookie, pnfs_path, \
			pnfs_id, sanity_size, sanity_crc, size, \
			uid, gid \
			from file, volume \
			where \
				file.volume = volume.id and \
				label = '%s' and \
				location_cookie = '%s';"%(label,
				location_cookie)

		res = self.db.query(q).dictresult()
		if len(res) == 0:
			ticket["status"] = (e_errors.NO_FILE,
				"Info Server: location %s:%s not found"%(label, location_cookie))
			Trace.log(e_errors.ERROR, "%s"%(ticket,))
			self.reply_to_caller(ticket)
			return

		finfo = self.file.export_format(res[0])

		for key in finfo.keys():
			ticket[key] = finfo[key]
	
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)
		return
				


	def find_same_file(self, ticket):
		try:
			
			bfid = ticket["bfid"]
		except KeyError, detail:
			msg = "File Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		# look up in our dictionary the request bit field id
		finfo = self.file[bfid] 
		if not finfo:
			ticket["status"] = (e_errors.NO_FILE,
				"Info Clerk: bfid %s not found"%(bfid,))
			Trace.log(e_errors.ERROR, "%s"%(ticket,))
			self.reply_to_caller(ticket)
			Trace.trace(10,"bfid_info %s"%(ticket["status"],))
			return

		#Copy all file information we have to user's ticket.  Copy the info
		# one key at a time to avoid cyclic dictionary references.

		q = "select bfid from file where size = %d and crc = %d and sanity_size = %d and sanity_crc = %d order by bfid asc;"%(finfo['size'], finfo['complete_crc'], finfo['sanity_cookie'][0], finfo['sanity_cookie'][1])

		res = self.db.query(q).getresult()

		files = []
		for i in res:
			files.append(self.file[i[0]])
		ticket["files"] = files
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)
		return

	# get the current database volume about a specific entry #### DONE
	def inquire_vol(self, ticket):
		try:
			external_label = ticket["external_label"]
		except KeyError, detail:
			msg="Volume Clerk: key %s is missing" % (detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		# guarded against external_label == None
		if external_label:
			# get the current entry for the volume
			record = self.volume[external_label]
			if not record:
				msg="Info Clerk: no such volume %s" % (external_label,)
				ticket["status"] = (e_errors.NO_VOLUME, msg)
				Trace.log(e_errors.ERROR, msg)
				self.reply_to_caller(ticket)
				return
			record["status"] = (e_errors.OK, None)
			self.reply_to_caller(record)
			return
		else:
			msg = "Info Clerk::inquire_vol(): external_label == None"
			ticket["status"] = (e_errors.INFO_SERVER_ERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

	# get a port for the data transfer
	# tell the user I'm your info clerk and here's your ticket
	def get_user_sockets(self, ticket):
		try:
			addr = ticket['callback_addr']
			if not hostaddr.allow(addr):
				return 0
			info_clerk_host, info_clerk_port, listen_socket = callback.get_callback()
			listen_socket.listen(4)
			ticket["info_clerk_callback_addr"] = (info_clerk_host, info_clerk_port)
			self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.control_socket.connect(addr)
			callback.write_tcp_obj(self.control_socket, ticket)

			r,w,x = select.select([listen_socket], [], [], 15)
			if not r:
				listen_socket.close()
				return 0
			data_socket, address = listen_socket.accept()
			if not hostaddr.allow(address):
				data_socket.close()
				listen_socket.close()
				return 0
			self.data_socket = data_socket
			listen_socket.close()
		# catch any error and keep going. server needs to be robust
		except:
			exc, msg = sys.exc_info()[:2]
			Trace.handle_error(exc,msg)
			return 0
		return 1

	# __history(vol) -- show state change history of vol
	def __history(self, vol):
		q = "select time, label, state_type.name as type, state.value \
			 from state, state_type, volume \
			 where \
				label = '%s' and \
				state.volume = volume.id and \
				state.type = state_type.id \
			 order by time desc;"%(vol)
		try:
			res = self.db.query(q).dictresult()
		except:
			exc_type, exc_value = sys.exc_info()[:2]
			msg = '__history(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
			Trace.log(e_errors.ERROR, msg)
			res = []
		return res

	# history(ticket) -- server version of __history()
	def history(self, ticket):
		try:
			vol = ticket['external_label']
			ticket["status"] = (e_errors.OK, None)
			self.reply_to_caller(ticket)
		except KeyError, detail:
			msg =  "Info Server: key %s is missing"  % (detail)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		# get a user callback
		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket,ticket)
		res = self.__history(vol)
		callback.write_tcp_obj_new(self.data_socket, res)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket,ticket)
		self.control_socket.close() 
		return

	# write_protect_status(self, ticket):
	def write_protect_status(self, ticket):
		try:
			vol = ticket['external_label']
		except KeyError, detail:
			msg =  "Info Server: key %s is missing"  % (detail)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		q = "select time, value from state, state_type, volume \
			 where \
				 state.type = state_type.id and \
				 state_type.name = 'write_protect' and \
				 state.volume = volume.id and \
				 volume.label = '%s' \
			 order by time desc limit 1;"%(vol)

		try:
			res = self.db.query(q).dictresult()
			if not res:
				status = "UNKNOWN"
			else:
				status = res[0]['value']
			ticket['status'] = (e_errors.OK, status)
		except:
			exc_type, exc_value = sys.exc_info()[:2]
			msg = 'write_protect_status(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
			Trace.log(e_errors.ERROR, msg)
			ticket["status"] = (e_errors.INFO_SERVER_ERROR, msg)
		self.reply_to_caller(ticket)
		return

	# return all the volumes in our dictionary.  Not so useful!
	#
	# This is the old and inefficient implementation in which the
	# server does all the work. IT has been replaced by the newer
	# and better implementation, get_vols2()
	# Currently, this is only preserved for backward compatibility
	# (remember, the old client is compiled in enstore binary which
	# is shipped with encp binary
	def get_vols(self,ticket):
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)

		# log it
		Trace.log(e_errors.INFO, "start listing all volumes")

		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket, ticket)

		msg = {}
		q = "select * from volume "
		if ticket.has_key('in_state'):
			state = ticket['in_state']
		else:
			state = None
		if ticket.has_key('not'):
			cond = ticket['not']
		else:
			cond = None
		if ticket.has_key('key'):
			key = ticket['key']
		else:
			key = None

		if key and state:
			if key == 'volume_family':
				sg, ff, wp = string.split(state, '.')
				if cond == None:
					q = q + "where storage_group = '%s' and file_family = '%s' and wrapper = '%s'"%(sg, ff, wp)
				else:
					q = q + "where not (storage_group = '%s' and file_family = '%s' and wrapper = '%s')"%(sg, ff, wp)

			else:
				if key in ['blocksize', 'capacity_bytes',
					'non_del_files', 'remaining_bytes', 'sum_mounts',
					'sum_rd_access', 'sum_rd_err', 'sum_wr_access',
					'sum_wr_err']:
					val = "%d"%(state)
				elif key in ['eod_cookie', 'external_label', 'library',
					'media_type', 'volume_family', 'wrapper',
					'storage_group', 'file_family', 'wrapper',
					'system_inhibit_0', 'system_inhibit_1',
					'user_inhibit_0', 'user_inhibit_1']:
					val = "'%s'"%(state)
				elif key in ['first_access', 'last_access', 'declared',
					'si_time_0', 'si_time_1']:
					val = "'%s'"%(edb.time2timestamp(state))
				else:
					val = state

				if key == 'external_label':
					key = 'label'

				if cond == None:
					q = q + "where %s = %s"%(key, val)
				else:
					q = q + "where %s %s %s"%(key, cond, val)
		elif state:
			if state in ['full', 'read_only', 'migrated']:
				q = q + "where system_inhibit_1 = '%s'"%(state)
			else:
				q = q + "where system_inhibit_0 = '%s'"%(state)
		else:
			msg['header'] = 'FULL'

		q = q + ' order by label;'

		try:
			res = self.db.query(q).dictresult()
		except:
			exc_type, exc_value = sys.exc_info()[:2]
			mesg = 'get_vols(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
			Trace.log(e_errors.ERROR, mesg)
			res = []
		msg['volumes'] = []
		for v2 in res:
			vol2 = {'volume': v2['label']}
			for k in ["capacity_bytes","remaining_bytes", "library",
				"non_del_files"]:
				vol2[k] = v2[k]
			vol2['volume_family'] = v2['storage_group']+'.'+v2['file_family']+'.'+v2['wrapper']
			vol2['system_inhibit'] = (v2['system_inhibit_0'], v2['system_inhibit_1'])
			vol2['user_inhibit'] = (v2['user_inhibit_0'], v2['user_inhibit_1'])
			vol2['si_time'] = (edb.timestamp2time(v2['si_time_0']),
				edb.timestamp2time(v2['si_time_1']))
			if len(v2['comment']):
				vol2['comment'] = v2['comment']
			msg['volumes'].append(vol2)
		callback.write_tcp_obj_new(self.data_socket, msg)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket, ticket)
		self.control_socket.close()

		Trace.log(e_errors.INFO, "stop listing all volumes")
		return

	# return all the volumes in our dictionary.  Not so useful!
	#
	# This is the newer and better implementation that replaces
	# get_vols(). Now the data formatting, which takes 90% of CPU
	# load in this request, is done in the client, freeing up
	# server for other requests
	def get_vols2(self,ticket):
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)

		# log it
		Trace.log(e_errors.INFO, "start listing all volumes (2)")

		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket, ticket)

		msg = {}
		# q = "select * from volume "
		q = "select label, capacity_bytes, remaining_bytes, library, system_inhibit_0, system_inhibit_1, si_time_0, si_time_1, storage_group, file_family, wrapper, comment from volume "
		if ticket.has_key('in_state'):
			state = ticket['in_state']
		else:
			state = None
		if ticket.has_key('not'):
			cond = ticket['not']
		else:
			cond = None
		if ticket.has_key('key'):
			key = ticket['key']
		else:
			key = None

		if key and state:
			if key == 'volume_family':
				sg, ff, wp = string.split(state, '.')
				if cond == None:
					q = q + "where storage_group = '%s' and file_family = '%s' and wrapper = '%s'"%(sg, ff, wp)
				else:
					q = q + "where not (storage_group = '%s' and file_family = '%s' and wrapper = '%s')"%(sg, ff, wp)

			else:
				if key in ['blocksize', 'capacity_bytes',
					'non_del_files', 'remaining_bytes', 'sum_mounts',
					'sum_rd_access', 'sum_rd_err', 'sum_wr_access',
					'sum_wr_err']:
					val = "%d"%(state)
				elif key in ['eod_cookie', 'external_label', 'library',
					'media_type', 'volume_family', 'wrapper',
					'storage_group', 'file_family', 'wrapper',
					'system_inhibit_0', 'system_inhibit_1',
					'user_inhibit_0', 'user_inhibit_1']:
					val = "'%s'"%(state)
				elif key in ['first_access', 'last_access', 'declared',
					'si_time_0', 'si_time_1']:
					val = "'%s'"%(edb.time2timestamp(state))
				else:
					val = state

				if key == 'external_label':
					key = 'label'

				if cond == None:
					q = q + "where %s = %s"%(key, val)
				else:
					q = q + "where %s %s %s"%(key, cond, val)
			q = q + "and not label like '%%.deleted'"
		elif state:
			if state in ['full', 'readonly', 'migrated']:
				q = q + "where system_inhibit_1 = '%s'"%(state)
			else:
				q = q + "where system_inhibit_0 = '%s'"%(string.upper(state))
			q = q + "and not label like '%%.deleted'"

		msg['header'] = 'FULL'

		q = q + ' order by label;'

		try:
			res = self.db.query(q).dictresult()
		except:
			exc_type, exc_value = sys.exc_info()[:2]
			mesg = 'get_vols(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
			Trace.log(e_errors.ERROR, mesg)
			res = []
		msg['volumes'] = res
		callback.write_tcp_obj_new(self.data_socket, msg)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket, ticket)
		self.control_socket.close()

		Trace.log(e_errors.INFO, "stop listing all volumes (2)")
		return

	# return the volumes that have set system_inhibits
	def get_pvols(self,ticket):
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)

		# log it
		Trace.log(e_errors.INFO, "start listing all problematic volumes")

		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket, ticket)

		msg = {}
		q = "select * from volume where \
			not label like '%.deleted' and \
			(system_inhibit_0 != 'none' or \
			system_inhibit_1 != 'none') \
			order by label;"

		try:
			res = self.db.query(q).dictresult()
		except:
			exc_type, exc_value = sys.exc_info()[:2]
			mesg = 'get_vols(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
			Trace.log(e_errors.ERROR, mesg)
			res = []
		msg['volumes'] = res
		callback.write_tcp_obj_new(self.data_socket, msg)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket, ticket)
		self.control_socket.close()

		Trace.log(e_errors.INFO, "stop listing all problematic volumes")
		return

	def get_sg_count(self, ticket):
		try:
			lib = ticket['library']
			sg = ticket['storage_group']
		except KeyError, detail:
			msg= "Info Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		ticket['count'] = self.sgdb.get_sg_counter(lib, sg)
		if ticket['count'] == -1:
			ticket['status'] = (e_errors.INFO_SERVER_ERROR, "failed to get %s.%s"%(lib,sg))
		else:
			ticket['status'] = (e_errors.OK, None)
		self.reply_to_caller(ticket)

	def list_sg_count(self, ticket):
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)

		sgcnt = self.sgdb.list_sg_count()

		try:
			if not self.get_user_sockets(ticket):
				return
			ticket["status"] = (e_errors.OK, None)
			callback.write_tcp_obj(self.data_socket, ticket)
			callback.write_tcp_obj_new(self.data_socket, sgcnt)
			self.data_socket.close()
			callback.write_tcp_obj(self.control_socket, ticket)
			self.control_socket.close()
		except:
			exc, msg = sys.exc_info()[:2]
			Trace.handle_error(exc,msg)
		return

	def __get_vol_list(self):
		q = "select label from volume order by label;"
		res2 = self.db.query(q).getresult()
		res = []
		for i in res2:
			res.append(i[0])
		return res

	# return a list of all the volumes
	def get_vol_list(self,ticket):
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)

		try:
			if not self.get_user_sockets(ticket):
				return
			ticket["status"] = (e_errors.OK, None)
			callback.write_tcp_obj(self.data_socket, ticket)
			vols = self.__get_vol_list()
			callback.write_tcp_obj_new(self.data_socket, vols)
			self.data_socket.close()
			callback.write_tcp_obj(self.control_socket, ticket)
			self.control_socket.close()
		except:
			exc, msg = sys.exc_info()[:2]
			Trace.handle_error(exc,msg)
		return

	# get_bfids(self, ticket) -- get bfids of a certain volume
	#		This is almost the same as tape_list() yet it does not
	#		retrieve any information from primary file database

	def get_bfids(self, ticket):
		try:
			external_label = ticket["external_label"]
			ticket["status"] = (e_errors.OK, None)
			self.reply_to_caller(ticket)
		except KeyError, detail:
			msg = "Info Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			return

		# get a user callback
		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket,ticket)

		bfids = self.get_all_bfids(external_label)
		callback.write_tcp_obj_new(self.data_socket, bfids)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket,ticket)
		self.control_socket.close()
		return

	# get_all_bfids(external_label) -- get all bfids of a particular volume

	def get_all_bfids(self, external_label):
		q = "select bfid from file, volume\
			 where volume.label = '%s' and \
				   file.volume = volume.id;"%(external_label)
		res = self.db.query(q).getresult()
		bfids = []
		for i in res:
			bfids.append(i[0])
		return bfids

	# This has been replaced by tape_list2()
	# It is perserved for backward compatibility
	def tape_list(self,ticket):
		try:
			external_label = ticket["external_label"]
			ticket["status"] = (e_errors.OK, None)
			self.reply_to_caller(ticket)
		except KeyError, detail:
			msg = "Info Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			####XXX client hangs waiting for TCP reply
			return

		# get a user callback
		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket,ticket)

		# log the activity
		Trace.log(e_errors.INFO, "start listing "+external_label)
		
		q = "select bfid, crc, deleted, drive, volume.label, \
					location_cookie, pnfs_path, pnfs_id, \
					sanity_size, sanity_crc, size \
			 from file, volume \
			 where \
				 file.volume = volume.id and volume.label = '%s' order by location_cookie;"%(
			 external_label)

		res = self.db.query(q).dictresult()

		vol = []

		for ff in res:
			value = self.file.export_format(ff)
			if not value.has_key('pnfs_name0'):
				value['pnfs_name0'] = "unknown"
			vol.append(value)

		# finishing up

		callback.write_tcp_obj_new(self.data_socket, vol)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket,ticket)
		self.control_socket.close()
		Trace.log(e_errors.INFO, "finish listing "+external_label)
		return

	# This is the newer implementation that off load to client
	def tape_list2(self,ticket):
		try:
			external_label = ticket["external_label"]
			ticket["status"] = (e_errors.OK, None)
			self.reply_to_caller(ticket)
		except KeyError, detail:
			msg = "Info Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			####XXX client hangs waiting for TCP reply
			return

		# get a user callback
		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket,ticket)

		# log the activity
		Trace.log(e_errors.INFO, "start listing "+external_label+" (2)")
		
		q = "select bfid, crc, deleted, drive, volume.label, \
					location_cookie, pnfs_path, pnfs_id, \
					sanity_size, sanity_crc, size \
			 from file, volume \
			 where \
				 file.volume = volume.id and volume.label = '%s' order by location_cookie;"%(
			 external_label)

		vol = self.db.query(q).dictresult()

		callback.write_tcp_obj_new(self.data_socket, vol)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket,ticket)
		self.control_socket.close()
		Trace.log(e_errors.INFO, "finish listing "+external_label+" (2)")
		return

	# list_active(self, ticket) -- list the active files on a volume
	#	 only the /pnfs path is listed
	#	 the purpose is to generate a list for deletion before the
	#	 deletion of a volume
	# This has been replaced by list_active2()
	def list_active(self,ticket):
		try:
			external_label = ticket["external_label"]
			ticket["status"] = (e_errors.OK, None)
			self.reply_to_caller(ticket)
		except KeyError, detail:
			msg = "Info Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			####XXX client hangs waiting for TCP reply
			return

		# get a user callback
		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket,ticket)

		q = "select bfid, crc, deleted, drive, volume.label, \
					location_cookie, pnfs_path, pnfs_id, \
					sanity_size, sanity_crc, size \
			 from file, volume \
			 where \
				 file.volume = volume.id and volume.label = '%s' order by location_cookie;"%(
			 external_label)

		res = self.db.query(q).dictresult()

		alist = []

		for ff in res:
			value = self.file.export_format(ff)
			if not value.has_key('deleted') or value['deleted'] == "no":
				if value.has_key('pnfs_name0') and value['pnfs_name0']:
					alist.append(value['pnfs_name0'])

		# finishing up

		callback.write_tcp_obj_new(self.data_socket, alist)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket,ticket)
		self.control_socket.close()
		return

	# list_active2(self, ticket) -- list the active files on a volume
	#	 only the /pnfs path is listed
	#	 the purpose is to generate a list for deletion before the
	#	 deletion of a volume

	def list_active2(self,ticket):
		try:
			external_label = ticket["external_label"]
			ticket["status"] = (e_errors.OK, None)
			self.reply_to_caller(ticket)
		except KeyError, detail:
			msg = "Info Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			####XXX client hangs waiting for TCP reply
			return

		# get a user callback
		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket,ticket)

		q = "select pnfs_path from \
			(select pnfs_path, location_cookie \
			from file, volume \
			where \
				file.volume = volume.id and volume.label = '%s' and\
				deleted = 'n' and not pnfs_path is null and \
				pnfs_path != '' order by location_cookie) a1;"%(
			 external_label)

		res = self.db.query(q).getresult()

		# finishing up

		callback.write_tcp_obj_new(self.data_socket, res)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket,ticket)
		self.control_socket.close()
		return

	def show_bad(self, ticket):
		ticket["status"] = (e_errors.OK, None)
		self.reply_to_caller(ticket)

		# get a user callback
		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket,ticket)

		q = "select label, bad_file.bfid, size, path \
			 from bad_file, file, volume \
			 where \
				 bad_file.bfid = file.bfid and \
				 file.volume = volume.id;"
		res = self.db.query(q).dictresult()

		# finishing up

		callback.write_tcp_obj_new(self.data_socket, res)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket,ticket)
		self.control_socket.close()
		return

	def query_db(self, ticket):
		try:
			q = ticket["query"]
			# only select is allowed
			qu = string.upper(q)
			query_parts = string.split(qu)

			if query_parts[0] != "SELECT" or "INTO" in query_parts:
				msg = "only simple select statement is allowed"
				ticket["status"] = (e_errors.INFO_SERVER_ERROR, msg)
			else:
				ticket["status"] = (e_errors.OK, None)
			self.reply_to_caller(ticket)
		except KeyError, detail:
			msg = "Info Clerk: key %s is missing"%(detail,)
			ticket["status"] = (e_errors.KEYERROR, msg)
			Trace.log(e_errors.ERROR, msg)
			self.reply_to_caller(ticket)
			####XXX client hangs waiting for TCP reply
			return

		# get a user callback
		if not self.get_user_sockets(ticket):
			return
		callback.write_tcp_obj(self.data_socket,ticket)

		result = {}
		try:
			res = self.db.query(q)
			result['result'] = res.getresult()
			result['fields'] = res.listfields()
			result['ntuples'] = res.ntuples()
			result['status'] = (e_errors.OK, None)
		except:
			exc_type, exc_value = sys.exc_info()[:2]
			msg = 'query_db(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
			result['status'] = (e_errors.INFO_SERVER_ERROR, msg)

		# finishing up

		callback.write_tcp_obj_new(self.data_socket, result)
		self.data_socket.close()
		callback.write_tcp_obj(self.control_socket,ticket)
		self.control_socket.close()
		return

if __name__ == '__main__':
	Trace.init(string.upper(MY_NAME))
	intf = Interface()
	csc = (intf.config_host, intf.config_port)
	infoServer = Server(csc)
	infoServer.handle_generic_commands(intf)

	while 1:
		try:
			Trace.log(e_errors.INFO, "Info Server (re)starting")
			infoServer.serve_forever()
		except edb.pg.error, exp:
			infoServer.reconnect(exp)
			continue
		except SystemExit, exit_code:
			infoServer.db.close()
			sys.exit(exit_code)
		except:
			infoServer.serve_forever_error(infoServer.log_name)
			continue
	
