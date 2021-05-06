#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

'''
Accounting Module -- record accounting information
'''

# system import
import os
import string
import sys

# enstore import
import accounting
import dbaccess
import dispatching_worker
import enstore_constants
import e_errors
import event_relay_messages
import generic_server
import monitored_server
import Trace
import time
import volume_clerk_client


MY_NAME = enstore_constants.ACCOUNTING_SERVER    #"accounting_server"

ACC_DAILY_SUMMARY_INTERVAL = 86400 # 24 hours
FILLER_INTERVAL = 1200 # twenty minutes

THREE_MINUTES_TTL = 180 #time forked ratekeepers are allowed to live

vcc = None

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
		dbport = att.get('dbport')
		try:
			self.accDB = accounting.accDB(host=att['dbhost'],
						      dbname=att['dbname'],
						      user=att['dbuser'],
						      port=dbport)
		except: # wait for 30 seconds and retry
			time.sleep(30)
			self.accDB = accounting.accDB(host=att['dbhost'],
						      dbname=att['dbname'],
						      user=att['dbuser'],
						      port=dbport)

		# setup the communications with the event relay task
		self.resubscribe_rate = 300
		self.erc.start([event_relay_messages.NEWCONFIGFILE],
			       self.resubscribe_rate)
		# start our heartbeat to the event relay process
		self.erc.start_heartbeat(enstore_constants.ACCOUNTING_SERVER,
                                 self.alive_interval)


		#Run this function, once a day at midnight.
		self.add_interval_func(self.acc_daily_summary_func,
				       ACC_DAILY_SUMMARY_INTERVAL,
				       one_shot=0, align_interval = True)
		#Run this function, every 20 minutes.
		self.add_interval_func(self.filler_func,
				       FILLER_INTERVAL,
				       one_shot=0, align_interval = True)

		return

	# The following are local methods

	# close the database connection
	def close(self):
		self.accDB.close()
		return

	#Run once a day at  midnight.
	def acc_daily_summary_func(self):
		if self.fork(THREE_MINUTES_TTL):
			#Parent
			return

		#child
		self.acc_daily_summary()
		os._exit(0)

	#Run every 20 minutes.
	def filler_func(self):
		if self.fork(THREE_MINUTES_TTL):
			#Parent
			return

		#child
		self.filler()
		os._exit(0)

	###
	### acc_daily_summary() and filler() don't use self.accDB.  Instead
	### acc_db is used.  Since, they run in forked processes, we need
	### to be able to close the connection without closing self.accDB.

	#Update the accounting summary tables.
	def acc_daily_summary(self):
		st = time.time()
		try:
			## Put the information into the accounting DB.
			acc_conf = self.csc.get(MY_NAME)
			acc_db = dbaccess.DatabaseAccess(maxconnections=1,
							 host=acc_conf.get('dbhost', "localhost"),
							 database=acc_conf.get('dbname', "accounting"),
							 port=acc_conf.get('dbport', 5432),
							 user=acc_conf.get('dbuser', None))

			acc_db.query("select * from make_daily_xfer_size()")
			acc_db.query("select * from make_daily_xfer_size_by_mover()")

			day = time.localtime(time.time())[2]
			if day == 1:	# beginning of the month
				acc_db.query("select * from make_monthly_xfer_size()")

			acc_db.close()
		except:
			# e, v = sys.exc_info()[:2]
			##Doesn't making tb a local cause a cyclic reference
			## and the memory never gets cleaned up?
			e, v, tb = sys.exc_info()
			Trace.handle_error(e, v, tb)
			Trace.log(e_errors.ERROR, err_msg('acc_daily_summary()', {}, e, v, tb))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'acc_daily_summary\t', dt

	#Update the accounting storage group tables.
	##As a side note, "filler" is a very non-descript name.
	def filler(self):
		#I'm guessing this is a fermi specific "zero" time.  It
		# should not matter for other sites, because they will
		# have started after it.
		zero_time  = 1045689052  #'Wed Feb 19 15:10:52 2003'

		#First obtain the most recent entry in the accounting DB.
		try:
			## Put the information into the accounting DB.
			acc_conf = self.csc.get(MY_NAME)
			acc_db = dbaccess.DatabaseAccess(maxconnections=1,
							 host=acc_conf.get('dbhost', "localhost"),
							 database=acc_conf.get('dbname', "accounting"),
							 port=acc_conf.get('dbport', 5432),
							 user=acc_conf.get('dbuser', None))

			SELECT_LAST_TIME="select coalesce(max(unix_time), {}) from encp_xfer_average_by_storage_group".format(zero_time)
			res = acc_db.query_getresult(SELECT_LAST_TIME);
			zero_time = res[0][0]
		except:
			# e, v = sys.exc_info()[:2]
			##Doesn't making tb a local cause a cyclic reference
			## and the memory never gets cleaned up?
			e, v, tb = sys.exc_info()
			Trace.handle_error(e, v, tb)
			Trace.log(e_errors.ERROR, err_msg('filler()', {}, e, v, tb))

		delta_time = 60 * 20
		zero_time  = int(zero_time+0.5*delta_time)
		now_time   =  int(time.time())

		#Insert info for each storage group.
		while zero_time < now_time:
			stop_time       = zero_time + delta_time;
			middle_time     = int(zero_time + 0.5*delta_time)
			str_middle_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(middle_time))
			str_from_time   = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(zero_time))
			str_to_time     = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(stop_time))
			select_stmt = "insert into encp_xfer_average_by_storage_group "
			select_stmt = select_stmt +  " ( select "
			select_stmt = select_stmt + str(middle_time)
			select_stmt = select_stmt + ",'"
			select_stmt = select_stmt + str_middle_time
			select_stmt = select_stmt + "','"
			select_stmt = select_stmt + str_from_time
			select_stmt = select_stmt + "','"
			select_stmt = select_stmt + str_to_time
			select_stmt = select_stmt + "',storage_group, rw,"
			select_stmt = select_stmt + "avg(overall_rate)/1024./1024,"
			select_stmt = select_stmt + "avg(network_rate)/1024./1024,"
			select_stmt = select_stmt + "avg(disk_rate)/1024./1024,"
			select_stmt = select_stmt + "avg(transfer_rate)/1024./1024,"
			select_stmt = select_stmt + "avg(drive_rate)/1024./1024,"
			select_stmt = select_stmt + "avg(size)/1024./1024,"
			select_stmt = select_stmt + "stddev(overall_rate)/1024./1024,"
			select_stmt = select_stmt + "stddev(network_rate)/1024./1024,"
			select_stmt = select_stmt + "stddev(disk_rate)/1024./1024,"
			select_stmt = select_stmt + "stddev(transfer_rate)/1024./1024,"
			select_stmt = select_stmt + "stddev(drive_rate)/1024./1024,"
			select_stmt = select_stmt + "stddev(size)/1024./1024, count(*) from"
			select_stmt  = select_stmt + " encp_xfer where date between '"
			select_stmt  = select_stmt + str_from_time
			select_stmt  = select_stmt + "' and '"
			select_stmt  = select_stmt + str_to_time
			select_stmt  = select_stmt + "' group by storage_group, rw)"
			try:
				acc_db.insert(select_stmt);
			except:
				# e, v = sys.exc_info()[:2]
				##Doesn't making tb a local cause a cyclic reference
				## and the memory never gets cleaned up?
				e, v, tb = sys.exc_info()
				Trace.handle_error(e, v, tb)
				Trace.log(e_errors.ERROR, err_msg('filler()', {}, e, v, tb))
			zero_time = zero_time + delta_time

		try:
			acc_db.close()
		except:
			# e, v = sys.exc_info()[:2]
			##Doesn't making tb a local cause a cyclic reference
			## and the memory never gets cleaned up?
			e, v, tb = sys.exc_info()
			Trace.handle_error(e, v, tb)
			Trace.log(e_errors.ERROR, err_msg('filler()', {}, e, v, tb))

	# The following are server methods ...
	# Most of them do not need confirmation/reply

	# test
	def hello(self, ticket):
		print 'hello, world'
		return

	# turn on/off the debugging
	def debugging(self, ticket):
		self.debug = ticket.get('level', 0)
		if self.debug:
			self.accDB.db.debug = "DB DEBUG> %s"
		else:
			self.accDB.db.debug = None
		print 'debug =', self.debug

	# These need confirmation
	def quit(self, ticket):
		#Collect children.
		while self.n_children > 0:
			self.collect_children()
			time.sleep(1)

		self.accDB.close()
		dispatching_worker.DispatchingWorker.quit(self, ticket)
		# can't go this far
		# self.reply_to_caller({'status':(e_errors.OK, None)})
		# sys.exit(0)

	#log drive info (suspected drives)
	def log_drive_info(self,ticket):
		dict={}
		for key in 'date','drive_id','drive_sn','volume', \
		    'drive_rate', 'rw', 'write_error_count','read_error_count', \
		    'bfid', 'file_size':
			if ticket.has_key(key):
				dict[key]=ticket.get(key,None)
		try:
			self.accDB.insert("drive_data",dict)
		except Exception, detail:
			Trace.alarm(e_errors.WARNING, "Failed to insert data into drive_info %s"%(str(detail),))


	# log_start_mount(self, node, volume, type, logname, start)
	def log_start_mount(self, ticket):
		st = time.time()
		# Trace.log(e_errors.INFO, `ticket`)
		try:
			lsm_type = ticket['type']
			if not lsm_type:
				lsm_type = 'unknown'
			self.accDB.log_start_mount(
				ticket['node'],
				ticket['volume'],
				ticket.get('storage_group'),
				lsm_type,
				ticket['logname'],
				ticket['start'])
		except:
			# e, v = sys.exc_info()[:2]
			e, v, tb = sys.exc_info()
			Trace.handle_error(e, v, tb)
			Trace.log(e_errors.ERROR, err_msg('log_start_mount()', ticket, e, v, tb))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'start_mount\t', dt

	# log_finish_mount(self, node, volume, finish, state='M')
	def log_finish_mount(self, ticket):
		st = time.time()
		# Trace.log(e_errors.INFO, `ticket`)
		try:
			self.accDB.log_finish_mount(
				ticket['node'],
				ticket['volume'],
				ticket['finish'],
				ticket['state'])
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_finish_mount()', ticket, e, v))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'finish_mount\t', dt

	# log_start_dismount(self, node, volume, type, logname, start)
	def log_start_dismount(self, ticket):
		st = time.time()
		# Trace.log(e_errors.INFO, `ticket`)
		try:
			self.accDB.log_start_dismount(
				ticket['node'],
				ticket['volume'],
				ticket.get('storage_group'),
				ticket.get('reads',0),
				ticket.get('writes',0),
				ticket['type'],
				ticket['logname'],
				ticket['start'])
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_start_dismount()', ticket, e, v))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'start_dismount\t', dt

	# log_finish_dismount(self, node, volume, finish, state='D')
	def log_finish_dismount(self, ticket):
		st = time.time()
		# Trace.log(e_errors.INFO, `ticket`)
		try:
			self.accDB.log_finish_dismount(
				ticket['node'],
				ticket['volume'],
				ticket['finish'],
				ticket['state'])
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_finish_dismount()', ticket, e, v))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'finish_dismount\t', dt

	# log_encp_xfer(....)
	def log_encp_xfer2(self, ticket):
		del ticket['work']
		try:
			self.accDB.log_encp_xfer(ticket)
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_encp_xfer()', ticket, e, v))

	# log_encp_xfer(....)
	def log_encp_xfer(self, ticket):
		st = time.time()
		try:
			if not ticket.has_key('encp_version'):
				ticket['encp_version'] = 'unknown'

			if not ticket.has_key('overall_rate'):
				ticket['overall_rate'] = ticket['rate']
				ticket['transfer_rate'] = 0
				ticket['disk_rate'] = 0
				ticket['network_rate'] = ticket['net_rate']

			if not ticket.has_key('file_family'):
				v = vcc.inquire_vol(ticket['volume'])
				sg, ticket['file_family'], ticket['wrapper'] = string.split(v['volume_family'], ".")

			if not ticket.has_key('library'):
				if len(ticket['volume'])>0:
					v = vcc.inquire_vol(ticket['volume'])
					if v.has_key('library'):
						ticket['library'] = v['library']
					else:
						ticket['library'] = None
				else:
					ticket['library'] = None

			self.accDB.log_encp_xfer(
				ticket['date'],
				ticket['node'],
				ticket['pid'],
				ticket['username'],
				ticket['src'],
				ticket['dst'],
				ticket['size'],
				ticket['volume'],
				ticket['network_rate'],
				ticket['drive_rate'],
				ticket['disk_rate'],
				ticket['overall_rate'],
				ticket['transfer_rate'],
				ticket['mover'],
				ticket['drive_id'],
				ticket['drive_sn'],
				ticket['elapsed'],
				ticket['media_changer'],
				ticket['mover_interface'],
				ticket['driver'],
				ticket['storage_group'],
				ticket['encp_ip'],
				ticket['encp_id'],
				ticket['rw'],
				ticket['encp_version'],
				ticket['file_family'],
				ticket['wrapper'],
				ticket['library'])
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_encp_xfer()', ticket, e, v))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'encp_xfer\t', dt


	# log_encp_error(....)
	def log_encp_error(self, ticket):
		st = time.time()
		if not ticket.has_key('file_family') and \
			ticket.has_key('volume'):
			v = vcc.inquire_vol(ticket['volume'])
			sg, ticket['file_family'], ticket['wrapper'] = string.split(v['volume_family'], ".")
		else:
			ticket['file_family'] = None
			ticket['wrapper'] = None
		if not ticket.has_key('library'):
			if not ticket.has_key('volume'):
				ticket['library']=None
			else:
				if len(ticket['volume'])>0:
					v = vcc.inquire_vol(ticket['volume'])
					if v.has_key('library'):
						ticket['library'] = v['library']
					else:
						ticket['library'] = None
				else:
					ticket['library'] = None

		if not ticket.has_key('mover'):
			ticket['mover'] = None
		if not ticket.has_key('drive_id'):
			ticket['drive_id'] = None
		if not ticket.has_key('drive_sn'):
			ticket['drive_sn'] = None
		if not ticket.has_key('rw'):
			ticket['rw'] = None
		if not ticket.has_key('volume'):
			ticket['volume'] = None

		# Trace.log(e_errors.INFO, `ticket`)
		try:
			self.accDB.log_encp_error(
				ticket['date'],
				ticket['node'],
				ticket['pid'],
				ticket['username'],
				ticket['src'],
				ticket['dst'],
				ticket['size'],
				ticket['storage_group'],
				ticket['encp_id'],
				ticket['version'],
				ticket['type'],
				ticket['error'],
				ticket['file_family'],
				ticket['wrapper'],
				ticket['mover'],
				ticket['drive_id'],
				ticket['drive_sn'],
				ticket['rw'],
				ticket['volume'],
				ticket['library'])
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_encp_error()', ticket, e, v))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'encp_error\t', dt

	# log_start_event
	def log_start_event(self, ticket):
		st = time.time()
		try:
			self.accDB.log_start_event(
				ticket['tag'],
				ticket['name'],
				ticket['node'],
				ticket['username'],
				ticket['start'])
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_start_event()', ticket, e, v))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'start_event\t', dt

	# log_finish_event
	def log_finish_event(self, ticket):
		st = time.time()
		try:
			if ticket.has_key('comment'):
				self.accDB.log_finish_event(
					ticket['tag'],
					ticket['finish'],
					ticket['status'],
					ticket['comment'])
			else:
				self.accDB.log_finish_event(
					ticket['tag'],
					ticket['finish'],
					ticket['status'])
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_finish_event()', ticket, e, v))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'finish_event\t', dt

if __name__ == '__main__':
	Trace.init(string.upper(MY_NAME))
	intf = Interface()
	csc = (intf.config_host, intf.config_port)
	accServer = Server(csc)
	accServer.handle_generic_commands(intf)
	vcc = volume_clerk_client.VolumeClerkClient(csc)

	while 1:
		try:
			Trace.log(e_errors.INFO, "Accounting Server (re)starting")
			accServer.serve_forever()
		except SystemExit, exit_code:
			accServer.accDB.close()
			sys.exit(exit_code)
		except:
			accServer.serve_forever_error(accServer.log_name)
			continue
