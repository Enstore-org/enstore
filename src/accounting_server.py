#!/usr/bin/env python

'''
Accounting Module -- record accounting information
'''

# system import
import os
import sys
import string
import pprint

# enstore import
import dispatching_worker
import generic_server
import Trace
import e_errors
import enstore_constants
import accounting
import monitored_server
import event_relay_messages
import time

MY_NAME = "accounting_server"

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
		generic_server.GenericServer.__init__(self, csc, MY_NAME)
		Trace.init(self.log_name)
		self.keys = self.csc.get(MY_NAME)

		self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME,
                                                                  self.keys)

		att = self.csc.get(MY_NAME)
		self.hostip = att['hostip']
		dispatching_worker.DispatchingWorker.__init__(self,
			(att['hostip'], att['port']))
		self.accDB = accounting.accDB(att['dbhost'],
						att['dbname'])
		# setup the communications with the event relay task
		self.resubscribe_rate = 300
		self.erc.start([event_relay_messages.NEWCONFIGFILE], self.resubscribe_rate)

		# start our heartbeat to the event relay process
		self.erc.start_heartbeat(enstore_constants.ACCOUNTING_SERVER, 
                                 self.alive_interval)
		return

	# The following are local methods

	# close the database connection
	def close(self):
		self.accDB.close()
		return

	# The following are server methods ...
	# Most of them do not need confirmation/reply

	# test
	def hello(self, ticket):
		print 'hello, world'
		return

	# turn on/off the debugging
	def debug(self, ticket):
		self.debug = ticket.get('level', 0)
		print 'debug =', self.debug

	# These need confirmation
	def quit(self, ticket):
		self.accDB.close()
		dispatching_worker.DispatchingWorker.quit(self, ticket)
		# can't go this far
		# self.reply_to_caller({'status':(e_errors.OK, None)})
		# sys.exit(0)

	# log_start_mount(self, node, volume, type, logname, start)
	def log_start_mount(self, ticket):
		st = time.time()
		# Trace.log(e_errors.INFO, `ticket`)
		try:
			type = ticket['type']
			if not type:
				type = 'unknown'
			self.accDB.log_start_mount(
				ticket['node'],
				ticket['volume'],
				type,
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
		# Trace.log(e_errors.INFO, `ticket`)
		try:
			self.accDB.log_encp_xfer(
				ticket['date'],
				ticket['node'],
				ticket['pid'],
				ticket['username'],
				ticket['src'],
				ticket['dst'],
				ticket['size'],
				ticket['volume'],
				ticket['rate'],
				ticket['net_rate'],
				ticket['drive_rate'],
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
				ticket['rw'])
		except:
			e, v = sys.exc_info()[:2]
			Trace.log(e_errors.ERROR, err_msg('log_encp_xfer()', ticket, e, v))
		dt = time.time() - st
		if self.debug:
			print time.ctime(st), 'encp_xfer\t', dt

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
	
