#!/usr/bin/env python

# system import
import os
import sys
import string
import pprint
import pwd

# enstore import
import generic_client
import option
import time

MY_NAME = "accounting_client"
MY_SERVER = "accounting_server"
RCV_TIMEOUT = 10
RCV_TRIES = 1

class accClient(generic_client.GenericClient):
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
		self.server_address = self.get_server_address(MY_SERVER, 10, 1)
		self.rcv_timeout = rcv_timeout
		self.rcv_tries = rcv_tries

	def send2(self, ticket):
		self.u.send_no_wait(ticket, self.server_address)

	def hello(self):
		ticket = {'work': 'hello'}
		return self.send(ticket, 30, 1)

	def hello2(self):
		ticket = {'work': 'hello'}
		return self.send2(ticket)

	def log_start_mount(self, volume, type, start=time.time()):
		ticket = {
			'work': 'log_start_mount',
			'node': self.node,
			'volume': volume,
			'type': type,
			'logname': self.logname,
			'start': start}
		self.send2(ticket)

	def log_finish_mount(self, volume, finish, state='M'):
		ticket = {
			'work': 'log_finish_mount',
			'node': self.node,
			'volume': volume,
			'finish': finish,
			'state': state}
		self.send2(ticket)

	def log_start_dismount(self, volume, type, start):
		ticket = {
			'work': 'log_start_dismount',
			'node': self.node,
			'volume': volume,
			'type': type,
			'logname': self.logname,
			'start': start}
		self.send2(ticket)

	def log_finish_dismount(self, volume, finish, state='D'):
		ticket = {
			'work': 'log_finish_dismount',
			'node': self.node,
			'volume': volume,
			'finish': finish,
			'state': state}
		self.send2(ticket)

	def log_encp_xfer(self, date, src, dst, size, volume, rate,
		net_rate, drive_rate, mover, drive_id, drive_sn,
		elapsed, media_changer, mover_interface, driver,
		storage_group, encp_ip, encp_id, rw):

		ticket = {
			'work'		: 'log_encp_xfer',
			'date'		: date,
			'node'		: self.node,
			'pid'		: self.pid,
			'username'	: self.uid,
			'src'		: src,
			'dst'		: dst,
			'size'		: size,
			'volume'	: volume,
			'rate'		: rate,
			'net_rate'	: net_rate,
			'drive_rate'	: drive_rate,
			'mover'		: mover,
			'drive_id'	: drive_id,
			'drive_sn'	: drive_sn,
			'elapsed'	: elapsed,
			'media_changer'	: media_changer,
			'mover_interface': mover_interface,
			'driver'	: driver,
			'storage_group'	: storage_group,
			'encp_ip': encp_ip,
			'encp_id': encp_id,
			'rw': rw}
		self.send2(ticket)

			


if __name__ == '__main__':
	intf = option.Interface()
	ac = accClient((intf.config_host, intf.config_port))
	if sys.argv[1] == 'hello':
		pprint.pprint(ac.hello())
	elif sys.argv[1] == 'quit':
		pprint.pprint(ac.quit())
	elif sys.argv[1] == 'hello2':
		ac.hello2()
