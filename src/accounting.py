#!/usr/bin/env python

import pg
import os
import pprint
import time

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"
def time2timestamp(t):
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))

# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time
def timestamp2time(s):
	return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))

class accDB:
	def __init__(self, host, dbname, logname='UNKNOWN'):
		self.logname = logname
		self.db = pg.DB(host=host, dbname=dbname)
		self.pid = os.getpid()

	def close(self):
		self.db.close()

	def insert(self, table, dict):
		return self.db.insert(table, dict)

	def log_start_mount(self, node, volume, mtype, logname, start):
		# This is the main table
		if type(start) != type(""):
			start = time2timestamp(start)
		res = self.db.insert('tape_mounts', {
			'node': node,
			'volume': volume,
			'type': mtype,
			'logname': logname,
			'start': start,
			'state': 'm'})

		# remember the start part
		try:
			res2 = self.db.insert('tape_mounts_tmp', {
				'volume': volume,
				'state': 'm',
				'id': res['oid_tape_mounts']})
		except:
			res2 = self.db.get('tape_mounts_tmp', {
				'volume': volume,
				'state': 'm'})

			res2 = self.db.update('tape_mounts_tmp', {
				'oid_tape_mounts_tmp': res2['oid_tape_mounts_tmp'],
				'id': res['oid_tape_mounts']})

	def log_finish_mount(self, node, volume, finish, state='M'):
		if type(finish) != type(""):
			finish = time2timestamp(finish)
		try:
			res = self.db.get('tape_mounts_tmp', {
				'volume': volume,
				'state': 'm'})
		except:
			return

		res2 = self.db.update('tape_mounts', {
			'oid_tape_mounts': res['id'],
			'finish': finish,
			'state': state})

		res2 = self.db.delete('tape_mounts_tmp', {
			'oid_tape_mounts_tmp': res['oid_tape_mounts_tmp']})

	def log_start_dismount(self, node, volume, mtype, logname, start):
		if type(start) != type(""):
			start = time2timestamp(start)
		
		# This is the main table
		res = self.db.insert('tape_mounts', {
			'node': node,
			'volume': volume,
			'type': mtype,
			'logname': logname,
			'start': start,
			'state': 'd'})

		# remember the start part
		try:
			res2 = self.db.insert('tape_mounts_tmp', {
				'volume': volume,
				'state': 'd',
				'id': res['oid_tape_mounts']})
		except:
			res2 = self.db.get('tape_mounts_tmp', {
				'volume': volume,
				'state': 'd'})

			res2 = self.db.update('tape_mounts_tmp', {
				'oid_tape_mounts_tmp': res2['oid_tape_mounts_tmp'],
				'id': res['oid_tape_mounts']})

	def log_finish_dismount(self, node, volume, finish, state='D'):
		if type(finish) != type(""):
			finish = time2timestamp(finish)
		try:
			res = self.db.get('tape_mounts_tmp', {
				'volume': volume,
				'state': 'd'})
		except:
			return

		res2 = self.db.update('tape_mounts', {
			'oid_tape_mounts': res['id'],
			'finish': finish,
			'state': state})

		res2 = self.db.delete('tape_mounts_tmp', {
			'oid_tape_mounts_tmp': res['oid_tape_mounts_tmp']})

	def log_encp_xfer2(self, xfer):
		if type(xfer['date']) != type(""):
			xfer['date'] = time2timestamp(xfer['date'])
		self.insert('encp_xfer', xfer)

	def log_encp_xfer(self, date, node, pid, username, src, dst,
		size, volume, network_rate, drive_rate, disk_rate,
		overall_rate, transfer_rate, mover,
		drive_id, drive_sn, elapsed, media_changer,
		mover_interface, driver, storage_group, encp_ip,
		encp_id, rw, encp_version='unknown'):

		if type(date) != type(""):
			date = time2timestamp(date)

		xfer = {'date'		: date,
			'node'		: node,
			'pid'		: pid,
			'username'	: username,
			'src'		: src,
			'dst'		: dst,
			'size'		: size,
			'volume'	: volume,
			'overall_rate'	: overall_rate,
			'network_rate'	: network_rate,
			'drive_rate'	: drive_rate,
			'disk_rate'	: disk_rate,
			'transfer_rate'	: transfer_rate,
			'mover'		: mover,
			'drive_id'	: drive_id,
			'drive_sn'	: drive_sn,
			'elapsed'	: elapsed,
			'media_changer'	: media_changer,
			'mover_interface': mover_interface,
			'driver'	: driver,
			'storage_group'	: storage_group,
			'encp_ip'	: encp_ip,
			'encp_id'	: encp_id,
			'rw'		: rw,
			'encp_version'	: encp_version}

		self.insert('encp_xfer', xfer)

	def log_encp_error(self, date, node, pid, username, src, dst,
		size, encp_id, version, type, error):
		if type(date) != type(""):
			date = time2timestamp(date)

		en_error = {
			'date'		: date,
			'node'		: node,
			'pid'		: pid,
			'username'	: username,
			'version'	: version,
			'error'		: error}

		if src:
			en_error['src'] = src
		if dst:
			en_error['dst'] = dst
		if size:
			en_error['size'] = size
		else:
			en_error['size'] = -1
		if encp_id:
			en_error['encp_id'] = encp_id
		

		self.insert('encp_error', en_error)
			
	# This pair of function need a unique tag to work
	# The calling function should provide such a tag
	# A simple key is host_ip.pid.time

	def log_start_event(self, tag, name, node, username, start):
		if type(start) != type(""):
			start = time2timestamp(start)

		res = self.db.insert('event', {
			'tag': tag,
			'name': name,
			'node': node,
			'username': username,
			'start': start})

	def log_finish_event(self, tag, finish, status = 0, comment = None):
		if type(finish) != type(""):
			finish = time2timestamp(finish)

		if comment:
			commentstr = ", comment = '%s'"%(comment)
		else:
			commentstr = ""
		
		qs = "update event set finish = '%s', status = %d%s where tag = '%s' and finish is null;"%(finish, status, commentstr, tag) 
		res = self.db.query(qs)
