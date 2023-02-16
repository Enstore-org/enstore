#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system import
import os
import sys
#import string
#import pprint
import pwd
import socket
import time

# enstore import
import generic_client
import option
import enstore_constants


MY_NAME = enstore_constants.DRIVESTAT_CLIENT    #"drivestat_client"
MY_SERVER = enstore_constants.DRIVESTAT_SERVER  #"drivestat_server"
RCV_TIMEOUT = 10
RCV_TRIES = 1

class dsClient(generic_client.GenericClient):
	def __init__(self, csc, logname='UNKNOWN', rcv_timeout = RCV_TIMEOUT,
		     rcv_tries = RCV_TRIES, server_address = None,
		     flags = 0, logc = None, alarmc = None):
		self.logname = logname
		self.node = os.uname()[1]
		self.pid = os.getpid()
		generic_client.GenericClient.__init__(
			self, csc, MY_NAME, server_address = server_address,
			flags = flags, logc = logc, alarmc = alarmc,
			rcv_timeout = rcv_timeout, rcv_tries = rcv_tries,
			server_name = MY_SERVER)
		try:
			self.uid = pwd.getpwuid(os.getuid())[0]
		except:
			self.uid = "unknown"
		self.rcv_timeout = rcv_timeout
		self.rcv_tries = rcv_tries
		#self.server_address = self.get_server_address(MY_SERVER,
		#                                             self.rcv_timeout,
		#					      self.rcv_tries)

	# send_no_wait
	def send2(self, ticket):
		if not self.server_address: return
		self.u.send_no_wait(ticket, self.server_address)


	def log_stat(self,
		drive_sn,
		drive_vendor,
		product_type,
		firmware_version,
		host,
		logical_drive_name,
		stat_type,
		time2,
		tape_volser,
		power_hrs,
		motion_hrs,
		cleaning_bit,
		mb_user_read,
		mb_user_write,
		mb_dev_read,
		mb_dev_write,
		read_errors,
		write_errors,
		track_retries,
		underrun,
		mount_count,
		wp = 0,
		mover_name=None):

		ticket = {
			"work": "log_stat",
			"drive_sn": drive_sn,
			"drive_vendor": drive_vendor,
			"product_type": product_type,
			"firmware_version": firmware_version,
			"host": host,
			"logical_drive_name": logical_drive_name,
			"stat_type": stat_type,
			"time": time2,
			"tape_volser": tape_volser,
			"power_hrs": power_hrs,
			"motion_hrs": motion_hrs,
			"cleaning_bit": cleaning_bit,
			"mb_user_read": mb_user_read,
			"mb_user_write": mb_user_write,
			"mb_dev_read": mb_dev_read,
			"mb_dev_write": mb_dev_write,
			"read_errors": read_errors,
			"write_errors": write_errors,
			"track_retries": track_retries,
			"underrun": underrun,
			"mount_count": mount_count,
			"wp": wp,
			"mover_name" : mover_name}

		self.send2(ticket)

	def test(self):
		self.log_stat(
		'12345678',
		'fermilab',
		'0000_ABC',
		'ver 0.0',
		'host',
		'DUMMY_DRIVE',
		'stat_type',
		time.time(),
		'tape_volser',
		10,
		5,
		1,
		1000,
		1001,
		2000,
		2001,
		10,
		11,
		100,
		10,
		1999,
		0,
		"DummyMover.mover")

if __name__ == "__main__":   # pragma: no cover
	intf = option.Interface()
	dsc = dsClient((intf.config_host, intf.config_port))
	dsc.test()
