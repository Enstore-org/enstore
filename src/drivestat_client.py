#!/usr/bin/env python

# system import
import os
import sys
import string
import pprint
import pwd
import socket

# enstore import
import generic_client
import option
import time

MY_NAME = "drivestat_client"
MY_SERVER = "drivestat_server"
RCV_TIMEOUT = 10
RCV_TRIES = 1

class dsClient(generic_client.GenericClient):
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


	def log_stat(self,
		drive_sn,
		drive_vendor,
		product_type,
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
		mount_count):
		
		ticket = {
			"work": "log_stat",
			"drive_sn": drive_sn,
			"drive_vendor": drive_vendor,
			"product_type": product_type,
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
			"mount_count": mount_count}

		self.send2(ticket)

if __name__ == '__main__':
	intf = option.Interface()
	dsc = dsClient((intf.config_host, intf.config_port))
