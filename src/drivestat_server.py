#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

'''
Drivestat Module -- record drivestat information
'''

# system import
import os
import sys
import string
import pprint
import time

# enstore import
import dispatching_worker
import generic_server
import Trace
import e_errors
import drivestat2
import enstore_constants
import monitored_server
import event_relay_messages

MY_NAME = enstore_constants.DRIVESTAT_SERVER   #"drivestat_server"

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
		generic_server.GenericServer.__init__(self, csc, MY_NAME,
						function = self.handle_er_msg)
		Trace.init(self.log_name)
		self.keys = self.csc.get(MY_NAME)
		self.alive_interval = monitored_server.get_alive_interval(self.csc, MY_NAME, self.keys)

		att = self.csc.get(MY_NAME)
		self.hostip = att['hostip']
		dispatching_worker.DispatchingWorker.__init__(self,
			(att['hostip'], att['port']))
		try:
			self.dsDB = drivestat2.dsDB(att['dbhost'], att['dbname'], att['dbuser'], att['dbport'])
		except: # wait for 30 seconds and retry
			time.sleep(30)
			self.dsDB = drivestat2.dsDB(att['dbhost'], att['dbname'], att['dbuser'], att['dbport'])
		# setup the communications with the event relay task
		self.resubscribe_rate = 300
		self.erc.start([event_relay_messages.NEWCONFIGFILE],
			       self.resubscribe_rate)
		# start our heartbeat to the event relay process
		self.erc.start_heartbeat(enstore_constants.DRIVESTAT_SERVER,
			self.alive_interval)

		return

	def log_stat(self, ticket):
		try:
			drive_sn = ticket['drive_sn']
			drive_vendor = ticket['drive_vendor']
			product_type = ticket['product_type']
			host = ticket['host']
			logical_drive_name = ticket['logical_drive_name']
			stat_type = ticket['stat_type']
			time2 = ticket['time']
			tape_volser = ticket['tape_volser']
			power_hrs = ticket['power_hrs']
			motion_hrs = ticket['motion_hrs']
			cleaning_bit = ticket['cleaning_bit']
			mb_user_read = ticket['mb_user_read']
			mb_user_write = ticket['mb_user_write']
			mb_dev_read = ticket['mb_dev_read']
			mb_dev_write = ticket['mb_dev_write']
			read_errors = ticket['read_errors']
			write_errors = ticket['write_errors']
			track_retries = ticket['track_retries']
			underrun = ticket['underrun']
			mount_count = ticket['mount_count']
		except KeyError, detail:
			Trace.log(e_errors.ERROR, "key %s is missing"  % (detail))

		try:
			firmware_version = ticket['firmware_version']
		except:
			firmware_version = ''

		try:
			wp = ticket['wp']
		except:
			wp = 0

		self.dsDB.log_stat(
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
			wp,
			ticket.get("mover_name",None))

	# The following are local methods

	# close the database connection
	def close(self):
		self.dsDB.close()
		return

	# These need confirmation
	def quit(self, ticket):
		self.dsDB.close()
		dispatching_worker.DispatchingWorker.quit(self, ticket)


if __name__ == "__main__":
	Trace.init(string.upper(MY_NAME))
	intf = Interface()
	csc = (intf.config_host, intf.config_port)
	dsServer = Server(csc)
	dsServer.handle_generic_commands(intf)

	while 1:
		try:
			Trace.log(e_errors.INFO, "Drivestat Server (re)starting")
			dsServer.serve_forever()
		except SystemExit, exit_code:
			dsServer.accDB.close()
			sys.exit(exit_code)
		except:
			dsServer.serve_forever_error(dsServer.log_name)
			continue

