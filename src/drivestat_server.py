#!/usr/bin/env python

'''
Drivestat Module -- record drivestat information
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
import drivestat

MY_NAME = "drivestat_server"

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
		generic_server.GenericServer.__init__(self, csc, MY_NAME)
		Trace.init(self.log_name)
		att = self.csc.get(MY_NAME)
		self.hostip = att['hostip']
		dispatching_worker.DispatchingWorker.__init__(self,
			(att['hostip'], att['dbport']))
		self.dsDB = drivestat.dsDB(att['dbhost'], att['dbname'], att['port'])
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

		self.dsDB.log_stat(
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
			mount_count)

	# The following are local methods

	# close the database connection
	def close(self):
		self.dsDB.close()
		return

	# These need confirmation
	def quit(self, ticket):
		self.dsDB.close()
		self.reply_to_caller({'status':(e_errors.OK, None)})
		sys.exit(0)


if __name__ == '__main__':
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
	
