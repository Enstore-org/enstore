#!/usr/bin/env python

import os
import sys
import time
import option
import configuration_client
import e_errors
import enstore_start

debug = True
dbserver_cmd = 'postmaster'
pid_file = dbserver_cmd+'.pid'
# db_stop_cmd = 'pg_ctl stop -m f -D '
db_servers = ['database', 'accounting_server', 'drivestat_server']

# find_pid(cmd, arg) -- find pid based on cmd and argument
def find_pid(cmd, arg):
	res = os.popen('ps axw').readlines()
	for i in res:
		if i.find(cmd) >=0  and i.find(str(arg)) >= 0:
			return(int(i.split()[0]))
	return 0

# start_database(dbname, dbport, dbarea) -- start database server
def start_database(dbport, dbarea, dbserverowner=''):
	pid = find_pid(dbserver_cmd, dbarea)
	if pid:
		# it's already started
		if debug:
			print dbserver_cmd, "(%d) is running on %s"%(pid, dbarea)
		return
	# check if dbarea is writeable
	if not os.access(dbarea, os.W_OK):
		if debug:
			print "Database area %s is not writable"%(dbarea)
		return

	# remove pid file if there is one
	if os.access(os.path.join(dbarea, pid_file), os.W_OK):
		try:
			os.remove(os.path.join(dbarea, pid_file))
		except:
			pass

	if dbserverowner:
		cmd = 'su %s -c "%s -p %d -D %s -i &"'%(dbserverowner, get_db_command(), dbport, dbarea)
	else:
		cmd = '%s -p %d -D %s -i &'%(get_db_command(), dbport, dbarea)
	os.system(cmd)

	# check if database server is really up
	psql = get_psql()
	cmd = psql + ' -p %d template1 -U %s -c "select now();"'%(
		dbport, dbserverowner)
	for i in range(5):
		time.sleep(5)
		if os.system(cmd) == 0:
			if debug:
				print "database is ready"
			return
		else:
			if debug:
				print "database is not ready, trying", i

	sys.exit(1)
	

# stop_database(dbarea) -- stop database server
def stop_database(dbarea, dbserverowner):
	if os.access(os.path.join(dbarea, pid_file), os.W_OK):
		cmd = 'su %s -c "%s %s"'%(dbserverowner, get_db_stop_command(), dbarea)
		os.system(cmd)

# get_db_command() -- get postmaster from the shell
def get_db_command():
	return os.popen('which postmaster').readline().strip()

# get_db_stop_command() -- get pg_ctl to stop the database
def get_db_stop_command():
	return os.popen('which pg_ctl').readline().strip()+"  stop -m f -D "

# get_psql() -- get psql
def get_psql():
	return os.popen('which psql').readline().strip()

# usage()
def usage():
	print "Usage: %s start|stop|restart"%(sys.argv[0])

def start_all(csc):
	for i in db_servers:
		server = csc.get(i)
                if enstore_start.is_on_host(server['dbhost']):
			start_database(
				server['dbport'],
				server['dbarea'],
				server['dbserverowner'])

def stop_all(csc):
	for i in db_servers:
		server = csc.get(i)
                if enstore_start.is_on_host(server['dbhost']):
			stop_database(
				server['dbarea'],
				server['dbserverowner'])

if __name__ == '__main__':
	intf = option.Interface()
	csc = configuration_client.ConfigurationClient(
		(intf.config_host, intf.config_port)
		)

	# see if configuration server is alive
	res = csc.alive(configuration_client.MY_SERVER, rcv_timeout=10)
	if res['status'][0] != e_errors.OK:
		if debug:
			print "configuration_server is not responding ... Get configuration from local file"
		csc = configuration_client.configdict_from_file()

	if len(sys.argv) < 2:
		usage()
		sys.exit()

	if sys.argv[1] == 'start':
		start_all(csc)
	elif sys.argv[1] == 'stop':
		stop_all(csc)
	elif sys.argv[1] == 'restart':
		stop_all(csc)
		start_all(csc)
	else:
		usage()
