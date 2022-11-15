#!/usr/bin/env python

# paranoid pulling backups

import sys
import os
import pprint
import string
import time

# local paranoid backup path
paranoid_backup_path = '/diska/paranoid-backup'

# path on backup node
database_backup_path = '/diska/enstore-backup'
pnfs_backup_path = '/diska/pnfs-backup'
database_pattern = 'dbase*'
pnfs_pattern = '*.*.Z'
db_log = 'source'
systems = ['stk', 'd0', 'cdf']

copies = 1	# number of pnfs backup cpoies to keep
keepall = 0
do_it = 1

def usage():
	print 'usage %s [-n] [-k]'%(sys.argv[0])

backup_hosts = {
	'cdf': 'cdfensrv3.fnal.gov',
	'd0' : 'd0ensrv3.fnal.gov',
	'stk': 'stkensrv3.fnal.gov'}

def paranoid_db(system):
	# handle database backup
	backup_host = backup_hosts[system]
	print '==== backup database files from '+backup_host+' ...'
	cmd = 'rsh '+backup_host+' ls -1td '+os.path.join(database_backup_path, database_pattern)
	result = os.popen(cmd).readlines()
	result = map(string.strip, result)
	dbase_path = result[0]
	cmd = 'rsh '+backup_host+' ls -1 '+dbase_path
	result = os.popen(cmd).readlines()
	database_file = map(string.strip, result)
	paranoid_db_path = os.path.join(os.path.join(paranoid_backup_path, system), 'db')

	# create paranoid_db_path if it is necessary
	if not os.access(paranoid_db_path, os.X_OK):
		print '==== making directory '+paranoid_db_path+' ...'
		if do_it:
			os.makedirs(paranoid_db_path)	
	for i in database_file:
		cmd = 'rcp '+backup_host+':'+os.path.join(dbase_path,i)+' '+ paranoid_db_path
		print cmd
		if do_it:
			os.system(cmd)
	if do_it:
		f = open(os.path.join(paranoid_db_path, db_log), 'w')
		f.write(backup_host+':'+dbase_path)
		f.close()

def paranoid_pnfs(system):
	# handle pnfs
	backup_host = backup_hosts[system]
	print '==== backup pnfs files from '+backup_host+' ...'
	# cmd = 'rsh '+backup_host+' ls -1td '+os.path.join(pnfs_backup_path, pnfs_pattern)
	cmd = 'rsh %s "cd %s; ls -1t %s"'%(backup_host, pnfs_backup_path,
		pnfs_pattern)
	result = os.popen(cmd).readlines()
	result = map(string.strip, result)
	files = {}
	for i in result:
		token = string.split(i, '.')
		if len(token) == 3 and token[2] == 'Z':
			file = token[0]
			if not files.has_key(file):
				files[file] = i
	paranoid_pnfs_path = os.path.join(os.path.join(paranoid_backup_path, system), 'pnfs')
	# create paranoid_pnfs_path if it is necessary
	if not os.access(paranoid_pnfs_path, os.X_OK):
		print '==== making directory '+paranoid_pnfs_path+' ...'
		if do_it:
			os.makedirs(paranoid_pnfs_path)
	for i in files.keys():
		cmd = 'rcp %s:%s %s'%(backup_host, os.path.join(pnfs_backup_path, files[i]), paranoid_pnfs_path)
		print cmd
		if do_it:
			os.system(cmd)

	# removing older files
	cmd = 'cd %s; ls -1t'%(paranoid_pnfs_path)
	result = os.popen(cmd).readlines()
	result = map(string.strip, result)
	files = {}
	for i in result:
		token = string.split(i, '.')
		if len(token) == 3 and token[2] == 'Z':
			file = token[0]
			if files.has_key(file):
				files[file].append(i)
			else:
				files[file] = [i]

	for i in files.keys():
		if not keepall and len(files[i]) > copies:
			for j in files[i][copies:]:
				print "rm "+os.path.join(paranoid_pnfs_path, j)
				if do_it:
					os.unlink(os.path.join(paranoid_pnfs_path, j))

			
				
def paranoid(system):
	paranoid_db(system)
	paranoid_pnfs(system)

if __name__ == "__main__":   # pragma: no cover

	print '====', sys.argv[0], 'starts at', time.ctime(time.time())

	if len(sys.argv) > 1 and sys.argv[1] == '-n':
		do_it = None

	for i in sys.argv[1:]:
		if i == '-h':
			usage()
			sys.exit(0)
		if i == '-n':
			do_it = None
		if i == '-k':
			keepall = 1

	for system in systems:
		paranoid(system)

	print '====', sys.argv[0], 'finishes at', time.ctime(time.time())
