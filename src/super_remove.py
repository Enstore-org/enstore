#!/usr/bin/env python

# Got to run this at the server that hosts pnfs!

import sys
import os
import volume_clerk_client
import file_clerk_client
import generic_client
import configuration_client
import e_errors
# just for now, while bfid_db is still alive
import bfid_db

class Interface(generic_client.GenericClientInterface):
	def __init__(self):
		self.delete = None
		generic_client.GenericClientInterface.__init__(self)

	def options(self):
		return(['delete='])

def usage():
	print "usage: %s [--delete] vol"%(sys.argv[0])

if __name__ == '__main__':
	# use GenericClientInterface to get basic environment
	intf = Interface()

	vcc = volume_clerk_client.VolumeClerkClient((intf.config_host, intf.config_port))
	# cheat generic_client
	generic_client.init_done = 0
	fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))

	# get dbInfo
	dbInfo = configuration_client.ConfigurationClient((intf.config_host, intf.config_port)).get('database')

	dbHome = dbInfo['db_dir']
	bfiddb = bfid_db.BfidDb(dbHome)

	if intf.delete:
		vol = intf.delete
	elif len(intf.args) == 1:
		vol = intf.args[0]
	else:
		usage()
		sys.exit(0)

	print 'checking', vol, '...'

	try:	
		files = bfiddb.get_all_bfids(vol)
	except:
		print 'can not find files for', vol
		sys.exit(1)

	# get info for every file
	file_list = {}
	for i in files:
		fcc.bfid = i
		fileInfo = fcc.bfid_info()
		file_list[i] = fileInfo

	error = 0
	for i in file_list.keys():
		fileInfo = file_list[i]
		volmap = fileInfo['pnfs_mapname']
		external_label = fileInfo['external_label']
		pnfsname = fileInfo['pnfs_name0']
		pnfsdir, pnfsfile = os.path.split(pnfsname)
		volmapdir, volmapfile = os.path.split(volmap)
		# check access permission to pnfs files
		if not os.access(pnfsdir, os.W_OK):
			if os.access(pnfsdir, os.F_OK):
				print 'No write permission to', pnfsdir
			else:
				print pnfsdir, 'does not exist'
			error = error + 1
		if not os.access(pnfsname, os.W_OK):
			if os.access(pnfsname, os.F_OK):
				print 'No write permission to', pnfsname
			else:
				print pnfsname, 'does not exist'
			error = error + 1
		if not os.access(volmapdir, os.W_OK):
			if os.access(volmapdir, os.F_OK):
				print 'No write permission to', volmapdir
				error = error + 1
			else:
				print '(Warning)', volmapdir, 'does not exist'
				# ignore it
		if not os.access(volmap, os.W_OK):
			if os.access(volmap, os.F_OK):
				print 'No write permission to', volmap
				error = error + 1
			else:
				print '(Warning)', volmap, 'does not exist'
				# ignore it

	if error:
		print vol, "can not be removed due to above reasons"
		sys.exit(1)
	else:
		print 'It is OK to remove', vol

	if not intf.delete:
		print 'use %s --delete %s to really delete it'%(sys.argv[0], vol)
		sys.exit(0)

	# let's get serious

	print 'volume =', vol
	for i in file_list.keys():
		fileInfo = file_list[i]
		volmap = fileInfo['pnfs_mapname']
		external_label = fileInfo['external_label']
		pnfsname = fileInfo['pnfs_name0']
		pnfsdir, pnfsfile = os.path.split(pnfsname)
		volmapdir, volmapfile = os.path.split(volmap)
		# check access permission to pnfs files
		print external_label, i, pnfsname, volmap

	print "Are you sure that you want to destroy everything listed above (y/n)?",
	ans = sys.stdin.readline()
	if ans[0] != 'y' and ans[0] != 'Y':
		sys.exit(0)

	# now, it is for real. Don't try this at home!

	for i in file_list.keys():
		fileInfo = file_list[i]
		volmap = fileInfo['pnfs_mapname']
		external_label = fileInfo['external_label']
		pnfsname = fileInfo['pnfs_name0']
		pnfsdir, pnfsfile = os.path.split(pnfsname)
		volmapdir, volmapfile = os.path.split(volmap)
		# check access permission to pnfs files
		if os.access(pnfsname, os.W_OK):
			os.unlink(pnfsname)
			print 'os.unlink('+pnfsname+')'
		if os.access(volmap, os.W_OK):
			os.unlink(volmap)
			print 'os.unlink('+volmap+')'

		# clean it up in file database

		fcc.bfid = i
		ticket = fcc.del_bfid()
		if ticket['status'][0] == e_errors.OK:
			print i, 'deleted'
		else:
			print 'fail to delete', i

	# delete from volume database

	ticket = vcc.rmvolent(vol)
	if ticket['status'][0] == e_errors.OK:
		print 'volume', vol, 'removed forever'
