#!/usr/bin/env python

import sys
import os
import volume_clerk_client
import file_clerk_client
import generic_client
import configuration_client
import e_errors
import string
import option

class Interface(generic_client.GenericClientInterface):
	def __init__(self, args=sys.argv, user_mode=0):
		self.delete = None
		self.force = 0
		self.skip_pnfs = 0
		self.dont_ask = 0
		self.bfids = None
		self.keep_vol = None
		generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

	def options(self):
		return(['delete', 'force', 'skip-pnfs', 'dont-ask', 'bfids'])

	def valid_dictionaries(self):
		return (self.help_options, self.super_remove_options)

        super_remove_options = {
		option.DONT_ASK: {
			option.HELP_STRING: "No confirmation needed",
			option.DEFAULT_VALUE:option.DEFAULT,
			option.VALUE_USAGE:option.IGNORED,
			option.USER_LEVEL:option.ADMIN},
		option.SKIP_PNFS: {
			option.HELP_STRING: "Ignore pnfs",
			option.DEFAULT_VALUE:option.DEFAULT,
			option.VALUE_USAGE:option.IGNORED,
			option.USER_LEVEL:option.ADMIN},
		option.KEEP_VOL: {
			option.HELP_STRING: "Keep volume record",
			option.DEFAULT_VALUE:option.DEFAULT,
			option.VALUE_USAGE:option.IGNORED,
			option.USER_LEVEL:option.ADMIN},
		option.FORCE: {
			option.HELP_STRING: "Proceed on errors",
			option.DEFAULT_VALUE:option.DEFAULT,
			option.VALUE_USAGE:option.IGNORED,
			option.USER_LEVEL:option.ADMIN},
		option.BFIDS: {
			option.HELP_STRING: "Take a file of bfids",
			option.VALUE_TYPE:option.STRING,
			option.VALUE_USAGE:option.REQUIRED,
			option.USER_LEVEL:option.ADMIN},
		option.DELETE: {
			option.HELP_STRING: "Actual deletion",
			option.DEFAULT_VALUE:option.DEFAULT,
			option.VALUE_USAGE:option.IGNORED,
			option.USER_LEVEL:option.ADMIN}
		}

def usage():
	print "usage: %s [[--dont-ask] [--skip-pnfs] [--force] [--bfids] --delete] [--keep-vol] vol]"%(sys.argv[0])

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

	if len(intf.args) == 1:
		vol = intf.args[0]
	else:
		usage()
		sys.exit(0)

	print 'checking', vol, '...'
	error = 0

	if intf.bfids:	# read from the file
		try:
			bf = open(vol)
		except:
			print 'can not find bfids file "%s"'%(vol)
			sys.exit(1)
		files = []
		for i in bf.readlines():
			files.append(string.strip(i))
		bf.close()
	else:		# get it from file clerk
		try:	
			files = fcc.get_bfids(vol)['bfids']
		except:
			error = error + 1
			print 'can not find files for', vol
			if not intf.force:
				sys.exit(1)
			files = []

	# get info for every file
	file_list = {}
	for i in files:
		fcc.bfid = i
		fileInfo = fcc.bfid_info()
		if fileInfo['status'][0] == e_errors.OK:
			file_list[i] = fileInfo
		else:
			print '(Warning)', i, fileInfo

	for i in file_list.keys():
		fileInfo = file_list[i]
		if not intf.bfids:
			if fileInfo['external_label'] != vol:
				print i, 'is not in', vol, `fileInfo`
				error = error + 1
		if not intf.skip_pnfs:
			try:
				volmap = fileInfo['pnfs_mapname']
			except KeyError, detail:
				print '(Warning)', i, 'does not have key', detail
				volmap = None
			external_label = fileInfo['external_label']
			try:
				pnfsname = fileInfo['pnfs_name0']
			except KeyError, detail:
				print '(Warning)', i, 'does not have key', detail
				pnfsname = None
			if pnfsname:
				pnfsdir, pnfsfile = os.path.split(pnfsname)
			else:
				pnfsdir = None
				pnfsfile = None
			if volmap:
				volmapdir, volmapfile = os.path.split(volmap)
				volmapdpd, junk = os.path.split(volmapdir)
			else:
				volmapdir = None
				volmapdpd = None
			# check access permission to pnfs files
			if pnfsdir and not os.access(pnfsdir, os.W_OK):
				if os.access(pnfsdir, os.F_OK):
					print 'No write permission to', pnfsdir
				else:
					print pnfsdir, 'does not exist'
				error = error + 1
			if pnfsname and not os.access(pnfsname, os.W_OK):
				if os.access(pnfsname, os.F_OK):
					print 'No write permission to', pnfsname
				else:
					print pnfsname, 'does not exist'
				error = error + 1
			if volmapdpd and not os.access(volmapdpd, os.W_OK):
				if os.access(volmapdpd, os.F_OK):
					print 'No write permission to', volmapdpd
					error = error + 1
				else:
					print '(Warning)', volmapdpd, 'does not exist'
					# ignore it
			if volmapdir and not os.access(volmapdir, os.W_OK):
				if os.access(volmapdir, os.F_OK):
					print 'No write permission to', volmapdir
					error = error + 1
				else:
					print '(Warning)', volmapdir, 'does not exist'
					# ignore it
			if volmap and not os.access(volmap, os.W_OK):
				if os.access(volmap, os.F_OK):
					print 'No write permission to', volmap
					error = error + 1
				else:
					print '(Warning)', volmap, 'does not exist'
					# ignore it

	if error:
		if intf.force:
			print "Force to proceed on errors ..."
		else:
			print vol, "can not be removed due to above reasons"
			sys.exit(1)
	else:
		print 'It is OK to remove', vol

	if not intf.delete:
		if intf.skip_pnfs:
			skip = '--skip-pnfs '
		else:
			skip = ''
		print 'use "%s %s--delete [--keep-vol] %s" to really delete it'%(sys.argv[0], skip, vol)
		sys.exit(0)

	# let's get serious

	# try to find "delfile"

	if not intf.skip_pnfs:
		delfile = os.path.join(os.environ['ENSTORE_DIR'], 'sbin', 'delfile.py')
		if not os.access(delfile, os.X_OK):
			print 'can not find executable', delfile
			sys.exit(0)

	print 'volume =', vol
	if not intf.dont_ask:
		for i in file_list.keys():
			fileInfo = file_list[i]
			try:
				volmap = fileInfo['pnfs_mapname']
			except KeyError, detail:
				volmap = '*No volmap*'
			external_label = fileInfo['external_label']
			try:
				pnfsname = fileInfo['pnfs_name0']
			except KeyError:
				pnfsname = '*no pnfs name*'
			# check access permission to pnfs files
			print external_label, i, pnfsname, volmap

		print "Are you sure that you want to destroy everything listed above (y/n)?",
		ans = sys.stdin.readline()
		if ans[0] != 'y' and ans[0] != 'Y':
			sys.exit(0)

	# now, it is for real. Don't try this at home!

	if not intf.skip_pnfs:
		error = 0
		volmapdir = '/tmp/none'
		for i in file_list.keys():
			fileInfo = file_list[i]
			try:
				volmap = fileInfo['pnfs_mapname']
			except KeyError, detail:
				volmap = None
			external_label = fileInfo['external_label']
			try:
				pnfsname = fileInfo['pnfs_name0']
			except KeyError, detail:
				pnfsname = None
			if pnfsname:
				pnfsdir, pnfsfile = os.path.split(pnfsname)
			else:
				pnfsdir = None
				pnfsfile = None
			if volmap:
				volmapdir, volmapfile = os.path.split(volmap)
			else:
				volmapdir = None
				volmapfile = None
			if volmapdir:
				volmapdpd, junk = os.path.split(volmapdir)
			else:
				volmapdpd = None
			# check access permission to pnfs files
			if pnfsname and os.access(pnfsname, os.W_OK):
				print 'removing', pnfsname, '...',
				try:
					os.unlink(pnfsname)
					print 'done'
				except:
					print 'failed'
					error = error + 1
			if volmap and os.access(volmap, os.W_OK):
				print 'removing', volmap, '...',
				try:
					os.unlink(volmap)
					print 'done'
				except:
					print 'failed'
					error = error + 1

		# now remove the volmap directory, too

		if volmapdir and os.access(volmapdir, os.W_OK):
			print 'removing', volmapdir, '...',
			try:
				os.unlink(volmapdir)
				print 'done'
			except:
				print 'failed'
				error = error + 1

		if error:
			print 'having trouble with /pnfs permission ... stop'
			sys.exit(1)

		# run delfile to clean it up

		print 'running', delfile, 'to clean up ...',
		if os.system(delfile):
			print 'failed'
		else:
			print 'done'

	# delete from file database

	for i in file_list.keys():
		fcc.bfid = i
		print 'removing', i, 'from file database ...',
		ticket = fcc.del_bfid()
		if ticket['status'][0] == e_errors.OK:
			print 'done'
		else:
			print 'failed'

	# delete from volume database

	if not intf.bfids and not intf.keep_vol:
		print 'removing', vol, 'from volume database ...',
		ticket = vcc.rmvolent(vol)
		if ticket['status'][0] == e_errors.OK:
			print 'done'
			print vol, 'is removed forever'
		else:
			print 'failed'
