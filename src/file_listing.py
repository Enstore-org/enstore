#!/usr/bin/env python

import db
import time
import sys
import string
import os
import volume_family
import configuration_client

vcc = None
_ff = {}

config_host = os.environ['ENSTORE_CONFIG_HOST']
config_port = int(os.environ['ENSTORE_CONFIG_PORT'])
csc = configuration_client.ConfigurationClient((config_host, config_port))

inventory = csc.get('inventory')
backup = csc.get('backup')

wd = inventory['inventory_dir']
db_path = backup['extract_dir']

dest_path = inventory['inventory_rcp_dir']
OUTFILE = 'COMPLETE_FILE_LISTING'
out_file = os.path.join(wd, OUTFILE)

def get_file_family(volume):
	global _ff
	if not _ff.has_key(volume):
		v = vcc[volume]
		_ff[volume] = volume_family.extract_file_family(v['volume_family'])
	return _ff[volume]
	
def bfid2time(bfid):
	if bfid[-1] == 'L':
		bfid = bfid[:-1]
	# work around for 'DMS'
	if bfid[:3] == 'DMS':
		bfid = bfid[3:]
	elif not bfid[0] in string.digits:
		bfid = bfid[4:]
	# deal with year 2021 issue
	bfid = bfid[:-5]
	t = string.atoi(bfid)
	if t > 1600000000:
		t = t - 619315199
	bfid = `t`
	return bfid
	# try:
	#	t = string.atoi(bfid)
	# except:
	#	t = -1
	# return t

def getinfo(v):
	fc = string.split(v, '\012')
	if fc[2] == 'p2':
		size = fc[3][1:]
	else:
		size = 'unknown'

	volume = 'unknown'
	deleted = 'U'
	crc = 'unknown'
	pnfs_name0 = 'unknown'
	for i in range(len(fc)):
		if fc[i] == "sS'external_label'" or \
		   fc[i][1:] == "sS'external_label'":
			i = i + 2
			volume = fc[i][2:-1]
		if fc[i] == "sS'deleted'":
			i = i + 2
			if fc[i] == "S'yes'":
				deleted = 'D'
			elif fc[i] == "S'no'":
				deleted = 'A'
			else:
				deleted = 'U'
		if fc[i] == "sS'pnfs_name0'":
			i = i + 2
			pnfs_name0 = fc[i][2:-1]
		if fc[i] == "sS'complete_crc'":
			i = i + 2
			crc = fc[i][1:]
		if fc[i] == "sS'pnfs_name0'":
			i = i + 2
			pnfs_name0 = fc[i]
	# fix for crc = None
	if crc[0] == 's':
		crc = "None"
	if volume[-8:] == ".deleted":
		if deleted == 'D':
			deleted = 'R'
		elif deleted == 'A':
			deleted = 'E'
	return volume, size, deleted, crc, pnfs_name0

def cleanup():
	cmd = '/bin/rm -rf file* volume* STORAGE_GROUPS log* __* dbase.tar.gz'
	os.system(cmd)
	return


def usage():
	print 'usage: %s d0|stk|cdf [outfile(default "result")]'%(sys.argv[0])
	return

if __name__ == '__main__':

	'''
	if len(sys.argv) < 2:
		usage()
		sys.exit(0)

	# find the backup node according to the systems
	if sys.argv[1] == 'd0':
		backup_node = 'd0ensrv3.fnal.gov'
	elif sys.argv[1] == 'stk':
		backup_node = 'stkensrv3.fnal.gov'
	elif sys.argv[1] == 'cdf':
		backup_node = 'cdfensrv3.fnal.gov'
	else:
		usage()
		sys.exit(0)

	# remove the previous database files
	cleanup()
	backup_path = '/diska/enstore-backup'

	# list the backup directories
	cmd = 'rsh '+backup_node+' "ls -F -x -1 -t -d '+backup_path+'/dbase*"'
	fl = os.popen(cmd).readlines()
	backup_dir = string.strip(fl[0])

	# This is the trick to see if the backup is complete:
	# file.tar.gz will be written only after dbase.tar.gz is written
	cmd = 'rsh '+backup_node+' ls -F -x -1 -t -d '+backup_dir+'file.tar.gz'
	
	ff = os.popen4(cmd)[1].readlines()
	if string.find(ff[0], 'No such file') != -1:
		# if this one is not good, take the previous one
		backup_dir = string.strip(fl[1])
	backup_full_path = backup_node+':'+backup_dir+'dbase.tar.gz'
	print 'retriving backup', backup_full_path, '...',
	cmd = 'rcp '+backup_full_path+' .'
	os.system(cmd)
	print 'done'
	print 'unpacking database files ...',
	cmd = 'gunzip -c dbase.tar.gz| tar xvf -'
	null = os.popen(cmd).readlines()
	print 'done'
	print 'checking database files ...',
	cmd = 'db_recover -h .'
	# use popen4() to mask off stderr
	fd0, fd1 = os.popen4(cmd)
	null = fd1.readlines()
	print 'done'
	'''

	f = db.DbTable('file', db_path, '/tmp',[],0)
	vcc = db.DbTable('volume', db_path, '/tmp', [],0)
	out = open(out_file, 'w')
	c = f.newCursor().C
	k,v = c.first()
	count = 0
	print 'scanning ...'
	time0 = time.time()
	out.write('Listed at %s\n\n'%(time.ctime(time.time())))
	out.write('%-20s %-10s %-12s %14s %14s %s\n\n'%('BFID', 'VOLUME', 'FILE FAMILY', 'SIZE', 'CRC', 'PATH'))
	while k:
		count = count + 1
		volume, size, deleted, crc, pnfs_path = getinfo(v)
		if deleted == 'A':
			try:
				ff = get_file_family(volume)
				out.write('%-20s %-10s %-12s %14s %14s %s\n'%(k, volume, ff, size, crc, pnfs_path))
			except:
				print "None existing volume %s\n"%(volume)
		try:
			k,v = c.next()
		except KeyError:
			kl,vl = c.last()
			if kl == k and vl == v:
				k = None
			else:
				raise EOFError

		# if count > 10:
		#	break

		if count % 1000 == 0:
			t = time.time() - time0
			r = count*1.0/t
			print "%8d records processed at %7.2f records/second"%(count, r)
			# print count, 'records processed at', r, 'records/second'


	# for i in summary.keys():
	#	print i, summary[i][0], summary[i][1]

	out.close()
	print "moving %s to %s"%(out_file, dest_path)
	cmd = "enrcp %s %s"%(out_file, dest_path)
	os.system(cmd)
	f.close()
	vcc.close()
