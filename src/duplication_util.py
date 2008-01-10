#!/usr/bin/env python

import configuration_client
import option
import e_errors
import file_clerk_client
import volume_clerk_client
import pg
import pnfs
import pprint

class DuplicationManager:
	def __init__(self, csc = None):
		self.good = True
		intf = option.Interface()

		# get configuration client
		if csc:
			self.csc = csc
		else:
			self.csc = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))

		self.fcc = file_clerk_client.FileClient(self.csc)
		self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)

		dbinfo = self.csc.get('database')
		if dbinfo['status'][0] != e_errors.OK:
			self.good = False
			return

		# database connection
		try:
			self.db = pg.DB(host = dbinfo['dbhost'],
					dbname = dbinfo['dbname'],
					port = dbinfo['dbport'],
					user = dbinfo['dbuser'])
		except:
			self.good = False
			return

	# make_duplicate(bfid1, bfid2): make bfid2 a copy of bfid1
	def make_duplicate(self, bfid1, bfid2):
		# make sure both are in file database
		f1 = self.fcc.bfid_info(bfid1)
		if f1['status'][0] != e_errors.OK:
			return "no such file %s"%(bfid1)
		f2 = self.fcc.bfid_info(bfid2)
		if f2['status'][0] != e_errors.OK:
			return "no such file %s"%(bfid2)

		# check if f1 and f2 are the same file
		for i in ['complete_crc', 'pnfs_name0',
			'pnfsid', 'sanity_cookie', 'size']:
			if f1[i] != f2[i]:
				return "different %s: (%s, %s)"%(i, `f1[i]`, `f2[i]`)

		# check if f1 and f2 are already copies
		q = "select * from file_copies_map where bfid = '%s' and alt_bfid = '%s';"%(bfid1, bfid2)
		res = self.db.query(q).getresult()
		if res:
			return "(%s, %s) are already copies"%(bfid1, bfid2)

		# get pnfs entry
		try:
			pnfs_path = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(f1['pnfsid'])
			if type(pnfs_path) == type([]):
				pnfs_path = pnfs_path[0]
		except:
			return "not a valid pnfs file: %s"%(f1['pnfsid'])

		pf = pnfs.File(pnfs_path)

		# check for consistency
		if long(pf.complete_crc) != f1['complete_crc']:
			return "wrong crc: pnfs(%s), file(%s)"%(`pf.complete_crc`, `f1['complete_crc']`)
		if pf.path != pnfs.get_abs_pnfs_path(f1['pnfs_name0']):
			return "wrong pnfs_path: pnfs(%s), file(%s)"%(pf.path, f1['pnfs_name0'])
		if pf.bfid != f1['bfid'] and pf.bfid != f2['bfid']:
			return "wrong bfids: pnfs(%s), f1(%s), f2(%s)"%(pf.bfid, f1['bfid'], f2['bfid'])
		if long(pf.size) != f1['size']:
			return "wrong size: pnfs(%s), file(%s))"%(pf.size, `f1['size']`)
		if pf.pnfsid != f1['pnfsid']:
			return "wrong pnfsids: pnfs(%s), file(%s)"%(pf.pnfsid, f1['pnfsid'])

		# NEED TO CHECK SOMETHING ELSE

		# undelete if it is necessary
		if f1['deleted'] == 'yes':
			res = self.fcc.modify({'bfid':bfid1, 'deleted':'no'})
		if res['status'][0] != e_errors.OK:
			return "failed to undelete file %s"%(bfid1)
		if f2['deleted'] == 'yes':
			res = self.fcc.modify({'bfid':bfid2, 'deleted':'no'})
		if res['status'][0] != e_errors.OK:
			return "failed to undelete file %s"%(bfid2)

		# register
		q = "insert into file_copies_map (bfid, alt_bfid) values ('%s', '%s');"%(bfid1, bfid2)
		try:
			res = self.db.query(q)
		except:
			return "failed to register copy (%s, %s)"%(bfid1, bfid2)

		# set pnfs entry
		if pf.bfid != bfid1:
			pf.bfid = bfid1
			pf.update()

		return

# make_original_as_duplicate(vol) -- make all files on the original volume
#	as a duplicate(copy) of the migrated files.

def make_original_as_duplicate(vol):
	dm = DuplicationManager()
	v = dm.vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		print "ERROR: no such volume '%s'"%(vol)
		return
	# make sure it is a migrated volume
	if v['system_inhibit'][1] != "migrated":
		print "ERROR: %s is not a migrated volume."%(vol)
		return
	q = "select dst_bfid, src_bfid from migration m, file f, volume v where f.volume = v.id and v.label = '%s' and f.bfid = m.src_bfid;"%(vol)
	res = dm.db.query(q).getresult()
	for i in res:
		print "dm.make_duplicate(%s, %s)"%(`i[0]`, `i[1]`)
		dm.make_duplicate(i[0], i[1])


# make_migrated_as_duplicate(vol) -- make all files on the migrated-to
#	volume as a duplicate(copy) of the original files.

def make_migrated_as_duplicate(vol):
	dm = DuplicationManager()
	v = dm.vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		print "ERROR: no such volume '%s'"%(vol)
		return
	q = "select src_bfid, dst_bfid from migration m, file f, volume v where f.volume = v.id and v.label = '%s' and f.bfid = m.dst_bfid;"%(vol)
	res = dm.db.query(q).getresult()
	for i in res:
		print "dm.make_duplicate(%s, %s)"%(`i[0]`, `i[1]`)
		dm.make_duplicate(i[0], i[1])


