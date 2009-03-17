#!/usr/bin/env python

import pg
import sys
import errno

import configuration_client
import e_errors
import file_clerk_client
import volume_clerk_client
import pnfs
import find_pnfs_file
import Trace
import enstore_functions2

class DuplicationManager:
	def __init__(self, csc = None):
		self.good = True

		# get configuration client
		if csc:
			self.csc = csc
		else:
			csc_addr = (enstore_functions2.default_host(),
				    enstore_functions2.default_port())
			self.csc = configuration_client.ConfigurationClient(
				csc_addr)

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
		# check the other way
		q = "select * from file_copies_map where bfid = '%s' and alt_bfid = '%s';"%(bfid2, bfid1)
		res = self.db.query(q).getresult()
		if res:
			return "(%s, %s) are already copies"%(bfid1, bfid2)

		# get pnfs entry
		try:
			# get the real path			
			#pnfs_path = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(f1['pnfsid'])
			pnfs_path = find_pnfs_file.find_pnfsid_path(
				f1['pnfsid'], f1['bfid'], file_record = f1)
		except (KeyboardInterrupt, SystemExit):
			raise (sys.exc_info()[0],
			       sys.exc_info()[1],
			       sys.exc_info()[2])
		except:
			return "not a valid pnfs file: %s"%(f1['pnfsid'])

		if type(pnfs_path) == type([]):
				pnfs_path = pnfs_path[0]

		pf = pnfs.File(pnfs_path)

		# check for consistency
		if long(pf.complete_crc) != f1['complete_crc']:
			return "wrong crc: pnfs(%s), file(%s)"%(`pf.complete_crc`, `f1['complete_crc']`)
		# npp means Normalized Pnfs Path.
		npp = pnfs.get_enstore_fs_path(pnfs.get_abs_pnfs_path(pf.path))
		# ndp means Normalized Database Path.
		ndp = pnfs.get_enstore_fs_path(pnfs.get_abs_pnfs_path(f1['pnfs_name0']))
		if npp != ndp:
			return "wrong pnfs_path: pnfs(%s), file(%s)" \
			       % (pf.path, f1['pnfs_name0'])
		if pf.bfid != f1['bfid'] and pf.bfid != f2['bfid']:
			return "wrong bfids: pnfs(%s), f1(%s), f2(%s)" \
			       % (pf.bfid, f1['bfid'], f2['bfid'])
		if long(pf.size) != f1['size']:
			return "wrong size: pnfs(%s), file(%s))" \
			       % (pf.size, `f1['size']`)
		if pf.pnfs_id != f1['pnfsid']:
			return "wrong pnfsids: pnfs(%s), file(%s)" \
			       % (pf.pnfs_id, f1['pnfsid'])

		# NEED TO CHECK SOMETHING ELSE

		# undelete if it is necessary
		if f1['deleted'] == 'yes':
			res = self.fcc.modify({'bfid':bfid1, 'deleted':'no'})
			if res['status'][0] != e_errors.OK:
				return "failed to undelete file %s" % (bfid1,)
		if f2['deleted'] == 'yes':
			res = self.fcc.modify({'bfid':bfid2, 'deleted':'no'})
			if res['status'][0] != e_errors.OK:
				return "failed to undelete file %s" % (bfid2,)

		# register
		q = "insert into file_copies_map (bfid, alt_bfid) values ('%s', '%s');"%(bfid1, bfid2)
		try:
			res = self.db.query(q)
		except:
			return "failed to register copy (%s, %s)" \
			       % (bfid1, bfid2)

		# set pnfs entry
		if pf.bfid != bfid1:
			pf.bfid = bfid1
			pf.volume = f1['external_label']
			pf.update()

		return

	# swap original with its first copy
	def swap_original_and_copy(self, bfid):
		# get file information
		f = self.fcc.bfid_info(bfid)
		if not e_errors.is_ok(f['status']):
			return "no such file %s" % (bfid,)
		# get the copy information
		q = "select bfid, alt_bfid from file_copies_map where bfid = '%s';" % (bfid,)
		res = self.db.query(q).getresult()
		if not res:
			return "%s does not have a copy" % (bfid,)
		copy = res[0][1]

		f2 = self.fcc.bfid_info(copy)
		if not e_errors.is_ok(f2['status']):
			return "no such file %s" % (copy,)

		# get pnfs entry - pnfsids should be equal - check both files
		pairs_to_search = [(f['pnfsid'], f['bfid']),
				   (f['pnfsid'], f2['bfid'])]
		for search_pnfsid, search_bfid in pairs_to_search:
			try:
				# get the real path			
				pnfs_path = find_pnfs_file.find_pnfsid_path(
					f['pnfsid'], f['bfid'], file_record = f)
			except (KeyboardInterrupt, SystemExit):
				raise (sys.exc_info()[0],
				       sys.exc_info()[1],
				       sys.exc_info()[2])
			except (OSError, IOError), msg:
				if msg.errno == errno.EEXIST and \
				   msg.args[1].find("replaced with") > -1:
					pnfs_path = msg.filename
					break
				else:
					continue
			except:
				exc_type, exc_value, exc_tb = sys.exc_info()
				Trace.handle_error(exc_type, exc_value, exc_tb)
				del exc_tb #avoid resource leaks
				return "%s %s %s %s is not a valid pnfs file" \
				       ": (%s, %s)" \
				       % (f['external_label'], f['bfid'],
					  f['location_cookie'],
					  f['pnfsid'], str(exc_type),
					  str(exc_value))

			break #Sucess in finding one of the files.
		else:
			return "not a valid pnfs file: %s" % (f['pnfsid'],)

		if type(pnfs_path) == type([]):
			pnfs_path = pnfs_path[0]

		pf = pnfs.File(pnfs_path)

		# now swap bfid and copy
		# make it a complete transaction
		self.db.query('begin transaction;')
		try:
			q = "update file_copies_map set bfid = '%s' where bfid = '%s';"%(copy, bfid)
			self.db.query(q)
			q = "update file_copies_map set alt_bfid = '%s' where alt_bfid = '%s';"%(bfid, copy)
			self.db.query(q)

			# set pnfs entry
			if pf.bfid != copy:
				pf.bfid = copy
				pf.volume = f2['external_label']
				pf.location_cookie = f2['location_cookie']
				pf.drive = f2['drive']
				pf.update()
		except:
			self.db.query('rollback transaction;')
			return "failed to swap %s and %s: %s" % \
			       (bfid, copy, str(sys.exc_info()[1]))
		self.db.query('commit transaction;')

		return

	# is_primary(bfid) check if bfid is a primary
	def is_primary(self, bfid):
		q = "select bfid from file_copies_map where bfid = '%s';"%(bfid)
		res = self.db.query(q).getresult()
		if res:
			return True
		return False

	# is_copy(bfid) check if bfid is a copy
	def is_copy(self, bfid):
		q = "select alt_bfid from file_copies_map where alt_bfid = '%s';"%(bfid)
		res = self.db.query(q).getresult()
		if res:
			return True
		return False
			
# make_original_as_duplicate(vol) -- make all files on the original volume
#	as a duplicate(copy) of the migrated files.

def make_original_as_duplicate(volume):
	rtn_code = 0
	
	dm = DuplicationManager()
	if type(volume) == type (""):
		volume = [volume]
	for vol in volume:
		print "making original %s as copy of the migrated files ..."%(vol)
		v = dm.vcc.inquire_vol(vol)
		if v['status'][0] != e_errors.OK:
			print "ERROR: no such volume '%s'"%(vol)
			return
		# make sure it is a migrated volume
		if v['system_inhibit'][1] not in ("migrated", "cloned"):
			print "ERROR: %s is not a migrated volume."%(vol)
			return
		q = "select dst_bfid, src_bfid from migration m, file f, volume v where f.volume = v.id and v.label = '%s' and f.bfid = m.src_bfid and not m.closed is null;" % (vol,)
		res = dm.db.query(q).getresult()
		for i in res:
			print "make_duplicate(%s, %s) ..."%(`i[0]`, `i[1]`),
			res = dm.make_duplicate(i[0], i[1])
			if res:
				print res, "... ERROR"
				rtn_code = 1
			else:
				print "OK"

		if not rtn_code: #No errors.
			#Update the system_inhibit.
			q = "update volume set system_inhibit_1 = 'duplicated'  where label = '%s';" % (vol,)
			try:
				dm.db.query(q)
			except:
				print "Unable to set system_inhibit.", " ...ERROR"
				rtn_code = 1

	return rtn_code


# make_migrated_as_duplicate(vol) -- make all files on the migrated-to
#	volume as a duplicate(copy) of the original files.

def make_migrated_as_duplicate(volume):
	rtn_code = 0
	
	dm = DuplicationManager()
	if type(volume) == type (""):
		volume = [volume]
	for vol in volume:
		print "making migrated %s as copy of its original files ..."%(vol)
		v = dm.vcc.inquire_vol(vol)
		if v['status'][0] != e_errors.OK:
			print "ERROR: no such volume '%s'"%(vol)
			return
		q = "select src_bfid, dst_bfid from migration m, file f, volume v where f.volume = v.id and v.label = '%s' and f.bfid = m.dst_bfid and not m.closed is null;"%(vol)
		res = dm.db.query(q).getresult()
		for i in res:
			print "make_duplicate(%s, %s) ..."%(`i[0]`, `i[1]`),
			res = dm.make_duplicate(i[0], i[1])
			if res:
				print res, "... ERROR"
				rtn_code = 1
			else:
				print "OK"

	return rtn_code
