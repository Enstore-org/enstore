#!/usr/bin/env python

import db
import sys
import time

def formatedf(file):
	bfid = file.get('bfid', None)
	complete_crc = file.get('complete_crc', 0)
	deleted = 'u'
	if file.has_key('deleted'):
		if file['deleted'] == 'no':
			deleted = 'n'
		elif file ['deleted'] == 'yes':
			deleted = 'y'
	drive = file.get('drive', None)
	external_label = file.get('external_label', None)
	location_cookie = file.get('location_cookie', None)
	pnfs_name0 = file.get('pnfs_name0', None)
	pnfsid = file.get('pnfsid', None)
	sanity_cookie_0 = file.get('sanity_cookie', (None, None))[0]
	sanity_cookie_1 = file.get('sanity_cookie', (None, None))[1]
	if sanity_cookie_0 == None:
		sanity_cookie_0 = 0
	if sanity_cookie_1 == None
		sanity_cookie_1 = 0
	size = file.get('size', 0)
	res = '%s\t%d\t%c\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d'% (
		bfid, complete_crc, deleted, drive, external_label,
		location_cookie, pnfs_name0, pnfsid,
		sanity_cookie_0, sanity_cookie_1, size)

	return res

if __name__ == '__main__':
	f = db.DbTable('file', '.', '/tmp', [], 0)
	c = f.newCursor()
	k, v = c.first()
	count = 0
	if len(sys.argv) > 1:
		outf = open(sys.argv[1], 'w')
	else:
		outf = open('db.dmp', 'w')

	last_time = time.time()
	while k:
		l = formatedf(v)
		outf.write(l+'\n')
		k, v = c.next()
		count = count + 1
		if count % 1000 == 0:
			time_now = time.time()
			print "%12d %14.2f records/sec"%(count,
				1000.0/(time_now - last_time))
			last_time = time_now
		# if count > 10:
		#	break
	outf.close()
	c.close()
	f.close()
