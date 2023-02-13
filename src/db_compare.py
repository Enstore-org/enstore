#!/usr/bin/env python
# db_compare -- compare two databases
#

import os
import sys
import restore

def db_compare(db1, dbHome1, db2, dbHome2):
	error = 0
	if dbHome1 != dbHome2:
		l1 = os.path.join(dbHome1, db1)
		l2 = os.path.join(dbHome2, db2)
	else:
		l1 = db1
		l2 = db2

	print 'opening', l1, '...',
	try:
		d1 = restore.DbTable(db1, dbHome1, "/tmp", [])
	except:
		print 'failed ... aborted'
	print 'ok'
	print 'opening', l2, '...',
	try:
		d2 = restore.DbTable(db2, dbHome2, "/tmp", [])
	except:
		print 'failed ... aborted'
	print 'ok'
	c = d1.newCursor()
	print "checking every record in", l1, "against", l2, "...",
	key, val = c.first()
	count = 0
	while key:
		if not d2.has_key(key):
			if error == 0:
				print	# new line
			print 'M> key "'+key+'" is in '+l1+' but not in '+l2
			error = error + 1
		elif restore.ddiff(val, d2[key]):
			if error == 0:
				print	# new line
			print 'D> disagree on key "'+key+'"'
			print ' '+l1+'["+key+"] =', repr(val)
			print ' '+l2+'["+key+"] =', repr(d2[key])
			error = error + 1
		key, val = c.next()
		count = count + 1
		if count % 100 == 0:
			print '.',
	c.close()
	if error == 0:
		print 'done'
	else:
		print error, 'difference(s)'
	c = d2.newCursor()
	key, val = c.first()
	error2 = 0
	count = 0
	print "checking every record in", l2, "against", l1, "...",
	while key:
		if not d1.has_key(key):
			if error2 == 0:
				print
			print 'M> key "'+key+'" is in '+l2+' but not in '+l1
			error2 = error2 + 1
		key, val = c.next()
		count = count + 1
		if count % 100 == 0:
			print '.',
	c.close()

	if error2 == 0:
		print 'done'
	else:
		print error2, 'difference(s)'

	error = error + error2
	if not error:
		print 'Done> '+l1+' and '+l2+' are the same'
	else:
		print 'Done> ', error, 'difference(s)'
	d1.close()
	d2.close()
	return error

if __name__ == "__main__":   # pragma: no cover

	l = len(sys.argv)

	if l > 3:
		db1 = sys.argv[1]
		dbHome1 = sys.argv[2]
		db2 = sys.argv[3]
		if l > 4:
			dbHome2 = sys.argv[4]
		else:
			dbHome2 = sys.argv[2]
	else:
		print "usage: %s db1 dbHome1 db2 dbHome2"%(sys.argv[0])

	error = db_compare(db1, dbHome1, db2, dbHome2)
