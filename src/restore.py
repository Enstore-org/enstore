#
# restore.py
#

import copy
import db
import regsub

cursor_open = 0

# similar to db.DbTable, without automatic journaling and backup up.

class DbTable(db.DbTable):

	# __init__() is almost the same as db.DbTable.__init__()
	# plus a "deletes" list containing the deleted keys

	def __init__(self, dbname, logc='', indlst=[]):
		self.journaling = 0
		self.dbname = dbname
		db.DbTable.__init__(self, dbname, logc, indlst)
		jfile = self.dbHome+"/"+dbname+".jou"
		self.deletes = []
		ji = open(jfile, 'r')
		while 1:
			l = ji.readline()
			if len(l) == 0:
				break
			if l[0:3] == 'del':
				k = regsub.split(l, "'")[1]
				if not self.jou.has_key(k):
					self.deletes.append(k)
		ji.close()

	def start_backup(self):
		pass		# mask out start_backup()

	def stop_backup(self):
		pass		# mask out stop_backup()

	def checkpoint(self):
		pass		# mask out checkpoint()

	# __setitem__() does not do automatic journaling

	def __setitem__(self,key,value):
		if 'db_flag' in value.keys(): del value['db_flag']
		if self.journaling:
			self.jou[key]=copy.deepcopy(value)
		for name in self.inx.keys():
			self.inx[name][value[name]]=key
		if cursor_open==1:
			self.cursor("update",key,value)
			return
		t=self.db.txn()
		self.db[(key,t)]=value
		t.commit()

	# __delitem__() does not do automatic journaling

	def __delitem__(self,key):
		if self.journaling:
			del self.jou[key]
		t=self.db.txn()
		del self.db[(key,t)]
		t.commit()
		for name in self.inx.keys():
			del self.inx[name][(key,value[name])]

	# cross_check() cross check journal dictionary and database

	def cross_check(self):

		error = 0

		# check if the items in db has the same value of that
		# in journal dictionary

		for i in self.jou.keys():
			if not self.has_key(i):
				print 'M> key('+i+') is not in database'
				error = error + 1
			elif self.jou[i] != self.__getitem__(i):
				print 'C> database and journal disagree on key('+i+')'
				print 'C>  journal['+i+'] =', self.jou[i]
				print 'C> database['+i+'] =', self.__getitem__(i)
				error = error + 1
		# check if the deleted items are still in db

		for i in self.deletes:
			if self.has_key(i):
				print 'D> database['+i+'] should be deleted'
				error = error + 1

		return error

	# fix_db() fix db according to journal dictionary

	def fix_db(self):

		error = 0

		# check if the items in db has the same value of that
		# in journal dictionary

		for i in self.jou.keys():
			if not self.has_key(i):
				# add it
				print 'M> key('+i+') is not in database'
				self.__setitem__(i, self.jou[i])
				error = error + 1
				print 'F> database['+i+'] =', self.jou[i]
			elif self.jou[i] != self.__getitem__(i):
				print 'C> database and journal disagree on key('+i+')'
				print 'C>  journal['+i+'] =', self.jou[i]
				print 'C> database['+i+'] =', self.__getitem__(i)
				self.__setitem__(i, self.jou[i])
				print 'F> database['+i+'] =', self.jou[i]
				error = error + 1
		# check if the deleted items are still in db

		for i in self.deletes:
			if self.has_key(i):
				print 'D> database['+i+'] should be deleted'
				self.__delitem__(i)
				print 'F> delete database['+i+']'
				error = error + 1

		return error

	def journaling_on(self):
		self.journaling = 1

	def journaling_off(self):
		self.journaling = 0
