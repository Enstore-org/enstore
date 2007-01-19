#!/usr/bin/env python
"""
The table is:

library storage_group count
"""
import pg
import os

table = "sg_count"

class SGDb:

	# use shelve for persistent storage
	def __init__(self, db):
		# two ways of initialize SGDb
		# a tuple of (host, dbname) or an instantiated db
		if type(db) == type(()):
			(host, dbname) = db
			self.db = pg.DB(host=host, dbname=dbname)
		else:
			self.db = db
		self.table = table

	# close
	def close(self):
		# nothing needs to be done
		pass

	# empty storage
	def clear(self):
		q = "delete from %s;"%(self.table)
		self.db.query(q)

	# rebuild_sg_count
	def rebuild_sg_count(self):
		self.clear()
		q = "insert into %s (library, storage_group, count) select library, storage_group, count(*) from volume where not label like '%%.deleted' group by library, storage_group;"%(self.table)
		self.db.query(q)

	# get the value
	def get_sg_counter(self, library, storage_group):
		q = "select count from %s where library = '%s' and storage_group = '%s';"%(self.table, library, storage_group)
		try:
			res = self.db.query(q).getresult()[0][0]
			return res
		except:
			return -1

	# set the value
	def set_sg_counter(self, library, storage_group, count=0):
		if count < 0:
			count = 0
		p = self.get_sg_counter(library, storage_group)
		if p < 0:
			q = "insert into %s (library, storage_group, count) values('%s', '%s', %d);"%(self.table, library, storage_group, count)
			self.db.query(q)
		else:
			q = "update %s set count = %d where library = '%s' and storage_group = '%s';"%(self.table, count, library, storage_group)
			self.db.query(q)
		return count

	# increase sg counter
	def inc_sg_counter(self, library, storage_group, increment=1):
		try:
			count = self.get_sg_counter(library, storage_group)
			
			q = "update %s set count = %d where library = '%s' and storage_group = '%s';"%(self.table, count+increment, library, storage_group)
			self.db.query(q)
		except:
			q = "insert into %s (library, storage_group, count) values('%s', '%s', 1);"%(self.table, library, storage_group)
			self.db.query(q)
			return 1

	# delete sg counter
	def delete_sg_counter(self, library, storage_group, forced=0):
		q = "delete from %s where library = '%s' and storage_group = '%s';"%(self.table, library, storage_group)
		try:
			self.db.query(q)
		except:
			pass

	# getitem
	def __getitem__(self, (library, storage_group)):
		return self.get_sg_counter(library, storage_group)

	# setitem
	def __setitem__(self, (library, storage_group), val):
		return self.set_sg_counter(library, storage_group, val)

	def has_key(self, (library, storage_group)):
		if self.get_sg_counter(library, storage_group) < 0:
			return 0
		else:
			return 1

	def keys(self):
		q = "select library, storage_group from %s;"%(self.table)
		r = self.db.query(q).dictresult()
		keys = []
		for i in r:
			lib = i['library']
			sg = i['storage_group']
			keys.append((lib, sg))
		return keys

	def list_sg_count(self):
		q = "select library || '.' || storage_group, count from sg_count;"
		res = {}
		res2 = self.db.query(q).getresult()
		for i in res2:
			res[i[0]] = i[1]
		return res

	def __len__(self):
		return self.db.query('select * from %s;'%(self.table)).ntuples()
