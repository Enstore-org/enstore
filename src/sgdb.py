#!/usr/bin/env python

import os
import db

dbname = "storage_group"

class SGDb:

	# use shelve for persistent storage
	def __init__(self, dbhome):
		self.path = os.path.join(dbhome, dbname)
		self.dict = db.DbTable(dbname, dbhome, '/tmp', [], 0)

	# close
	def close(self):
		self.dict.close()

	# empty storage
	def clear(self):
		for i in self.dict.keys():
			del self.dict[i]

	# get the value
	def get_sg_counter(self, library, storage_group):
		k = library+'.'+storage_group
		if self.dict.has_key(k):
			return self.dict[k]
		else:
			return -1

	# set the value
	def set_sg_counter(self, library, storage_group, count=0):
		k = library+'.'+storage_group
		if type(count) != type(1):
			return -1
		else:
			if count < 0:
				count = 0
			self.dict[k] = count
			return count

	# increase sg counter
	def inc_sg_counter(self, library, storage_group, increment=1):
		k = library+'.'+storage_group
		if self.dict.has_key(k):
			self.dict[k] = self.dict[k] + increment
		else:
			self.dict[k] = increment
		return self.dict[k]

	# delete sg counter
	def delete_sg_counter(self, library, storage_group, forced=0):
		k = library+'.'+storage_group
		counter = self.get_sg_counter(library, storage_group)
		if counter == 0 or forced:
			del self.dict[k]

	# getitem
	def __getitem__(self, (library, storage_group)):
		k = library+'.'+storage_group
		try:
			return self.dict[k]
		except:
			return -1

	# setitem
	def __setitem__(self, (library, storage_group), val):
		k = library+'.'+storage_group
		if type(val) != type(1):
			return -1

		self.dict[k] = val
		return val

	def has_key(self, key):
		return self.dict.has_key(key)

	def keys(self):
		return self.dict.keys()

	def __len__(self):
		return self.dict.__len__()
