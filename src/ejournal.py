#!/usr/bin/env python

import time
import os

journal_size = 1000

class Journal:
	def __init__(self, journalfile, dict={}, mode=0, limit=journal_size):
		self.dict = dict
		if journalfile[-4:] == ".jou":
			self.journalfile = journalfile
		else:
			self.journalfile = journalfile+".jou"
		if mode == 0:
			self.load()
		self.jfile = open(self.journalfile, "a")
		self.count = 0
		self.limit = limit

	def load(self):
		if os.access(self.journalfile, os.R_OK):
			f = open(self.journalfile, "r")
			l = f.readline()
			while l:
				try:
					exec(l)
				except:
					pass
				l = f.readline()
			f.close()

	def keys(self):
		return self.dict.keys()

	def __len__(self):
		return self.dict.__len__()

	def has_key(self, key):
		return self.dict.has_key(key)

	def __getitem__(self, key):
		return self.dict[key]

	def __setitem__(self, key, value):
		self.dict[key] = value
		j = "self.dict['%s'] = %s\n" % (key, value)
		self.jfile.write(j)
		self.jfile.flush()
		self.count = self.count + 1
		if self.limit and self.count >= self.limit:
			self.checkpoint()

	def __delitem__(self, key):
		v = self.dict[key]
		j = "del self.dict['%s'] # %s\n" % (key, v)
		self.jfile.write(j)
		self.jfile.flush()
		del self.dict[key]
		self.count = self.count + 1
		if self.limit and self.count >= self.limit:
			self.checkpoint()

	def close(self):
		self.jfile.close()
		self.dict = {}

	def __del__(self):
		self.close()

	def list(self):
		for i in self.keys():
			print "self.dict['%s'] = %s"%(i, `self.dict[i]`)

	def __repr__(self):
		return `self.dict`

	def checkpoint(self):
		self.jfile.close()
		os.rename(self.journalfile, self.journalfile+'.'+repr(time.time()))
		self.jfile = open(self.journalfile, "w")
		self.dict = {}
		self.count = 0

if __name__ == '__main__':
	jou = Journal('test.jou')
	for i in range(20000):
		jou[`i`] = {'count': i}
