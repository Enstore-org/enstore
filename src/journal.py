###############################################################################
# src/$RCSfile$   $Revision$
#
"""Journaled dictionary

To summarize the interface (key is a string, data is an arbitrary
object):

	import journaled_dict
	d = JournaledDict(dict, jname)
			# make a new journaled dictionary, with journal
			# file jname. If jname exists, replay into dict.

	d[key] = data	# store data at key (overwrites old data if
			# using an existing key)
	data = d[key]	# retrieve data at key (raise KeyError if no
			# such key)
	del d[key]	# delete data stored at key (raises KeyError
			# if no such key)
	flag = d.has_key(key)	# true if the key exists
	list = d.keys()	# a list of all existing keys (slow!)
	d.close()	# close it

Dependent on the implementation, closing a persistent dictionary may
or may not be necessary to flush changes to disk.
"""


# system imports
#import time

# enstore imports
import Trace
import e_errors

class JournalDict:

	def __init__(self, dict, journalfile):
		self.dict = dict
		have_old_file = 1
		try:
			f = open(journalfile,"r")
		except IOError :
			have_old_file = 0
		if have_old_file :
			while 1:
				l = f.readline()
				if len(l) == 0 : break
				exec(l)
			f.close()
		self.jfile = open(journalfile,"a")

	def keys(self):
		return self.dict.keys()

	def __len__(self):
		return len(self.dict)

	def has_key(self, key):
		return self.dict.has_key(key)

	def __getitem__(self, key):
		return self.dict[key]


	def __setitem__(self, key, value):
		self.dict[key] = value
		j = "self.dict['%s'] = %s\n" % (key, value)
		self.jfile.write(j)
		self.jfile.flush()

	def __delitem__(self, key):
		# log the value of deleted item so that it may be
		# recovered later on
		v = self.dict[key]
		j = "del self.dict['%s'] # %s\n" % (key, v)
		self.jfile.write(j)
		self.jfile.flush()

	def close(self):
#		if hasattr(self.dict, 'close'):
#		self.dict.close()
		self.dict = {}

	def __del__(self):
		self.close()
		self.jfile.close()

def printdict(dict) :
	Trace.log(e_errors.INFO, repr(dict))
	for k in dict.keys():
	        msg = "%s %s" % (k, `dict[k]`)
		Trace.log(e_errors.INFO, repr(msg))
		Trace.log(e_errors.INFO, type(dict[k]))

if __name__ == "__main__" :

	jd1 = JournalDict({}, "test.jou")
	jd1["first"] = {'work' : 'test'}
	jd1["trick"] = {'work' : 'funkycharfs\n\'"'}
	jd1["second"] = {'work' : 'testmore'}
	del jd1["first"]
	del jd1

	jd2 = JournalDict({}, "test.jou")
	printdict(jd2)
	jd2["dog"] = {"more" : "more"}
	printdict(jd2)
