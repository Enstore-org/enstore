# @(#) $Id$
# $Log$
# Revision 1.5  1999/11/22 20:23:49  huangch
# check for None before closeing a cursor
#
# Revision 1.4  1999/11/12 18:29:42  huangch
# remove the KeyError in the trail blocks in LibtpCursor methods since it won't happen
#
# Revision 1.3  1999/09/29 23:49:29  huangch
# add support for duplicated key, cursor, and join
#
# Revision 1.2  1999/08/23 21:02:46  huangch
# add status and __len__
#
# Revision 1.1.1.1  1998/09/11 16:54:52  huangch
# ivm's initial version of libtppy -- python interface to libtp
#
# Revision 1.5  1998/05/13  20:49:39  ivm
# Added cursor::delete() method
#
# Revision 1.4  1998/05/12  17:26:39  ivm
# Fixed update() method
#
# Revision 1.3  1998/05/12  16:53:08  ivm
# Added update() method to cursor class
#
# Revision 1.2  1998/05/12  16:45:36  ivm
# Added upd() method
#
#

"""Manage shelves of pickled objects.

A "shelf" is a persistent, dictionary-like object.  The difference
with dbm databases is that the values (not the keys!) in a shelf can
be essentially arbitrary Python objects -- anything that the "pickle"
module can handle.  This includes most class instances, recursive data
types, and objects containing lots of shared sub-objects.  The keys
are ordinary strings.

To summarize the interface (key is a string, data is an arbitrary
object):

	import shelve
	d = libtpshelve.open(filename) # open, with (g)dbm filename -- no suffix

	d[key] = data	# store data at key (overwrites old data if
			# using an existing key)
	data = d[key]	# retrieve data at key (raise KeyError if no
			# such key)
	del d[key]	# delete data stored at key (raises KeyError
			# if no such key)
	flag = d.has_key(key)	# true if the key exists

	d.close()	# close it

Dependent on the implementation, closing a persistent dictionary may
or may not be necessary to flush changes to disk.
"""


import cPickle

_CheckpointLockName = "__ckp_lock__"

class	LibtpTxn:
	def	__init__(self, db):
		self.Lock = db.lock(_CheckpointLockName,"r")
		self.Txn = db.txn()

	def	commit(self):
		self.Txn.commit()
		self.Lock.release()

	def	abort(self):
		self.Txn.abort()
		self.Lock.release()

	def	__del__(self):
		self.Lock.release()

class Shelf:
	"""Base class for shelf implementations.

	This is initialized with a dictionary-like object.
	See the module's __doc__ string for an overview of the interface.
	"""

	def __init__(self, dict):
		self.dict = dict
	
	def keys(self):
		return self.dict.keys()
	
	def __len__(self):
		return len(self.dict)
	
	def has_key(self, key):
		return self.dict.has_key(key)
	
	def __getitem__(self, key):
		#print '__getitem__: Key=\'%s\'' % `key`
		val = self.dict[key]
		#print '__getitem__: Value=\'%s\'' % `val`
		unp = self._unpack(val)
		#print '__getitem__: Unpacked \'%s\'' % unp
		return unp
	
	def __setitem__(self, key, value):
		self.dict[key] = self._pack(value)
	
	def __delitem__(self, key):
		del self.dict[key]
	
	def close(self):
		#print 'shelve.close()'
		try:
			if self.dict:
				#print 'shelve.close(): dict.close()'
				self.dict.close()
		except:
			pass
		self.dict = 0

	#def __del__(self):
		#print 'Shelve.del()'
		#self.close()

	def sync(self):
		if hasattr(self.dict, 'sync'):
			self.dict.sync()

	def	_pack(self, x):
                return cPickle.dumps(x)

	def	_unpack(self, x):
            try:
                r= cPickle.loads(x)
            except:
                print "Pickle load error", repr(x)
                raise
            return r
        
	def status(self):
		return self.dict.status()

class	LibtpCursor:    
	"""Python Pickler wrap-up of LIBTP cursor
	"""

	def __init__(self, shlv, txn = None):
		self.Shelve = shlv
		if txn == None: 	self.C = shlv.dict.cursor()
		else:			self.C = shlv.dict.cursor(txn)
		self.Key = None
		self.Value = None

	def get(self):
		return self.Key, self.Value

	def setRange(self, key):
	     	try:	
			key, value = self.C.setRange(key)
			self.Key, self.Value = key, self.Shelve._unpack(value)
			return key, self.Value
		except:
			self.Key, self.Value = None, None
			return None, None

	def set(self, key, value = None):
	     	try:	
			if value == None:
				key, value = self.C.set(key)
			else:
				key, value = self.C.set(key,
					self.Shelve._pack(value))
			self.Key, self.Value = key, self.Shelve._unpack(value)
			return key, self.Value
		except:
			self.Key, self.Value = None, None
			return None, None

	def next(self):
	     	try:	
			key, value = self.C.next()
			self.Key, self.Value = key, self.Shelve._unpack(value)
			return key, self.Value
		except KeyError:
			self.Key, self.Value = None, None
			return None, None

	def nextDup(self):
	     	try:	
			key, value = self.C.nextDup()
			self.Key, self.Value = key, self.Shelve._unpack(value)
			return key, self.Value
		except:
			self.Key, self.Value = None, None
			return None, None

	def previous(self):
	     	try:	
			key, value = self.C.prev()
			self.Key, self.Value = key, self.Shelve._unpack(value)
			return key, self.Value
		except:
			self.Key, self.Value = None, None
			return None, None

	def first(self):
	     	try:	
			key, value = self.C.first()
			self.Key, self.Value = key, self.Shelve._unpack(value)
			return key, self.Value
		except:
			self.Key, self.Value = None, None
			return None, None

	def last(self):
	     	try:	
			key, value = self.C.last()
			self.Key, self.Value = key, self.Shelve._unpack(value)
			return key, self.Value
		except:
			self.Key, self.Value = None, None
			return None, None

	def current(self):
	     	try:	
			key, value = self.C.current()
			self.Key, self.Value = key, self.Shelve._unpack(value)
			return key, self.Value
		except:
			self.Key, self.Value = None, None
			return None, None

	def close(self):
		if self.C != None:
			self.C.close()
			self.C = None
		self.Key = None
		self.Value = None

	def	__len__(self):
		if self.Key == None:
			return 0
		else:
			return 1

	#def __del__(self):
	#	#print 'Cursor.del()'
	#	if self.C:
	#		#print 'helve.C.close()'
	#		self.C.close()

	def update(self, value):
		return self.C.update(self.Shelve._pack(value))

	def delete(self):
		return self.C.delete()

class LibtpShelf(Shelf):
	"""Shelf implementation using the "BSD" db V2.3 (LIBTP) interface.

	This adds method cursor() to create DB cursor object
	wrapped up in Python Pickler inetrface

	The actual database must be opened using one of the "bsddb"
	modules "open" routines (i.e. bsddb.hashopen, bsddb.btopen or
	bsddb.rnopen) and passed to the constructor.

	See the module's __doc__ string for an overview of the interface.
	"""

	def __init__(self, dict):
	    Shelf.__init__(self, dict)

	def cursor(self, txn=None):
		return LibtpCursor(self, txn)

	def txn(self, parent=None):
		if parent == None:	return self.dict.txn()
		else:			return self.dict.txn(parent)

	def lock(self, name, mode):
		return self.dict.lock(name, mode)

def open(env, filename, flag='c', prot=0666, type='hash', **opts):
	"""Open a persistent dictionary for reading and writing.

	Argument is the filename for the database.
	See the module's __doc__ string for an overview of the interface.
	"""

	if type == 'hash':
		return LibtpShelf(env.hashopen(filename, flag, prot, opts))
	elif type == 'btree':
		return LibtpShelf(env.btreeopen(filename, flag, prot, opts))
	elif type == 'rn':
		return LibtpShelf(env.rnopen(filename, flag, prot, opts))

def env(home, opts1=None, **opts2):
	import libtp
	if opts1 == None:
		return libtp.init(home, opts2)
	else:
		return libtp.init(home, opts1)

