"""
edb.py -- a replacement for db.py
edb.py -- a shelve wrapper to a postgreSQL database

class DbTable is the base class that assumes a table/view with a single
column primary key. The primary key is used to access each record and
the record is returned in a dictionary. DbTable has no specific
knowledge about the schema in the database. It is up to the derived
class to specify the schema related information.

Each derived class has to provide the following:

	self.retrive_query   -- string of select statement template
	self.insert_query    -- string of insert statement template
	self.update_query    -- string of update statement template
	self.delete_query    -- string of delete statement template
        (see the code for examples)

	self.import_format() -- map input dictionary to database format
	self.export_format() -- map database format to output dictionary
"""

import time
import string
import types
import pg
import ejournal
import os

# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time 
def timestamp2time(s):
	return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"
def time2timestamp(t):
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


# from a dictionary, get field name and values
def get_fields_and_values(s):
	keys = s.keys()
	fields = ""
	values = ""
	for i in keys[:-1]:
		fields = fields+i+', '
		values = values+str_value(s[i])+', '
	fields = fields+keys[-1]
	values = values+str_value(s[keys[-1]])
	return fields, values

# from two dictionaries, get the difference based on the second one
def diff_fields_and_values(s1, s2):
	d = {}
	key1 = s1.keys()
	for k in s2.keys():
		if k in key1 and s1[k] != s2[k]:
			d[k] = s2[k]
	return d

# str_value() -- properly represent the value in string
def str_value(v):
	t = type(v)

	if t == types.NoneType:
		return 'NULL'
	elif t == types.LongType:
		return `v`[:-1]
	elif t == types.TupleType:	# storage procedure
		args = ''
		for i in v[1:-1]:
			args = args + str_value(i) + ','
		args = args + str_value(v[-1])
		return v[0]+'('+args+')'
	else:
		return `v`

# This is the base DbTable class
#
# All derived classes need to provide the following:
#
# self.retrieve_query -- the query to take information out of database
# self.exprot_format(self, s) -- translate database output to external format

class DbTable:
	def __init__(self, host, database, table, pkey, jouHome ='.', auto_journal=0):
		self.host = host
		self.database = database
		self.table = table
		self.pkey = pkey
		self.auto_journal = auto_journal

		if self.auto_journal:
			self.jou = ejournal.Journal(os.path.join(
					jouHome, self.table))
		else:
			self.jou = None

		self.retrieve_query = "select * from "+self.table+" where "+self.pkey+" = '%s';"
		self.insert_query = "insert into "+self.table+" (%s) values (%s);"
		self.update_query = "update "+self.table+" set %s where "+self.pkey+" = '%s';"
		self.delete_query = "delete from "+self.table+" where "+self.pkey+" = '%s';"

		self.db = pg.DB(host=self.host, dbname=self.database)

	# translate database output to external format
	def export_format(self, s):
		return s

	# translate external format to database internal format
	def import_format(self, s):
		return s

	def __getitem__(self, key):
		res = self.db.query(self.retrieve_query%(key)).dictresult()
		if len(res) == 0:
			return None
		else:
			return self.export_format(res[0])

	def __setitem__(self, key, value):
		if self.auto_journal:
			self.jou[key] = value
		v1 = self.import_format(value)
		# figure out whether this is an insert or an update
		res = self.db.query(self.retrieve_query%(key)).dictresult()

		if len(res) == 0:	# insert
			cmd = self.insert_query%get_fields_and_values(v1)
			print cmd
			res = self.db.query(cmd)
		else:			# update
			d = diff_fields_and_values(res[0], v1)
			if d:	# only if there is any difference
				setstmt = ''
				for i in d.keys():
					setstmt = setstmt + i + ' = ' + str_value(d[i]) + ', '
				setstmt = setstmt[:-2]	# get rid of last ', '
				cmd = self.update_query%(setstmt, key)
				print cmd
				res = self.db.query(cmd)

	def __delitem__(self, key):
		if self.auto_journal:
			if not self.jou.has_key(key):
				self.jou[key] = self.__getitem__(key)
			del self.jou[key]
		res = self.db.query(self.delete_query%(key))
			

	def keys(self):
		res = self.db.query('select %s from %s;'%(self.pkey, self.table)).getresult()
		keys = []
		for i in res:
			keys.append(i[0])
		return keys

	def has_key(self, key):
		if self.__getitem__(key):
			return 1
		else:
			return 0

	def __len__(self):
		return self.db.query('select %s from %s;'%(self.pkey, self.table)).ntuples()

class FileDB(DbTable):
	def __init__(self, host='localhost', database='enstore'):
		DbTable.__init__(self, host, database, table='file', pkey='bfid')
		self.retrieve_query = "\
        		select \
                		bfid, crc, deleted, drive, \
				volume.label, location_cookie, pnfs_path, \
                		pnfs_id, sanity_cookie_0, sanity_cookie_1, size \
        		from file, volume \
        		where \
                		file.volume = volume.id and \
                		bfid = '%s';"
		self.insert_query = "\
			insert into file (%s) values (%s);"

		self.update_query = "\
			update file set %s where bfid = '%s';"

		self.delete_query = "\
			delete from file where bfid = '%s';"



	def export_format(self, s):
		return {
			'bfid': s['bfid'],
			'complete_crc': s['crc'],
			'deleted': s['deleted'],
			'drive': s['drive'],
			'external_label': s['label'],
			'location_cookie': s['location_cookie'],
			'pnfs_name0': s['pnfs_path'],
			'pnfsid': s['pnfs_id'],
			'santy_cookie': (s['sanity_cookie_0'], s['sanity_cookie_1']),
			'size': s['size']
			}

	def import_format(self, s):
		return {
			'bfid': s['bfid'],
			'crc': s['complete_crc'],
			'deleted': s['deleted'],
			'drive': s['drive'],
			'volume': ('lookup_vol', s['external_label']),
			'location_cookie': s['location_cookie'],
			'pnfs_path': s['pnfs_name0'],
			'pnfs_id': s['pnfsid'],
			'sanity_cookie_0': s['santy_cookie'][0],
			'sanity_cookie_1': s['santy_cookie'][1],
			'size': s['size']
			}

class VolumeDB(DbTable):
	def __init__(self, host='localhost', database='enstore'):
		DbTable.__init__(self, host, database, table='volume', pkey='label')
		self.retrieve_query = "\
        		select \
				label, block_size, capacity_bytes, \
				declared, eod_cookie, first_access, \
				last_access, library, \
				media_type, \
				non_del_files, \
				remaining_bytes, sum_mounts, \
				sum_rd_access, sum_rd_err, \
				sum_wr_access, sum_wr_err, \
				system_inhibit_0, \
				system_inhibit_1, \
				si_time_0, \
				si_time_1, \
				user_inhibit_0, \
				user_inhibit_1, \
				storage_group, \
				file_family, \
				wrapper, \
				comment \
        		from volume \
			where \
				label = '%s';"
		self.insert_query = "\
			insert into volume (%s) values (%s);"

		self.update_query = "\
			update volume set %s where lable = '%s';"

		self.delete_query = "\
			delete from volume where label = '%s';"


	def import_format(self, s):
		sts = string.split(s['volume_family'], '.')
		return {
			'block_size': s['blocksize'],
			'capacity_bytes': s['capacity_bytes'],
			'declared': time2timestamp(s['declared']),
			'eod_cookie': s['eod_cookie'],
			'label': s['external_label'],
			'first_access': time2timestamp(s['first_access']),
			'last_access': time2timestamp(s['last_access']),
			'library': s['library'],
			'media_type': s['media_type'],
			'non_del_files': s['non_del_files'],
			'remaining_bytes': s['remaining_bytes'],
			'sum_mounts': s['sum_mounts'],
			'sum_rd_access': s['sum_rd_access'],
			'sum_rd_err': s['sum_rd_err'],
			'sum_wr_access': s['sum_wr_access'],
			'sum_wr_err': s['sum_wr_err'],
			'system_inhibit_0': s['system_inhibit'][0],
			'system_inhibit_1': s['system_inhibit'][1],
			'si_time_0': time2timestamp(s['si_time'][0]),
			'si_time_1': time2timestamp(s['si_time'][1]),
			'user_inhibit_0': s['user_inhibit'][0],
			'user_inhibit_1': s['user_inhibit'][1],
			'storage_group': sts[0],
			'file_family': sts[1],
			'wrapper': sts[2],
			'comment': s['comment']
			}

	def export_format(self, s):
		return {
			'blocksize': s['block_size'],
			'capacity_bytes': s['capacity_bytes'],
			'declared': timestamp2time(s['declared']),
			'eod_cookie': s['eod_cookie'],
			'external_label': s['label'],
			'first_access': timestamp2time(s['first_access']),
			'last_access': timestamp2time(s['last_access']),
			'library': s['library'],
			'media_type': s['media_type'],
			'non_del_files': s['non_del_files'],
			'remaining_bytes': s['remaining_bytes'],
			'sum_mounts': s['sum_mounts'],
			'sum_rd_access': s['sum_rd_access'],
			'sum_rd_err': s['sum_rd_err'],
			'sum_wr_access': s['sum_wr_access'],
			'sum_wr_err': s['sum_wr_err'],
			'system_inhibit': [s['system_inhibit_0'],
						s['system_inhibit_1']],
			'si_time': [timestamp2time(s['si_time_0']),
				timestamp2time(s['si_time_1'])],
			'user_inhibit': [s['user_inhibit_0'],
						s['user_inhibit_1']],
			'volume_family': s['storage_group']+'.'+ \
					s['file_family']+'.'+ \
					s['wrapper'],
			'wrapper': s['wrapper'],
			'comment': s['comment']
			}
