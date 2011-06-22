#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

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
import random
import datetime
import string
import types
import copy
import ejournal
import os
import Trace
import e_errors
import pg
import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB


default_database = 'enstoredb'

#
# this function converts datetime.datetime key in a list of dictionaries
# to string date representation. Input argument : list of dictionaries
# (e.g. returned by query_dictresult())
#
def sanitize_datetime_values(dictionaries) :
    for item in dictionaries:
        if type(item) == psycopg2.extras.RealDictRow:
            for key in item.keys():
                if isinstance(item[key],datetime.datetime):
                    item[key] = item[key].isoformat(' ')
        elif type(item) == psycopg2.extras.DictRow:
            for i in range(0,len(item)):
                if isinstance(item[i],datetime.datetime):
                    item[i] = item[i].isoformat(' ')
    return dictionaries


# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time
def timestamp2time(s):
        if not s : return -1
	if s == '1969-12-31 17:59:59':
		return -1
	if s == '1970-01-01 00:59:59':
		return -1
	if isinstance(s,datetime.datetime) :
		try:
			return time.mktime(s.timetuple())
		except OverflowError:
			return -1
	else:
		tt=[]
		try:
			# take care of daylight saving time
			tt = list(time.strptime(s, "%Y-%m-%d %H:%M:%S"))
			tt[-1] = -1
		except TypeError:
			Trace.log( e_errors.ERROR,'wrong time format: '+s);
			tt=list(time.localtime(s))
			tt[-1] = -1
                try:
                    rc = time.mktime(tuple(tt))
                except OverflowError:
                    rc = -1
		return rc

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"
def time2timestamp(t):
	if isinstance(t,datetime.datetime) :
		t = time.mktime(t.timetuple())
	try:
		return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
	except TypeError:
		return t;

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
		return "'" + str(v) + "'"

# from a dictionary, get field name and values
# From a dictionary, s, return two strings to be injected
# as part of SQL insert statement
# First part is a comma separated list of dictionary keys.
# The second part is a comma seperated list of the values.
def get_fields_and_values(s):
    fields = string.join(s.keys(), ",")
    values = string.join(map(str_value, s.values()), ",")
    return fields, values


def get_fields_and_values(s):
        return string.join(s.keys(),","),string.join(map(str_value,s.values()),",")

# This is the base DbTable class
#
# All derived classes need to provide the following:
#
# self.retrieve_query -- the query to take information out of database
# self.exprot_format(self, s) -- translate database output to external format

class DbTable:
	def __init__(self,
                     host,
                     port,
                     user,
                     database,
                     table,
                     pkey,
                     jouHome ='.',
                     auto_journal=0,
                     rdb=None,
                     max_connections=20):

		self.host = host
		self.port = port
                self.user = user
		self.database = database
		self.table = table
		self.name = table	# backward compatible
		self.pkey = pkey
		self.auto_journal = auto_journal
		self.backup_flag = 1
		self.jouHome = jouHome

		if self.auto_journal:
			self.jou = ejournal.Journal(os.path.join(
					jouHome, self.table))
		else:
			self.jou = None

		self.retrieve_query = "select * from "+self.table+" where "+self.pkey+" = '%s';"
		self.insert_query = "insert into "+self.table+" (%s) values (%s);"
		self.update_query = "update "+self.table+" set %s where "+self.pkey+" = '%s';"
		self.delete_query = "delete from "+self.table+" where "+self.pkey+" = '%s';"

		if rdb:
			self.db = rdb
		else:
			try:
				self.db = pg.DB(host=self.host,
                                                port=self.port,
                                                dbname=self.database,
                                                user=self.user)
			except:	# wait for 30 seconds and retry
				time.sleep(30)
				self.db = pg.DB(host=self.host,
                                                port=self.port,
                                                dbname=self.database,
                                                user=self.user)
		self.pool =  PooledDB(psycopg2,
				      maxconnections=max_connections,
				      blocking=True,
				      host=self.host,
				      port=self.port,
                                      user=self.user,
				      database=self.database)

	def query(self,s,cursor_factory=None) :
            db = None
            cursor = None
            try:
                db=self.pool.connection();
                if cursor_factory :
                    cursor=db.cursor(cursor_factory=cursor_factory)
                else:
                    cursor=db.cursor()
                cursor.execute(s)
                res=cursor.fetchall()
                cursor.close()
                db.close()
                cursor = None
                db     = None
                return res
            except psycopg2.Error, msg:
                try:
                    if cursor:
                        cursor.close()
                    if db:
                        db.close()
                except:
                    # if we failed to close just silently ignore the exception
                    pass
                curor = None
                db    = None
                #
                # propagate exception to caller
                #
                raise e_errors.EnstoreError(None,
                                            str(msg),
                                            e_errors.DATABASE_ERROR)
            except:
                try:
                    if cursor:
                        cursor.close()
                    if db:
                        db.close()
                except:
                    # if we failed to close just silently ignore the exception
                    pass
                #
                # propagate exception to caller
                #
                raise


	def update(self,s):
            db = None
            cursor = None
            try:
                db=self.pool.connection();
                cursor=db.cursor()
                cursor.execute(s)
                db.commit()
                cursor.close()
                db.close()
            except psycopg2.Error, msg:
                try:
                    if db:
                        db.rollback()
                    if cursor:
                        cursor.close()
                    if db:
                        db.close()
                except:
                    # if we failed to close just silently ignore the exception
                    pass
                curor = None
                db    = None
                #
                # propagate exception to caller
                #
                raise e_errors.EnstoreError(None,
                                            str(msg),
                                            e_errors.DATABASE_ERROR)
            except:
                if db:
                    db.rollback()
                if cursor:
                    cursor.close()
                if db:
                    db.close()
                #
                # propagate exception to caller
                #
                raise

	def insert(self,s):
		return self.update(s)

	def remove(self,s):
		return self.update(s)

	def delete(self,s):
		return self.remove(s)

	def query_dictresult(self,s):
                result=self.query(s,cursor_factory=psycopg2.extras.RealDictCursor)
                #
                # code below converts the result, which is
                # psycopg2.extras.RealDictCursor object into ordinary
                # dictionary. We need it b/c some parts of volume_clerk, file_clerk
                # send the result over the wire to the client, and client
                # chokes on psycopg2.extras.RealDictCursor is psycopg2.extras is not
                # installed on the client side
                #
                res=[]
                for row in result:
                    r={}
                    for key in row.keys():
                        if isinstance(row[key],datetime.datetime):
                            r[key] = row[key].isoformat(' ')
                        else:
                            r[key] = row[key]
                    res.append(r)
		return res

	def query_getresult(self,s):
		result=self.query(s,cursor_factory=psycopg2.extras.DictCursor)
                #
                # code below converts the result, which is
                # psycopg2.extras.DictCursor object into list lists
                # We need it b/c some parts of volume_clerk, file_clerk
                # send the result over the wire to the client, and client
                # chokes on psycopg2.extras.DictCursor is psycopg2.extras is not
                # installed on the client side
                #
                res=[]
                for row in result:
                    r=[]
                    for item in row:
                        if isinstance(item,datetime.datetime):
                            r.append(item.isoformat(' '))
                        else:
                            r.append(item)
                    res.append(r)
                return res

	def query_tuple(self,s):
		return self.query(s)

	# translate database output to external format
	def export_format(self, s):
		return s

	# translate external format to database internal format
	def import_format(self, s):
		return s

	def __getitem__(self, key):
		res=self.query_dictresult(self.retrieve_query%(key))
		if len(res) == 0:
			return None
		else:
			return self.export_format(res[0])

	def __setitem__(self, key, value):
		if self.auto_journal:
			self.jou[key] = value
		v1 = self.import_format(value)
		res = self.query_dictresult(self.retrieve_query%(key))
		if len(res) == 0:	# insert
			cmd = self.insert_query%get_fields_and_values(v1)
			# print cmd
			self.insert(cmd)
		else:			# update
			d = diff_fields_and_values(res[0], v1)
			if d:	# only if there is any difference
				setstmt = ''
				for i in d.keys():
					setstmt = setstmt + i + ' = ' + str_value(d[i]) + ', '
				setstmt = setstmt[:-2]	# get rid of last ', '
				cmd = self.update_query%(setstmt, key)
				Trace.log(e_errors.MISC, "Updating  "+cmd)
				# print cmd
				res = self.update(cmd)

	def __delitem__(self, key):
		if self.auto_journal:
			if not self.jou.has_key(key):
				self.jou[key] = self.__getitem__(key)
			del self.jou[key]
		res = self.delete(self.delete_query%(key))


	def keys(self):
		res = self.query_getresult('select %s from %s order by %s;'%(self.pkey, self.table, self.pkey))
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
		return int(self.query_getresult('select count(*) from %s;'%(self.table))[0][0])

	def start_backup(self):
		self.backup_flag = 0
		Trace.log(e_errors.INFO, "Start backup for "+self.table)
		self.checkpoint()

	def stop_backup(self):
		Trace.log(e_errors.INFO, "End backup for "+self.table)
		self.backup_flag = 1

	def checkpoint(self):
		if self.auto_journal:
			self.jou.checkpoint()
		return

	def backup(self):
		try:
			cwd=os.getcwd()
		except OSError:
			cwd = "/tmp"

		os.chdir(self.jouHome)
		cmd="tar rf "+self.name+".tar"+" "+self.name+".jou.*"
		Trace.log(e_errors.INFO, repr(cmd))
		os.system(cmd)
		cmd="rm "+ self.name +".jou.*"
		Trace.log(e_errors.INFO, repr(cmd))
		os.system(cmd)
		os.chdir(cwd)

		return

	def reconnect(self):
		# close existing connection
		try:
			self.close()
		except:
			pass
		self.db = pg.DB(host=self.host,
                                port=self.port,
                                user=self.user,
                                dbname=self.database)

	def close(self):	# don't know what to do
		self.db.close()
		pass

class FileDB(DbTable):

	def __init__(self,
                     host='localhost',
                     port=8888,
                     user=None,
                     jou='.',
                     database=default_database,
                     rdb=None,
                     auto_journal=1,
                     max_connections=20):

		DbTable.__init__(self,
                                 host=host,
                                 port=port,
                                 user=user,
                                 database=database,
                                 jouHome=jou,
                                 table='file',
                                 pkey='bfid',
                                 auto_journal=auto_journal,
                                 rdb = rdb,
                                 max_connections = max_connections)

		self.retrieve_query = "\
        		select \
                		bfid, crc, deleted, drive, \
				volume.label, location_cookie, pnfs_path, \
                		pnfs_id, sanity_size, sanity_crc, size, \
				uid, gid, update \
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
		# take care of deleted
		if s['deleted'] == 'y':
			deleted = 'yes'
		elif s['deleted'] == 'n':
			deleted = 'no'
		else:
			deleted = 'unknown'

		# take care of sanity_cookie
		if s['sanity_size'] == -1:
			sanity_size = None
		else:
			sanity_size = s['sanity_size']
		if s['sanity_crc'] == -1:
			sanity_crc = None
		else:
			sanity_crc = s['sanity_crc']

		# take care of crc
		if s['crc'] == -1:
			crc = None
		else:
			crc = s['crc']

		record = {
			'bfid': s['bfid'],
			'complete_crc': crc,
			'deleted': deleted,
			'drive': s['drive'],
			'external_label': s['label'],
			'location_cookie': s['location_cookie'],
			'pnfs_name0': s['pnfs_path'],
			'pnfsid': s['pnfs_id'],
			'sanity_cookie': (sanity_size, sanity_crc),
			'size': s['size']
			}

		# handle uid and gid
		if s.has_key('uid'):
			record['uid'] = s['uid']
		if s.has_key('gid'):
			record['gid'] = s['gid']
		if s.has_key('update'):
			if isinstance(s['update'],datetime.datetime):
				record['update'] = (s['update']).isoformat(' ')
			else:
				record['update'] = s['update']

		return record

	def import_format(self, s):
		if s['deleted'] == 'yes' or s['deleted'] == 'y':
			deleted = 'y'
		elif s['deleted'] == 'no' or s['deleted'] == 'n':
			deleted = 'n'
		else:
			deleted = 'u'

		# Take care of sanity_cookie
		if s['sanity_cookie'][0] == None:
			sanity_size = -1
		else:
			sanity_size = s['sanity_cookie'][0]
		if s['sanity_cookie'][1] == None:
			sanity_crc = -1
		else:
			sanity_crc = s['sanity_cookie'][1]

		# take care of crc
		if s['complete_crc'] == None:
			crc = -1
		else:
			crc = s['complete_crc']

		escape_string = getattr(pg, "escape_string", None)
		if escape_string:
			#For pg.py 3.8.1 and later.  This escapes the SQL
			# special characters.
			pnfs_path = escape_string(s['pnfs_name0'])
		else:
			#At least handle this one character if using too old
			# of a version of pg.py.
			pnfs_path = s['pnfs_name0'].replace("'", "''")

		record = {
			'bfid': s['bfid'],
			'crc': crc,
			'deleted': deleted,
			'drive': s['drive'],
			'volume': ('lookup_vol', s['external_label']),
			'location_cookie': s['location_cookie'],
			'pnfs_path': pnfs_path,
			'pnfs_id': s['pnfsid'],
			'sanity_size': sanity_size,
			'sanity_crc': sanity_crc,
			'size': s['size'],
			}

		# handle uid and gid
		if s.has_key("uid"):
			record["uid"] = s["uid"]
		if s.has_key("gid"):
			record["gid"] = s["gid"]
		return record

class VolumeDB(DbTable):
	def __init__(self,
                     host='localhost',
                     port=8888,
                     user=None,
                     jou='.',
                     database=default_database,
                     rdb=None,
                     auto_journal=1,
                     max_connections=20):

		DbTable.__init__(self,
                                 host,
                                 port,
                                 user=user,
                                 database=database,
                                 jouHome=jou,
                                 table='volume',
                                 pkey='label',
                                 auto_journal=auto_journal,
                                 rdb = rdb,
                                 max_connections=max_connections)

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
				comment, \
				write_protected, \
				modification_time \
        		from volume \
			where \
				label = '%s';"
		self.insert_query = "\
			insert into volume (%s) values (%s);"

		self.update_query = "\
			update volume set %s where label = '%s';"

		self.delete_query = "\
			delete from volume where label = '%s';"

	def import_format(self, s):
		sts = string.split(s['volume_family'], '.')
	        data =  {
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
			'comment': s['comment'],
			'write_protected': s['write_protected']
			}
		if s.has_key('modification_time') :
			data['modification_time'] = time2timestamp(s['modification_time'])
		else:
			data['modification_time']=-1
		return data;

	def export_format(self, s):
		data = {
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
			'comment': s['comment'],
			'write_protected': s['write_protected']
			}
		if s.has_key('modification_time') :
			data['modification_time'] = timestamp2time(s['modification_time'])
		else:
			data['modification_time']=-1
		return data;

if __name__ == '__main__':
	v=VolumeDB();
	number = random.randint(0,len(v)-1)
	print v.keys(), len(v)
	volume = v[v.keys()[number]]
	print volume
	local_copy=copy.deepcopy(volume)
	v[v.keys()[number]]=local_copy # update
	label='XXXX01'
	del v[label]
	local_copy['external_label']=label
	print local_copy
	v[label]=local_copy #insert
	del v[label]

	f=FileDB()
	print f['CDMS115744790100000']

