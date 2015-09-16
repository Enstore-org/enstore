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

    self.retrieve_query   -- string of select statement template
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
import dbaccess


default_database = 'enstoredb'

#
# this function converts datetime.datetime key in a list of dictionaries
# to string date representation. Input argument : list of dictionaries
# (e.g. returned by query_dictresult())
#
def sanitize_datetime_values(dictionaries) :
    return dbaccess.sanitize_datetime_values(dictionaries)

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
    #
    # the comprehension below is to protect fields modified
    # by triggers. This will be revisited.
    #
    key1 = [k for k in s1.keys() if k not in ('active_files',
                                              'deleted_files',
                                              'unknown_files',
                                              'active_bytes',
                                              'deleted_bytes',
                                              'unknown_bytes')]
    for k in s2.keys():
        if k in key1 and s1[k] != s2[k]:
            #
            # s1['volume'] is an integer, s2['volume']=('lookup_vol',external_label)
            # therefore never equal. The condition below is to
            # avoid updating file.volume all the time
            if k == 'volume' and s1['label'] == s2['volume'][1] :
                continue
            d[k] = s2[k]
    return d

# str_value() -- properly represent the value in string
def str_value(v):
    t = type(v)

    if t == types.NoneType:
        return 'NULL'
    elif t == types.LongType:
        return `v`[:-1]
    elif t == types.TupleType:        # storage procedure
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
                 max_connections=20,
                 max_idle=5):

        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.table = table
        self.name = table        # backward compatible
        self.pkey = pkey
        self.auto_journal = auto_journal
        self.backup_flag = 1
        self.jouHome = jouHome

        if self.auto_journal:
            self.jou = ejournal.Journal(os.path.join(
                            jouHome, self.table))
        else:
            self.jou = None

        self.retrieve_query = "select * from "+self.table+" where "+self.pkey+" = %s"
        self.delete_query = "delete from "+self.table+" where "+self.pkey+" = %s"

        self.dbaccess =  dbaccess.DatabaseAccess(maxconnections=max_connections,
                                                 maxcached=max_idle,
                                                 blocking=True,
                                                 host=self.host,
                                                 port=self.port,
                                                 user=self.user,
                                                 database=self.database)

    def query(self,s,values=None,cursor_factory=None) :
        return self.dbaccess.query(s,values,cursor_factory)

    def update(self,s,values=None):
        self.dbaccess.update(s,values)

    def insert(self,s,record=None):
        self.dbaccess.insert(s,record)

    def remove(self,s,values=None):
        self.dbaccess.remove(s,values)

    def delete(self,s,values=None):
        return self.remove(s,values)

    def query_dictresult(self,s,values=None):
        return self.dbaccess.query_dictresult(s,values)

    def query_getresult(self,s,values=None):
        return self.dbaccess.query_getresult(s,values)

    def query_tuple(self,s,values=None):
        return self.dbaccess.query_tuple(s,values)

    # translate database output to external format
    def export_format(self, s):
        return s

    # translate external format to database internal format
    def import_format(self, s):
        return s

    def __getitem__(self, key):
        res=self.dbaccess.query_dictresult(self.retrieve_query,(key,))
        if len(res) == 0:
            return None
        else:
            return self.export_format(res[0])

    def __setitem__(self, key, value):
        if self.auto_journal:
            self.jou[key] = value
        v1 = self.import_format(value)
        res = self.dbaccess.query_dictresult(self.retrieve_query,(key,))
        if len(res) == 0:        # insert
            self.dbaccess.insert(self.table,v1)
        else:                        # update
            d = diff_fields_and_values(res[0], v1)
            if d:        # only if there is any difference
                query = dbaccess.generate_update_query(self.table,d.keys())
                query += " WHERE {}=%s".format(self.pkey)
                v=d.values()
                v.append(key)
                res = self.dbaccess.update(query,tuple(v))


    def insert_new_record(self,key,value):
        """
        Insert new record into database, provided a key
        and record dictionary. A better alternative to  __setitem__

        :type key: :obj: `str`
        :arg key: record key (label or bfid)

        :type value: :obj: `dict`
        :arg value: record corresonding to the key

        """
        v1 = self.import_format(value)
        self.dbaccess.insert(self.table,v1)
        if self.auto_journal:
            self.jou[key] = value


    def update_record(self,key,value):
        """
        Update existing record providing key and
        a dictionary. A dictionary may contain
        a subset of key,value pairs. This method
        is alternative to __setitem__ method which
        is not transactionally secure.

        :type key: :obj: `str`
        :arg key: record key (label or bfid)

        :type value: :obj: `dict`
        :arg value: dictionary containing updated pieces of record
        :rtype: :obj:`dict` updated record
        """
        v1 = self.import_format(value)
        query = dbaccess.generate_update_query(self.table,v1.keys())
        query += " WHERE {}=%s".format(self.pkey)
        v=v1.values()
        v.append(key)
        res = self.dbaccess.update(query,tuple(v))
        updated_record = self.__getitem__(key)
        if self.auto_journal:
            self.jou[key] = updated_record
        return updated_record



    def __delitem__(self, key):
        if self.auto_journal:
            if not self.jou.has_key(key):
                self.jou[key] = self.__getitem__(key)
            del self.jou[key]
        res = self.dbaccess.delete(self.delete_query,(key,))


    def keys(self):
        res = self.dbaccess.query_getresult('select %s from %s order by %s;'%(self.pkey, self.table, self.pkey))
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
        return int(self.dbaccess.query_getresult('select count(*) from %s;'%(self.table))[0][0])

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

    def close(self):
        self.dbaccess.close()

class FileDB(DbTable):

    def __init__(self,
                 host='localhost',
                 port=8888,
                 user=None,
                 jou='.',
                 database=default_database,
                 rdb=None,
                 auto_journal=1,
                 max_connections=20,
                 max_idle=5):

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
                         max_connections = max_connections,
                         max_idle=max_idle)

        #
        # This query is used in __getitem__
        # Typically is invoked by bfid_info
        #
        self.retrieve_query = """
        SELECT f.*,
               v.label,
               v.file_family,
               v.storage_group,
               v.library,
               v.wrapper
        FROM file f
        INNER JOIN volume v ON f.volume = v.id
        AND bfid = %s
        """

    def __setitem__(self, key, value):
        res = self.dbaccess.query_dictresult(self.retrieve_query,(key,))
        if len(res) == 0:        # insert
            self.insert_new_record(key,value)
        else:                        # update
            v1 = self.import_format(value)
            d = diff_fields_and_values(res[0], v1)
            if d:        # only if there is any difference
                if not d.has_key("volume"):
                    query = dbaccess.generate_update_query(self.table,d.keys())
                    query += " WHERE {}=%s".format(self.pkey)
                    v=d.values()
                    v.append(key)
                    res = self.dbaccess.update(query,tuple(v))
                else:
                    query="""
                    UPDATE file SET
                    """
                    for k in d.keys():
                        if k != "volume":
                            query += "{}=%s,".format(k)
                        else:
                            query += "{}=(SELECT id FROM volume WHERE label=%s),".format(k)
                    query = query[:-1] + " WHERE {}=%s".format(self.pkey)
                    v=d.values()
                    v.append(key)
                    res = self.dbaccess.update(query,tuple(v))
        if self.auto_journal:
            self.jou[key] = value


    def insert_new_record(self,key,value):
        """
        Insert new record into database, provided a key
        and record dictionary. A better alternative to  __setitem__

        :type key: :obj: `str`
        :arg key: record key (label or bfid)

        :type value: :obj: `dict`
        :arg value: record corresonding to the key

        """
        if not value.has_key("external_label"):
            DbTable.insert_new_record(self,key,value)
            return
        v1 = self.import_format(value)
        query = """
        INSERT INTO {} ({}) VALUES (
        """
        query=query.format(self.table,string.join(v1.keys(), ","))
        for k in v1.keys():
            if k != "volume":
                query += "%s,"
            else:
                query += "(SELECT id FROM volume where label=%s),"
        query=query[:-1]+")"
        self.dbaccess.update(query,v1.values())
        if self.auto_journal:
            self.jou[key] = value

    def update_record(self,key,value):
        """
        Update existing record providing key and
        a dictionary. A dictionary may contain
        a subset of key,value pairs. This method
        is alternative to __setitem__ method which
        is not transactionally secure.

        :type key: :obj: `str`
        :arg key: record key (label or bfid)

        :type value: :obj: `dict`
        :arg value: dictionary containing updated pieces of record
        :rtype: :obj:`dict` updated record

        """
        if not value.has_key("external_label"):
            return DbTable.update_record(self,key,value)
        v1 = self.import_format(value)
        query="""
        UPDATE file SET
        """
        for k in v1.keys():
            if k != "volume":
                query += "{}=%s,".format(k)
            else:
                query += "{}=(SELECT id FROM volume WHERE label=%s),".format(k)
        query = query[:-1] + " WHERE {}=%s".format(self.pkey)
        v=v1.values()
        v.append(key)
        res = self.dbaccess.update(query,tuple(v))
        updated_record =  self.__getitem__(key)
        if self.auto_journal:
            self.jou[key] = updated_record
        return updated_record

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
        for key in ('package_files_count', 'active_package_files_count'):
            record[key] = s.get(key,0)
        for key in ('package_id','cache_status','archive_status',\
                    'cache_mod_time','archive_mod_time',\
                    'storage_group','file_family','library','wrapper','cache_location',
                    'original_library','file_family_width','tape_label'):
            record[key] = s.get(key,None)
        return record

    def import_format(self, s):
        deleted=None
        if s.get('deleted') in ('yes','y') :
            deleted='y'
        elif s.get('deleted') in ('n','no') :
            deleted='n'
        elif s.has_key('deleted'):
            deleted='u'

        sanity_size, sanity_crc, crc= (None, None, None)
        if s.has_key('sanity_cookie'):
            sanity_size = s['sanity_cookie'][0] if s['sanity_cookie'][0] != None else -1
            sanity_crc  = s['sanity_cookie'][1] if s['sanity_cookie'][1] != None else -1

        if s.has_key('complete_crc'):
            crc = s['complete_crc'] if s['complete_crc'] != None else -1

        pnfs_path = s.get("pnfs_name0")

        record={}

        if s.has_key('external_label'):
            record['volume'] = s['external_label']
        if s.has_key('pnfsid'):
            record['pnfs_id'] = s['pnfsid']

        if deleted != None :
            record['deleted']=deleted
        if crc != None:
            record['crc']=crc
        if pnfs_path != None :
            record['pnfs_path']=pnfs_path
        if sanity_size != None :
            record['sanity_size'] = sanity_size
        if sanity_crc != None :
            record['sanity_crc'] = sanity_crc

        for key in ('package_files_count', 'active_package_files_count'):
            if s.has_key(key):
                record[key] = s.get(key,0)

        for key in ('bfid','drive','location_cookie','size','uid','gid',
                    'package_id','cache_status','archive_status',
                    'cache_mod_time','archive_mod_time',
                    'cache_location',
                    'original_library','file_family_width'):
            if s.has_key(key):
                record[key] = s.get(key,None)

        return record

    def __getitem__(self, key):
        res=self.dbaccess.query_dictresult(self.retrieve_query,(key,))
        if len(res) == 0:
            return None
        else:
            file=res[0]
            #
            # get volume info from parent
            #
            if file.get('package_id',None) and \
                   file.get('package_id',None) != file.get('bfid',key) :
                res1=self.dbaccess.query_dictresult(self.retrieve_query,(file.get('package_id'),))
                if len(res1) != 0 :
                    package=res1[0]
                    file["tape_label"] = package.get("label",None)
            else:
                file["tape_label"]=file.get("label",None)
            return self.export_format(file)

class VolumeDB(DbTable):
    def __init__(self,
                 host='localhost',
                 port=8888,
                 user=None,
                 jou='.',
                 database=default_database,
                 rdb=None,
                 auto_journal=1,
                 max_connections=20,
                 max_idle=5):

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
                         max_connections=max_connections,
                         max_idle=5)

        #
        # This query is used in __getitem__
        # Typically is invoked by inquire_vol
        #
        self.retrieve_query = """
        SELECT block_size,
               capacity_bytes,
               declared,
               eod_cookie,
               label,
               first_access,
               last_access,
               library,
               media_type,
               non_del_files,
               remaining_bytes,
               sum_mounts,
               sum_rd_access,
               sum_rd_err,
               sum_wr_access,
               sum_wr_err,
               system_inhibit_0,
               system_inhibit_1,
               si_time_0,
               si_time_1,
               user_inhibit_0,
               user_inhibit_1,
               storage_group,
               file_family,
               wrapper,
               comment,
               write_protected,
               modification_time,
               COALESCE(active_files,0) AS active_files,
               COALESCE(deleted_files,0) AS deleted_files,
               COALESCE(unknown_files,0) AS unknown_files,
               COALESCE(active_bytes,0) AS active_bytes,
               COALESCE(deleted_bytes,0) AS deleted_bytes,
               COALESCE(unknown_bytes,0) AS unknown_bytes
        FROM volume
        WHERE label=%s
        """

    def import_format(self, s):
        sts = string.split(s['volume_family'],'.') if s.has_key('volume_family') else None
        data={}
        if s.has_key('blocksize') :
            data['block_size'] = s['blocksize']
        if s.has_key('external_label') :
            data['label']= s['external_label']
        for k in ('capacity_bytes','eod_cookie',
                  'library','media_type','non_del_files',
                  'remaining_bytes','sum_mounts','sum_rd_access',
                  'sum_rd_err','sum_wr_access','sum_wr_err',
                  'comment','write_protected'):
            if s.has_key(k):
                data[k] = s[k]

        for k in ('declared','first_access','last_access','modification_time'):
            if s.has_key(k) :
                data[k] = time2timestamp(s[k])

        if s.has_key('system_inhibit'):
            data['system_inhibit_0'],data['system_inhibit_1'] = s['system_inhibit']

        if s.has_key('user_inhibit'):
            data['user_inhibit_0'],data['user_inhibit_1'] = s['user_inhibit']

        if s.has_key('si_time'):
            data['si_time_0'],data['si_time_1'] = map(time2timestamp,s['si_time'])

        if sts and len(sts) == 3:
            data['storage_group'],data['file_family'],data['wrapper'] = sts

        for k in ("active_files","deleted_files","unknown_files",\
                  "active_bytes","deleted_bytes","unknown_bytes"):
            if s.has_key(k):
                data[k]=s[k]

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
        for k in ("active_files","deleted_files","unknown_files",\
                  "active_bytes","deleted_bytes","unknown_bytes"):
            data[k]=s.get(k,0)
        return data

if __name__ == '__main__':
    v=VolumeDB(host='localhost',
               port=9999,
               user="enstore",
               database="enstoredb",
               max_connections=100)

    number = random.randint(0,len(v)-1)
    print v.keys(), len(v)
    random_label=v.keys()[number]
    volume = v[random_label]
    print volume
    local_copy=copy.deepcopy(volume)
    v[v.keys()[number]]=local_copy # update
    label='XXXX01'
    del v[label]
    local_copy['external_label']=label
    print local_copy
    v[label]=local_copy #insert
    del v[label]

    f=FileDB(host='localhost',
             port=9999,
             user="enstore",
             database="enstoredb",
             max_connections=100)

    con = f.dbaccess.pool.connection()
    record =  f['GCMS136213823000000']
    print record
    record['bfid']='GCMS999999999999997'
    f['GCMS999999999999997'] = record



    record['pnfsid']='3000188D464066A343F1A9839864C9B4F3C1'
    f['GCMS999999999999997'] = record

    record["external_label"]='TTC037'
    f['GCMS999999999999997'] = record

    del f['GCMS999999999999997']
    del f['GCMS999999999999998']
    del f['GCMS99999999999999']


