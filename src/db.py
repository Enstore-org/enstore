from __future__ import print_function
import libtpshelve
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
from future.utils import raise_
import os
import time

# enstore imports
import setpath

import journal
import Trace
import traceback
import configuration_client
import e_errors

setpath.addpath('$LIBTPPY_DIR/lib')

JOURNAL_LIMIT = 1000
backup_flag = 1

# advance_all(max, curlist) -- advance all cursor such that no current
# value is less than max. When all current values equal to max, there is
# a common element that we want. Otherwise, if after advancing, any
# current becomes larger than max, the current max is useless


def advance_all(max, curlist):
    for c in curlist:
        # Although libtpshelve.py should and will address this,
        # it is not a bad idea to safe guard it in here.
        try:
            key, value = c.current()
        except BaseException:		# c is a funny cursor
            return None

        while value < max:
            key, value = c.nextDup()
            if value is None:
                return None
        if value > max:
            return value
    return max

# Jcursor(primary_db, curlist) -- join cursor
#
# 	Join cursor looks like a db cursor but has different
#	implementation. Upon initialization, primary_db is the primary
#	db which is an instance of db.DbTable. curlist is a list of
#	(index) cursors which have been preset through set() method.
#	That is, each cursor will iterate through the duplicate keys of
#	a certian value, and its value is a primary key in the primary
#	db. Jcursor walks through all common values, which are some
#	indexed primary keys, in all index cursors. The common value is
#	used as key to the primary db. The key, value pair in primary db
#	is thus returned by Jcursor.current() & Jcursor.next() methods.
#	When there is no more common indexed key, Jcursor.next() returns
#	None, None and sets the current() to None, None, too. When
#	initialized, Jcursor.current() points to None. The first call
#	to Jcursor.next() will set it to the first common index keyed
#	value. Jcursor can not be traversed backward ...


class Jcursor:
    def __init__(self, primary, curlist):
        self.primary = primary
        self.curlist = curlist
        key, value = curlist[0].current()
        # got to find the first one at init
        self.crnt = None
        self.end = 0  # this is ugly

    def current(self):
        if self.crnt:
            return self.crnt, self.primary[self.crnt]
        else:
            return None, None

    def next(self):
        if self.end:
            return None, None
        if self.crnt:  # not the first time
            key, value = self.curlist[0].nextDup()
        else:		# this first time
            key, value = self.curlist[0].current()
        self.crnt = value
        if self.crnt is None:
            self.end = 1
            return None, None
        max = advance_all(self.crnt, self.curlist)
        while max > self.crnt:
            self.crnt = max
            max = advance_all(self.crnt, self.curlist)
            if max is None:
                break
        self.crnt = max
        if self.crnt is None:
            self.end = 1
            return None, None
        else:
            return self.crnt, self.primary[self.crnt]

    # need to close all related cursors
    def close(self):
        for c in self.curlist:
            # This is not necessary ... just to be safe
            try:
                c.close()
            except BaseException:
                traceback.print_exc()
                pass

    def __del__(self):
        self.close()

# join -- return a join cursor


def join(primary, curlist):
    c = Jcursor(primary, curlist)
    return c


indexError = "indexError"

# Index
#	Index is a special database
#	Its (key, value) is unique but the keys may have duplicated
#	values. (the value portion is unique :-) Its keys come from the
#	values of the indexed field in the primary database and its
#	values are the corrosponding keys.


class Index:
    def __init__(self, db, dbHome, dbName, field):
        self.missing = []
        self.extra = []
        # primary_db could be None
        self.primary_db = db  # primary db
        self.dbHome = dbHome  # DBHOME for all
        self.dbName = dbName  # Name of the primary db
        self.name = field  # name of the indexed colume
        self.idxFile = self.dbName + "." + field + ".index"  # name of index
        # primary_db determines how the Index is opened
        # do not test primary_db directly!
        # never say 'if self.primary_db: ...' since it is going
        # to be evaluated as the 'length' of db, which is going
        # to iterate through entire database ...

        if not isinstance(self.primary_db, type(None)):
            dbEnvSet = {'create': 1, 'init_mpool': 1, 'init_lock': 1,
                        'init_txn': 1}
        else:  # do not create if it does not exist
            dbEnvSet = {'create': 0, 'init_mpool': 1, 'init_lock': 0,
                        'init_txn': 0}
        dbEnv = libtpshelve.env(self.dbHome, dbEnvSet)
        # check to see if the index file exists?
        if os.path.isfile("%s/%s" % (self.dbHome, self.idxFile)):
            self.db = libtpshelve.open(
                dbEnv, self.idxFile, type='btree', dup=1, dupsort=1)
            Trace.log(e_errors.INFO, "Index %s opened" % (self.idxFile))
        else:  # build index file here
            if not isinstance(self.primary_db, type(None)):
                Trace.log(
                    e_errors.INFO,
                    "Building index %s ..." %
                    (self.idxFile))
                self.db = libtpshelve.open(
                    dbEnv, self.idxFile, type='btree', dup=1, dupsort=1)
                count = 0
                t = self.primary_db.txn()
                c = self.primary_db.cursor(t)
                key, val = c.first()
                while val is not None:
                    self.db[(val[field], t)] = key
                    key, val = next(c)
                    count = count + 1
                    if count % 100 == 0:
                        Trace.log(
                            e_errors.INFO,
                            "%d entries have been inserted" %
                            (count))
                Trace.log(e_errors.INFO, "%d entries in total" % (count))
                c.close()
                t.commit()
            else:
                Trace.log(
                    e_errors.ERROR,
                    "Index file %s does not exist" %
                    (self.idxFile))
                raise_(
                    indexError,
                    "Index file %s does not exist" %
                    (self.idxFile))

    # insert an index entry
    def insert(self, key, value, txn=None):
        self.db[(key, txn)] = value

    # point a cursor to the beginning of certain key value
    # No need to commit on locate
    def locate(self, key, txn=None):
        c = self.db.cursor(txn)
        key, val = c.set(key)
        return c

    # delete the index according to key/value
    def delete(self, key, val, txn=None):
        c = self.db.cursor(txn)
        key, val = c.set(key, val)
        if (key, val) != (None, None):
            c.delete()
            c.close()

    # txn -- to get an transction
    def txn(self):
        return self.db.txn()

    # cursor -- get an cursor on itself
    def cursor(self, txn=None):
        return self.db.cursor(txn)

    # __getitem__(): actually, a list is returned
    def __getitem__(self, key):
        res = []
        c = self.db.cursor(None)
        k, v = c.set(key)
        while k:
            res.append(v)
            k, v = c.nextDup()
        c.close()
        return res

    # close -- close Index db
    def close(self):
        self.db.close()
        return

    # check the consistency of index
    def check(self):
        # meaningless if there is no primary db
        # don't test primary_db directly, explained above
        if isinstance(self.primary_db, type(None)):
            Trace.log(
                e_errors.INFO,
                "Index.check(): no primary db to check against")
            return(0)

        status = 0
        c = self.db.cursor()
        pc = self.primary_db.cursor()
        pck, pcv = pc.first()
        while pck is not None:
            ipk, ipv = c.set(pcv[self.name], pck)
            if ipk is None:
                status = status + 1
                self.missing.append((pcv[self.name], pck))
            pck, pcv = next(pc)
        ck, cv = c.first()
        while ck is not None:
            try:
                if self.primary_db[cv][self.name] != ck:
                    status = status + 1
                    self.extra.append((ck, cv))
            except BaseException:
                status = status + 1
                self.extra.append((ck, cv))
            ck, cv = next(c)
        c.close()
        pc.close()
        return status

    # fix the index according to previous check
    def fix(self):
        # meaningless if there is no primary db
        if isinstance(self.primary_db, type(None)):
            Trace.log(
                e_errors.INFO,
                "Index.fix(): no primary db to check against")
            return

        t = self.txn()
        for (k, v) in self.missing:
            self.insert(k, v, t)
        for (k, v) in self.extra:
            self.delete(k, v, t)
        t.commit()
        self.missing = []
        self.extra = []

    # check_and_fix: do both
    def check_and_fix(self):
        if self.check():
            self.fix()

# cacheCursor -- work on a dictionary


class cacheCursor:
    def __init__(self, dict):
        self.length = len(dict)
        self.position = 0
        self.dict = dict
        self.keys = dict.keys()
        self.values = dict.values()

    def first(self):
        self.position = 0
        return self.keys[0], self.values[0]

    def next(self):
        if self.position < (self.length - 1):
            self.position = self.position + 1
            k, v = self.keys[self.position], self.values[self.position]
            return k, v
        else:
            return None, None

    def previous(self):
        if self.position <= 0:
            return None, None
        else:
            self.position = self.position - 1
            return self.keys[self.position], self.values[self.position]

    def last(self):
        self.position = self.length - 1
        k, v = self.keys[self.position], self.values[self.position]
        return k, v

    def close(self):
        pass


class DbTable:
    def __init__(self, dbname, db_home, jou_home,
                 indlst=None, auto_journal=1, auto_cache=0):
        if indlst is None:
            indlst = []
        self.auto_journal = auto_journal
        self.name = dbname
        self.dbHome = db_home
        self.jouHome = jou_home
        self.cursor_open = 0
        self.c = None
        self.t = None
        self.cache = {}
        self.cached = 0
        # open database file
        dbEnvSet = {
            'create': 1,
            'init_mpool': 1,
            'init_lock': 1,
            'init_txn': 1}
        dbEnv = libtpshelve.env(self.dbHome, dbEnvSet)
        self.db = libtpshelve.open(dbEnv, dbname, type='btree')

        # Now, take care of the index
        self.inx = {}
        for field in indlst:
            self.inx[field] = Index(self.db, self.dbHome, self.name, field)

# junk     self.dbindex=libtpshelve.open(dbEnv,"index",type='btree')
# junk     self.inx={}

# junk     for name in indlst:
# junk     	self.inx[name]=MyIndex(self.dbindex,name)

        if self.auto_journal:
            self.jou = journal.JournalDict(
                {}, self.jouHome + "/" + dbname + ".jou", 1)
            self.count = 0

        if self.auto_journal:
            if len(self.jou):
                self.start_backup()
                self.checkpoint()
                self.stop_backup()

        if auto_cache:
            self.load_cache()
            self.cached = 1

    # def next(self):
    #  return self.cursor("next")

    def newCursor(self, txn=None):
        if self.cached:
            return cacheCursor(self.cache)
        else:
            return self.db.cursor(txn)

    # This is not backward compatible
    def cursor(self, action, KeyOrValue=None):

        if not self.cursor_open and action != "open":
            self.cursor("open")

        if action == "open":
            self.t = self.db.txn()
            self.c = self.db.cursor(self.t)
            self.cursor_open = 1
            return

        if action == "close":
            self.c.close()
            self.t.commit()
            self.cursor_open = 0
            return

        if action == "first":
            return self.c.first()

        if action == "last":
            return self.c.last()

        if action == "next":
            return next(self.c)

        # The implementation of "len" is wrong!
        # this should really be replaced by the db_stat command
        if action == "len":
            pos, value = self.c.get()
            len = 0
            last, value = self.c.last()
            key, value = self.c.first()
            while key != last:
                key, val = next(self.c)
                len = len + 1
                self.c.set(pos)
            return len + 1

        if action == "has_key":
            pos, value = self.c.get()
            key, value = self.c.set(KeyOrValue)
            self.c.set(pos)
            if key:
                return 1
            else:
                return 0

        if action == "delete":
            if self.auto_journal:
                if (self.c.Key in self.jou) == 0:
                    self.jou[self.c.Key] = self.db[self.c.Key]  # was deepcopy
                del self.jou[self.c.Key]
            return self.c.delete()

        if action == "get":
            return self.c.set(KeyOrValue)

        if action == "update":
            if self.auto_journal:
                if 'db_flag' in KeyOrValue.keys():
                    del KeyOrValue['db_flag']
                self.jou[self.c.Key] = KeyOrValue  # was deepcopy
                self.count = self.count + 1
                if self.count > JOURNAL_LIMIT and backup_flag:
                    self.checkpoint()

            status = self.c.update(KeyOrValue)

            return status

    def keys(self):
        return self.db.keys()

    def status(self):
        try:  # to be backward compatible
            return self.db.status()
        except BaseException:  # in case self.db.status() was not implement
            Trace.log(e_errors.INFO, "self.db.status() was not implemented")
            return None

    def sync(self):  # Flush a database to stable storage
        return self.db.sync()

    def load_cache(self):
        c = self.db.cursor()
        k, v = c.first()
        while k:
            self.cache[k] = v
            k, v = next(c)
        c.close()

    def __len__(self):
        try:  # to be backward compatible
            return self.db.__len__()
        except BaseException:  # in case self.db.__len__() was not implemented
            c = self.db.cursor()
            last, val = c.last()
            key, val = c.first()
            len = 0
            while key != last:
                key, val = next(c)
                len = len + 1
            c.close()
            return len + 1

    def has_key(self, key):
        return key in self.db

    def __setitem__(self, key, value):
        if self.auto_journal:
            if 'db_flag' in value.keys():
                del value['db_flag']
            self.jou[key] = value  # was deepcopy
            self.count = self.count + 1
            if self.count > JOURNAL_LIMIT and backup_flag:
                self.checkpoint()

        t = self.db.txn()
        # check if this is an update
        try:
            v0 = self.db[key]
            for name in self.inx.keys():
                self.inx[name].delete(v0[name], key, t)
        except KeyError:
            # traceback.print_exc()
            pass
        except BaseException:
            traceback.print_exc()
            pass

        # take care of index

        for name in self.inx.keys():
            self.inx[name].insert(value[name], key, t)
        self.db[(key, t)] = value
        t.commit()

        if self.cached:
            self.cache[key] = value

    def __getitem__(self, key):
        if self.cached and key in self.cache.keys():
            return(self.cache[key])
        else:
            return self.db[key]

    def __delitem__(self, key):
        value = self.db[key]

        if self.auto_journal:
            if (key in self.jou) == 0:
                self.jou[key] = self.db[key]  # was deepcopy
            del self.jou[key]

        t = self.db.txn()
        # take care of index
        for name in self.inx.keys():
            self.inx[name].delete(value[name], key, t)
        del self.db[(key, t)]
        t.commit()
        if self.cached:
            del self.cache[key]
        if self.auto_journal:
            self.count = self.count + 1
            if self.count > JOURNAL_LIMIT and backup_flag:
                self.checkpoint()
# junk      for name in self.inx.keys():
# junk         del self.inx[name][(key,value[name])]

    def dump(self):
        t = self.db.txn()
        c = self.db.cursor(t)
        key, value = next(c)
        while key:
            print(repr(key) + ':' + repr(value))
            key, value = next(c)
        c.close()
        t.commit()

    def close(self):
        if self.auto_journal:
            self.jou.close()
        if self.cursor_open == 1:
            self.cursor("close")
        # take care of Index
        for i in self.inx.keys():
            self.inx[i].close()
        self.db.close()

    def checkpoint(self):
        #import regex,string
        if self.auto_journal:
            del self.jou
        Trace.log(
            e_errors.INFO,
            "Start checkpoint for " +
            self.name +
            " journal")
        cmd = "mv " + self.jouHome + "/" + self.name + ".jou " + \
            self.jouHome + "/" + self.name + ".jou." + \
            repr(time.time())
        os.system(cmd)
        if self.auto_journal:
            self.jou = journal.JournalDict(
                {}, self.jouHome + "/" + self.name + ".jou", 1)
        self.count = 0
        Trace.log(e_errors.INFO, "End checkpoint for " + self.name)

    def start_backup(self):
        global backup_flag
        backup_flag = 0
        Trace.log(e_errors.INFO, "Start backup for " + self.name)
        self.checkpoint()

    def stop_backup(self):
        global backup_flag
        backup_flag = 1
        Trace.log(e_errors.INFO, "End backup for " + self.name)

    # backup is a method of DbTable
    def backup(self):
        try:
            cwd = os.getcwd()
        except OSError:
            cwd = self.dbHome
        # os.chdir(self.dbHome)

        # Do not backup database files any more. Those would be done in
        # system's database backup.

        # cmd="tar cf "+self.name+".tar "+self.name
        # Trace.log(e_errors.INFO, repr(cmd))
        # os.system(cmd)

        os.chdir(self.jouHome)
        cmd = "tar rf " + self.dbHome + "/" + \
            self.name + ".tar" + " " + self.name + ".jou.*"
        Trace.log(e_errors.INFO, repr(cmd))
        os.system(cmd)
        cmd = "rm " + self.name + ".jou.*"
        Trace.log(e_errors.INFO, repr(cmd))
        os.system(cmd)
        os.chdir(cwd)

    # cross_check() cross check journal dictionary and database

    def cross_check(self):

        error = 0

        # check if the items in db has the same value of that
        # in journal dictionary

        for i in self.dict.keys():
            if i not in self:
                print('M> key(' + i + ') is not in database')
                error = error + 1
            elif repr(self.dict[i]) != repr(self.__getitem__(i)):
                print('C> database and journal disagree on key(' + i + ')')
                print('C>  journal[' + i + '] =', self.dict[i])
                print('C> database[' + i + '] =', self.__getitem__(i))
                error = error + 1
#        # check if the deleted items are still in db
#
#        for i in self.deletes:
#            if self.has_key(i):
#                print 'D> database['+i+'] should be deleted'
#                error = error + 1

        return error


def do_backup(name, dbHome, jouHome):
    cwd = os.getcwd()
#     interface.py should no longer be used, please use option.py
#     try:
#         dbHome = configuration_client.ConfigurationClient(\
#		(interface.default_host(),\
#		interface.default_port()), 3).get('database')['db_dir']
#
#     except:
#         dbHome = os.environ['ENSTORE_DIR']
    os.chdir(dbHome)
    cmd = "tar cf " + name + ".tar " + name
    Trace.log(e_errors.INFO, repr(cmd))
    os.system(cmd)
    os.chdir(jouHome)
    cmd = "tar rf " + dbHome + "/" + name + ".tar" + " " + name + ".jou.*"
    Trace.log(e_errors.INFO, repr(cmd))
    os.system(cmd)
    cmd = "rm " + name + ".jou.*"
    Trace.log(e_errors.INFO, repr(cmd))
    os.system(cmd)
    os.chdir(cwd)
