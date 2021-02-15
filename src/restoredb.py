#!/usr/bin/env python

"""
restoredb.py -- synchronize database with journals

It assumes the following:
[1] an enstoredb running at port 8888 locally
[2] a set of journal files in a path specified by sys.argv[1], or
    current directory, if sys.argv[1] is not specified.

It does the following:
[1] reconstruct some file and volume records from the journals
[2] assume such records are more reliable than that in database
[3] compare such records against database, if there is any discrepency,
    update database records using these.
"""
from __future__ import print_function

import pg
import edb
import os
import sys
import string
import pprint

# parameters

db_port = 8888
db_name = "enstoredb"


def ddiff(o1, o2):
    '''
ddiff(o1, o2) -- comparing two objects
            Complex objects, like lists and dictionaries, are
            compared recurrsively.

            Simple objects are compared by their text representation
            Truncating error may happen.
            This is on purpose so that internal time stamp, which is
            a float, will not be considered different from the same
            in journal file, which is a text representation and
            probably with truncated precision
    '''

    # different if of different types
    if not isinstance(o1, type(o2)):
        return 1

    # list?
    if isinstance(o1, type([])):
        if len(o1) != len(o2):
            return 1
        for i in range(0, len(o1)):
            if ddiff(o1[i], o2[i]):
                return 1
        return 0

    # dictionary?
    if isinstance(o1, type({})):
        # if len(o1) != len(o2):
        #	return 1
        for i in o1.keys():
            if ddiff(o1[i], o2[i]):
                return 1
        return 0

    # floating point (only time) is compared as int
    if isinstance(o1, type(1.1)):
        return int(o1) != int(o2)

    # for everything else
    return repr(o1) != repr(o2)


def load_journal(dir, name):
    jfile = name + '.jou'
    cmd = "ls -1 " + os.path.join(dir, jfile + ".*") + " 2>/dev/null"
    jfiles = map(string.strip, os.popen(cmd).readlines())
    if os.access(jfile, os.F_OK):
        jfiles.append(jfile)
    dict = {}
    deletes = []
    for jf in jfiles:
        try:
            f = open(jf, "r")
        except IOError:
            print(jf + ": not found")
            sys.exit(0)
        l = f.readline()
        while l:
            # is it an deletion?
            if l[:3] == "del":
                k = string.split(l, "'")[1]
                if k not in dict:
                    deletes.append(k)
                else:
                    del dict[k]
            else:
                exec(l[5:])
            l = f.readline()
        f.close()
    return dict, deletes


def cross_check(db, jou, deleted, fix=False):
    error = 0
    # check if the items in db has the same value of that
    # in journal dictionary
    for i in jou.keys():
        if i not in db:
            print("M> key('" + i + "') is not in database")
            error = error + 1
            if fix:
                db[i] = jou[i]
                print('F> database[' + i + '] =', repr(db[i]))
        elif ddiff(jou[i], db[i]):
            print("C> database and journal disagree on key('" + i + "')")
            print("C>  journal['" + i + "'] =", repr(jou[i]))
            print("C> database['" + i + "'] =", repr(db[i]))
            error = error + 1
            if fix:
                db[i] = jou[i]
                print('F> database[' + i + '] =', repr(db[i]))
            # debugging
            # pprint.pprint(jou[i])
            # pprint.pprint(db[i])
            # sys.exit(0)

    # check if the deleted items are still in db
    for i in deleted:
        if i in db:
            print("D> database['" + i + "'] should be deleted")
            error = error + 1
    return error


if __name__ == "__main__":
    # get journal directory
    if len(sys.argv) > 1:
        journal_dir = sys.argv[1]
    else:
        journal_dir = os.getcwd()

    # get journal files
    fj, fd = load_journal(journal_dir, "file")
    vj, vd = load_journal(journal_dir, "volume")

    # clean up file journal
    # ignore 'pnfsvid' and 'update'
    for i in fj:
        if 'pnfsvid' in fj[i]:
            del fj[i]['pnfsvid']
        if 'update' in fj[i]:
            del fj[i]['update']

    # get database connection
    db = pg.DB(dbname=db_name, user="enstore", port=db_port)
    # instantiate file DB and volume DB
    fdb = edb.FileDB(jou="/tmp", auto_journal=0, rdb=db)
    vdb = edb.VolumeDB(jou="/tmp", auto_journal=0, rdb=db)

    print("%4d   file records in journal, %4d deletion" % (len(fj), len(fd)))
    print("%4d volume records in journal, %4d deletion" % (len(vj), len(vd)))

    error = cross_check(fdb, fj, fd, fix=True) + \
        cross_check(vdb, vj, vd, fix=True)

    print("%4d error(s) detected and fixed" % (error))
