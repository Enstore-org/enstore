#!/usr/bin/env python

# $Id$

""" This is a simple flat-file based database for holding counts of volumes assigned
    to specific storage group
"""        

import sys, os
import string
import errno
import Trace
import e_errors

verbose=0

class SGDb:

    
    def __init__(self, dbhome):
        self.base_dir = os.path.join(dbhome,'STORAGE_GROUPS')

    def dbdir_for_sg(self, library, storage_group):
        #we don't want too many files in the same dir, because unix gets
        # too slow when we get big directories
        dirname=os.path.join(self.base_dir, library)
        depth=0
        for c in storage_group[:-1]:
            if c not in string.digits and c not in string.letters:
                continue
            dirname=os.path.join(dirname,c)
            depth = depth+1
            if depth>5:
                break #too much nesting is also inefficient
        return dirname

    def dbfile_for_sg(self, library, storage_group):
        fname=os.path.join(self.dbdir_for_sg(library, storage_group), storage_group)
        return fname
        
    def get_sg_counter(self, library, storage_group):
        fname = self.dbfile_for_sg(library, storage_group)
        if fname:
            try:
                f = open(fname, 'r')
                count = string.atoi(f.read()[:-1])
                f.close()
            except IOError:
                count = -1
        else: count = -1
        return count

    def inc_sg_counter(self, library, storage_group, increment=1):
        dirname = self.dbdir_for_sg(library, storage_group)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        
        fname = self.dbfile_for_sg(library, storage_group)
        if fname:
            try:
                f = open(fname, 'r+')
                count = string.atoi(f.read())
                if increment < 0 and count < abs(increment):
                    increment = 0
                count = count + increment
                f.seek(0)
            except IOError:
                try:
                    f = open(fname, 'w+')
                except IOError, detail:
                    detmsg = "%s"%(detail,)
                    Trace.alarm(e_errors.ERROR,"IOError", {'IOError':detmsg,
                                                           'library': library,
                                                           'storage_group': storage_group,
                                                           'file': fname})
                    return
                count = increment
            try:
                f.seek(0)
                f.write(repr(count)+"\n")
                f.close()
            except IOError, detail:
                detmsg = "%s"%(detail,)
                Trace.alarm(e_errors.ERROR,"IOError", {'IOError':detmsg,
                                                       'library': library,
                                                       'storage_group': storage_group,
                                                       'file': fname})
        else:
            count = -1
        return count

    def delete_sg_counter(self, library, storage_group, force=0):
        counter = self.get_sg_counter(library, storage_group)
        if counter != 0 and not force: return
        try:
            os.remove(self.dbfile_for_sg(library, storage_group))
        except OSError:
            pass
    
def testit():
    dbhome='/tmp/junk'
    sg='TS'
    library = 'TESTLIB'
    db = SGDb(dbhome)
    print "increment SG counter", db.inc_sg_counter(library, sg)
    print "get SG counter", db.get_sg_counter(library, sg)
    print "decrement sg counter", db.inc_sg_counter(library, sg, increment=-1)
    print "get SG counter", db.get_sg_counter(library, sg)
    print "delete SG counter", db.delete_sg_counter(library, sg)

if __name__=='__main__':
    verbose=1
    testit()
    
    
