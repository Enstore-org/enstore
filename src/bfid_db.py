#!/usr/bin/env python

# $Id$

""" This is a real simple flat-file based database for holding lists of bit-file ID's.
        You can add records, or delete all records, but you can't delete or modify
        individual records.
"""        

import sys, os
import string
import errno

import checksum

BfidDbError = "BfidDbError"

verbose=0

def safe_atol(s):
    if s[-1]=='L':
        s=s[:-1]
    return string.atol(s)

class BfidDb:

    
    def __init__(self, dbhome):
        self.base_dir = os.path.join(dbhome,'BFIDS')

    def dbdir_for_volume(self,vol):
        #we don't want too many files in the same dir, because unix gets
        # too slow when we get big directories
        dirname=self.base_dir
        depth=0
        for c in vol[:-2]:
            if c not in string.digits and c not in string.letters:
                continue
            dirname=os.path.join(dirname,c)
            depth = depth+1
            if depth>5:
                break #too much nesting is also inefficient
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return dirname

    def dbfile_for_volume(self,vol):
        fname=os.path.join(self.dbdir_for_volume(vol),vol)
        return fname
        
    def delete_all_bfids(self, vol):
        fname=self.dbfile_for_volume(vol)
        if os.path.exists(fname):
            os.unlink(fname)

    def parse_summary(self, summary, vol):
        w=string.split(summary)
        if len(w) != 4:
            raise BfidDbError, "invalid summary %s for vol %s"%(summary,vol)
        try:
            n_entries=safe_atol(w[1])
            file_checksum=safe_atol(w[3])
        except:
            raise BfidDbError, "invalid summary %s for vol %s"%(summary,vol)
        return n_entries, file_checksum
        
    def get_all_bfids(self, vol):
        r=[]
        csum=0L
        fname=self.dbfile_for_volume(vol)
        f=open(fname,'r')
        lines=f.readlines()
        for line in lines[:-1]:
            line=string.strip(line)
            r.append(line)
            csum=checksum.adler32(csum,line,len(line))
        summary=lines[-1]
        n_entries,file_checksum = self.parse_summary(summary,vol)

        if n_entries != len(r):
            raise BfidDbError, "vol %s has %s entries, should have %s"%(
                vol,len(r),n_entries)
        if  file_checksum != csum:
            raise BfidDbError, "vol %s: checksum error: has %s, should have %s"%(
                vol,csum,file_checksum)
        return r

    def summary_line(self, n_entries, csum):
        return "\nentries: %s checksum: %s\n"%(n_entries,csum)
        


    def rename_volume(self, old_label, new_label):
        oldf = self.dbfile_for_volume(old_label)
        newf = self.dbfile_for_volume(new_label) #make sure directory exists
        try:
            os.rename(oldf, newf)
        except OSError, err:
            if err.errno==errno.EXDEV:  #if target is on a different device, try copying the file
                import shutil
                shutil.copyfile(oldf,newf)
                os.unlink(oldf)
            else: 
                raise OSError, err

    def init_dbfile(self, vol):
        fname = self.dbfile_for_volume(vol)
        f=open(fname,'w')
        f.write(self.summary_line(0,0L))
        f.close()
                
        
    def add_bfid(self, vol, bfid):
        r=[]
        csum=0L
        fname=self.dbfile_for_volume(vol)
        f=open(fname,'r+')
        f.seek(0,2)
        eof=f.tell()
        pos=eof-2

        if pos<=0:  #how can this be?
            raise BfidDbError, "vol %s: database file truncated"%(vol,)

        while pos>=0:
            pos=f.tell()
            c=f.read(1)
            if c=='\n':
                break
            else:
                f.seek(-2,1)
        if pos>0:
            pos=pos+1 #keep newlines except for bogus newline at beginning of file
        if verbose: print "pos=",pos
        summary = f.readline()
        if verbose: print summary
        n_entries, file_checksum=self.parse_summary(summary,vol)
        n_entries = n_entries+1
        csum = checksum.adler32(file_checksum, bfid, len(bfid))
        f.seek(pos,0)
        f.write(bfid)
        f.write(self.summary_line(n_entries, csum))
        f.close()
        
def testit():
    dbhome='/tmp/junk'
    db = BfidDb(dbhome)
    vol='TEST111'
    db.delete_all_bfids(vol)
    db.init_dbfile(vol)
    db.add_bfid(vol,'00000000L')
    db.add_bfid(vol,'00000001L')
    db.add_bfid(vol,'00000011L')
    db.add_bfid(vol,'00000011L')
    db.add_bfid(vol,'00000011L')
    db.add_bfid(vol,'00000011L')
    print db.get_all_bfids(vol)

if __name__=='__main__':
    verbose=1
    testit()
    
    
