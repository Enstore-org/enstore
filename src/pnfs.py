import sys
import os
import regex
import errno
import stat
import pwd
import grp
import string
import time
import fcntl
import regsub

enabled = "enabled"
disabled = "disabled"
valid = "valid"
invalid =  "invalid"
unknown = "unknown"
exists = "file exists"
direxists = "directory exists"
error = -1

class pnfs :
    # initialize - we will be needing all these things soon, get them now
    def __init__(self,pnfsFilename) :
        self.pnfsFilename = pnfsFilename
        (dir,file) =os.path.split(pnfsFilename)
        self.dir = dir
        self.file = file
        self.exists = unknown
        self.check_valid_pnfsFilename()
        self.statinfo()
        self.get_bit_file_id()
        self.get_library()
        self.get_file_family()
        self.get_file_family_width()

    # list what is in the current object
    def dump(self) :
        print "Current object values:"
        keys = self.__dict__.keys()
        keys.sort()
        for k in keys :
            if k == 'mode' :
                print " ",k," = ",oct(self.__dict__[k])
            else :
                print " ",k," = ",self.__dict__[k]

    #################################################################################

    # simple test configuration
    def jon1(self) :
        if self.valid == valid :
            self.touch()
            self.statinfo()
            self.set_bit_file_id("1234567890987654321",123)
            self.statinfo()
        else:
            raise self.pnfsFilename+" is invalid"

    # simple test configuration
    def jon2(self) :
        if self.valid == valid :
            self.set_bit_file_id("1234567890987654321",45678)
            self.set_library("active")
            self.set_file_family("raw")
            self.set_file_family_width(2)
            self.statinfo()
        else:
            raise self.pnfsFilename+" is invalid"

    #################################################################################

    # check for the existance of a wormhole file called disabled
    # if this file exists, then the system is "off"
    def check_pnfs_enabled(self) :
        if self.valid == valid :
            try :
                os.stat(self.dir+'/.(config)(flags)/disabled')
            except os.error :
                if sys.exc_info()[1][0] == errno.ENOENT :
                    return enabled
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]
                f = open(self.dir+'/.(config)(flags)/disabled')
                why = f.readlines()
                f.close()
                return disabled+": "+why
        else :
            return invalid

    # check if file is really part of pnfs file space
    def check_valid_pnfsFilename(self) :
        try :
            f = open(self.dir+'/.(const)('+self.file+')','r')
            self.const = f.readlines()
            f.close()
            self.valid = valid
        except :
            self.valid = invalid

    #################################################################################

    # create a new file or update its times
    def touch(self) :
        if self.valid == valid :
            t = int(time.time())
            try :
                os.utime(self.pnfsFilename,(t,t))
            except os.error :
                if sys.exc_info()[1][0] == errno.ENOENT :
                    f = open(self.pnfsFilename,'w')
                    f.close()
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]


    # update the access/mod time of a file
    # this function also seems to flush the nfs cache
    def utime(self) :
        if self.valid == valid :
            if 0 :
                try :
                    f = open(self.pnfsFilename,'r')
                except :
                    # if we can not open the file, we can't set the times either
                    return
                #try :
                    # I can't find these in python - got them from /usr/include/sys/file.h
                    # LOCK_EX 2    /* Exclusive lock.  */
                    # LOCK_UN 8    /* Unlock.  */
                    # LOCK_NB 4    /* Don't block when locking.  */
                    #fcntl.flock(f.fileno(),2+4)
                    #fcntl.flock(f.fileno(),8)
                    #print "locked/unlocked"
                #except :
                    #print "Could not lock or unlock ",self.pnfsFilename,sys.exc_info()[1]
                f.close()
            t = int(time.time())
            try :
                os.utime(self.pnfsFilename,(t,t))
            except os.error :
                pass

    # delete a pnfs file including its metadata
    def rm(self) :
        if self.valid == valid and self.exists == exists :
            self.writelayer(1,"")
            #>>>>>> It would be better to move the file to some trash space. I don't know how right now. <<<<<<<<<
            os.remove(self.pnfsFilename)
            self.exists = unknown
            self.statinfo()

    #################################################################################

    # write a new value to the specified file layer (1-7)
    # the file needs to exist before you call this
    def writelayer(self,layer,value) :
        if self.valid == valid and self.exists == exists :
            f = open(self.dir+'/.(use)('+repr(layer)+')('+self.file+')','w')
            f.write(value)
            f.close()

    # read the value stored in the requested file layer
    def readlayer(self,layer) :
        if self.valid == valid and self.exists == exists :
            self.utime()
            f = open(self.dir+'/.(use)('+repr(layer)+')('+self.file+')','r')
            l = f.readlines()[0]
            f.close()
            return l
        else :
            return invalid

    #################################################################################

    # write a new value to the specified tag
    # the file needs to exist before you call this
    # remember, tags are a propery of the directory, not of a file
    def writetag(self,tag,value) :
        if self.valid == valid :
            f = open(self.dir+'/.(tag)('+tag+')','w')
            f.write(value)
            f.close()

    # read the value stored in the requested tag
    def readtag(self,tag) :
        if self.valid == valid :
            self.utime()
            f = open(self.dir+'/.(tag)('+tag+')','r')
            t = f.readlines()[0]
            f.close()
            return t
        else :
            return invalid

    #################################################################################

    # get all the extra pnfs information
    def get_pnfs_info(self) :
        if self.valid == valid :
            self.get_id()
            self.get_showid()
            self.get_nameof()
            self.get_parent()
            self.get_cursor()
            self.get_counters()

    # get the numeric pnfs id of the file
    def get_id(self) :
        if self.valid == valid and self.exists == exists :
            f = open(self.dir+'/.(id)('+self.file+')','r')
            i = f.readlines()
            f.close()
            self.id = regsub.sub("\012","",i[0])
            return

    # get the showid information
    def get_showid(self) :
        if self.valid == valid and self.exists == exists :
            try:
                id = self.id
            except :
                self.get_id()
            f = open(self.dir+'/.(showid)('+self.id+')','r')
            self.showid = f.readlines()
            f.close()
            return

    # get the nameof information
    def get_nameof(self) :
        if self.valid == valid and self.exists == exists :
            try:
                id = self.id
            except :
                self.get_id()
            f = open(self.dir+'/.(nameof)('+self.id+')','r')
            self.nameof = f.readlines()
            f.close()
            return

    # get the showid information
    def get_parent(self) :
        if self.valid == valid and self.exists == exists :
            try:
                id = self.id
            except :
                self.get_id()
            f = open(self.dir+'/.(parent)('+self.id+')','r')
            self.parent = f.readlines()
            f.close()
            return

    # get the cursor information
    def get_cursor(self) :
        if self.valid == valid :
            try:
                id = self.id
            except :
                self.get_id()
            f = open(self.dir+'/.(get)(cusor)','r')
            self.cursor = f.readlines()
            f.close()
            return

    # get the cursor information
    def get_counters(self) :
        if self.valid == valid :
            try:
                id = self.id
            except :
                self.get_id()
            f = open(self.dir+'/.(get)(counters)','r')
            self.counters = f.readlines()
            f.close()
            return

    #################################################################################

    # get the stat of file, or if non-existant, its directory
    def get_stat(self) :
        if self.valid == valid :
            try :
                self.utime()
                self.stat = os.stat(self.pnfsFilename)
                self.exists = exists
                #print "stat-file: ",self.pnfsFilename,": ",self.stat
            except os.error :
                if sys.exc_info()[1][0] == errno.ENOENT :
                    try :
                        self.stat = os.stat(self.dir)
                        self.exists = direxists
                        #print "stat-dir: ",self.dir,": ",self.stat
                    except :
                        self.stat = (error,repr(sys.exc_info()[1]),"directory: "+self.dir)
                        self.exists = invalid
                        #print "stat-fail: on file and dir failed ",self.pnfsFilename," ",self.dir," ",self.stat
                else :
                    self.stat = (error,repr(sys.exc_info()[1]),"file: "+self.pnfsFilename)
                    self.exists = invalid
                    #print "stat-fail: on file failed ",self.pnfsFilename," ",self.stat
        else :
            self.stat = (error,invalid)
            self.exists = invalid
            #print "stat-fail: invalid file ",self.pnfsFilename," ",self.stat

    #################################################################################

    # set a new file size
    # the file needs to exist before you call this
    # you can't change the file size once you set it
    def set_file_size(self,size) :
        if self.valid == valid and self.exists == exists :
            self.utime()
            f = open(self.dir+'/.(id)('+self.file+')','r')
            i = f.readlines()
            f.close()
            id = regsub.sub("\012","",i[0])
            f = open(self.dir+'/.(showid)('+id+')','r')
            sid = f.readlines()
            f.close()
            if self.file_size != 0 :
                try :
                    os.remove(self.dir+'/.(fset)('+self.file+')(size)')
                except os.error :
                    if sys.exc_info()[1][0] == errno.ENOENT :
                        print "failed to remove size attribute"
                        pass
                    else :
                        raise sys.exc_info()[0],sys.exc_info()[1]
            f = open(self.dir+'/.(showid)('+id+')','r')
            sid = f.readlines()
            f.close()
            f = open(self.dir+'/.(fset)('+self.file+')(size)('+repr(size)+')','w')
            f.close()
            f = open(self.dir+'/.(showid)('+id+')','r')
            sid = f.readlines()
            f.close()
            self.utime()
            self.statinfo()

    # get the size of the file from the stat member
    # this routine does not call stat - use statinfo to get updated info
    def get_file_size(self) :
        if self.valid == valid and self.exists == exists :
            try :
                self.file_size = self.stat[stat.ST_SIZE]
            except :
                self.file_size = error
        else :
            self.file_size = error

    #################################################################################

    # set a new mode for the existing file
    def chmod(self,mode) :
        if self.valid == valid and self.exists == exists :
            chmod(self.pnfsFilename,mode)
            self.statinfo()

    # get the mode of stat member
    # this routine does not call stat - use statinfo to get updated info
    def get_mode(self) :
        if self.stat[0] != error :
            try :
                self.mode = self.stat[stat.ST_MODE]
            except :
                self.mode = 0
        else :
            self.mode = 0


    #################################################################################
    # change the ownership of the existing file
    def chown(self,uid,gid) :
        if self.valid == valid and self.exists == exists :
            chown(self.pnfsFilename,uid,gid)
            self.statinfo()

    #################################################################################

    # store a new bit file id
    def set_bit_file_id(self,value,size=0) :
        if self.valid == valid :
            if self.exists == direxists :
                self.touch()
            self.writelayer(1,value)
            self.get_bit_file_id()
            if size != 0 :
                self.set_file_size(size)

    # get the bit file id
    def get_bit_file_id(self) :
        if self.valid == valid and self.exists == exists :
            try :
                self.bit_file_id = self.readlayer(1)
            except :
                self.bit_file_id = unknown
        else :
            self.bit_file_id = unknown


    #################################################################################

    # store a new tape library tag
    def set_library(self,value) :
        if self.valid == valid  :
            self.writetag("library",value)
            self.get_library()

    # get the tape library
    def get_library(self) :
        if self.valid == valid :
            try :
                self.library = self.readtag("library")
            except :
                self.library = unknown
        else :
            self.library = unknown

    #################################################################################

    # store a new file family tag
    def set_file_family(self,value) :
        if self.valid == valid :
            self.writetag("file_family",value)
            self.get_file_family()

    # get the file family
    def get_file_family(self) :
        if self.valid == valid :
            try :
                self.file_family = self.readtag("file_family")
            except :
                pass
        else:
            self.file_family = unknown

    #################################################################################

    # store a new file family width tag
    # this is the number of open files (ie simultaneous tapes) at one time
    def set_file_family_width(self,value) :
        if self.valid == valid :
            self.writetag("file_family_width",repr(value))
            self.get_file_family_width()

    # get the file family width
    def get_file_family_width(self) :
        if self.valid == valid :
            try :
                self.file_family_width = string.atoi(self.readtag("file_family_width"))
            except :
                self.file_family_width = error
        else :
            self.file_family_width = error

    #################################################################################

    # update all the stat info on the file, or if non-existant, its directory
    def statinfo(self) :
        self.get_stat()
        self.get_uid()
        self.get_uname()
        self.get_gid()
        self.get_gname()
        self.get_mode()
        self.get_file_size()

    #################################################################################

    # get the uid from the stat member
    def get_uid(self) :
        if self.stat[0] != error :
            try :
                self.uid = self.stat[stat.ST_UID]
            except :
                self.uid = error
        else :
            self.uid = error

    # get the username from the uid member
    def get_uname(self) :
        if self.stat[0] != error :
            try :
                self.uname = pwd.getpwuid(self.uid)[0]
            except :
                self.uname = unknown
        else :
            self.uname = unknown

    #################################################################################

    # get the gid from the stat member
    def get_gid(self) :
        if self.stat[0] != error :
            try :
                self.gid = self.stat[stat.ST_GID]
            except :
                self.gid = error
        else :
            self.gid = error

    # get the group name of the gid member
    def get_gname(self) :
        if self.stat[0] != error :
            try :
                self.gname = grp.getgrgid(self.gid)[0]
            except :
                self.gname = unknown
        else :
            self.gname = unknown

    #################################################################################




#################################################################################

if __name__ == "__main__" :

    list = 0
    base = "/pnfs/user/test1"
    count = 0
    for pf in base+"/"+repr(time.time()), "/impossible/path/test" :
        count = count+1;
        if list : print ""
        if list : print "Self test from ",__name__," using file ",count,": ",pf

        p = pnfs(pf)

        e = p.check_pnfs_enabled()
        if list : print "enabled: ", e

        if p.valid == valid :
            if count==2 :
                print "ERROR: File ",count," is invalid - but valid flag is set"
                continue
            p.jon1()
	    p.get_pnfs_info()
            if list : p.dump()
            l = p.library
            f = p.file_family
            w = p.file_family_width
            i=p.bit_file_id
            s=p.file_size

            nv = "crunch"
            nvn = 222222
            if list : print ""
            if list : print "Changing to new values"

            p.set_library(nv)
            if p.library == nv :
                if list : print " library changed"
            else :
                print " ERROR: didn't change library tag: still is ",p.library

            p.set_file_family(nv)
            if p.file_family == nv :
                if list : print " file_family changed"
            else :
                print " ERROR: didn't change file_family tag: still is ",p.file_family

            p.set_file_family_width(nvn)
            if p.file_family_width == nvn :
                if list : print " file_family_width changed"
            else :
                print " ERROR: didn't change file_family_width tag: still is ",p.file_family_width

            p.set_bit_file_id(nv,nvn)
            if p.bit_file_id == nv :
                if list : print " bit_file_id changed"
            else :
                print " ERROR: didn't change bit_file_id layer: still is ",p.bit_file_id

            if p.file_size == nvn :
                if list : print " file_size changed"
            else :
                print " ERROR: didn't change file_size: still is ",p.file_size

            if list : p.dump()
            if list : print ""
            if list : print "Restoring original values"

            p.set_library(l)
            if p.library == l :
                if list : print " library restored"
            else :
                print " ERROR: didn't restore library tag: still is ",p.library

            p.set_file_family(f)
            if p.file_family == f :
                if list : print " file_family restored"
            else :
                print " ERROR: didn't restore file_family tag: still is ",p.file_family

            p.set_file_family_width(w)
            if p.file_family_width == w :
                if list : print " file_family_width restored"
            else :
                print " ERROR: didn't restore file_family_width tag: still is ",p.file_family_width

            p.set_bit_file_id(i,s)
            if p.bit_file_id == i :
                if list : print " bit_file_id restored"
            else :
                print " ERROR: didn't restore bit_file_id layer: still is ",p.bit_file_id

            if p.file_size == s :
                if list : print " file size restored"
            else :
                print " ERROR: didn't restore file_size: still is ",p.file_size

            if list : p.dump()
            p.rm()
            if p.exists != exists :
                if list : print p.pnfsFilename," deleted"
            else :
                print "ERROR: could not delete ",p.pnfsFilename

        else :
            if count==2 :
                continue
            else :
                print "ERROR: File ",count," is valid - but invvalid flag is set"
            print p.pnfsFilename, "file is not a valid pnfs file"
