import sys
import os
import posixpath
import regex
import errno
import stat
import pwd
import string

enabled = "enabled"
disabled = "disabled"
valid = "valid"
invalid =  "file is not part of pnfs file system"
unknown = "unknown"
error = -1

class pnfs :
    # initialize - we will be needing all these things soon, get them now
    def __init__(self,pnfsFilename) :
	self.pnfsFilename = pnfsFilename
	(dir,file) = posixpath.split(pnfsFilename)
	self.dir = dir
	self.file = file
	self.valid = self.get_valid()
	self.statinfo()
	self.bit_file_id = self.get_bit_file_id()
	self.library = self.get_library()
	self.file_family = self.get_file_family()
	self.file_family_width = self.get_file_family_width()
	
    # list what is in the current object
    def dump(self) :
	print "Current object values:"
	print "  pnfsFilename: ",self.pnfsFilename
	print "  dir: ",self.dir
	print "  file: ",self.file
	print "  valid: ",self.valid
	print "  stat: ",self.stat
	print "  uid: ",self.uid
	print "  uname: ",self.uname
	print "  gid: ",self.gid
	print "  gname: ",self.gname
	print "  mode: %o" % self.mode
	print "  bit_file_id: ",self.bit_file_id
	print "  library: ",self.library
	print "  file_family: ",self.file_family
	print "  file_family_width: ",self.file_family_width

    def jon1(self) :
	self.set_bit_file_id("1234567890987654321")
	self.set_library("active")
	self.set_file_family("raw")
	self.set_file_family_width(2)

    #################################################################################

    # check for the existance of a wormhole file called disabled
    # if this file exists, then the system is "off"
    def enabled(self) :
	if self.valid != valid :
	    return invalid
        try :
            os.stat(self.dir+'/.(config)(flags)/disabled')
        except os.error :
            if sys.exc_value[0] == errno.ENOENT :
                return enabled
            else :
                raise sys.exc_type,sys.exc_value
        why=open(self.dir+'/.(config)(flags)/disabled').readlines()
        return disabled+": "+why

    # check if file is really part of pnfs file space
    def get_valid(self) :
	try :
	    open(self.dir+'/.(const)('+self.file+')','r')
	    self.valid = valid
	except :
	    self.valid = invalid
	return self.valid
    
    #################################################################################

    # write a new value to the specified file layer (1-7)
    def writelayer(self,layer,value) :
	if self.valid != valid :
	    return
        os.popen('touch '+self.pnfsFilename+' 2>&1','r').readlines()
        open(self.dir + '/.(use)(' + repr(layer) + ')(' + self.file + ')','w').write(value)


    # read the value stored in the requested file layer
    def readlayer(self,layer) :
	if self.valid != valid :
	    return invalid
        return open(self.dir+'/.(use)('+repr(layer)+')('+self.file+')','r').readlines()[0]

    #################################################################################

    # write a new value to the specified tag
    # remember, tags are a propery of the directory, not of a file
    def writetag(self,tag,value) :
	if self.valid != valid :
	    return
        open(self.dir+'/.(tag)('+tag+')','w').write(value)


    # read the value stored in the requested tag
    def readtag(self,tag) :
	if self.valid != valid :
	    return invalid
        return open(self.dir+'/.(tag)('+tag+')','r').readlines()[0]

    #################################################################################

    # return the stat of file, or if non-existant, its directory
    def get_stat(self) :
        try :
            self.stat = os.stat(self.pnfsFilename)
        except os.error :
            if sys.exc_value[0] == errno.ENOENT :
		try :
		    self.stat = os.stat(self.dir)
		except :
		    if sys.exc_value[0] == errno.ENOENT :
			self.stat = (error,repr(sys.exc_value))
		    else :
			raise sys.exc_type,sys.exc_value
            else :
                raise sys.exc_type,sys.exc_value
	return self.stat

    #################################################################################

    # store a new bit file id
    def set_bit_file_id(self,value) :
        self.writelayer(1,value)
	self.get_bit_file_id()
	self.statinfo()

    # return the bit file id
    def get_bit_file_id(self) :
	try : 
	    self.bit_file_id = self.readlayer(1)
	except :
	    self.bit_file_id = unknown
	return self.bit_file_id 

    #################################################################################

    # store a new tape library tag
    def set_library(self,value) :
	self.writetag("library",value)
	self.get_library()

    # return the tape library
    def get_library(self) :
	try :
	    self.library = self.readtag("library")
	except :
	    self.library = unknown
	return self.library

    #################################################################################

    # store a new file family tag
    def set_file_family(self,value) :
        self.writetag("file_family",value)
	self.get_file_family()

    # return the file family
    def get_file_family(self) :
	try :
	    self.file_family = self.readtag("file_family")
	except :
	    self.file_family = unknown
	return self.file_family

    #################################################################################

    # store a new file family width tag
    # this is the number of open files (ie simultaneous tapes) at one time
    def set_file_family_width(self,value) :
        self.writetag("file_family_width",repr(value))
	self.get_file_family_width()

    # return  the file family width
    def get_file_family_width(self) :
	try :
	    self.file_family_width = string.atoi(self.readtag("file_family_width"))
	except :
	    self.file_family_width = error
	return self.file_family_width
    
    #################################################################################

    # update all the stat info on the file, or if non-existant, its directory
    def statinfo(self) :
	self.get_stat()
	self.get_uid()
	self.get_uname()
	self.get_gid()
	self.get_gname()
	self.get_mode()

    # return the uid from the stat member
    def get_uid(self) :
	if self.stat[0] == error :
	    self.uid = error
	else :
	    try :
		self.uid = self.stat[stat.ST_UID]
	    except :
		self.uid = error
	return self.uid

    # return the username from the uid member
    def get_uname(self) :
	if self.stat[0] == error :
	    self.uname = unknown
	else :
	    try :
		self.uname = pwd.getpwuid(self.uid)[0]
	    except :
		self.uname = unknown
	return self.uname

    # return the gid from the stat member
    def get_gid(self) :
	if self.stat[0] == error :
	    self.gid = error
	else :
	    try :
		self.gid = self.stat[stat.ST_GID]
	    except :
		self.gid = error
	return self.gid

    # return the group name of the gid member
    def get_gname(self) :
	if self.stat[0] == error :
	    self.gname = unknown
	else :
	    try :
		self.gname = pwd.getgrgid(self.gid)[0]
	    except :
		self.gname = unknown
	return self.gname

    # return the mode of file, or if non-existant, its directory
    def get_mode(self) :
	if self.stat[0] == error :
	    self.mode = 0
	else :
	    try :
		self.mode = self.stat[stat.ST_MODE]
	    except :
		self.mode = 0
	return self.mode
	

#################################################################################

if __name__ == "__main__" :

    for pf in "/pnfs/user/test1/jon1","~/jon-crunch" :
	print
	print "Self test from ",__name__," using file ",pf

	p = pnfs(pf)
	p.dump()

	e = p.enabled()
	print "enabled: ", e

	if p.valid == valid :
	    l = p.library
	    f = p.file_family
	    w = p.file_family_width
	    i=p.bit_file_id
	    
	    nv = "crunch"
	    nvn = 222222
	    print "Changing to new values"
	    
	    p.set_library(nv)
	    if p.library == nv :
		print " library changed"
	    else :
		print " ERROR: didn't change library tag: still is ",p.library

	    p.set_file_family(nv)
	    if p.file_family == nv :
		print " file_family changed"
	    else :
		print " ERROR: didn't change file_family tag: still is ",p.file_family

	    p.set_file_family_width(nvn)
	    if p.file_family_width == nvn :
		print " file_family_width changed"
	    else :
		print " ERROR: didn't change file_family_width tag: still is ",p.file_family_width

	    p.set_bit_file_id(nv)
	    if p.bit_file_id == nv :
		print " bit_file_id changed"
	    else :
		print " ERROR: didn't change bit_file_id layer: still is ",p.bit_file_id

	    print "Restoring original values"

	    p.set_library(l)
	    if p.library == l :
		print " library restored"
	    else :
		print " ERROR: didn't restore library tag: still is ",p.library

	    p.set_file_family(f)
	    if p.file_family == f :
		print " file_family restored"
	    else :
		print " ERROR: didn't restore file_family tag: still is ",p.file_family

	    p.set_file_family_width(w)
	    if p.file_family_width == w :
		print " file_family_width restored"
	    else :
		print " ERROR: didn't restore file_family_width tag: still is ",p.file_family_width

	    p.set_bit_file_id(i)
	    if p.bit_file_id == i :
		print " bit_file_id restored"
	    else :
		print " ERROR: didn't restore bit_file_id layer: still is ",p.bit_file_id

	    p.statinfo()
	    p.dump()

