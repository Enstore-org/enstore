###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import os
import copy
import posix
import posixpath
import regex
import errno
import stat
import pwd
import grp
import string
import time
import fcntl
import regsub
import pprint

# enstore imports
import lockfile
try:
    import Devcodes # this is a compiled enstore module
except ImportError:
    print "Devcodes unavailable"

ENABLED = "enabled"
DISABLED = "disabled"
VALID = "valid"
INVALID =  "invalid"
UNKNOWN = "unknown"
EXISTS = "file exists"
DIREXISTS = "directory exists"
ERROR = -1

##############################################################################

class Pnfs:
    # initialize - we will be needing all these things soon, get them now
    def __init__(self,pnfsFilename,all=0,timeit=0):
        t1 = time.time()
        self.pnfsFilename = pnfsFilename
        (dir,file) = os.path.split(pnfsFilename)
        if dir == '':
            dir = '.'
        self.dir = dir
        self.file = file
        self.exists = UNKNOWN
        self.check_valid_pnfs_filename()
        self.pstatinfo()
        self.rmajor = 0
        self.rminor = 0
        self.get_bit_file_id()
        self.get_library()
        self.get_file_family()
        self.get_file_family_width()
        self.get_xreference()
        self.get_lastparked()
        self.get_id()
        if all:
            self.get_pnfs_info()
        if timeit != 0:
            print "pnfs__init__ dt:",time.time()-t1

    # list what is in the current object
    def dump(self):
        pprint.pprint(self.__dict__)

    ##########################################################################

    # simple test configuration
    def jon1(self):
        if self.valid == VALID:
            self.set_bit_file_id("1234567890987654321",123)
        else:
            raise errno.errorcode[errno.EINVAL],"pnfs.jon1: "\
                  +self.pnfsfile+" is an invalid pnfs filename"

    # simple test configuration
    def jon2(self):
        if self.valid == VALID:
            self.set_bit_file_id("1234567890987654321",45678)
            self.set_library("activelibrary")
            self.set_file_family("raw")
            self.set_file_family_width(2)
            self.pstatinfo()
        else:
            raise errno.errorcode[errno.EINVAL],"pnfs.jon1: "\
                  +self.pnfsfile+" is an invalid pnfs filename"

    ##########################################################################

    # check for the existance of a wormhole file called disabled
    # if this file exists, then the system is "off"
    def check_pnfs_enabled(self):
        if self.valid == VALID:
            try:
                os.stat(self.dir+'/.(config)(flags)/disabled')
            except os.error:
                if sys.exc_info()[1][0] == errno.ENOENT:
                    return ENABLED
                else:
                    raise sys.exc_info()[0],sys.exc_info()[1]
                f = open(self.dir+'/.(config)(flags)/disabled')
                why = f.readlines()
                f.close()
                return DISABLED+": "+why
        else:
            return INVALID

    # check if file is really part of pnfs file space
    def check_valid_pnfs_filename(self):
        try:
            f = open(self.dir+'/.(const)('+self.file+')','r')
            f.close()
            self.valid = VALID
        except:
            self.valid = INVALID

    ##########################################################################

    # create a new file or update its times
    def touch(self):
        if self.valid == VALID:
            t = int(time.time())
            try:
                os.utime(self.pnfsFilename,(t,t))
            except os.error:
                if sys.exc_info()[1][0] == errno.ENOENT:
                    f = open(self.pnfsFilename,'w')
                    f.close()
                else:
                    print "problem with pnfsFilename =",self.pnfsFilename
                    raise sys.exc_info()[0],sys.exc_info()[1]
            self.pstatinfo()
            self.get_id()

    # update the access/mod time of a file
    # this function also seems to flush the nfs cache
    def utime(self):
        if self.valid == VALID and self.exists == EXISTS:
            try:
                t = int(time.time())
                os.utime(self.pnfsFilename,(t,t))
            except os.error:
                print "can not utime:",sys.exc_info()[0],sys.exc_info()[1]
            self.pstatinfo()


    # delete a pnfs file including its metadata
    def rm(self):
        if self.valid == VALID and self.exists == EXISTS:
            self.writelayer(1,"")
            self.writelayer(2,"")
            self.writelayer(3,"")
            # It would be better to move the file to some trash space.
            # I don't know how right now.
            os.remove(self.pnfsFilename)
            self.exists = UNKNOWN
            #self.utime()
            self.pstatinfo()

    ##########################################################################

    # lock/unlock the file
    # this doesn't work - no nfs locks available
    def readlock(self):
        if self.valid == VALID and self.exists == EXISTS:
            try:
                f = open(self.pnfsFilename,'r')
            # if we can not open the file, we can't set the times either
            except:
                return

            if 0:
                try:
                    # I can't find these in python -
                    #  got them from /usr/include/sys/file.h
                    # LOCK_EX 2    /* Exclusive lock.  */
                    # LOCK_UN 8    /* Unlock.  */
                    # LOCK_NB 4    /* Don't block when locking.  */
                    fcntl.flock(f.fileno(),2+4)
                    fcntl.flock(f.fileno(),8)
                    print "locked/unlocked - worked, a miracle"
                except:
                    print "Could not lock or unlock "\
                          ,self.pnfsFilename,sys.exc_info()[1]

            if 0:
                try:
                    lockfile.readlock(f)
                    lockfile.unlock(f)
                    print "locked/unlocked - worked, a miracle"
                except:
                    print "Could not lock or unlock "\
                          ,self.pnfsFilename,sys.exc_info()[1]

            f.close()

    ##########################################################################

    # write a new value to the specified file layer (1-7)
    # the file needs to exist before you call this
    def writelayer(self,layer,value):
        if self.valid == VALID and self.exists == EXISTS:
            f = open(self.dir+'/.(use)('+repr(layer)+')('+self.file+')','w')
            f.write(value)
            f.close()
            #self.utime()
            self.pstatinfo()

    # read the value stored in the requested file layer
    def readlayer(self,layer):
        if self.valid == VALID and self.exists == EXISTS:
            f = open(self.dir+'/.(use)('+repr(layer)+')('+self.file+')','r')
            l = f.readlines()
            f.close()
            return l
        else:
            return INVALID

    ##########################################################################

    # write a new value to the specified tag
    # the file needs to exist before you call this
    # remember, tags are a propery of the directory, not of a file
    def writetag(self,tag,value):
        if self.valid == VALID:
            f = open(self.dir+'/.(tag)('+tag+')','w')
            f.write(value)
            f.close()

    # read the value stored in the requested tag
    def readtag(self,tag):
        if self.valid == VALID:
            f = open(self.dir+'/.(tag)('+tag+')','r')
            t = f.readlines()
            f.close()
            return t
        else:
            return INVALID

    ##########################################################################

    # get all the extra pnfs information
    def get_pnfs_info(self):
        self.get_const()
        self.get_id()
        self.get_nameof()
        self.get_parent()
        self.get_path()
        self.get_cursor()
        self.get_counters()

    # get the const info of the file, given the filename
    def get_const(self):
        if self.valid == VALID and self.exists == EXISTS:
            f = open(self.dir+'/.(const)('+self.file+')','r')
            self.const = f.readlines()
            f.close()
        else:
            self.const = UNKNOWN

    # get the numeric pnfs id, given the filename
    def get_id(self):
        if self.valid == VALID and self.exists == EXISTS:
            f = open(self.dir+'/.(id)('+self.file+')','r')
            i = f.readlines()
            f.close()
            self.id = regsub.sub("\012","",i[0])
        else:
            self.id = UNKNOWN

    # get the nameof information, given the id
    def get_nameof(self):
        if self.valid == VALID and self.exists == EXISTS:
            f = open(self.dir+'/.(nameof)('+self.id+')','r')
            self.nameof = f.readlines()
            f.close()
        else:
            self.nameof = UNKNOWN

    # get the parent information, given the id
    def get_parent(self):
        if self.valid == VALID and self.exists == EXISTS:
            f = open(self.dir+'/.(parent)('+self.id+')','r')
            self.parent = f.readlines()
            f.close()
        else:
            self.parent = UNKNOWN

    # get the total path of the id
    def get_path(self):
        x=1
    # get the cursor information
    def get_cursor(self):
        if self.valid == VALID and self.exists == EXISTS:
            f = open(self.dir+'/.(get)(cursor)','r')
            self.cursor = f.readlines()
            f.close()
        else:
            self.cursor = UNKNOWN

    # get the cursor information
    def get_counters(self):
        if self.valid == VALID and self.exists == EXISTS:
            f = open(self.dir+'/.(get)(counters)','r')
            self.counters = f.readlines()
            f.close()
        else:
            self.counters = UNKNOWN


    ##########################################################################

    # get the stat of file, or if non-existant, its directory
    def get_stat(self):
        if self.valid == VALID:
            # first the file itself
            try:
                self.pstat = os.stat(self.pnfsFilename)
                self.exists = EXISTS
            # if that fails, try the directory
            except os.error:
                if sys.exc_info()[1][0] == errno.ENOENT:
                    try:
                        self.pstat = os.stat(self.dir)
                        self.exists = DIREXISTS
                    except:
                        self.pstat = (ERROR,repr(sys.exc_info()[1])\
                                     ,"directory: "+self.dir)
                        self.exists = INVALID
                else:
                    self.pstat = (ERROR,repr(sys.exc_info()[1])\
                                 ,"file: "+self.pnfsFilename)
                    self.exists = INVALID
                    self.major,self.minor = (0,0)

        else:
            self.pstat = (ERROR,INVALID)
            self.exists = INVALID

    ##########################################################################

    def set_file_size(self,size):
        if self.valid == VALID and self.exists == EXISTS:
            if self.file_size != 0:
                try:
                    os.remove(self.dir+'/.(fset)('+self.file+')(size)')
                    #self.utime()
                    self.pstatinfo()
                except os.error:
                    print "enoent path taken again!"
                    if sys.exc_info()[1][0] == errno.ENOENT:
                        # maybe this works??
                        f = open(self.dir+'/.(fset)('\
                                 +self.file+')(size)('+repr(size)+')','w')
                        f.close()
                        self.utime()
                        self.pstatinfo()
                    else:
                        raise sys.exc_info()[0],sys.exc_info()[1]
                if self.file_size != 0:
                    print "can not set file size to 0 - oh well!"
            f = open(self.dir+'/.(fset)('+self.file+')(size)('\
                     +repr(size)+')','w')
            f.close()
            self.utime()
            self.pstatinfo()

    ##########################################################################

    # set a new mode for the existing file
    def chmod(self,mode):
        if self.valid == VALID and self.exists == EXISTS:
            os.chmod(self.pnfsFilename,mode)
            self.utime()
            self.pstatinfo()

    # change the ownership of the existing file
    def chown(self,uid,gid):
        if self.valid == VALID and self.exists == EXISTS:
            os.chown(self.pnfsFilename,uid,gid)
            self.utime()
            self.pstatinfo()

    ##########################################################################

    # store a new bit file id
    def set_bit_file_id(self,value,size=0):
        if self.valid == VALID:
            if self.exists == DIREXISTS:
                self.touch()
            self.writelayer(1,value)
            self.get_bit_file_id()
            if size != 0:
                self.set_file_size(size)

    # store place where we last parked the file
    def set_lastparked(self,value):
        if self.valid == VALID and self.exists == EXISTS:
            self.writelayer(2,value)
            self.get_lastparked()

    # store new info and transaction log
    def set_info(self,value):
        if self.valid == VALID and self.exists == EXISTS:
            self.writelayer(3,value)
            self.get_info()

    # store the cross-referencing data
    def set_xreference(self,volume,cookie):
        self.volmap_filename(volume,cookie)
        self.make_volmap_file()
        print "self.dump"
        self.dump()
        value=volume+'\012' + \
               cookie+'\012'+ \
               self.file_family+'\012' + \
               self.pnfsFilename+'\012' + \
               self.volume_file+'\012' + \
               self.id+'\012' + \
               self.volume_fileP.id+'\012'
        self.writelayer(4,value)
        self.get_xreference()
        self.fill_volmap_file()

    # get the bit file id
    def get_bit_file_id(self):
        if self.valid == VALID and self.exists == EXISTS:
            try:
                self.bit_file_id = self.readlayer(1)[0]
            except:
                self.bit_file_id = UNKNOWN
        else:
            self.bit_file_id = UNKNOWN

    # get the last parked layer
    def get_lastparked(self):
        if self.valid == VALID and self.exists == EXISTS:
            try:
                self.lastparked = self.readlayer(2)[0]
            except:
                self.lastparked = UNKNOWN
        else:
            self.lastparked = UNKNOWN

    # get the information layer
    def get_info(self):
        if self.valid == VALID and self.exists == EXISTS:
            try:
                self.info = self.readlayer(3)
            except:
                self.info = UNKNOWN
        else:
            self.info = UNKNOWN

    # get the cross reference layer
    def get_xreference(self):
        if self.valid == VALID and self.exists == EXISTS:
            try:
                xinfo = self.readlayer(4)
                self.volume   = regsub.sub("\012","",xinfo[0])
                self.cookie   = regsub.sub("\012","",xinfo[1])
                self.origff   = regsub.sub("\012","",xinfo[2])
                self.origname = regsub.sub("\012","",xinfo[3])
                self.mapfile  = regsub.sub("\012","",xinfo[3])
            except:
                self.volume   = UNKNOWN
                self.cookie   = UNKNOWN
                self.origff   = UNKNOWN
                self.origname = UNKNOWN
                self.mapfile  = UNKNOWN
        else:
            self.volume   = UNKNOWN
            self.cookie   = UNKNOWN
            self.origff   = UNKNOWN
            self.origname = UNKNOWN
            self.mapfile  = UNKNOWN

    ##########################################################################

    # store a new tape library tag
    def set_library(self,value):
        if self.valid == VALID :
            self.writetag("library",value)
            self.get_library()

    # get the tape library
    def get_library(self):
        if self.valid == VALID:
            try:
                self.library = self.readtag("library")[0]
            except:
                self.library = UNKNOWN
        else:
            self.library = UNKNOWN

    ##########################################################################

    # store a new file family tag
    def set_file_family(self,value):
        if self.valid == VALID:
            self.writetag("file_family",value)
            self.get_file_family()

    # get the file family
    def get_file_family(self):
        if self.valid == VALID:
            try:
                self.file_family = self.readtag("file_family")[0]
            except:
                pass
        else:
            self.file_family = UNKNOWN

    ##########################################################################

    # store a new file family width tag
    # this is the number of open files (ie simultaneous tapes) at one time
    def set_file_family_width(self,value):
        if self.valid == VALID:
            self.writetag("file_family_width",repr(value))
            self.get_file_family_width()

    # get the file family width
    def get_file_family_width(self):
        if self.valid == VALID:
            try:
                self.file_family_width = string.atoi(\
                    self.readtag("file_family_width")[0])
            except:
                self.file_family_width = ERROR
        else:
            self.file_family_width = ERROR

    ##########################################################################

    # update all the stat info on the file, or if non-existant, its directory
    def pstatinfo(self,update=1):
        if update:
            self.get_stat()
        self.pstat_decode()

        try:
            code_dict = Devcodes.MajMin(self.pnfsFilename)
        except:
            code_dict={"Major":0,"Minor":0}
        self.major = code_dict["Major"]
        self.minor = code_dict["Minor"]

        command="if test -w "+self.dir+"; then echo ok; else echo no; fi"
        writable = os.popen(command,'r').readlines()
        if "ok\012" == writable[0]:
            self.writable = ENABLED
        else:
            self.writable = DISABLED

    ##########################################################################

    # get the uid from the stat member
    def pstat_decode(self):
        if self.valid == VALID and self.pstat[0] != ERROR:
            try:
                self.uid = self.pstat[stat.ST_UID]
            except:
                self.uid = ERROR
            try:
                self.uname = pwd.getpwuid(self.uid)[0]
            except:
                self.uname = UNKNOWN
            try:
                self.gid = self.pstat[stat.ST_GID]
            except:
                self.gid = ERROR
            try:
                self.gname = grp.getgrgid(self.gid)[0]
            except:
                self.gname = UNKNOWN
            try:
                # always return mode as if it were a file, not directory, so
                #  it can use used in enstore cpio creation  (we will be
                #  creating a file in this directory)
                # real mode is available in self.stat for people who need it
                self.mode = (self.pstat[stat.ST_MODE] % 0777) | 0100000
                self.mode_octal = repr(oct(self.mode))
            except:
                self.mode = 0
                self.mode_octal = 0
            if self.exists == EXISTS:
                try:
                    self.file_size = self.pstat[stat.ST_SIZE]
                except:
                    self.file_size = ERROR
            else:
                self.file_size = ERROR

        else:
            self.uid = ERROR
            self.uname = UNKNOWN
            self.gid = ERROR
            self.gname = UNKNOWN
            self.mode = 0
            self.mode_octal = 0
            self.file_size = ERROR

##############################################################################

    # generate volmap directory name and filename
    # the volmap directory looks like:
    #         /pnfs/xxxx/volmap/origfilefamily/volumename/file_number_on_tape
    def volmap_filename(self,vol="",kookie=""):
        if self.valid == VALID and self.exists == EXISTS:

            # origff not set until xref layer is filled in, use current file
            #    family in this case.
            if self.origff!=UNKNOWN:
                ff = self.origff
            else:
                ff = self.file_family
            # volume not set until xref layer is filled in, has to be specified
            if self.volume!=UNKNOWN:
                volume = self.volume
            else:
                volume = vol
            # cookie  not set until xref layer is filled in, has to be specified
            if self.cookie!=UNKNOWN:
                cookie = self.cookie
            else:
                cookie = kookie

            # cookies are usually just the filenumber, but in some instances
            # they are byte oriented (offset,length). In these cases, just use
            # offset as the file number
            try:
                size = len(cookie)
                exec("(volfile,size)="+cookie)
            except:
                volfile = cookie

            dir_elements = string.split(self.dir,'/')
            self.voldir = '/'+dir_elements[1]+'/'+dir_elements[2]+'/volmap/'+ \
                          ff+'/'+volume
            # make the filename lexically sortable.  since this could be a byte offset,
            #     allow for 100 GB offsets
            self.volume_file = self.voldir+'/%12.12d'%volfile
        else:
            self.volume_file = UNKNOWN

    # create a duplicate entry in pnfs that is ordered by file number on tape
    def make_volmap_file(self):
        if self.volume_file!=UNKNOWN:
            if posixpath.exists(self.voldir) == 0:
                dir = ""
                dir_elements = string.split(self.voldir,'/')
                for element in dir_elements:
                    dir=dir+'/'+element
                    #print dir
                    if posixpath.exists(dir) == 0:
                        # try to make the directory - just bomb out if we fail
                        #   since we probably require user intervention to fix
                        os.mkdir(dir)

            # create the volume map file and set its size the same as main file
            self.volume_fileP = Pnfs(self.volume_file)
            self.volume_fileP.touch()
            self.volume_fileP.set_file_size(self.file_size)
            print "self.volume_fileP.dump"
            self.volume_fileP.dump()


    # file in the already existing volume map file
    def fill_volmap_file(self):
        if self.volume_file!=UNKNOWN:
            # now copy the appropriate layers to the volmap file
            for layer in [1,4]: # bfid and xref
                inlayer = self.readlayer(layer)
                value = ""
                for e in range(0,len(inlayer)):
                    value=value+inlayer[e]
                    self.volume_fileP.writelayer(layer,value)

            # protect it against accidental deletion - and give ownership to root.root
            os.chmod(self.volume_file,0644)  # disable write access except for owner
            os.chown(self.volume_file,0,0)   # make the owner root.root

    # retore the original entry based on info from the duplicate
    def restore_from_volmap(self):
        # create the original entry and set its size
        orig = pnfs(self.origname)
        orig.touch()
        orig.set_file_size(self.file_size)

        # now copy the appropriate layers to the volmap file
        for layer in [1,4]: # bfid and xref
            inlayer = self.readlayer(layer)
            value = ""
            for e in range(0,len(inlayer)):
                value=value+inlayer[e]
                orig.writelayer(layer,value)


##############################################################################

# this routine returns a list of (filenames,bit_file_ids) given a list of file
# numbers on a specified tape

def findfiles(mainpnfsdir,                  # directory above volmap directory
                                            #  zB: /pnfs/enstore or /pnfs/d0sam
              label,                        # tape label
              filenumberlist):              # list of files wanted (count from 0)
                                            #  zB: [1,2,3] or [8,45,31] or 19
    # use a unix find command to determine the volume directory
    command="find "+mainpnfsdir+\
             "/volmap -mindepth 1 -maxdepth 2 -name "+label+" -print"
    tape = os.popen(command,'r').readlines()
    if len(tape) == 0:
        return ("","")
    voldir = regsub.sub("\012","",tape[0])

    # get the list of files in the volume directory
    #   note that files are they are lexically sortable
    volfiles = posix.listdir(voldir)
    volfiles.sort()

    # create a sorted list of file number requests
    try:
        n = len(filenumberlist)
        files = copy.deepcopy(filenumberlist)
    except:
        files = [filenumberlist]
    files.sort()
    n = len(files)

    # now just loop over each request and return the original filename
    #  and the bfid to the user
    last = ""
    filenames = []
    bfids = []
    for i in range(0,n):
        if i == last:
            print "skipping duplicate request: ",i
            continue
        else:
            last = i
        v = pnfs(voldir+'/'+volfiles[files[i]])
        filenames.append(v.origname)
        bfids.append(v.bit_file_id)
    return (filenames,bfids)

##############################################################################
if __name__ == "__main__":

    import getopt

    # defaults
    test = 0
    status = 0
    info = 0
    file = ""
    list = 0
    restore = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["test","status","file=","list","restore=","verbose""help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist:
        if opt == "--test":
            test = 1
        elif opt == "--status":
            status = 1
        elif opt == "--file":
            info = 1
            file = value
        elif opt == "--restore":
            restore = 1
            file = value
        elif opt == "--list" or opt == "--verbose":
            list = 1
        elif opt == "--help":
            print "python",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    if info:
        p=pnfs(file,1,1)
        if list:
            p.dump()

    elif status:
        print "not yet"

    elif restore:
        p=pnfs(file)
        p.restore_from_volmap()

    elif test:

        base = "/pnfs/enstore/test2"
        count = 0
        for pf in base+"/"+repr(time.time()), "/impossible/path/test":
            count = count+1;
            if list: print ""
            if list:
                print "Self test from ",__name__," using file ",count,": ",pf

            p = pnfs(pf)

            e = p.check_pnfs_enabled()
            if list: print "enabled: ", e

            if p.valid == VALID:
                if count==2:
                    print "ERROR: File ",count\
                          ," is invalid - but valid flag is set"
                    continue
                p.jon1()
                p.get_pnfs_info()
                if list: p.dump()
                l = p.library
                f = p.file_family
                w = p.file_family_width
                i=p.bit_file_id
                s=p.file_size

                nv = "crunch"
                nvn = 222222
                if list: print ""
                if list: print "Changing to new values"

                p.set_library(nv)
                if p.library == nv:
                    if list: print " library changed"
                else:
                    print " ERROR: didn't change library tag: still is "\
                          ,p.library

                p.set_file_family(nv)
                if p.file_family == nv:
                    if list: print " file_family changed"
                else:
                    print " ERROR: didn't change file_family tag: still is "\
                          ,p.file_family

                p.set_file_family_width(nvn)
                if p.file_family_width == nvn:
                    if list: print " file_family_width changed"
                else:
                    print " ERROR: didn't change file_family_width tag: "\
                          +"still is ",p.file_family_width

                p.set_bit_file_id(nv,nvn)
                if p.bit_file_id == nv:
                    if list: print " bit_file_id changed"
                else:
                    print " ERROR: didn't change bit_file_id layer: still is "\
                          ,p.bit_file_id

                if p.file_size == nvn:
                    if list: print " file_size changed"
                else:
                    print " ERROR: didn't change file_size: still is "\
                          ,p.file_size

                if list: p.dump()
                if list: print ""
                if list: print "Restoring original values"

                p.set_library(l)
                if p.library == l:
                    if list: print " library restored"
                else:
                    print " ERROR: didn't restore library tag: still is "\
                          ,p.library

                p.set_file_family(f)
                if p.file_family == f:
                    if list: print " file_family restored"
                else:
                    print " ERROR: didn't restore file_family tag: still is "\
                          ,p.file_family

                p.set_file_family_width(w)
                if p.file_family_width == w:
                    if list: print " file_family_width restored"
                else:
                    print " ERROR: didn't restore file_family_width tag: "\
                          +"still is ",p.file_family_width

                p.set_bit_file_id(i,s)
                if p.bit_file_id == i:
                    if list: print " bit_file_id restored"
                else:
                    print " ERROR: didn't restore bit_file_id layer: "\
                          +"still is ",p.bit_file_id

                if p.file_size == s:
                    if list: print " file size restored"
                else:
                    print " ERROR: didn't restore file_size: still is "\
                          ,p.file_size

                if list: p.dump()
                p.rm()
                if p.exists != EXISTS:
                    if list: print p.pnfsFilename," deleted"
                else:
                    print "ERROR: could not delete ",p.pnfsFilename

            else:
                if count==2:
                    continue
                else:
                    print "ERROR: File ",count\
                          ," is valid - but invvalid flag is set"
                    print p.pnfsFilename, "file is not a valid pnfs file"

