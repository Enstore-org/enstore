###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import os
import errno
import stat
import pwd
import grp
import string
import time
import fcntl


# enstore imports
import Trace
import lockfile
import e_errors
try:
    import Devcodes # this is a compiled enstore module
except ImportError:
    Trace.log(e_errors.INFO, "Devcodes unavailable")
import interface

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
        self.print_id = "PNFS"
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
        self.get_file_family_wrapper()
        self.get_file_family_width()
        self.get_xreference()
        self.get_lastparked()
        self.get_id()
        if all:
            self.get_pnfs_info()
        if timeit != 0:
            Trace.log(e_errors.INFO, "pnfs__init__ dt: "+time.time()-t1)

    # list what is in the current object
    def dump(self):
        Trace.trace(14, repr(self.__dict__))

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
        if self.valid != VALID:
            return INVALID
        try:
            os.stat(self.dir+'/.(config)(flags)/disabled')
        except os.error, msg:
            if msg.errno == errno.ENOENT:
                return ENABLED
            else:
                raise os.error,msg
        f = open(self.dir+'/.(config)(flags)/disabled','r')
        why = f.readlines()
        reason = string.replace(why[0],'\n','')
        f.close()
        return DISABLED+": "+reason


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
        if self.valid != VALID:
            return
        t = int(time.time())
        try:
            os.utime(self.pnfsFilename,(t,t))
        except os.error, msg:
            if msg.errno == errno.ENOENT:
                f = open(self.pnfsFilename,'w')
                f.close()
            else:
                Trace.log(e_errors.INFO, "problem with pnfsFilename = "+ 
                                   self.pnfsFilename)
                raise os.error,msg
        self.pstatinfo()
        self.get_id()

    # update the access/mod time of a file
    # this function also seems to flush the nfs cache
    def utime(self):
        if self.valid != VALID or self.exists != EXISTS:
            return
        try:
            t = int(time.time())
            os.utime(self.pnfsFilename,(t,t))
        except os.error, msg:
            Trace.log(e_errors.INFO, "can not utime: %s %s"%(os.error,msg))
        self.pstatinfo()


    # delete a pnfs file including its metadata
    def rm(self):
        if self.valid != VALID or self.exists != EXISTS:
            return
        self.writelayer(1,"")
        self.writelayer(2,"")
        self.writelayer(3,"")
        self.writelayer(4,"")
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
        if self.valid != VALID or self.exists != EXISTS:
            return
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
                Trace.log(e_errors.INFO, "locked/unlocked - worked, a miracle")
            except:
                exc,msg,tb=sys.exc_info()
                Trace.log(e_errors.INFO, "Could not lock or unlock %s: %s"%
                          (self.pnfsFilename, msg))

        if 0:
            try:
                lockfile.readlock(f)
                lockfile.unlock(f)
                Trace.log(e_errors.INFO, "locked/unlocked - worked, a miracle")
            except:
                exc,msg,tb=sys.exc_info()
                Trace.log(e_errors.INFO, "Could not lock or unlock %s: %s"%
                          (self.pnfsFilename, msg))

        f.close()

    ##########################################################################

    # write a new value to the specified file layer (1-7)
    # the file needs to exist before you call this
    def writelayer(self,layer,value):
        if self.valid != VALID or self.exists != EXISTS:
            return
        fname = "%s/.(use)(%s)(%s)"%(self.dir,layer,self.file)
        f = open(fname,'w')
        if type(value)!=type(''):
            value=str(value)
        f.write(value)
        f.close()
        #self.utime()
        self.pstatinfo()

    # read the value stored in the requested file layer
    def readlayer(self,layer):
        if self.valid != VALID or self.exists != EXISTS:
            return INVALID
        fname = "%s/.(use)(%s)(%s)"%(self.dir,layer,self.file)
        f = open(fname,'r')
        l = f.readlines()
        f.close()
        return l


    ##########################################################################

    # write a new value to the specified tag
    # the file needs to exist before you call this
    # remember, tags are a propery of the directory, not of a file
    def writetag(self,tag,value):
        if self.valid != VALID:
            return
        if type(value) != type(''):
            value=str(value)
        fname = "%s/.(tag)(%s)"%(self.dir,tag)
        f = open(fname,'w')
        f.write(value)
        f.close()

    # read the value stored in the requested tag
    def readtag(self,tag):
        if self.valid != VALID:
            return INVALID
        fname = "%s/.(tag)(%s)"%(self.dir,tag)
        f = open(fname,'r')
        t = f.readlines()
        f.close()
        return t

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
        if self.valid != VALID or self.exists != EXISTS:
            self.const = UNKNOWN
            return
        fname ="%s/.(const)(%s)"%(self.dir,self.file)
        f=open(fname,'r')
        self.const = f.readlines()
        f.close()


    # get the numeric pnfs id, given the filename
    def get_id(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.id = UNKNOWN
            return
        fname = "%s/.(id)(%s)"%(self.dir,self.file)
        f = open(fname,'r')
        i = f.readlines()
        f.close()
        self.id = string.replace(i[0],'\n','')


    # get the nameof information, given the id
    def get_nameof(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.nameof = UNKNOWN
            return
        fname = "%s/.(nameof)(%s)"%(self.dir,self.id)
        f = open(fname,'r')
        self.nameof = f.readlines()
        f.close()

    # get the parent information, given the id
    def get_parent(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.parent = UNKNOWN
            return
        fname = "%s/.(parent)(%s)"%(self.dir,self.id)
        f = open(fname,'r')
        self.parent = f.readlines()
        f.close()

    # get the total path of the id
    def get_path(self):
        # not implemented
        pass

    # get the cursor information
    def get_cursor(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.cursor = UNKNOWN
            return
        f = open(self.dir+'/.(get)(cursor)','r')
        self.cursor = f.readlines()
        f.close()

    # get the cursor information
    def get_counters(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.counters = UNKNOWN
            return
        fname = "%s/.(get)(counters)"%(self.dir,)
        f=open(fname,'r')
        self.counters = f.readlines()
        f.close()

    ##########################################################################

    # get the stat of file, or if non-existant, its directory
    def get_stat(self):
        if self.valid != VALID:
            self.pstat = (ERROR,INVALID)
            self.exists = INVALID
            return
            # first the file itself
        try:
            self.pstat = os.stat(self.pnfsFilename)
            self.exists = EXISTS
        # if that fails, try the directory
        except os.error, msg:
            if msg.errno == errno.ENOENT:
                try:
                    self.pstat = os.stat(self.dir)
                    self.exists = DIREXISTS
                except:
                    self.pstat = (ERROR,str(msg),"directory: %s"%(self.dir,))
                    self.exists = INVALID
            else:
                self.pstat = (ERROR,str(msg),"file: %s"%(self.pnfsFilename,))
                self.exists = INVALID
                self.major,self.minor = (0,0)


    ##########################################################################

    def set_file_size(self,size):
        if self.valid != VALID or self.exists != EXISTS:
            return
        if self.file_size != 0:
            try:
                fname="%s/.(fset)(%s)(size)"%(self.dir,self.file)
                os.remove(fname)
                #self.utime()
                self.pstatinfo()
            except os.error, msg:
                Trace.log(e_errors.INFO, "enoent path taken again!")
                if msg.errno == errno.ENOENT:
                    # maybe this works??
                    fname = "%s/.(fset)(%s)(size)(%s)"%(self.dir,self.file,size)
                    f = open(fname,'w')
                    f.close()
                    self.utime()
                    self.pstatinfo()
                else:
                    raise os.error, msg
            if self.file_size != 0:
                Trace.log(e_errors.INFO, "can not set file size to 0 - oh well!")
        fname = "%s/.(fset)(%s)(size)(%s)"%(self.dir,self.file,size)
        f = open(fname,'w')
        f.close()
        self.utime()
        self.pstatinfo()

    ##########################################################################

    # set a new mode for the existing file
    def chmod(self,mode):
        if self.valid != VALID or self.exists != EXISTS:
            return
        os.chmod(self.pnfsFilename,mode)
        self.utime()
        self.pstatinfo()

    # change the ownership of the existing file
    def chown(self,uid,gid):
        if self.valid != VALID or self.exists != EXISTS:
            return
        os.chown(self.pnfsFilename,uid,gid)
        self.utime()
        self.pstatinfo()

    ##########################################################################

    # store a new bit file id
    def set_bit_file_id(self,value,size=0):
        if self.valid != VALID:
            return
        if self.exists == DIREXISTS:
            self.touch()
        self.writelayer(1,value)
        self.get_bit_file_id()
        if size:
            self.set_file_size(size)

    # store place where we last parked the file
    def set_lastparked(self,value):
        if self.valid != VALID or self.exists != EXISTS:
            return
        self.writelayer(2,value)
        self.get_lastparked()

    # store new info and transaction log
    def set_info(self,value):
        return ##XXX disabled function
        if self.valid != VALID or self.exists != EXISTS:
            self.writelayer(3,value)
            self.get_info()

    # store the cross-referencing data
    def set_xreference(self,volume,location_cookie,size):
        Trace.trace(11,'pnfs.set_xref %s %s %s'%(volume,location_cookie,size))
        self.volmap_filename(volume,location_cookie)
        Trace.trace(11,'making volume_file=%s'%(self.volume_file,))
        self.make_volmap_file()
        value = (9*"%s\n")%(volume,
                            location_cookie,
                            size,
                            self.file_family,
                            self.pnfsFilename,
                            self.volume_file,
                            self.id,
                            self.volume_fileP.id,
                            self.bit_file_id)
        Trace.trace(11,'value='+value)
        self.writelayer(4,value)
        self.get_xreference()
        self.fill_volmap_file()

    # get the bit file id
    def get_bit_file_id(self):
        self.bit_file_id = UNKNOWN
        if self.valid != VALID or self.exists != EXISTS:
            return
        try:
            self.bit_file_id = self.readlayer(1)[0]
        except:
            self.log_err("get_bit_file_id")


    # get the last parked layer
    def get_lastparked(self):
        self.lastparked = UNKNOWN
        if self.valid != VALID or self.exists != EXISTS:
            return
        try:
            self.lastparked = self.readlayer(2)[0]
        except:
            self.log_err("get_lastparked")


    # get the information layer
    def get_info(self):
        self.info = UNKNOWN
        if self.valid != VALID or self.exists != EXISTS:
            return
        try:
            self.info = self.readlayer(3)
        except:
            self.log_err("get_info")


    # get the cross reference layer
    def get_xreference(self):
        (self.volume, self.location_cookie, self.size,
         self.origff, self.origname, self.mapfile) = (UNKNOWN,)*6
        if self.valid == VALID and self.exists == EXISTS:
            try:
                xinfo = self.readlayer(4)
                xinfo = map(string.strip,xinfo)
                (self.volume, self.location_cookie, self.size,
                 self.origff, self.origname, self.mapfile) = xinfo
            except:
                self.log_err("get_xreference")


    ##########################################################################

    # store a new tape library tag
    def set_library(self,value):
        if self.valid != VALID :
            return
        self.writetag("library",value)
        self.get_library()

    # get the tape library
    def get_library(self):
        self.library = UNKNOWN
        if self.valid != VALID:
            return
        try:
            self.library = self.readtag("library")[0]
        except:
            self.log_err("get_library")


    ##########################################################################

    # store a new file family tag
    def set_file_family(self,value):
        if self.valid != VALID:
            return
        self.writetag("file_family",value)
        self.get_file_family()

    # get the file family
    def get_file_family(self):
        self.file_family = UNKNOWN
        if self.valid != VALID:
            return
        try:
            self.file_family = self.readtag("file_family")[0]
        except:
            self.log_err("get_file_family")


    ##########################################################################

    # store a new file family wrapper tag
    def set_file_family_wrapper(self,value):
        if self.valid != VALID:
            return
        self.writetag("file_family_wrapper",value)
        self.get_file_family_wrapper()

    # get the file family
    def get_file_family_wrapper(self):
        self.file_family_wrapper = UNKNOWN
        if self.valid != VALID:
            return
        try:
            self.file_family_wrapper = self.readtag("file_family_wrapper")[0]
        except:
            self.log_err("get_file_family_wrapper")

    ##########################################################################

    # store a new file family width tag
    # this is the number of open files (ie simultaneous tapes) at one time
    def set_file_family_width(self,value):
        if self.valid != VALID:
            return
        self.writetag("file_family_width",value)
        self.get_file_family_width()

    # get the file family width
    def get_file_family_width(self):
        self.file_family_width = ERROR
        if self.valid != VALID:
            return
        try:
            self.file_family_width = string.atoi(
                self.readtag("file_family_width")[0])
        except:
            self.log_err("get_file_family_width")


    ##########################################################################

    # update all the stat info on the file, or if non-existent, its directory
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

        if os.access(self.dir,os.W_OK):
            self.writable = ENABLED
        else:
            self.writable = DISABLED            



    ##########################################################################

    # get the uid from the stat member
    def pstat_decode(self):
        self.uid = ERROR
        self.uname = UNKNOWN
        self.gid = ERROR
        self.gname = UNKNOWN
        self.mode = 0
        self.mode_octal = 0
        self.file_size = ERROR
        if self.valid != VALID or self.pstat[0] == ERROR:
            return

        try:
            self.uid = self.pstat[stat.ST_UID]
        except:
            self.log_err("pstat_decode uid")
        try:
            self.uname = pwd.getpwuid(self.uid)[0]
        except:
            self.log_err("pstat_decode uid")
        try:
            self.gid = self.pstat[stat.ST_GID]
        except:
            self.log_err("pstat_decode gid")
        try:
            self.gname = grp.getgrgid(self.gid)[0]
        except:
            self.log_err("pstat_decode gname")
        try:
            # always return mode as if it were a file, not directory, so
            #  it can use used in enstore cpio creation  (we will be
            #  creating a file in this directory)
            # real mode is available in self.stat for people who need it
            self.mode = (self.pstat[stat.ST_MODE] % 0777) | 0100000
            self.mode_octal = str(oct(self.mode))
        except:
            self.log_err("pstat_decode mode")
            self.mode = 0
            self.mode_octal = 0
        if self.exists == EXISTS:
            try:
                self.file_size = self.pstat[stat.ST_SIZE]
            except:
                self.log_err("pstat_decode file_size")


##############################################################################

    # generate volmap directory name and filename
    # the volmap directory looks like:
    #         /pnfs/xxxx/volmap/origfilefamily/volumename/file_number_on_tape
    def volmap_filename(self,vol="",cookie=""):
        self.volume_file = UNKNOWN
        if self.valid != VALID or self.exists != EXISTS:
            return

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
        if self.location_cookie!=UNKNOWN:
            cookie = self.location_cookie

        dir_elements = string.split(self.dir,'/')
        if dir_elements[1] != "pnfs":
            Trace.trace(6,'bad filename for - no pnfs as first element'+self.file)
            self.voldir=UNKNOWN
        else:
            vd="/pnfs"
            # march from top 
            for e in range(2,len(dir_elements)):
                vd=os.path.join(vd,dir_elements[e])
                try:
                    d=os.path.join(vd,'volmap')
                    os.stat(d)
                    self.voldir=os.path.join(d,ff,volume)
                    break
                except:
                    pass
        Trace.trace(11,'Voldir='+self.voldir)

        # make the filename lexically sortable.  since this could be a byte offset,
        #     allow for 100 GB offsets
        self.volume_file = os.path.join(self.voldir,cookie)



    # create a directory
    def make_dir(self, dir_path, mod):
        print "MD",dir_path
        try:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, mod)
            return e_errors.OK, None
        except:
            exc, val, tb = e_errors.handle_error()
            return e_errors.ERROR, (str(exc), str(val))

    # create a duplicate entry in pnfs that is ordered by file number on tape
    def make_volmap_file(self):
        if self.volume_file==UNKNOWN:
            return
        ret=self.make_dir(self.voldir, 0777)
        if ret[0]!=e_errors.OK:
            return
        # create the volume map file and set its size the same as main file
        self.volume_fileP = Pnfs(self.volume_file)
        self.volume_fileP.touch()
        self.volume_fileP.set_file_size(self.file_size)


    # file in the already existing volume map file
    def fill_volmap_file(self):
        if self.volume_file==UNKNOWN:
            return
        # now copy the appropriate layers to the volmap file
        for layer in [1,4]: # bfid and xref
            inlayer = self.readlayer(layer)
            value = ""
            for e in range(0,len(inlayer)):
                value=value+inlayer[e]
                self.volume_fileP.writelayer(layer,value)

        # protect it against accidental deletion - and give ownership to root.root
        Trace.trace(11,'changing write access')
        os.chmod(self.volume_file,0644)  # disable write access except for owner
        Trace.trace(11,'changing to root.root ownership')
        try:
            os.chown(self.volume_file,0,0)   # make the owner root.root
        except:
            self.log_err("fill_volmap_file")

    # retore the original entry based on info from the duplicate
    def restore_from_volmap(self, restore_dir):
        try:
            # check if directory exists
            (d,file) = os.path.split(self.origname)
            if os.path.exists(d) == 0:  # directory does not exist
                if restore_dir == "yes":
                    status = self.make_dir(d, 0755)
                else:
                    status = "ENOENT", None
                if status[0] != e_errors.OK:
                    Trace.log(e_errors.INFO,
                              "restore_from_volmap: directory %s does not exist"%(d,))
                    return status
            # create the original entry and set its size
            orig = Pnfs(self.origname)
            orig.touch()
            orig.set_file_size(self.file_size)

            # now copy the appropriate layers to the volmap file
            for layer in [1,4]: # bfid and xref
                inlayer = self.readlayer(layer)
                value = ""
                for e in range(0,len(inlayer)):
                    value=value+inlayer[e]
                    orig.writelayer(layer,value)
            Trace.log(e_errors.INFO, 
                      "file %s restored from volmap"%(self.origname,))
            return e_errors.OK, None
        except:
            exc, val, tb = e_errors.handle_error()
            return e_errors.ERROR, (str(exc), str(val))

    def log_err(self,func_name):
        exc,msg,tb=sys.exc_info()
        Trace.log(e_errors.ERROR,"pnfs %s %s %s %s"%(
                func_name, self.file, exc,msg))
        
##############################################################################

# this routine returns a list of (filenames,bit_file_ids) given a list of file
# numbers on a specified tape

def findfiles(mainpnfsdir,                  # directory above volmap directory
                                            #  zB: /pnfs/enstore 
              label,                        # tape label
              filenumberlist):              # list of files wanted (count from 0)
                                            #  zB: [1,2,3] or [8,45,31] or 19
    # use a unix find command to determine the volume directory
    command="find %s -mindepth 1 -maxdepth 2 -name %s -print" %(
        os.path.join(mainpnfsdir,'volmap'),
        label)
    tape = os.popen(command,'r').readlines()
    if len(tape) == 0:
        return ("","")
    voldir = string.strip(tape[0])

    # get the list of files in the volume directory
    #   note that files are they are lexically sortable
    volfiles = os.listdir(voldir)
    volfiles.sort()

    # create a sorted list of file number requests
    if type(filenumberlist) == type([]):
        files = filenumberlist 
    else:
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
            Trace.log(e_errors.INFO, "skipping duplicate request: %s"%(i,))
            continue
        else:
            last = i
        v = Pnfs(os.path.join(voldir,volfiles[files[i]]))
        filenames.append(v.origname)
        bfids.append(v.bit_file_id)
    return (filenames,bfids)

class PnfsInterface(interface.Interface):

    def __init__(self):
        # fill in the defaults for the possible options
        self.test = 0
        self.status = 0
        self.info = 0
        self.file = ""
        self.restore = 0
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return ["test","status","file=","restore="
                ] + self.help_options()

##############################################################################
if __name__ == "__main__":

    intf = Pnfs.interface()

    if intf.info:
        p=Pnfs(intf.file,1,1)
        p.dump()

    elif intf.status:
        Trace.log(e_errors.INFO, "not yet")

    elif intf.restore:
        p=Pnfs(intf.file)
        p.restore_from_volmap("no")

    elif intf.test:

        base = "/pnfs/enstore/test2"
        count = 0
        for pf in os.path.join(base,str(time.time())), "/impossible/path/test":
            count = count+1;
            
            Trace.trace(14,"Self test from %s using file %s: %s"%(
                __name__,count,pf))

            p = Pnfs(pf)

            e = p.check_pnfs_enabled()
            Trace.trace(14, "enabled: %s"%(e,))

            if p.valid == VALID:
                if count==2:
                    Trace.log(e_errors.INFO, "ERROR: File %s is invalid, but valid flag is set"%(count,))
                    continue
                p.jon1()
                p.get_pnfs_info()
                p.dump()
                l = p.library
                f = p.file_family
                w = p.file_family_width
                i=p.bit_file_id
                s=p.file_size

                nv = "crunch"
                nvn = 222222
                Trace.trace(14, "Changing to new values")

                p.set_library(nv)
                if p.library == nv:
                    Trace.trace(14, " library changed")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't change library tag: still is "
                          +p.library)

                p.set_file_family(nv)
                if p.file_family == nv:
                    Trace.trace(14, " file_family changed")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't change file_family tag: still is "
                          +p.file_family)

                p.set_file_family_width(nvn)
                if p.file_family_width == nvn:
                    Trace.trace(14, " file_family_width changed")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't change file_family_width tag: "
                          +"still is "+repr(p.file_family_width))

                p.set_bit_file_id(nv,nvn)
                if p.bit_file_id == nv:
                    Trace.trace(14, " bit_file_id changed")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't change bit_file_id layer: still is "
                          +repr(p.bit_file_id))

                if p.file_size == nvn:
                    Trace.trace(14, " file_size changed")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't change file_size: still is "
                          +repr(p.file_size))

                p.dump()
                Trace.trace(14, "Restoring original values")

                p.set_library(l)
                if p.library == l:
                    Trace.trace(14, " library restored")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't restore library tag: still is "
                          +p.library)

                p.set_file_family(f)
                if p.file_family == f:
                    Trace.trace(14, " file_family restored")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't restore file_family tag: still is "
                          +p.file_family)

                p.set_file_family_width(w)
                if p.file_family_width == w:
                    Trace.trace(14, " file_family_width restored")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't restore file_family_width tag: "
                          +"still is "+repr(p.file_family_width))

                p.set_bit_file_id(i,s)
                if p.bit_file_id == i:
                    Trace.trace(14, " bit_file_id restored")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't restore bit_file_id layer: "
                          +"still is "+repr(p.bit_file_id))

                if p.file_size == s:
                    Trace.trace(14, " file size restored")
                else:
                    Trace.log(e_errors.INFO, " ERROR: didn't restore file_size: still is "
                          +repr(p.file_size))

                p.dump()
                p.rm()
                if p.exists != EXISTS:
                    Trace.trace(14, p.pnfsFilename+ "deleted")
                else:
                    Trace.log(e_errors.INFO, "ERROR: could not delete "+
                                       p.pnfsFilename)

            else:
                if count==2:
                    continue
                else:
                    Trace.log(e_errors.INFO, "ERROR: File "+repr(count)
                          +" is valid - but invvalid flag is set")
                    Trace.log(e_errors.INFO, p.pnfsFilename+
                                       "file is not a valid pnfs file")

