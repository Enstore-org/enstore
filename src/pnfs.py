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
import pprint
import pdb
import traceback
import select

# enstore imports
import Trace
import e_errors
try:
    import Devcodes # this is a compiled enstore module
except ImportError:
    Trace.log(e_errors.INFO, "Devcodes unavailable")
#import interface
import option
import enstore_constants

ENABLED = "enabled"
DISABLED = "disabled"
VALID = "valid"
INVALID =  "invalid"
UNKNOWN = "unknown"
EXISTS = "file exists"
DIREXISTS = "directory exists"
ERROR = -1

do_log = 0 #If this is set, PNFS errors will be logged

##############################################################################

#This is used to print out some of the results to the terminal that are more
# than one line long and contained in a list.  The list is usually generated
# by a f.readlines() where if is a file object.  Otherwise the result is
# printed as is.
def print_results(result):
    if type(result) == type([]):
         for line in result:
            print line, #constains a '\012' at the end.
    else:
        print result

##############################################################################

class Pnfs:# pnfs_common.PnfsCommon, pnfs_admin.PnfsAdmin):
    # initialize - we will be needing all these things soon, get them now
    def __init__(self, pnfsFilename="", pnfsDirectory="", pnfs_id="",
                 get_details=1, get_pinfo=0, timeit=0, mount_point=""):
        t1 = time.time()
        self.print_id = "PNFS"
        self.mount_point = mount_point

        #verify the filename (or directory).  If valid, place the directory
        # inside self.dir and the file in self.file.  If a pnfs id was
        # specified, this will set internal directory values to the current
        # working directory.
        #self.check_valid_pnfs_filename(pnfsFilename, pnfsDirectory)

        #Some commands take a pnfs id instead of a filename.  Set the self.id
        # variable and determine the pnfsFilename.
        if pnfs_id:
            self.id = pnfs_id
            self.check_valid_pnfs_id(pnfs_id)
        else:
            self.check_valid_pnfs_filename(pnfsFilename, pnfsDirectory)
            try:
                self.get_id()
            except IOError:
                for file in os.listdir(self.dir):
                    p = Pnfs(os.path.join(self.dir, file), get_details=1,
                             mount_point=self.mount_point)
                    p.get_parent()
                    if p.id != None and p.id != p.parent:
                        self.id = p.parent
                        break
                else:
                    self.id = UNKNOWN
            
        self.get_bit_file_id()
        #self.get_const()

        if get_details:
            self.get_library()
            self.get_file_family()
            self.get_file_family_wrapper()
            self.get_file_family_width()
            self.get_storage_group()
            try:
                self.get_xreference()
            except IOError: #If id is a directory
                pass

        #if get_pinfo:
        #    self.get_pnfs_info()
        if timeit != 0:
            Trace.log(e_errors.INFO, "pnfs__init__ dt: "+time.time()-t1)

    # list what is in the current object
    def dump(self):
        #Trace.trace(14, repr(self.__dict__))
        print repr(self.__dict__)

    ##########################################################################

    # simple test configuration
    #def jon1(self):
    #    if self.valid == VALID:
    #        self.set_bit_file_id("1234567890987654321",123)
    #    else:
    #        raise errno.errorcode[errno.EINVAL],"pnfs.jon1: "\
    #              +self.pnfsfile+" is an invalid pnfs filename"
    #
    # simple test configuration
    #def jon2(self):
    #    if self.valid == VALID:
    #        self.set_bit_file_id("1234567890987654321",45678)
    #        self.set_library("activelibrary")
    #        self.set_file_family("raw")
    #        self.set_file_family_width(2)
    #        self.pstatinfo()
    #    else:
    #        raise errno.errorcode[errno.EINVAL],"pnfs.jon1: "\
    #              +self.pnfsfile+" is an invalid pnfs filename"

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


    # check if file is really exists inside the pnfs file system.
    def check_valid_pnfs_filename(self, pnfsFilename, pnfsDirectory):
        self.pnfsFilename = pnfsFilename
        try:
            #This block of code, takes a filename and determines the absolute
            # path to the file and splits the path from the file.
            if self.pnfsFilename:
                self.pnfsFilename = os.path.abspath(self.pnfsFilename)
                if os.path.isfile(self.pnfsFilename):
                    (self.dir, self.file) = os.path.split(self.pnfsFilename)
                elif os.path.isdir(self.pnfsFilename): #is directory
                    self.dir = self.pnfsFilename #os.path.abspath(directory)
                    self.file = os.path.basename(self.pnfsFilename) #file
                else: #File doesn't exist (yet).
                    (self.dir, self.file) = os.path.split(self.pnfsFilename)
            elif pnfsDirectory:
                self.dir = os.path.abspath(pnfsDirectory)
                self.file = ""
            else: #self.pnfs_id: ??
                self.dir = os.getcwd()
                self.file = ""

            #print "dir", self.dir
            #print "file", self.file
            #print "name", self.pnfsFilename

            #If the directory is valid, then this will return a file with a
            # lot of complex pnfs data.
            #Note: Only the first character of self.file is given.  The output
            # is the same either way.  This is in responce to "stale NFS
            # handle" errors that occording to Patrick were caused by the
            # string in self.file being to long.
            if self.file:
                try:
                    fname = self.dir + '/.(const)(' + self.file[0] + ')'
                    f = open(fname,'r')
                    f.close()
                except (OSError, IOError):
                    exc, msg, tb = sys.exc_info()
                    message = "Possible unrecoverable error detected.\n"
                    sys.stderr.write(message)
                    sys.stderr.write("%s %s\n" % (exc, msg))
                    sys.stderr.write("Continuing.\n")

            #If we set this then the path is valid.
            self.valid = VALID

            #Next determine if the file exists.  If not, then check the
            # directory.
            self.exists = UNKNOWN
            self.pstatinfo()

            self.get_cursor()
            self.dir_id = string.replace(self.cursor[0], "\n", "")
            self.dir_perm = string.replace(self.cursor[1], "\n", "")
            self.mount_id = string.replace(self.cursor[2], "\n", "")

            #something with devcodes???
            self.rmajor = 0
            self.rminor = 0
        except:
            exc, msg, tb = sys.exc_info()
            sys.stderr.write("%s %s" % (exc, msg))
            #traceback.print_tb(tb)
            self.valid = INVALID

    # check if file is really exists inside the pnfs file system.
    def check_valid_pnfs_id(self, pnfsID):
        id = self.id = pnfsID #Remember this for later.
        path = ""

        #Determine the mount point that should be used.
        dirs = os.listdir("/pnfs/")
        if self.mount_point: #A mount point was given by the user.
            mount = "/"
            for dir in string.split(self.mount_point, "/")[1:]:
                mount = os.path.join(mount, dir)
                if os.path.ismount(mount):
                    mount_points = [mount]
                    break
        elif os.getcwd()[:6] == "/pnfs/": #Determine the mount point of cwd.
            mount = "/"
            for dir in string.split(os.getcwd(), "/")[1:]:
                mount = os.path.join(mount, dir)
                if os.path.ismount(mount):
                    mount_points = [mount]
                    break
        else: #Scan all directories in /pnfs for mountpoints.
            mount_points = []
            for directory in dirs:
                test_dir = os.path.join("/pnfs/" + directory)
                if os.path.ismount(test_dir):
                    mount_points.append(test_dir)
                else:
                    dirs = dirs + os.listdir(test_dir)

        self.valid = VALID
        self.exists = DIREXISTS
        matched_id_list = []
        matched_mp_list = []

        #Determine which mount point is being refered to, if possible.
        # This only determines if they really exist for the specified and
        # cwd cases above.  For the last case it attempts to pick the
        # correct mount point.
        for directory in mount_points:
            try:
                fname = "%s/.(nameof)(%s)"%(directory, self.id)
                try:
                    f = open(fname, "r")
                    nameof = f.readlines()[0][:-1]
                    f.close()
                except IOError:
                    nameof=UNKNOWN

                if nameof != UNKNOWN or nameof == "":
                    matched_id_list.append(nameof)
                    matched_mp_list.append(directory)
            except IOError, detail:
                pass

        if len(matched_id_list) == 0:
            #print "No references"
            self.check_valid_pnfs_filename("", "")
            self.valid = INVALID
            return
        elif len(matched_id_list) == 1:
            #print "One reverence"
            #Don't use check_valid_pnfs_filename() here.  This function tries
            # to determine that the file really exists.  Since, the directory
            # isn't known yet, (because that is what is being determined)
            # this would always return unknown status.
            self.file = matched_id_list[0]
            self.dir = matched_mp_list[0]
        else:
            #print "Too many references"
            self.check_valid_pnfs_filename("", "")
            self.valid = INVALID
            return

        self.get_parent()

        #Create a new instance of Pnfs for the parent directory.  This makes
        # the algorithm recursive.  Until it tries to instantiate a class
        # for a directory that is returned UNKNOWN.  When that happens the
        # recursion breaks and the code continues onward building the path
        # string of the original file.
        p = Pnfs(pnfs_id=self.parent, get_details=1,
                 mount_point=matched_mp_list[0])

        #Base case for when the recursion first stops recursing.
        if self.file == "root":
            self.dir = os.path.join("/", self.file)
        #Case for all other recursion cases.
        else:
            self.dir = os.path.join(p.pnfsFilename, self.file)

        #At some point it will be possible to remove the local pnfs directory
        # with the remote pnfs directory.  Thus, for example
        # /root/fs/usr/mist/zaa/100MB_002 becomes /pnfs/mist/zaa/100MB_002,
        try:
            #For each directory in self.dir, append it to the mount point to
            # determine if it exists.  If it exists, then for future
            # iterations use the known existing directory to append to for the
            # remaining existence tests.
            path = matched_mp_list[0]
            for d in string.split(self.dir, "/"):
                test_dir = os.path.join(path, d)
                if os.path.exists(test_dir):
                    path = test_dir
        except:
            pass
        
        self.check_valid_pnfs_filename(path, "")

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
        fname ="%s/.(const)(%s)"%(self.dir, self.file[0])
        f=open(fname,'r')
        self.const = f.readlines()
        f.close()


    # get the numeric pnfs id, given the filename
    def get_id(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.id = UNKNOWN
            return
        fname = "%s/.(id)(%s)" % os.path.split(self.pnfsFilename)
        f = open(fname,'r')
        self.id = f.readlines()
        f.close()
        self.id = string.replace(self.id[0],'\n','')

    def get_showid(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.showid = UNKNOWN
            return
        fname = "%s/.(showid)(%s)"%(self.dir, self.id)
        f = open(fname,'r')
        self.showid = f.readlines()
        f.close()

    # get the nameof information, given the id
    def get_nameof(self):
        if self.valid != VALID:
            self.nameof = UNKNOWN
            self.file = ""
            return
        fname = "%s/.(nameof)(%s)"%(self.dir, self.id)
        f = open(fname,'r')
        self.nameof = f.readlines()
        f.close()
        self.file = self.nameof = string.replace(self.nameof[0],'\n','')

    # get the parent information, given the id
    def get_parent(self, id=None):
        if self.valid != VALID:# or self.exists != DIREXISTS:
            self.parent = UNKNOWN
            return
        fname = "%s/.(parent)(%s)"%(self.dir, self.id)
        f = open(fname,'r')
        self.parent = f.readlines()
        f.close()
        self.parent = string.replace(self.parent[0],'\n','')

    # get the total path of the id
    def get_path(self):
        if self.valid != VALID: #
            self.path = UNKNOWN
            self.pnfsFilename = ""
            return
        self.pnfsFilename = self.path = os.path.join(self.dir, self.file)
        
    # get the cursor information
    def get_cursor(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.cursor = UNKNOWN
            return
        fname = "%s/.(get)(cursor)"%(self.dir,)
        f = open(fname,'r')
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

    # get the cursor information
    def get_countersN(self, dbnum):
        fname = "%s/.(get)(counters)(%s)"%(self.dir, dbnum)
        f=open(fname,'r')
        self.countersN = f.readlines()
        f.close()

    # get the position information
    def get_position(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.position = UNKNOWN
            return
        fname = "%s/.(get)(postion)"%(self.dir,)
        f=open(fname,'r')
        self.position = f.readlines()
        f.close()

    # get the position information
    def get_database(self):
        if self.valid != VALID or self.exists != EXISTS:
            self.database = UNKNOWN
            return
        fname = "%s/.(get)(database)"%(self.dir,)
        f=open(fname,'r')
        self.database = f.readlines()
        f.close()
        self.database = string.replace(self.database[0], "\n", "")

    # get the position information
    def get_databaseN(self, dbnum):
        fname = "%s/.(get)(database)(%s)"%(self.dir, dbnum)
        f=open(fname,'r')
        self.databaseN = f.readlines()
        f.close()
        self.databaseN = string.replace(self.databaseN[0], "\n", "")

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


    # store new info and transaction log
    def set_info(self,value):
        return ##XXX disabled function
        if self.valid != VALID or self.exists != EXISTS:
            self.writelayer(3,value)
            self.get_info()

    # store the cross-referencing data
    def set_xreference(self,volume,location_cookie,size,drive):
        Trace.trace(11,'pnfs.set_xref %s %s %s %s'%(volume, location_cookie,
                                                    size,drive))

        self.volmap_filepath(volume=volume, cookie=location_cookie)
        Trace.trace(11,'making volume_filepath=%s'%(self.volume_filepath,))
        self.make_volmap_file()

        value = (10*"%s\n")%(volume,
                             location_cookie,
                             size,
                             self.file_family,
                             self.pnfsFilename,
                             self.volume_filepath,
                             self.id,
                             self.volume_fileP.id,
                             self.bit_file_id,
                             drive)

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

    # get the information layer
    def get_info(self):
        self.info = UNKNOWN
        if self.valid != VALID or self.exists != EXISTS:
            return
        try:
            self.info = self.readlayer(3)
        except:
            self.log_err("get_info")

        return self.info

    # get the cross reference layer
    def get_xreference(self):
        (self.volume, self.location_cookie, self.size, self.origff,
         self.origname, self.mapfile, self.pnfsid_file, self.pnfsid_map,
         self.bfid, self.origdrive) = self.xref = [UNKNOWN]*10

        if self.valid == VALID and self.exists == EXISTS:
            try:
                xinfo = self.readlayer(4)
                xinfo = map(string.strip, xinfo[:10])

                xinfo = xinfo + ([UNKNOWN] * (10 - len(xinfo)))

                try:
                    self.volume = xinfo[0]
                    self.location_cookie = xinfo[1]
                    self.size = xinfo[2]
                    self.origff = xinfo[3]
                    self.origname = xinfo[4]
                    self.mapfile = xinfo[5]
                    self.pnfsid_file = xinfo[6]
                    self.pnfsid_map = xinfo[7]
                    self.bfid = xinfo[8]
                    self.origdrive = xinfo[9]
                except ValueError:
                    pass

                self.xref = xinfo

            except ValueError:
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

    # store a new storage group tag
    # this is group of volumes assigned to one experiment or group of users
    def set_storage_group(self,value):
        if self.valid != VALID:
            return
        self.writetag("storage_group",value)
        self.get_storage_group()

    # get the storage group
    def get_storage_group(self):
        self.storage_group = UNKNOWN
        if self.valid != VALID:
            return
        try:
            self.storage_group = self.readtag("storage_group")[0]
        except:
            # do not record this exception
            # for the backward compatibility
            # storage grop is essential only for version higher
            # enstore versions 
            #self.log_err("get_storage_group")
            return

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

    #This function determines the volmap directory if the cwd is inside
    # /pnfs.  The directory, if determined, is placed inside
    # self.volume_filepath and is also returned.  If the volmap directory
    # is not found, then it is set to an empty string.  The optional
    # arguments are appended to the path in the following pattern:
    #  /pnfs/XXXX/volmap/off/volume/cookie
    # If they are not specified, then code doesn't use them.
    def volmap_filepath(self, origff="", volume="", cookie=""):
        dir_elements = string.split(self.dir,"/")
        directory = "/"

        #Handle file family stuff.
        if origff:
            ff = origff
        elif self.origff != UNKNOWN:
            ff = self.origff
        elif self.file_family != UNKNOWN:
            ff = self.file_family
        else:
            ff = ""

        #Handle volume stuff
        if volume:
            vol = volume
        elif self.volume != UNKNOWN:
            vol = self.volume
        else:
            vol = ""

        # cookie  not set until xref layer is filled in, has to be specified
        if cookie:
            lc = cookie
        elif self.location_cookie != UNKNOWN:
            lc = self.location_cookie
        else:
            lc = ""

        #March through each segment of the current directory (aka self.dir)
        # testing for the following pattern:
        # /pnfs/xxxxx/volmap/origfilefamily/volumename/
        for d_e in dir_elements:
            directory = os.path.join(directory, d_e)
            try:
                test_dir = os.path.join(directory, "volmap")

                #If the volmap directory hasn't been found, keep trying.
                #Don't include ff, vol and lc in the existence test,
                # otherwise write operations will fail.
                if not os.path.exists(test_dir):
                    raise errno.ENOENT
                self.volume_filepath = os.path.join(test_dir, ff, vol, lc)
                return self.volume_filepath
            except:
                pass

        #If we get here, then the directory wasn't found.
        self.volume_filepath = UNKNOWN
        return self.volume_filepath


    # create a directory
    def make_dir(self, dir_path, mod):
        try:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, mod)
            return e_errors.OK, None
        except:
            exc, val, tb = Trace.handle_error()
            return e_errors.ERROR, (str(exc), str(val))

    # create a duplicate entry in pnfs that is ordered by file number on tape
    def make_volmap_file(self):

        if self.volume_filepath==UNKNOWN:
            return

        old_mask = os.umask(0)

        try:
            ret=self.make_dir(os.path.dirname(self.volume_filepath), 0777)
        except:
            exc, msg, tb = sys.exc_info()
            print traceback.print_tb(tb)
            print exc, msg
            raise exc, msg, tb

        os.umask(old_mask)

        if ret[0]!=e_errors.OK:
            return

        # create the volume map file and set its size the same as main file
        self.volume_fileP = Pnfs(self.volume_filepath, get_details=0)
        self.volume_fileP.touch()
        self.volume_fileP.set_file_size(self.file_size)


    # file in the already existing volume map file
    def fill_volmap_file(self):
        if self.volume_filepath==UNKNOWN:
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
        #Disable write access for all but owner.

        os.chmod(self.volume_filepath,0644)
        Trace.trace(11,'changing to root.root ownership')
        try:
            os.chown(self.volume_filepath,0,0)   # make the owner root.root
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
        if not do_log:
            return
        exc,msg,tb=sys.exc_info()
        Trace.log(e_errors.INFO,"pnfs %s %s %s %s"%(
                func_name, self.file, exc,msg))
        ##Note:  I had e_errors.ERROR, and lots of non-fatal errors
        ##were showing up in the weblog

##############################################################################

    #Print out the current settings for all directory tags.
    def ptags(self, intf):
        if hasattr(intf, "directory"):
            filename = os.path.join(os.path.abspath(intf.directory),
                                    ".(tags)(all)")
        else:
            filename = os.path.join(os.getcwd(), ".(tags)(all)")

        try:
            file = open(filename, "r")
            data = file.readlines()
            rtn_val = file.close()
        except IOError, detail:
            print detail
            return

        #print the top portion of the output.  Note: the values placed into
        # line have a newline at the end of them, this is why line[:-1] is
        # used to remove it.
        for line in data:
            try:
                tag = string.split(line[7:], ")")[0]
                tag_info = self.readtag(tag)
                print line[:-1], "=",  tag_info[0]
            except IOError, detail:
                print line[:-1], ":", detail

        #Print the bottom portion of the output.
        for line in data:
            tag_file = os.path.join(intf.directory, line[:-1])
            os.system("ls -l \"" + tag_file + "\"")

    def ptag(self, intf):
        tag = self.readtag(intf.named_tag)
        print tag[0]

    #Print or edit the library
    def plibrary(self, intf):
        if intf.library == 1:
            print self.library
        else:
            self.set_library(intf.library)

    #Print or edit the file family.
    def pfile_family(self, intf):
        if intf.file_family == 1:
            print self.file_family
        else:
            self.set_file_family(intf.file_family)

    #Print or edit the file family wrapper.
    def pfile_family_wrapper(self, intf):
        if intf.file_family_wrapper == 1:
            print self.file_family_wrapper
        else:
            self.set_file_family_wrapper(intf.file_family_wrapper)

    #Print or edit the file family width.
    def pfile_family_width(self, intf):
        if intf.file_family_width == 1:
            print self.file_family_width
        else:
            self.set_file_family_width(intf.file_family_width)

    #Print or edit the storage group.
    def pstorage_group(self, intf):
        if intf.storage_group == 1:
            print self.storage_group
        else:
            self.set_storage_group(intf.storage_group)

    #Returns the files on a specified volume.
    #Volume is the volume that a listing of all files contained on it has
    # been requested.
    def pfiles(self, intf):
        #make sure this data is available
        #self.get_file_family() #stores file-family in self.file_family

        #Determine he location of the specified volumes volmap file.
        self.volmap_filepath(self.file_family,  intf.volume_tape)

        #If this wasn't set, then it wasn't found.
        #set by self.volmap_filepath()
        if not self.volume_filepath or self.volume_filepath == UNKNOWN:
            return

        #Note: contains the file names, but since the names of the file are
        # the file ids, then using the name works just fine.
        for file in os.listdir(self.volume_filepath):
            #create the full filename of the volmap file.
            # Example filename (is based on location cookie):
            # /pnfs/rip6/volmap/null0/null00//0000_000000000_0000001
            filename = self.volume_filepath + "/" + file

            #Using the location cookie, retrieve the full pnfs filename.
            self.__init__(filename)            #re-init this instance
            #get the pnfs filename and remove the newline.
            filename = self.readlayer(enstore_constants.XREF_LAYER)[4][:-1]
            os.system("ls -l " + filename)

    #Prints out the specified layer of the specified file.
    def player(self, intf):
        data = self.readlayer(intf.named_layer)
        for datum in data:
            print datum,
    
    #Snag the cross reference of the file inside self.file.
    #***LAYER 4***
    def pxref(self, intf):
        #First make sure the file does exist.
        if not os.path.isfile(self.pnfsFilename):
            print "Error: file " + self.file + " not found."
            return

        data = self.xref
        names = ["volume", "location_cookie", "size", "file_family",
                 "original_name", "map_file", "pnfsid_file", "pnfsid_map",
                 "bfid", "origdrive"]
        
        #With the data stored in lists, with corresponding values based on the
        # index, then just pring them out.
        for i in range(10):
            print "%s: %s" % (names[i], data[i])

    #Prints out the bfid value for the specified file.
    #***LAYER 1***
    def pbfid(self, intf):
        data = self.readlayer(enstore_constants.BFID_LAYER)
        print data[0]

    #If dupl is empty, then show the duplicate data for the file
    # (in self.file).  If dupl is there then set the duplicate for the file
    # in self.file to that in dupl.
    #***LAYER 3***
    def pduplicate(self, intf):
        #Handle the add/edit duplicate feature.
        if intf.file and intf.duplicate_file:
            if os.path.isfile(intf.duplicate_file):
                self.writelayer(enstore_constants.DUPLICATE_LAYER,
                                intf.duplicate_file)
            else:
                print "Specified duplicate file does not exist."
                
        #Display only the duplicates for the specified file.
        elif intf.file:
            print "%s:" % (self.file),
            for filename in self.readlayer(enstore_constants.DUPLICATE_LAYER):
                print filename,
            print
        else:
            #Display all the duplicates for every file specified.
            for file in os.listdir(os.getcwd()):
                intf.file = file
                self.__init__(intf.file)
                self.pduplicate(intf)

    def penstore_state(self, intf):
        fname = os.path.join(self.dir, ".(config)(flags)/disabled")
        print fname
        if os.access(fname, os.F_OK):# | os.R_OK):
            f=open(fname,'r')
            self.enstore_state = f.readlines()
            f.close()
            print "Enstore disabled:", self.enstore_state[0],
        else:
            print "Enstore enabled"
            
    def ppnfs_state(self, intf):
        fname = "%s/.(config)(flags)/.(id)(pnfs_state)" % self.dir
        if os.access(fname, os.F_OK | os.R_OK):
            f=open(fname,'r')
            self.pnfs_state = f.readlines()
            f.close()
            print "Pnfs:", self.pnfs_state[0],
        else:
            print "Pnfs: unknown"

##############################################################################

    def pls(self, intf):
        filename = "\".(use)(%s)(%s)\"" % (intf.named_layer, self.file)
        os.system("ls -alsF " + filename)
        
    def pvolume(self, intf):
        self.volmap_filepath(self.file_family, intf.volumename)
        print self.volume_filepath
    
    def pecho(self, intf):
        self.writelayer(intf.named_layer, intf.text)
        
    def prm(self, intf):
        self.writelayer(intf.named_layer, "")

    def pcp(self, intf):
        f = open(intf.unixfile, 'r')

        data = f.readlines()
        file_data_as_string = ""
        for line in data:
            file_data_as_string = file_data_as_string + line

        f.close()

        self.writelayer(intf.named_layer, file_data_as_string)
        
    def psize(self, intf):
        self.set_file_size(intf.filesize)
    
    def ptagecho(self, intf):
        self.writetag(intf.named_tag, intf.text)
        
    def ptagrm(self, intf):
        print "Feature not yet implemented."

    def pio(self, intf):
        print "Feature not yet implemented."

        #fname = "%s/.(fset)(%s)(io)(on)" % (self.dir, self.file)
        #os.system("touch" + fname)
    
    def pid(self, intf):
        print self.id
        
    def pshowid(self, intf):
        self.get_showid()
        print_results(self.showid)
    
    def pconst(self, intf):
        self.get_const()
        print_results(self.const)
        
    def pnameof(self, intf):
        self.get_nameof()
        print_results(self.nameof)
        
    def ppath(self, intf):
        self.get_path()
        print_results(self.path)
        
    def pparent(self, intf):
        self.get_parent()
        print_results(self.parent)
    
    def pcounters(self, intf):
        self.get_counters()
        print_results(self.counters)
        
    def pcountersN(self, intf):
        self.get_countersN(intf.dbnum)
        print_results(self.countersN)
    
    def pcursor(self, intf):
        self.get_cursor()
        print_results(self.cursor)
            
    def pposition(self, intf):
        self.get_position()
        print_results(self.position)
        
    def pdatabase(self, intf):
        self.get_database()
        print_results(self.database)
            
    def pdatabaseN(self, intf):
        self.get_databaseN(intf.dbnum)
        print_results(self.databaseN)


    def pdown(self, intf):
        if os.environ['USER'] != "root":
            print "must be root to create enstore system-down wormhole"
            return
        
        dname = "/pnfs/fs/admin/etc/config/flags"
        if not os.access(dname, os.F_OK | os.R_OK):
            print "/pnfs/fs is not mounted"
            return

        fname = "/pnfs/fs/admin/etc/config/flags/disabled"
        f = open(fname,'w')
        f.write(intf.reason)
        f.close()

        os.system("touch .(fset)(disabled)(io)(on)")
        
    def pup(self, intf):
        if os.environ['USER'] != "root":
            print "must be root to create enstore system-down wormhole"
            return
        
        dname = "/pnfs/fs/admin/etc/config/flags"
        if not os.access(dname, os.F_OK | os.R_OK):
            print "/pnfs/fs is not mounted"
            return

        os.remove("/pnfs/fs/admin/etc/config/flags/disabled")

    def pdump(self, intf):
        self.dump()

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

##############################################################################

class PnfsInterface(option.Interface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.test = 0
        #self.status = 0
        #self.info = 0
        #self.file = ""
        #self.restore = 0
        #These my be used, they may not.
        #self.duplicate_file = None
        #interface.Interface.__init__(self)
        option.Interface.__init__(self, args, user_mode)

    pnfs_user_options = {
        option.TAGS:{option.HELP_STRING:"lists tag values and permissions",
                option.DEFAULT_VALUE:option.DEFAULT,
                option.DEFAULT_NAME:"tags",
                option.DEFAULT_TYPE:option.INTEGER,
                option.VALUE_USAGE:option.IGNORED,
                option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                      option.DEFAULT_NAME:"directory",
                                      option.DEFAULT_TYPE:option.STRING,
                                      option.VALUE_NAME:"directory",
                                      option.VALUE_TYPE:option.STRING,
                                      option.VALUE_USAGE:option.OPTIONAL,
                                      option.FORCE_SET_DEFAULT:option.FORCE,
                                      }]
                },
        option.TAG:{option.HELP_STRING:"lists the tag of the directory",
                    option.DEFAULT_VALUE:option.DEFAULT,
                    option.DEFAULT_NAME:"tag",
                    option.DEFAULT_TYPE:option.INTEGER,
                    option.VALUE_NAME:"named_tag",
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"tag",
                    option.FORCE_SET_DEFAULT:1,
                    option.USER_LEVEL:option.USER,
                    option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                          option.DEFAULT_NAME:"directory",
                                          option.DEFAULT_TYPE:option.STRING,
                                          option.VALUE_NAME:"directory",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,
                                         option.FORCE_SET_DEFAULT:option.FORCE,
                                          }]
               },
        option.LIBRARY:{option.HELP_STRING:"gets library tag, default; " \
                                      "sets library tag, optional",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"library",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.FILE_FAMILY:{option.HELP_STRING:"gets file family tag, " \
                            "default; sets file family tag, optional",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"file_family",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.FILE_FAMILY_WIDTH:{option.HELP_STRING:"gets file family " \
                        "width tag, default; sets file family tag, optional",
                             option.DEFAULT_VALUE:option.DEFAULT,
                             option.DEFAULT_NAME:"file_family_width",
                             option.DEFAULT_TYPE:option.INTEGER,
                             option.VALUE_TYPE:option.STRING,
                             option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.FILE_FAMILY_WRAPPER:{option.HELP_STRING:"gets file family " \
                       "width tag, default; sets file family tag, optional",
                               option.DEFAULT_VALUE:option.DEFAULT,
                               option.DEFAULT_NAME:"file_family_wrapper",
                               option.DEFAULT_TYPE:option.INTEGER,
                               option.VALUE_TYPE:option.STRING,
                               option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.STORAGE_GROUP:{option.HELP_STRING:"gets storage group tag, " \
                            "default; sets storage group tag, optional",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"storage_group",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.FILES:{option.HELP_STRING:"lists all the files on specified " \
                                         "tape in volmap-tape",
                 option.DEFAULT_VALUE:option.DEFAULT,
                 option.DEFAULT_NAME:"files",
                 option.DEFAULT_TYPE:option.INTEGER,
                 option.VALUE_NAME:"volume_tape",
                 option.VALUE_TYPE:option.STRING,
                 option.VALUE_USAGE:option.REQUIRED,
                 option.FORCE_SET_DEFAULT:option.FORCE,
                   },
        option.BFID:{option.HELP_STRING:"lists the bit file id for file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"bfid",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.FORCE_SET_DEFAULT:option.FORCE,
                },
        option.XREF:{option.HELP_STRING:"lists the cross reference " \
                                        "data for file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"xref",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.FORCE_SET_DEFAULT:option.FORCE,
                },
        option.LAYER:{option.HELP_STRING:"lists the layer of the file",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"layer",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"file",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"filename",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.USER,
                      option.EXTRA_VALUES:[{option.DEFAULT_VALUE:
                                                                option.DEFAULT,
                                            option.DEFAULT_NAME:"named_layer",
                                            option.DEFAULT_TYPE:option.INTEGER,
                                            option.VALUE_NAME:"named_layer",
                                            option.VALUE_TYPE:option.INTEGER,
                                            option.VALUE_USAGE:option.OPTIONAL,
                                            option.VALUE_LABEL:"layer",
                                            }]
                 },
        option.DUPLICATE:{option.HELP_STRING:"gets/sets duplicate file values",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"duplicate",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                           option.DEFAULT_NAME:"file",
                                           option.DEFAULT_TYPE:option.STRING,
                                           option.VALUE_NAME:"file",
                                           option.VALUE_TYPE:option.STRING,
                                           option.VALUE_USAGE:option.OPTIONAL,
                                           option.VALUE_LABEL:"filename",
                                         option.FORCE_SET_DEFAULT:option.FORCE,
                                           },
                                          {option.DEFAULT_VALUE:"",
                                          option.DEFAULT_NAME:"duplicate_file",
                                           option.DEFAULT_TYPE:option.STRING,
                                           option.VALUE_NAME:"duplicat_file",
                                           option.VALUE_TYPE:option.STRING,
                                           option.VALUE_USAGE:option.OPTIONAL,
                                       option.VALUE_LABEL:"duplicate_filename",
                                         option.FORCE_SET_DEFAULT:option.FORCE,
                                           },]
                     },
        option.ENSTORE_STATE:{option.HELP_STRING:"lists whether enstore " \
                                                 "is still alive",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"enstore_state",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"directory",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.FORCE_SET_DEFAULT:option.FORCE,
                     },
        option.PNFS_STATE:{option.HELP_STRING:"lists whether pnfs is " \
                                              "still alive",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"pnfs_state",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"directory",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      },
        }

    pnfs_admin_options = {
        option.LS:{option.HELP_STRING:"does an ls on the named layer " \
                                      "in the file",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"ls",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   option.EXTRA_VALUES:[{option.DEFAULT_VALUE:option.DEFAULT,
                                         option.DEFAULT_NAME:"named_layer",
                                         option.DEFAULT_TYPE:option.INTEGER,
                                         option.VALUE_NAME:"named_layer",
                                         option.VALUE_TYPE:option.STRING,
                                         option.VALUE_USAGE:option.OPTIONAL,
                                         option.VALUE_LABEL:"layer",
                                         }]
              },
        option.VOLUME:{option.HELP_STRING:"lists all the volmap-tape for the" \
                                     " specified volume",
                 option.DEFAULT_VALUE:option.DEFAULT,
                 option.DEFAULT_NAME:"volume",
                 option.DEFAULT_TYPE:option.INTEGER,
                 option.VALUE_NAME:"volumename",
                 option.VALUE_TYPE:option.STRING,
                 option.VALUE_USAGE:option.REQUIRED,
                 option.FORCE_SET_DEFAULT:option.FORCE,
                 option.USER_LEVEL:option.ADMIN,
                   },
        option.ID:{option.HELP_STRING:"prints the pnfs id",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"id",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
              },
        option.SHOWID:{option.HELP_STRING:"prints the pnfs id information",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"showid",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_NAME:"pnfs_id",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.FORCE_SET_DEFAULT:option.FORCE,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.CONST:{option.HELP_STRING:"",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"const",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"file",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"filename",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.ADMIN,
                      },
        option.NAMEOF:{option.HELP_STRING:"prints the filename of the pnfs id",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"nameof",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_NAME:"pnfs_id",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.FORCE_SET_DEFAULT:option.FORCE,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.PATH:{option.HELP_STRING:"prints the file path of the pnfs id",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"path",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"pnfs_id",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.FORCE_SET_DEFAULT:option.FORCE,
                     option.USER_LEVEL:option.ADMIN,
                     },
        option.PARENT:{option.HELP_STRING:"prints the pnfs id of the parent " \
                                          "directory",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"parent",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_NAME:"pnfs_id",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.FORCE_SET_DEFAULT:option.FORCE,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.CURSOR:{option.HELP_STRING:"",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"cursor",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_NAME:"file",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_LABEL:"filename",
                       option.FORCE_SET_DEFAULT:option.FORCE,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.COUNTERS:{option.HELP_STRING:"",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"counters",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"file",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"filename",
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.ADMIN,
                         },
        option.COUNTERSN:{option.HELP_STRING:"(must have cwd in pnfs)",
                          option.DEFAULT_VALUE:option.DEFAULT,
                          option.DEFAULT_NAME:"countersN",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.VALUE_NAME:"dbnum",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.FORCE_SET_DEFAULT:option.FORCE,
                          option.USER_LEVEL:option.ADMIN,
                          },
        option.POSITION:{option.HELP_STRING:"",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"position",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"file",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"filename",
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.ADMIN,
                         },
        option.DATABASE:{option.HELP_STRING:"",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"database",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"file",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"filename",
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.ADMIN,
                         },
        option.DATABASEN:{option.HELP_STRING:"(must have cwd in pnfs)",
                          option.DEFAULT_VALUE:option.DEFAULT,
                          option.DEFAULT_NAME:"databaseN",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.VALUE_NAME:"dbnum",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.FORCE_SET_DEFAULT:option.FORCE,
                          option.USER_LEVEL:option.ADMIN,
                          },
        option.ECHO:{option.HELP_STRING:"sets text to named layer of the file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"echo",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"text",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.FORCE_SET_DEFAULT:option.FORCE,
                     option.USER_LEVEL:option.ADMIN,
                     option.EXTRA_VALUES:[{option.VALUE_NAME:"file",
                                           option.VALUE_TYPE:option.STRING,
                                           option.VALUE_USAGE:option.REQUIRED,
                                           option.VALUE_LABEL:"filename",
                                           },
                                          {option.VALUE_NAME:"named_layer",
                                           option.VALUE_TYPE:option.INTEGER,
                                           option.VALUE_USAGE:option.REQUIRED,
                                           option.VALUE_LABEL:"layer",
                                           },]
                },
        option.RM:{option.HELP_STRING:"deletes (clears) named layer of the file",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"rm",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   option.EXTRA_VALUES:[{option.VALUE_NAME:"named_layer",
                                         option.VALUE_TYPE:option.INTEGER,
                                         option.VALUE_USAGE:option.REQUIRED,
                                         option.VALUE_LABEL:"layer",
                                         },]
                   },
        option.CP:{option.HELP_STRING:"echos text to named layer of the file",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"cp",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"unixfile",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   option.EXTRA_VALUES:[{option.VALUE_NAME:"file",
                                         option.VALUE_TYPE:option.STRING,
                                         option.VALUE_USAGE:option.REQUIRED,
                                         option.VALUE_LABEL:"filename",
                                         },
                                        {option.VALUE_NAME:"named_layer",
                                         option.VALUE_TYPE:option.INTEGER,
                                         option.VALUE_USAGE:option.REQUIRED,
                                         option.VALUE_LABEL:"layer",
                                         },]
                   },
        option.TAGECHO:{option.HELP_STRING:"echos text to named tag",
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.DEFAULT_NAME:"tagecho",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.VALUE_NAME:"text",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.FORCE_SET_DEFAULT:option.FORCE,
                        option.USER_LEVEL:option.ADMIN,
                        option.EXTRA_VALUES:[{option.VALUE_NAME:"named_tag",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_LABEL:"tag",
                                              },]
                   },
        option.SIZE:{option.HELP_STRING:"sets the size of the file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"size",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.FORCE_SET_DEFAULT:option.FORCE,
                     option.USER_LEVEL:option.ADMIN,
                     option.EXTRA_VALUES:[{option.VALUE_NAME:"filesize",
                                           option.VALUE_TYPE:option.INTEGER,
                                           option.VALUE_USAGE:option.REQUIRED,
                                           },]
                },
        option.TAGRM:{option.HELP_STRING:"removes the tag (tricky, see DESY "
                                         "documentation)",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"tagrm",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"named_tag",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"tag",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.ADMIN,
                 },
        option.IO:{option.HELP_STRING:"sets io mode (can't clear it easily)",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"io",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   },
        option.DOWN:{option.HELP_STRING:"creates enstore system-down " \
                                        "wormhole to prevent transfers",
                option.DEFAULT_VALUE:option.DEFAULT,
                option.DEFAULT_NAME:"down",
                option.DEFAULT_TYPE:option.INTEGER,
                option.VALUE_NAME:"reason",
                option.VALUE_TYPE:option.STRING,
                option.VALUE_USAGE:option.REQUIRED,
                option.FORCE_SET_DEFAULT:option.FORCE,
                option.USER_LEVEL:option.ADMIN,
                },
        option.UP:{option.HELP_STRING:"removes enstore system-down wormhole",
              option.DEFAULT_VALUE:option.DEFAULT,
              option.DEFAULT_NAME:"up",
              option.DEFAULT_TYPE:option.INTEGER,
              option.VALUE_USAGE:option.IGNORED,
              option.USER_LEVEL:option.ADMIN,
              },
        option.DUMP:{option.HELP_STRING:"dumps info",
              option.DEFAULT_VALUE:option.DEFAULT,
              option.DEFAULT_NAME:"dump",
              option.DEFAULT_TYPE:option.INTEGER,
              option.VALUE_USAGE:option.IGNORED,
              option.USER_LEVEL:option.ADMIN,
              },
        }
    
    def valid_dictionaries(self):
        return (self.help_options, self.pnfs_user_options,
                self.pnfs_admin_options)

    # parse the options like normal but make sure we have other args
    def parse_options(self):
        self.pnfs_id = "" #Assume the command is a dir and/or file.
        self.file = ""
        self.directory = ""
        option.Interface.parse_options(self)

        if getattr(self, "help", None):
            self.print_help()

        if getattr(self, "usage", None):
            self.print_usage()

##############################################################################
def do_work(intf):

    p=Pnfs(intf.file, intf.directory, intf.pnfs_id, 1, 1)
    for arg in intf.option_list:
        if string.replace(arg, "_", "-") in intf.options.keys():
            arg = string.replace(arg, "-", "_")
            apply(getattr(p, "p" + arg), (intf,))

    return

##############################################################################
if __name__ == "__main__":

    intf = PnfsInterface()

    intf._mode = "admin"

    do_work(intf)
