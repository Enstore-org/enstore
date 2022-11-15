#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
import os
import errno
import stat
import string
import time
import re
import types

# enstore imports
import option
import charset
import ExtendedAttributes
import file_utils


UNKNOWN = "unknown"

#For Linux and FreeBSD the 2 common possible namespaces are:
# 1) user
# 2) system
#Linx defines too additional namespaces:
# 3) security
# 4) trusted
#MacOS X and Solars do not appear to enforce different attribute namespaces.
#For sanity, we will include the namespace anyway on all systems.
DEFAULT_XATTR_NAMESPACE = "user"

#Common error string patterns.
INVALID_CHARACTERS_ERROR = "Directory extended attribute, %s, " \
                           "contains invalid characters."
INVALID_PATTERN_ERROR = "File familes ending in %s are forbidden."

##############################################################################

#Raise exception if file is not a regular file.
def confirm_regular_file(path):
    #Check to make sure that this is a regular file.
    stat_info = file_utils.wrapper(os.stat, (path,))
    if stat.S_ISDIR(stat_info[stat.ST_MODE]):
        raise OSError(errno.EISDIR, os.strerror(errno.EISDIR))
    if not stat.S_ISREG(stat_info[stat.ST_MODE]):
        raise OSError(errno.EIO, "Not a file")

#Raise exception if file is not a directory.
def confirm_directory(path):
    #Check to make sure that this is a regular file.
    stat_info = file_utils.wrapper(os.stat, (path,))
    if not stat.S_ISDIR(stat_info[stat.ST_MODE]):
        raise OSError(errno.ENOTDIR, os.strerror(errno.EISDIR))

##############################################################################

#This is used to print out some of the results to the terminal that are more
# than one line long and contained in a list.  The list is usually generated
# by a f.readlines() where if is a file object.  Otherwise the result is
# printed as is.
def print_results(result):
    if type(result) == types.ListType:
         for line in result:
            print line, #constains a '\012' at the end.
    else:
        print result
def print_results2(result):
    if type(result) == types.ListType:
         for line in result:
            print line #no '\012' at the end.
    else:
        print result

##############################################################################

class Fs:# pnfs_common.PnfsCommon, pnfs_admin.PnfsAdmin):
    # initialize - we will be needing all these things soon, get them now
    #
    #pnfsFilename: The filename of a file in pnfs.  This may also be the
    #              pnfs id of a file in pnfs.
    #mount_point: The mount point that the file should be under when
    #             pnfsFilename is really a pnfsid or pnfsFilename does
    #             not contain an absolute path.
    #shortcut: If passed a pnfsid and this is true, don't lookup the
    #          full filepath.  Use the .../.(access)(%s) name instead.
    def __init__(self, Filepath=""):
        self.Filepath = Filepath
        if Filepath:
            if Filepath[-1] == "/":
                self.Directory = Filepath
            else:
                try:
                    stat_info = self.stat(Filepath)
                    if stat.S_ISDIR(stat_info[stat.ST_MODE]):
                        self.Directory = Filepath
                    else:
                        self.Directory = os.path.dirname(Filepath)
                except (OSError, IOError):
                    self.Directory = None  #What to do?
        else:
            self.dir = os.getcwd()

    #Returns the directory to try.  Useful if the argument is left empty
    # and then needed to determine the directory from member values.
    # The second part of the return value, is if the directory value
    # came from a class member variable or not.
    def get_directory(self, path=None):
        if path:
            #If self.stat() raises an exception, so be it.  We can't do
            # anything else here.
            stat_info = self.stat(path)
            if stat.S_ISDIR(stat_info[stat.ST_MODE]):
                use_directory = path  #The target is a directory.
            else:
                use_directory = os.path.dirname(path)
            from_member = False
        elif getattr(self, 'Directory', None):
            use_directory = self.Directory
            from_member = True
        elif not getattr(self, 'Directory', None) and self.Filepath:
            #If self.stat() raises an exception, so be it.  We can't do
            # anything else here.
            stat_info = self.stat(self.Filepath)
            if stat.S_ISDIR(stat_info[stat.ST_MODE]):
                use_directory = self.Filepath  #The target is a directory.
            else:
                use_directory = os.path.dirname(self.Filepath)
            from_member = True
        else:
            use_directory = os.getcwd()
            from_member = False

        return use_directory, from_member

    # list what is in the current object
    def dump(self):
        #Trace.trace(14, repr(self.__dict__))
        print repr(self.__dict__)

    ##########################################################################

    # set a new mode for the existing file
    def chmod(self, mode, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath
            
        file_utils.wrapper(os.chmod, (use_filepath, mode))

        t = int(time.time())
        file_utils.wrapper(os.utime, (use_filepath, (t, t)))
        
        """
        if filepath:
            self.utime(filepath)
        else:
            self.utime()
            self.pstatinfo()
        """

    # change the ownership of the existing file
    def chown(self, uid, gid, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath
        
        file_utils.wrapper(os.chown, (use_filepath, uid, gid))

        t = int(time.time())
        file_utils.wrapper(os.utime, (use_filepath,(t,t)))
        """
        if filepath:
            self.utime(filepath)
        else:
            self.utime()
            self.pstatinfo()
        """

    # get the stat of the file.
    def stat(self, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath

        fstat = file_utils.wrapper(os.stat, (use_filepath))

        fstat = tuple(fstat)

        if not filepath:
            self.fstat = fstat

        return fstat

    ##########################################################################

    # The extended attribute names, for getxattr() and setxattr() underlying
    # C implementations, need a namespace in the "name" arguement.
    # The functions in this module that use these two functions specifiy
    # the "user" namespace.

    def read_extended_attributes(self, name, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath

        #Use the C module to read the extended attribute.
        try:
            ea_dict = ExtendedAttributes.extendedAttributesGet(use_filepath,
                                                               name)
        except ExtendedAttributes.error, msg:
            raise OSError(msg.args[1],
                          "%s: %s: %s" % (msg.args[0], msg.args[2], name),
                          use_filepath)

        if name != None:
            rtn = ea_dict.get(name, None)
            if rtn == None:
                if len(ea_dict) == 1 \
                       and ea_dict.keys()[0][-len(name):] == name:
                    rtn = ea_dict.values()[0]
                else:
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
        else:
            rtn = ea_dict
                
        return rtn

    def write_extended_attribute(self, name, value, filepath=None):
        if filepath:
            use_filepath = filepath
        elif self.Filepath:
            use_filepath = self.Filepath
        elif getattr(self, "Directory", None):
            use_filepath = self.Directory
        else:
            sys.exit(1)

        #Use the C module to write the extended attribute.
        try:
            ExtendedAttributes.extendedAttributesPut(use_filepath, name, value)
        except ExtendedAttributes.error, msg:
            raise OSError(msg.args[1],
                          "%s: %s: %s" % (msg.args[0], msg.args[2], name),
                          use_filepath)

    ##########################################################################
        
    def get_file_size(self, filepath=None):
        if filepath:
            fstat = self.stat(filepath)
        else:
            fstat = self.stat()

        os_filesize = long(fstat[stat.ST_SIZE])
        
        if not filepath:
            self.file_size = os_filesize
            
        return os_filesize

    def set_file_size(self, length, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath

        fd = file_utils.wrapper(os.open, (use_filepath, os.O_RDWR))
        file_utils.wrapper(os.ftruncate, (fd, length))
        file_utils.wrapper(os.close, (fd,))
            
    def get_bit_file_id(self, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath

        #Check to make sure that this is a regular file.
        confirm_regular_file(use_filepath)

        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "bfid")

        value = self.read_extended_attributes(use_name, use_filepath)

        if filepath:
            self.bit_file_id = value
        
        return value

    def set_bit_file_id(self, bfid, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath

        #Check to make sure that this is a regular file.
        confirm_regular_file(use_filepath)

        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "bfid")
            
        self.write_extended_attribute(use_name, bfid, use_filepath)

    def get_xreference(self, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath

        #Check to make sure that this is a regular file.
        confirm_regular_file(use_filepath)

        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "xref")
        
        value = self.read_extended_attributes(use_name, use_filepath)

        #Strip off whitespace from each line.
        xinfo = map(string.strip, value[:11])
        #Make sure there are 11 elements.  Early versions only contain 9.
        # Some contain 10.  This prevents problems.
        xinfo = xinfo + ([UNKNOWN] * (11 - len(xinfo)))

        #If the class member value was used, store the values seperatly.
        if not filepath:
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
                self.crc = xinfo[10]
            except ValueError:
                pass

            self.xref = xinfo

        
        return xinfo
            
    def set_xreference(self, volume, location_cookie, size, file_family,
                       pnfsFilename, volume_filepath, id, volume_fileP,
                       bit_file_id, drive, crc, filepath=None):

        value = (11*"%s\n")%(volume,
                             location_cookie,
                             size,
                             file_family,
                             pnfsFilename,
                             volume_filepath,
                             id,
                             volume_fileP,  #.id,
                             bit_file_id,
                             drive,
                             crc)
        
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.Filepath

        #Check to make sure that this is a regular file.
        confirm_regular_file(use_filepath)
        
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "xref")
        
        self.write_extended_attribute(use_name, value, use_filepath)

    ######################################################################

    #Common function for setting the named attributes of a directory.
    # xattr_name - should be fully quallified attribute name (user.<something>)
    # value - is the new value to write the the extended attribute
    # directory - if set, it must contain the directory to set the
    #             extended attribute
    def set_dir_xattr(self, xattr_name, value, directory=None):
        #Get the directory to use and if we need to set a member variable,
        # (set_member will be True or False).
        use_directory, set_member = self.get_directory(directory)
 
        #Check to make sure that this is a directory.
        confirm_directory(use_directory)

        try:
            self.write_extended_attribute(xattr_name, value, use_directory)
        except IndexError:
            #Only OSError and IOError should be raised.
            raise IOError(errno.EIO, "Library tag is empty.")

        if set_member:
            setattr(self, xattr_name, value)
        
        return value

    # store a new tape library tag
    def set_library(self, value, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "library")

        return self.set_dir_xattr(use_name, value, directory)

    # store a new tape file_family tag
    def set_file_family(self, value, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "file_family")

        return self.set_dir_xattr(use_name, value, directory)

    # store a new tape file_family_wrapper tag
    def set_file_family_wrapper(self, value, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "file_family_wrapper")

        return self.set_dir_xattr(use_name, value, directory)

    # store a new tape file_family_width tag
    def set_file_family_width(self, value, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "file_family_width")

        return self.set_dir_xattr(use_name, value, directory)

    # store a new tape storage_group tag
    def set_storage_group(self, value, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "storage_group")

        return self.set_dir_xattr(use_name, value, directory)

    #Common function for getting the named attributes of a directory.
    # xattr_name - should be fully quallified attribute name (user.<something>)
    # directory - if set, it must contain the directory to get the
    #             extended attribute
    def get_dir_xattr(self, xattr_name, directory=None):
        #Get the directory to use and if we need to set a member variable,
        # (set_member will be True or False).
        use_directory, set_member = self.get_directory(directory)
 
        #Check to make sure that this is a directory.
        confirm_directory(use_directory)

        try:
            library = self.read_extended_attributes(xattr_name, use_directory)
        except IndexError:
            #Only OSError and IOError should be raised.
            raise IOError(errno.EIO, "Library tag is empty.")

        if set_member:
            self.library = library
        
        return library

    # get the tape library
    def get_library(self, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "library")

        return self.get_dir_xattr(use_name, directory)

    # get the tape file_family
    def get_file_family(self, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "file_family")

        return self.get_dir_xattr(use_name, directory)

    # get the tape file_family_wrapper
    def get_file_family_wrapper(self, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "file_family_wrapper")

        return self.get_dir_xattr(use_name, directory)

    # get the tape file_family_width
    def get_file_family_width(self, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "file_family_width")

        return self.get_dir_xattr(use_name, directory)

    # get the tape storage_group
    def get_storage_group(self, directory=None):
        #Get the name of the attribute.
        use_name = "%s.%s" % (DEFAULT_XATTR_NAMESPACE, "storage_group")

        return self.get_dir_xattr(use_name, directory)

    #Return just the mount point section of a storage file system path.
    def get_mount_point(self, filepath=None):
        if filepath:
            fname = filepath
        else:
            fname = self.Filepath

        #Make sure that the original exits.
        old_stat_info = self.stat(fname)

        #Strip off one directory segment at a time.  We are looking for
        # where pnfs stops.
        old_path = fname
        current_path = os.path.dirname(fname)
        while current_path:
            new_stat_info = self.stat(current_path)

            #If the device values are different, we found the mount point.
            if old_stat_info[stat.ST_DEV] != new_stat_info[stat.ST_DEV]:
                return old_path
            
            #If we've reached the top of the namespace, stop looking.
            if current_path == "/":
                return "/"

            #Setup the variables for the next loop.
            old_path = current_path
            current_path = os.path.dirname(current_path)

        return None

    ######################################################################

    #Prints out the bfid extended attribute for the specified file.
    def fs_bfid(self):
        try:
            print self.get_bit_file_id()
            return 0
        except IndexError:
            print UNKNOWN
            return 1
        except (IOError, OSError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    #Prints out the xref extended attribute for the specified file.
    def fs_xref(self):
        names = ["volume", "location_cookie", "size", "file_family",
                 "original_name:", "map_file", "fs_id", "id_map",
                 "bfid", "origdrive", "crc"]
        try:
            data =  self.get_xreference()
            #With the data stored in lists, with corresponding values
            # based on the index, then just print them out.
            for i in range(len(names)):
                print "%s: %s" % (names[i], data[i])
            return 0
        except IndexError:
            print UNKNOWN
            return 1
        except (IOError, OSError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    #Prints out the specified extended attribute of the specified file
    # or directory.
    def fs_xattr(self, intf):
        try:
            data = self.read_extended_attributes(intf.extended_attribute)
            print data
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    #For legacy purposes.
    fs_cat = fs_xattr
    fs_layer = fs_xattr

    #Prints out all the extended attributes of the file or directory.
    def fs_xattrs(self, intf):
        try:
            data = self.read_extended_attributes(None, filepath=intf.filename)
            for key in data.keys():
                try:
                    print key, "=",  data[key]
                except (OSError, IOError, IndexError), detail:
                    print key, ":", detail
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def fs_xattrchmod(self):
        #It should be noted that only the Solaris version of extended
        # attributes supports the attributes having different persmissions
        # from the data file itself.
        sys.stderr.write("%s\n" % ("Feature not yet implemented.",))
        return 1

    def fs_xattrchown(self):
        #It should be noted that only the Solaris version of extended
        # attributes supports the attributes having different persmissions
        # from the data file itself.
        sys.stderr.write("%s\n" % ("Feature not yet implemented.",))
        return 1

    #For legacy purposes.
    fs_tagchmod = fs_xattrchmod
    fs_tagchown = fs_xattrchown

    #Removes the named extended attribute for the target.
    def fs_xattrrm(self, intf, target=None):
        try:
            self.write_extended_attribute(intf.extended_attribute, None,
                                          filepath=target)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    #For legacy purposes.
    fs_rm = fs_xattrrm

    #Prints the filesize.
    def fs_filesize(self):
        try:
            self.get_file_size()
            print self.file_size
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    #Common function for fs_library(), fs_file_family_wrapper(),
    # fs_file_family_width(), fs_storage_group() and fs_tag()
    def __fs_dir_xattr(self, xattr_name, intf, directory=None,
                       valid_charset=None):
        try:
            #Most UNIX systems impose a namespace on the xattr_name, for
            # this function we need to strip this information away.
            if xattr_name.find(".") != -1:
                xattr_namespace, xattr_short_name = xattr_name.split(".")
            else:
                xattr_namespace, xattr_short_name = "", xattr_name

            if getattr(intf, 'extended_attribute', None):
                #From --tag.
                xattr_value = intf.extended_attribute
            else:
                #From --library, --storage-group, etc.
                xattr_value = getattr(intf, "%s" % (xattr_short_name,))

            #Get or set the indicated value.
            if xattr_value == 1 or getattr(intf, 'tag:', None):
                #Obtain the function for getting the info.
                func = getattr(self, "get_%s" % (xattr_short_name,))
                #Run this function and print the results.
                print func(directory=directory)
            else:
                if (valid_charset and
                    charset.is_string_in_character_set(xattr_value,
                                                       valid_charset)
                    ) or \
                    (not valid_charset and
                     charset.is_in_charset(xattr_value)):
                    #Obtain the function for setting the value.
                    func = getattr(self, "set_%s" % (xattr_short_name,))
                    ##Run this function to set the value.
                    func(xattr_value, directory=directory)
                else:
                    message = INVALID_CHARACTERS_ERROR % (xattr_short_name,)
                    sys.stderr.write("%s\n" % (message,))
                    return 1
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1
        
    #Print or edit the library.
    def fs_library(self, intf):
        #As of encp v3_6a allow the comma (,) character so that multiple
        # copies can be enabled.
        return self.__fs_dir_xattr("library", intf,
                                 valid_charset=charset.charset + ",")

    #Print or edit the file family.
    def fs_file_family(self, intf):
        try:
            if intf.file_family == 1:
                print self.get_file_family()
            else:
                #Restrict the characters allowed in the file_family.
                if not charset.is_in_charset(intf.file_family):
                    message = INVALID_CHARACTERS_ERROR % ("file_family",)
                    sys.stderr.write("%s\n" % (message,))
                    return 1
                #Don't allow users to set file_families with the
                # migration pattern.
                elif re.search(".*-MIGRATION$", intf.file_family):
                    message = INVALID_PATTERN_ERROR % ("-MIGRATION",)
                    sys.stderr.write("%s\n" % (message,))
                    return 1
                #Don't allow users to set file_families with the
                # duplication pattern.
                elif re.search("_copy_[0-9]*$", intf.file_family):
                    message = INVALID_PATTERN_ERROR % ("_copy_#",)
                    sys.stderr.write("%s\n" % (message,))
                    return 1
                else:
                    self.set_file_family(intf.file_family)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1
    
    #Print or edit the file family wrapper.
    def fs_file_family_wrapper(self, intf):
        return self.__fs_dir_xattr("file_family_wrapper", intf)

    #Print or edit the file family width.
    def fs_file_family_width(self, intf):
        return self.__fs_dir_xattr("file_family_width", intf)

    #Print or edit the storage group.
    def fs_storage_group(self, intf):
        return self.__fs_dir_xattr("storage_group", intf)

    #Print the named extened attribute.  For legacy purposes.  See fs_xattr().
    def fs_tag(self, intf):
        return self.__fs_dir_xattr(intf.extended_attribute, intf,
                                   directory=intf.directory)
    
    #For legacy purposes.  Resticts the file type to directories.
    def fs_tags(self, intf):
        #Get the directory to use.
        use_directory, unused = self.get_directory(intf.file)
 
        #Check to make sure that this is a directory.
        confirm_directory(use_directory)

        return self.fs_xattrs(intf)

##########################################################################

    #Copies the contents of the unix file to the specified extended attribute.
    def fs_cp(self):
        try:
            f = file_utils.wrapper(os.open, (intf.unixfile, 'r'))

            data = f.readlines()
            file_data_as_string = ""
            for line in data:
                file_data_as_string = file_data_as_string + line

            f.close()

            self.write_extended_attribute(intf.extended_attribute,
                                          file_data_as_string)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    #Writes the supplied text to the specified extened attribute.
    def fs_echo(self, intf, target=None):
        try:
            self.write_extended_attribute(intf.extended_attribute, intf.text,
                                          filepath=target)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    #Set the size of the file.
    def fs_size(self, intf):
        try:
            self.set_file_size(intf.filesize)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def fs_dump(self):
        return self.dump()

    #For legacy purposes.
    def fs_tagecho(self, intf):
        return self.fs_echo(intf, target=os.getcwd())

    #For legacy purposes.
    def fs_tagrm(self, intf):
        return self.fs_xattrrm(intf, target=os.getcwd())

    #Return the mount point that the target file or directory is under.
    def fs_mount_point(self):
        try:
            print_results(self.get_mount_point(intf.file))
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1
        except (AttributeError, ValueError), detail:
            sys.stderr.write("A valid file or directory was not entered.\n")
            return 1
        
##########################################################################

class FsInterface(option.Interface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.test = 0
        #self.status = 0
        #self.info = 0
        #self.file = ""
        #self.restore = 0
        #These my be used, they may not.
        #self.duplicate_file = None
        option.Interface.__init__(self, args=args, user_mode=user_mode)

    pnfs_user_options = {
        option.BFID:{option.HELP_STRING:"lists the bit file id for file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"bfid",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.FORCE_SET_DEFAULT:option.FORCE,
		     option.USER_LEVEL:option.USER
                     },
        option.CAT:{option.HELP_STRING:"see --layer",
                    option.DEFAULT_VALUE:option.DEFAULT,
                    option.DEFAULT_NAME:"xattr",
                    option.DEFAULT_TYPE:option.INTEGER,
                    option.VALUE_NAME:"file",
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"filename",
                    option.FORCE_SET_DEFAULT:option.FORCE,
                    option.USER_LEVEL:option.USER,
                    option.EXTRA_VALUES:[{option.DEFAULT_VALUE:option.DEFAULT,
                                          option.DEFAULT_NAME:"extended_attribute",
                                          option.DEFAULT_TYPE:option.STRING,
                                          option.VALUE_NAME:"extended_attribute",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,
                                          option.VALUE_LABEL:"extended_attribute",
                                          }]
                    },
        #option.DUPLICATE:{option.HELP_STRING:"gets/sets duplicate file values",
        #             option.DEFAULT_VALUE:option.DEFAULT,
        #             option.DEFAULT_NAME:"duplicate",
        #             option.DEFAULT_TYPE:option.INTEGER,
        #             option.VALUE_USAGE:option.IGNORED,
	#	     option.USER_LEVEL:option.ADMIN,
        #             option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
        #                                   option.DEFAULT_NAME:"file",
        #                                   option.DEFAULT_TYPE:option.STRING,
        #                                   option.VALUE_NAME:"file",
        #                                   option.VALUE_TYPE:option.STRING,
        #                                   option.VALUE_USAGE:option.OPTIONAL,
        #                                   option.VALUE_LABEL:"filename",
        #                                 option.FORCE_SET_DEFAULT:option.FORCE,
        #                                   },
        #                                  {option.DEFAULT_VALUE:"",
        #                                  option.DEFAULT_NAME:"duplicate_file",
        #                                   option.DEFAULT_TYPE:option.STRING,
        #                                   option.VALUE_NAME:"duplicat_file",
        #                                   option.VALUE_TYPE:option.STRING,
        #                                   option.VALUE_USAGE:option.OPTIONAL,
        #                               option.VALUE_LABEL:"duplicate_filename",
        #                                 option.FORCE_SET_DEFAULT:option.FORCE,
        #                                   },]
        #             },
        #option.ENSTORE_STATE:{option.HELP_STRING:"lists whether enstore " \
        #                                         "is still alive",
        #                 option.DEFAULT_VALUE:option.DEFAULT,
        #                 option.DEFAULT_NAME:"enstore_state",
        #                 option.DEFAULT_TYPE:option.INTEGER,
        #                 option.VALUE_NAME:"directory",
        #                 option.VALUE_TYPE:option.STRING,
        #                 option.VALUE_USAGE:option.REQUIRED,
        #                 option.USER_LEVEL:option.USER,
        #                 option.FORCE_SET_DEFAULT:option.FORCE,
        #             },
        option.FILE_FAMILY:{option.HELP_STRING: \
                            "gets file family xattr, default; "
                            "sets file family xattr, optional",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_NAME:"file_family",
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_TYPE:option.STRING,
                            option.USER_LEVEL:option.USER,
                            option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.FILE_FAMILY_WIDTH:{option.HELP_STRING: \
                                  "gets file family width xattr, default; "
                                  "sets file family width xattr, optional",
                                  option.DEFAULT_VALUE:option.DEFAULT,
                                  option.DEFAULT_NAME:"file_family_width",
                                  option.DEFAULT_TYPE:option.INTEGER,
                                  option.VALUE_TYPE:option.STRING,
                                  option.USER_LEVEL:option.USER,
                                  option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.FILE_FAMILY_WRAPPER:{option.HELP_STRING: \
                                    "gets file family wrapper xattr, default; "
                                    "sets file family wrapper xattr, optional",
                                    option.DEFAULT_VALUE:option.DEFAULT,
                                    option.DEFAULT_NAME:"file_family_wrapper",
                                    option.DEFAULT_TYPE:option.INTEGER,
                                    option.VALUE_TYPE:option.STRING,
                                    option.USER_LEVEL:option.USER,
                                    option.VALUE_USAGE:option.OPTIONAL,
                   },
	option.FILESIZE:{option.HELP_STRING:"print out real filesize",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"filesize",
                         option.DEFAULT_TYPE:option.INTEGER,
			 option.VALUE_NAME:"file",
			 option.VALUE_TYPE:option.STRING,
			 option.VALUE_LABEL:"filename",
                         option.USER_LEVEL:option.USER,
			 option.VALUE_USAGE:option.REQUIRED,
			 },
        option.INFO:{option.HELP_STRING:"see --xref",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"xref",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.USER_LEVEL:option.USER,
                     option.FORCE_SET_DEFAULT:option.FORCE,
                },
        option.LAYER:{option.HELP_STRING:"see --xattr",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"xattr",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"file",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"filename",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.USER,
                      option.EXTRA_VALUES:[{option.DEFAULT_VALUE:
                                                                option.DEFAULT,
                                            option.DEFAULT_NAME:"extended_attribute",
                                            option.DEFAULT_TYPE:option.INTEGER,
                                            option.VALUE_NAME:"extended_attribute",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.OPTIONAL,
                                            option.VALUE_LABEL:"named_xattr",
                                            }]
                 },
        option.LIBRARY:{option.HELP_STRING:"gets library xattr, default; " \
                                      "sets library xattr, optional",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"library",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_TYPE:option.STRING,
                   option.USER_LEVEL:option.USER,
                   option.VALUE_USAGE:option.OPTIONAL,
                   },
        #option.PNFS_STATE:{option.HELP_STRING:"lists whether pnfs is " \
        #                                      "still alive",
        #              option.DEFAULT_VALUE:option.DEFAULT,
        #              option.DEFAULT_NAME:"pnfs_state",
        #              option.DEFAULT_TYPE:option.INTEGER,
        #              option.VALUE_NAME:"directory",
        #              option.VALUE_TYPE:option.STRING,
        #              option.VALUE_USAGE:option.REQUIRED,
        #              option.USER_LEVEL:option.USER,
        #              option.FORCE_SET_DEFAULT:option.FORCE,
        #              },
        option.STORAGE_GROUP:{option.HELP_STRING:"gets storage group xattr, " \
                              "default; sets storage group xattr, optional",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"storage_group",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_TYPE:option.STRING,
                         option.USER_LEVEL:option.ADMIN,
                         option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.TAG:{option.HELP_STRING:"see --xattr",
                    #option.DEFAULT_VALUE:option.DEFAULT,
                    #option.DEFAULT_NAME:"extended_attribute",
                    #option.DEFAULT_TYPE:option.STRING,
                    option.VALUE_NAME:"extended_attribute",
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"extended_attribute",
                    option.FORCE_SET_DEFAULT:1,
                    option.USER_LEVEL:option.USER,
                    option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                          option.DEFAULT_NAME:"file",
                                          option.DEFAULT_TYPE:option.STRING,
                                          option.VALUE_NAME:"file",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,
                                          option.VALUE_LABEL:"directory",
                                         option.FORCE_SET_DEFAULT:option.FORCE,
                                          }]
               },
        option.TAGCHMOD:{option.HELP_STRING:"see --xattrchmod",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"tagchmod",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"permissions",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.USER,
                         option.EXTRA_VALUES:[{option.VALUE_NAME:"extend_attribute",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_LABEL:"extended_attribute",
                                              },]
                         },
        option.TAGCHOWN:{option.HELP_STRING:"see --xattrchown",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"tagchown",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"owner",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.USER,
                         option.EXTRA_VALUES:[{option.VALUE_NAME:"extended_attribute",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_LABEL:"extended_attribute",
                                              },]
                         },
        option.TAGS:{option.HELP_STRING:"see --xattrs",
                option.DEFAULT_VALUE:option.DEFAULT,
                option.DEFAULT_NAME:"tags",
                option.DEFAULT_TYPE:option.INTEGER,
                option.VALUE_USAGE:option.IGNORED,
                option.USER_LEVEL:option.USER,
                option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                      option.DEFAULT_NAME:"file",
                                      option.DEFAULT_TYPE:option.STRING,
                                      option.VALUE_NAME:"file",
                                      option.VALUE_TYPE:option.STRING,
                                      option.VALUE_USAGE:option.OPTIONAL,
                                      option.VALUE_LABEL:"directory",
                                      option.FORCE_SET_DEFAULT:option.FORCE,
                                      }]
                },
        option.XATTR:{option.HELP_STRING:"lists the named extended " \
                      "attribute for the file or directory",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"xattr",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"file",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"filename",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.USER,
                      option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"user.bfid",
                                            option.DEFAULT_NAME:"extended_attribute",
                                            option.DEFAULT_TYPE:option.STRING,
                                            option.VALUE_NAME:"extended_attribute",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.OPTIONAL,
                                            option.VALUE_LABEL:"extended_attribute",
                                            }]
                      },
        option.XATTRCHMOD:{option.HELP_STRING:"changes the permissions"
                         " for the xattr; use UNIX chmod style permissions",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"tagchmod",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"permissions",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.USER,
                         option.EXTRA_VALUES:[{
                                      option.VALUE_NAME:"extended_attribute",
                                      option.VALUE_TYPE:option.STRING,
                                      option.VALUE_USAGE:option.REQUIRED,
                                      option.VALUE_LABEL:"extemded_attribute",
                                              },]
                         },
        option.XATTRCHOWN:{option.HELP_STRING:"changes the ownership"
                         " for the xattr; OWNER can be 'owner' or 'owner.group'",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"tagchown",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"owner",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.USER,
                         option.EXTRA_VALUES:[{option.VALUE_NAME:"extended_attribute",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_LABEL:"extended_attribute",
                                              },]
                         },
        option.XATTRS:{option.HELP_STRING:"lists xattr values and permissions",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"xattrs",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.USER,
                       option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                             option.DEFAULT_NAME:"file",
                                             option.DEFAULT_TYPE:option.STRING,
                                             option.VALUE_NAME:"file",
                                             option.VALUE_TYPE:option.STRING,
                                             option.VALUE_LABEL:"filename",
                                             option.VALUE_USAGE:option.OPTIONAL,
                                             option.FORCE_SET_DEFAULT:option.FORCE,
                                             }]
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
                     option.USER_LEVEL:option.USER,
                     option.FORCE_SET_DEFAULT:option.FORCE,
                },
        }

    pnfs_admin_options = {
        option.CP:{option.HELP_STRING:"echos text to named xattr of the file" \
                   " or directory",
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
                                        {option.VALUE_NAME:"extended_attribute",
                                         option.VALUE_TYPE:option.STRING,
                                         option.VALUE_USAGE:option.REQUIRED,
                                         option.VALUE_LABEL:"extended_attribute",
                                         },]
                   },
        #option.CONST:{option.HELP_STRING:"",
        #              option.DEFAULT_VALUE:option.DEFAULT,
        #              option.DEFAULT_NAME:"const",
        #              option.DEFAULT_TYPE:option.INTEGER,
        #              option.VALUE_NAME:"file",
        #              option.VALUE_TYPE:option.STRING,
        #              option.VALUE_USAGE:option.REQUIRED,
        #              option.VALUE_LABEL:"filename",
        #              option.FORCE_SET_DEFAULT:option.FORCE,
        #              option.USER_LEVEL:option.ADMIN,
        #              },
        #option.COUNTERS:{option.HELP_STRING:"",
        #                 option.DEFAULT_VALUE:option.DEFAULT,
        #                 option.DEFAULT_NAME:"counters",
        #                 option.DEFAULT_TYPE:option.INTEGER,
        #                 option.VALUE_NAME:"file",
        #                 option.VALUE_TYPE:option.STRING,
        #                 option.VALUE_USAGE:option.REQUIRED,
        #                 option.VALUE_LABEL:"filename",
        #                 option.FORCE_SET_DEFAULT:option.FORCE,
        #                 option.USER_LEVEL:option.ADMIN,
        #                 },
        #option.COUNTERSN:{option.HELP_STRING:"(must have cwd in pnfs)",
        #                  option.DEFAULT_VALUE:option.DEFAULT,
        #                  option.DEFAULT_NAME:"countersN",
        #                  option.DEFAULT_TYPE:option.INTEGER,
        #                  option.VALUE_NAME:"dbnum",
        #                  option.VALUE_TYPE:option.STRING,
        #                  option.VALUE_USAGE:option.REQUIRED,
        #                  option.FORCE_SET_DEFAULT:option.FORCE,
        #                  option.USER_LEVEL:option.ADMIN,
        #                  },
        #option.CURSOR:{option.HELP_STRING:"",
        #               option.DEFAULT_VALUE:option.DEFAULT,
        #               option.DEFAULT_NAME:"cursor",
        #               option.DEFAULT_TYPE:option.INTEGER,
        #               option.VALUE_NAME:"file",
        #               option.VALUE_TYPE:option.STRING,
        #               option.VALUE_USAGE:option.REQUIRED,
        #               option.VALUE_LABEL:"filename",
        #               option.FORCE_SET_DEFAULT:option.FORCE,
        #               option.USER_LEVEL:option.ADMIN,
        #               },
        #option.DATABASE:{option.HELP_STRING:"",
        #                 option.DEFAULT_VALUE:option.DEFAULT,
        #                 option.DEFAULT_NAME:"database",
        #                 option.DEFAULT_TYPE:option.INTEGER,
        #                 option.VALUE_NAME:"file",
        #                 option.VALUE_TYPE:option.STRING,
        #                 option.VALUE_USAGE:option.REQUIRED,
        #                 option.VALUE_LABEL:"filename",
        #                 option.FORCE_SET_DEFAULT:option.FORCE,
        #                 option.USER_LEVEL:option.ADMIN,
        #                 },
        #option.DATABASEN:{option.HELP_STRING:"(must have cwd in pnfs)",
        #                  option.DEFAULT_VALUE:option.DEFAULT,
        #                  option.DEFAULT_NAME:"databaseN",
        #                  option.DEFAULT_TYPE:option.INTEGER,
        #                  option.VALUE_NAME:"dbnum",
        #                  option.VALUE_TYPE:option.STRING,
        #                  option.VALUE_USAGE:option.REQUIRED,
        #                  option.FORCE_SET_DEFAULT:option.FORCE,
        #                  option.USER_LEVEL:option.ADMIN,
        #                  },
        #option.DOWN:{option.HELP_STRING:"creates enstore system-down " \
        #                                "wormhole to prevent transfers",
        #        option.DEFAULT_VALUE:option.DEFAULT,
        #        option.DEFAULT_NAME:"down",
        #        option.DEFAULT_TYPE:option.INTEGER,
        #        option.VALUE_NAME:"reason",
        #        option.VALUE_TYPE:option.STRING,
        #        option.VALUE_USAGE:option.REQUIRED,
        #        option.FORCE_SET_DEFAULT:option.FORCE,
        #        option.USER_LEVEL:option.ADMIN,
        #        },
        option.DUMP:{option.HELP_STRING:"dumps info",
              option.DEFAULT_VALUE:option.DEFAULT,
              option.DEFAULT_NAME:"dump",
              option.DEFAULT_TYPE:option.INTEGER,
              option.VALUE_USAGE:option.IGNORED,
              option.USER_LEVEL:option.ADMIN,
              },
        option.ECHO:{option.HELP_STRING:"sets text to named xattr of the " \
                     "file or directory",
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
                                          {option.VALUE_NAME:"extended_attribute",
                                           option.VALUE_TYPE:option.STRING,
                                           option.VALUE_USAGE:option.REQUIRED,
                                           option.VALUE_LABEL:"extended_attribute",
                                           },]
                },
        #option.ID:{option.HELP_STRING:"prints the pnfs id",
        #           option.DEFAULT_VALUE:option.DEFAULT,
        #           option.DEFAULT_NAME:"id",
        #           option.DEFAULT_TYPE:option.INTEGER,
        #           option.VALUE_NAME:"file",
        #           option.VALUE_TYPE:option.STRING,
        #           option.VALUE_USAGE:option.REQUIRED,
        #           option.VALUE_LABEL:"filename",
        #           option.FORCE_SET_DEFAULT:option.FORCE,
        #           option.USER_LEVEL:option.ADMIN,
        #      },
        #option.IO:{option.HELP_STRING:"sets io mode (can't clear it easily)",
        #           option.DEFAULT_VALUE:option.DEFAULT,
        #           option.DEFAULT_NAME:"io",
        #           option.DEFAULT_TYPE:option.INTEGER,
        #           option.VALUE_NAME:"file",
        #           option.VALUE_TYPE:option.STRING,
        #           option.VALUE_USAGE:option.REQUIRED,
        #           option.VALUE_LABEL:"filename",
        #           option.FORCE_SET_DEFAULT:option.FORCE,
        #           option.USER_LEVEL:option.ADMIN,
        #           },
        #option.LS:{option.HELP_STRING:"does an ls on the named layer " \
        #                              "in the file",
        #           option.DEFAULT_VALUE:option.DEFAULT,
        #           option.DEFAULT_NAME:"ls",
        #           option.DEFAULT_TYPE:option.INTEGER,
        #           option.VALUE_NAME:"file",
        #           option.VALUE_TYPE:option.STRING,
        #           option.VALUE_USAGE:option.REQUIRED,
        #           option.VALUE_LABEL:"filename",
        #           option.FORCE_SET_DEFAULT:option.FORCE,
        #           option.USER_LEVEL:option.ADMIN,
        #           option.EXTRA_VALUES:[{option.DEFAULT_VALUE:option.DEFAULT,
        #                                 option.DEFAULT_NAME:"named_layer",
        #                                 option.DEFAULT_TYPE:option.INTEGER,
        #                                 option.VALUE_NAME:"named_layer",
        #                                 option.VALUE_TYPE:option.STRING,
        #                                 option.VALUE_USAGE:option.OPTIONAL,
        #                                 option.VALUE_LABEL:"layer",
        #                                 }]
        #      },
        option.MOUNT_POINT:{option.HELP_STRING:"prints the mount point of " \
                            "the pnfs file or directory",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_NAME:"mount_point",
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_NAME:"file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"filename",
                            option.FORCE_SET_DEFAULT:option.FORCE,
                            option.USER_LEVEL:option.ADMIN,
                            },
        #option.NAMEOF:{option.HELP_STRING:"prints the filename of the pnfs id"\
        #               " (CWD must be under /pnfs)",
        #               option.DEFAULT_VALUE:option.DEFAULT,
        #               option.DEFAULT_NAME:"nameof",
        #               option.DEFAULT_TYPE:option.INTEGER,
        #               option.VALUE_NAME:"pnfs_id",
        #               option.VALUE_TYPE:option.STRING,
        #               option.VALUE_USAGE:option.REQUIRED,
        #               option.FORCE_SET_DEFAULT:option.FORCE,
        #               option.USER_LEVEL:option.ADMIN,
        #               },
        #option.PARENT:{option.HELP_STRING:"prints the pnfs id of the parent " \
        #               "directory (CWD must be under /pnfs)",
        #               option.DEFAULT_VALUE:option.DEFAULT,
        #               option.DEFAULT_NAME:"parent",
        #               option.DEFAULT_TYPE:option.INTEGER,
        #               option.VALUE_NAME:"pnfs_id",
        #               option.VALUE_TYPE:option.STRING,
        #               option.VALUE_USAGE:option.REQUIRED,
        #               option.FORCE_SET_DEFAULT:option.FORCE,
        #               option.USER_LEVEL:option.ADMIN,
        #               },
        #option.PATH:{option.HELP_STRING:"prints the file path of the pnfs id"\
        #                                " (CWD must be under /pnfs)",
        #             option.DEFAULT_VALUE:option.DEFAULT,
        #             option.DEFAULT_NAME:"path",
        #             option.DEFAULT_TYPE:option.INTEGER,
        #             option.VALUE_NAME:"pnfs_id",
        #             option.VALUE_TYPE:option.STRING,
        #             option.VALUE_USAGE:option.REQUIRED,
        #             option.FORCE_SET_DEFAULT:option.FORCE,
        #             option.USER_LEVEL:option.ADMIN,
        #             },
        #option.POSITION:{option.HELP_STRING:"",
        #                 option.DEFAULT_VALUE:option.DEFAULT,
        #                 option.DEFAULT_NAME:"position",
        #                 option.DEFAULT_TYPE:option.INTEGER,
        #                 option.VALUE_NAME:"file",
        #                 option.VALUE_TYPE:option.STRING,
        #                 option.VALUE_USAGE:option.REQUIRED,
        #                 option.VALUE_LABEL:"filename",
        #                 option.FORCE_SET_DEFAULT:option.FORCE,
        #                 option.USER_LEVEL:option.ADMIN,
        #                 },
        option.RM:{option.HELP_STRING:"see --xattrrm",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"rm",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   option.EXTRA_VALUES:[{option.VALUE_NAME:"extended_attribute",
                                         option.VALUE_TYPE:option.INTEGER,
                                         option.VALUE_USAGE:option.REQUIRED,
                                         option.VALUE_LABEL:"extended_attribute",
                                         },]
                   },
        #option.SHOWID:{option.HELP_STRING:"prints the pnfs id information",
        #               option.DEFAULT_VALUE:option.DEFAULT,
        #               option.DEFAULT_NAME:"showid",
        #               option.DEFAULT_TYPE:option.INTEGER,
        #               option.VALUE_NAME:"pnfs_id",
        #               option.VALUE_TYPE:option.STRING,
        #               option.VALUE_USAGE:option.REQUIRED,
        #               option.FORCE_SET_DEFAULT:option.FORCE,
        #               option.USER_LEVEL:option.ADMIN,
        #               },
        option.SIZE:{option.HELP_STRING:"sets the size of the file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"size",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.FORCE_SET_DEFAULT:option.FORCE,
                     option.USER_LEVEL:option.USER2,
                     option.EXTRA_VALUES:[{option.VALUE_NAME:"filesize",
                                           option.VALUE_TYPE:option.LONG,
                                           option.VALUE_USAGE:option.REQUIRED,
                                           },]
                },
        option.TAGECHO:{option.HELP_STRING:"see --echo",
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.DEFAULT_NAME:"tagecho",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.VALUE_NAME:"text",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.FORCE_SET_DEFAULT:option.FORCE,
                        option.USER_LEVEL:option.ADMIN,
                        option.EXTRA_VALUES:[{option.VALUE_NAME:"extended_attribute",
                                             option.VALUE_TYPE:option.STRING,
                                             option.VALUE_USAGE:option.REQUIRED,
                                             option.VALUE_LABEL:"extended_attribute",
                                             },]
                   },
        option.TAGRM:{option.HELP_STRING:"see --xattrrm",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"tagrm",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"extended_attribute",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"extended_attribute",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.ADMIN,
                 },
        option.XATTRRM:{option.HELP_STRING:"deletes extended attribute of " \
                        "the file or directory",
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.DEFAULT_NAME:"xattrrm",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.VALUE_NAME:"file",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"filename",
                        option.FORCE_SET_DEFAULT:option.FORCE,
                        option.USER_LEVEL:option.ADMIN,
                        option.EXTRA_VALUES:[{option.VALUE_NAME:"extended_attribute",
                                              option.VALUE_TYPE:option.STRING,
                                              option.VALUE_USAGE:option.REQUIRED,
                                              option.VALUE_LABEL:"extended_attribute",
                                              },]
                        },
        #option.UP:{option.HELP_STRING:"removes enstore system-down wormhole",
        #           option.DEFAULT_VALUE:option.DEFAULT,
        #           option.DEFAULT_NAME:"up",
        #           option.DEFAULT_TYPE:option.INTEGER,
        #           option.VALUE_USAGE:option.IGNORED,
        #           option.USER_LEVEL:option.ADMIN,
        #           },
        }
    
    def valid_dictionaries(self):
        return (self.help_options, self.pnfs_user_options,
                self.pnfs_admin_options)

    # parse the options like normal but make sure we have other args
    def parse_options(self):
        self.pnfs_id = "" #Assume the command is a dir and/or file.
        self.file = ""
        self.dir = ""
        option.Interface.parse_options(self)

        if not self.option_list:
            self.print_usage("No valid options were given.")

        #No pnfs options take extra arguments beyond those specifed in the
        # option dictionaries.  If there are print message and exit.
        self.check_correct_count()

        if getattr(self, "help", None):
            self.print_help()

        if getattr(self, "usage", None):
            self.print_usage()



##############################################################################
def do_work(intf):

    rtn = 0

    try:
        fs=Fs(intf.file)
    except OSError, msg:
        sys.stderr.write("%s\n" % (str(msg),))
        return 1
        
    for arg in intf.option_list:
        if string.replace(arg, "_", "-") in intf.options.keys():
            arg = string.replace(arg, "-", "_")
            for instance in [fs]:
                if getattr(instance, "fs_"+arg, None):
                    try:
                        #Not all functions use/need intf passed in.
                        rtn = apply(getattr(instance, "fs_" + arg), ())
                    except TypeError:
                        rtn = apply(getattr(instance, "fs_" + arg), (intf,))
                    break
            else:
                print "fs_%s not found" % arg 
                rtn = 1

    return rtn

##############################################################################
if __name__ == "__main__":   # pragma: no cover

    intf = FsInterface(user_mode=0)

    intf._mode = "admin"

    sys.exit(do_work(intf))
