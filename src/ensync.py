#!/usr/bin/env python
#
# $Id$
#

# system imports
import sys
import os
import stat
import string

# enstore modules
import Trace
import option
import generic_client
import encp


MY_NAME = "ENSYNC"

############################################################################
############################################################################

#Returns a string representing the mount point the path is under.
def get_mount_point(path):
    temp = os.path.abspath(path)
    while temp != "":
        if os.path.ismount(temp):
            return temp
        else:
            temp = os.path.split(temp)[0]

    temp = "/"
    return temp
    
#Returns a string that gives the relative path from from_file to to_file.
def get_relative_cd(from_file, to_file):

    #Remove the last item from the from path since it is known and my share
    # beginning characters wich could cause commonprefix to give incorrect
    # result.  However, be sure the leave the trailing "/".  Also, remove
    # the comman part from both paths.
    use_from_file = os.path.split(from_file)[0] + "/"
    remove_abs = os.path.commonprefix([use_from_file, to_file])

    #This leaves the remaing path without a leading /.  This is necessary to
    # get an accurate count of remaining directories.
    temp = from_file[len(remove_abs):]
    remaining_dirs = string.count(temp, "/")

    #Take the remaing part of the to_file path and append the correct number
    # of "../" to make it relative.
    temp = to_file[len(remove_abs):]
    temp = "../" * remaining_dirs + temp

    return temp

def ensync(original_dir, pnfs_backup_dir):

    #Get the contents of the current directories.
    original_list = os.listdir(original_dir)
    pnfs_backup_list = os.listdir(pnfs_backup_dir)

    #Loop through each file in the original directory.  Only those that are
    # in this list and not in the matching pnfs directory are looked at.
    for file in original_list:

        filepath = os.path.join(original_dir, file)
        new_filepath = os.path.join(pnfs_backup_dir, os.path.basename(file))

        #If the file (directory, symbolic link, etc.) is not in pnfs.
        if file not in pnfs_backup_list:
            #This is not a good thing to jump accros file systems.
            if os.path.ismount(filepath) or os.path.ismount(new_filepath):
                sys.stderr.write("Detected a mount point.  Aborting.\n")
                sys.exit(1)
            
            #Handle symbolic links carefully.  Not all possible links can be
            # correctly handled.
            if os.path.islink(filepath):
                #Determine the inode that the link points to.
                if os.path.isabs(os.readlink(filepath)):
                    target = os.readlink(filepath)
                else:
                    target = os.path.join(original_dir, os.readlink(filepath))
                target = os.path.abspath(target)

                #If the target is on the same file system, handle it.
                if get_mount_point(target) == get_mount_point(filepath):
                    new_target = os.path.join(pnfs_backup_dir,
                                              get_relative_cd(filepath,target))
                    new_target = os.path.normpath(new_target)
                else:
                    sys.stderr.write(
                        "Unable to handle original link: %s -> %s\n" % \
                        (filepath, target))
                    continue

                Trace.trace(1, "Creating symbolic link: %s -> %s" % \
                            (new_filepath, new_target))
                
                #The first argument is the old thing that already exists, the
                # second is the new link that is to be created.
                os.symlink(new_target, new_filepath)

            #This is for the making of directories.  It keeps the permissions
            # the original directory has.
            elif os.path.isdir(filepath):
                Trace.trace(1, "Making new directory %s." % new_filepath)
                os.mkdir(new_filepath, os.stat(filepath)[stat.ST_MODE])

            #This copies new files into enstore.
            elif os.path.isfile(filepath):
                Trace.trace(1, "Copying file %s to %s." % \
                            (filepath, new_filepath))
                cmd = "encp --verbose 4 %s %s" % (filepath, new_filepath)
                fo = os.popen(cmd)
                encp_output = fo.readlines()
                rtn = fo.close()
                if rtn:
                    sys.stderr.write("ENCP ERROR\n")
                    sys.stderr.write(string.join(encp_output))

        #If the current "file" is a directory descend into it and repeat.
        if os.path.isdir(filepath):
            try:
                ensync(filepath, new_filepath)
            except (OSError, IOError), msg:
                msg2 = "Sync. failed for %s from %s.  Continuing.\n" % \
                       (new_filepath, str(msg))
                sys.stderr.write(msg2)

############################################################################
############################################################################

class EnsyncInterface(option.Interface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        self.verbose = 0
        
        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.ensync_options)
    
    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

        #Process these at the beginning.
        if getattr(self, "help") and self.help:
            ret = self.print_help()
        if getattr(self, "usage") and self.usage:
            ret = self.print_usage()

        #There should be two directories in self.args.  They correspond to
        # the to values in the parameters list (see below).
        if len(self.args) != 2:
            self.print_usage()

    #Required non switch options.
    parameters = ["<src directory> <dst directory>"]
    
    ensync_options = {
        option.VERBOSE:{option.HELP_STRING:"print out information.",
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_TYPE:option.INTEGER,
                        option.USER_LEVEL:option.USER,},
        }

############################################################################
############################################################################
        
def main(intf):

    Trace.init(MY_NAME)
    for x in xrange(0, intf.verbose+1):
        Trace.do_print(x)
    Trace.trace( 6, 'Ensync called with args: %s'%(sys.argv,) )

    original_dir = os.path.abspath(intf.args[0])
    pnfs_backup_dir = os.path.abspath(intf.args[1])

    #Run some checks to make sure that things will work.

    #Make sure the inputs are directories.
    if not os.path.isdir(original_dir):
        sys.stderr.write("Target %s is not a directory.\n" % original_dir)
        sys.exit(1)
    if not os.path.isdir(pnfs_backup_dir):
        sys.stderr.write("Target %s is not a directory.\n" % pnfs_backup_dir)
        sys.exit(1)
        
    #Make sure that one is and one is not a pnfs directory.
    if original_dir[:5] == "/pnfs":
        sys.stderr.write("Source directory %s in pnfs.\n" % original_dir)
        sys.exit(1)
    if pnfs_backup_dir[:5] != "/pnfs":
        sys.stderr.write("Destination directory %s not in pnfs.\n" % \
                         pnfs_backup_dir)
        sys.exit(1)

    try:
        ensync(original_dir, pnfs_backup_dir)
    except (OSError, IOError), msg:
        msg2 = "Sync. failed for %s from %s.  Aborting at top level.\n" \
               % (pnfs_backup_dir, str(msg))
        sys.stderr.write(msg2)
        sys.exit(1)

############################################################################

def do_work(intf):
    
    encp.setup_signal_handling()

    main(intf)

############################################################################
############################################################################

if __name__ == '__main__':

    intf = EnsyncInterface(user_mode=0) #defualt admin

    do_work(intf)
