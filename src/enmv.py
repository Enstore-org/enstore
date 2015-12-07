#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import sys
#import pprint
import os
import re
import string
import errno
import stat
import time

import option
import delete_at_exit
import e_errors
import namespace
import enstore_functions2
import configuration_client
import file_clerk_client
import volume_clerk_client
import volume_family
import charset
import Trace
import file_utils

description = "enmv stands for ENstore MV.  It is a PNFS/Chimera and Enstore" \
              " aware tool for moving\nfiles around in PNFS/Chimera.  enmv" \
              " is required to move files between PNFS directories\nlocated" \
              " in different databases.\n"

#def quit(exit_code=1):
#    delete_at_exit.quit(exit_code)

def print_error(errcode,errmsg):
    output_format = str(errcode)+" "+str(errmsg) + '\n'
    output_format = "ERROR: "+output_format
    try:
        sys.stderr.write(output_format)
        sys.stderr.flush()
    except IOError:
        pass

# same_cookie(c1, c2) -- to see if c1 and c2 are the same
#Copied from encp.py.
def same_cookie(c1, c2):
    lc_re = re.compile("[0-9]{4}_[0-9]{9,9}_[0-9]{7,7}")
    match1=lc_re.search(c1)
    match2=lc_re.search(c2)
    if match1 and match2:
        #The location cookie resembles that of null and tape cookies.
        #  Only the last section of the cookie is important.
        try: # just to be paranoid
            return string.split(c1, '_')[-1] == string.split(c2, '_')[-1]
        except (ValueError, AttributeError, TypeError, IndexError):
            return 0
    else:
        #The location cookie is a disk cookie.
        return c1 == c2

def move_file(input_filename, output_filename, intf):

    #Check the input file.

    #The input file should exist.
    if not os.path.exists(input_filename):
        print_error(e_errors.USERERROR, "Input file does not exists.")
        sys.exit(1)
    #The input file should be a regular file.
    if not os.path.isfile(input_filename):
        print_error(e_errors.USERERROR, "Source file is not a file.")
        sys.exit(1)

    #Check the output file.

    #If the output file is a directory, append the filename to it.
    if os.path.exists(output_filename):
        if os.path.isdir(output_filename):
            #If the destination is a directory, append the filename.
            output_filename = os.path.join(output_filename,
                                           os.path.basename(input_filename))
        elif input_filename == output_filename:
            #If the file move is to itself, don't error.
            pass
        else:
            print_error(e_errors.USERERROR, "Output file already exists.")
            sys.exit(1)
    #Filenames must not contain control characters.
    if not charset.is_in_filenamecharset(output_filename):
        print_error(e_errors.USERERROR,
                    "Output filename contains invalid characters.")
        sys.exit(1)
    #Filenames must be short enough.
    for directory in output_filename.split("/"):
        if len(directory) > 199:
            print_error(e_errors.USERERROR,
                        os.strerror(errno.ENAMETOOLONG) + ": " + directory)
            sys.exit(1)

    #Obtain layer 1 and layer 4 information.
    try:
        sfs = namespace.StorageFS(input_filename)
    except (IOError, OSError), msg:
        print_error(e_errors.USERERROR,
                    "Trouble with storage file system: %s" % (str(msg),))
        sys.exit(1)
    try:
        sfs.get_bit_file_id()
    except (IOError, OSError), msg:
        print_error(e_errors.USERERROR,
                    "Unable to read layer 1: %s" % str(msg))
        sys.exit(1)
    try:
        sfs.get_xreference()
    except (IOError, OSError), msg:
        print_error(e_errors.USERERROR,
                    "Unable to read layer 4: %s" % str(msg))
        sys.exit(1)
    try:
        sfs.get_stat()
    except:
        print_error(e_errors.USERERROR,
                    "Unable to stat: %s" % str(msg))
        sys.exit(1)

    #Consistancy check that the bfids in layers 1 and 4 match.
    if sfs.bfid != sfs.bit_file_id:
        print_error(e_errors.CONFLICT, "Bit file ids do not match.")
        sys.exit(1)

    # Get the configuration server.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))

    #Get the file clerk information.
    fcc = file_clerk_client.FileClient(csc, rcv_timeout = 5, rcv_tries = 20)
    if fcc.server_address == None:
        print_error(e_errors.TIMEDOUT, "Unable to contact file clerk.")
        sys.exit(1)

    file_info = fcc.bfid_info(sfs.bit_file_id)
    if not e_errors.is_ok(file_info):
        print_error(file_info['status'][0], file_info['status'][1])
        sys.exit(1)

    #Get the volume clerk information.
    vcc = volume_clerk_client.VolumeClerkClient(csc, rcv_timeout = 5,
                                                rcv_tries = 20)
    if vcc.server_address == None:
        print_error(e_errors.TIMEDOUT, "Unable to contact volume clerk.")
        sys.exit(1)

    volume_info = vcc.inquire_vol(file_info['external_label'])
    if not e_errors.is_ok(file_info):
        print_error(file_info['status'][0], file_info['status'][1])
        sys.exit(1)

    #Check for inconsistancies.
    if sfs.volume != file_info['external_label']:
        print_error(e_errors.CONFLICT,
                    "Volume information does not match.")
        sys.exit(1)
    elif not same_cookie(sfs.location_cookie, file_info['location_cookie']):
        print_error(e_errors.CONFLICT,
                    "Location cookie information does not match.")
        sys.exit(1)
    elif long(sfs.size) != long(file_info['size']):
        print_error(e_errors.CONFLICT,
                    "File size information does not match.")
        sys.exit(1)
    ##In order to squeeze small file_families together on one tape using
    ## migration this check needs to be disabled.
    #elif sfs.origff != volume_family.extract_file_family(db_volume_family):
    #    #Note: Due to automigration the file family check in encp has been
    #    # removed.  We keep it here because changing the metadata while
    #    # the automigration is proceding is a bad idea.
    #    print_error(e_errors.CONFLICT,
    #                "File family information does not match.")
    #    sys.exit(1)
    #Mapfile is obsolete.
    elif sfs.pnfsid_file != file_info['pnfsid']:
        print_error(e_errors.CONFLICT,
                    "Pnfs/Chimera id information does not match.")
        sys.exit(1)
    elif sfs.bfid != file_info['bfid']:
        print_error(e_errors.CONFLICT,
                    "Bfid information does not match.")
        sys.exit(1)
    #Original drive not always recorded.
    elif sfs.origdrive != file_info['drive']:
        print_error(e_errors.CONFLICT,
                    "Original drive information does not match.")
        sys.exit(1)
    elif sfs.crc != namespace.UNKNOWN and \
             long(sfs.crc) != file_info['complete_crc']:
        print_error(e_errors.CONFLICT,
                    "CRC information does not match.")
        sys.exit(1)


    #Log the current information in case something goes wrong.

    #Record the input -> output file.
    Trace.message(1, "Moving %s to %s." % (input_filename, output_filename))
    Trace.log(e_errors.INFO,
              "Moving %s to %s." % (input_filename, output_filename))
    #List the input file's metadata.
    file_information = ("Old File Name: %s" % input_filename,
                        "Volume: %s" % sfs.volume,
                        "Location Cookie: %s" % sfs.location_cookie,
                        "Size: %s" % sfs.size,
                        "File Family: %s" % sfs.origff,
                        "PNFS ID: %s" % sfs.pnfsid_file,
                        "BFID: %s" % sfs.bfid,
                        "Original Drive: %s" % sfs.origdrive,
                        "CRC: %s" % file_info['complete_crc'],
                        "UID: %s" % sfs.pstat[stat.ST_UID],
                        "GID: %s" % sfs.pstat[stat.ST_GID],
                        "Permissions: %s (%s)" %
                 (enstore_functions2.bits_to_rwx(sfs.pstat[stat.ST_MODE]),
                  sfs.pstat[stat.ST_MODE]),
                        "Last Access: %s (%s)" %
                 (time.ctime(sfs.pstat[stat.ST_ATIME]),
                  sfs.pstat[stat.ST_ATIME]),
                        "Last Modification: %s (%s)" %
                 (time.ctime(sfs.pstat[stat.ST_MTIME]),
                  sfs.pstat[stat.ST_MTIME]))
    Trace.message(5, ("%s\n" * len(file_information) % file_information))
    Trace.log(e_errors.INFO,
              ("%s  " * len(file_information) % file_information))

    #Get the layer information that is NOT layer 1 or 4.
    layer_info = {}
    for i in [2, 3, 5, 6, 7]:
        try:
            #Copy layer N metadata.
            layer_info[i] = sfs.readlayer(i)
        except (OSError, IOError), msg:
            #Ignore EACCESS errors.  That is the error given when a layer is
            # turned off in the pnfs configuration file.
            if msg.args[0] != errno.EACCES:
                print_error(e_errors.OSERROR,
                            "%s layer %s update failed: %s" % \
                            (sfs.print_id, i, str(msg)))
                sys.exit(1)

    #Try to set euid and egid.  This is useful for using enmv on the
    # /pnfs/xyz type paths (not /pnfs/fs/usr/xyz) while being user root.
    try:
        os.setregid(os.getgid(), sfs.pstat[stat.ST_GID])
        os.setreuid(os.getuid(), sfs.pstat[stat.ST_UID])
    except OSError:
        pass

    #If the original file is set read-only and the target file is within
    # the same pnfs database area, then the outputfile at this point will
    # also be read-only (because rename() would succed).  Thus, we need to
    # turn on the write bits.
    try:
        #The 'other' bits don't help with reading/writing to the layers.
        # Thus we need to ignore this as a safe possiblity.
        #
        #The 'group' bits don't give you the ability to chmod() a file.

        if os.geteuid() == 0:
            pass
        elif os.geteuid() == sfs.pstat[stat.ST_UID]:
             if not (sfs.pstat[stat.ST_MODE] & stat.S_IRUSR) or \
                  not (sfs.pstat[stat.ST_MODE] & stat.S_IWUSR):
                 file_utils.chmod(output_filename,
                          sfs.pstat[stat.ST_MODE] | stat.S_IRUSR | stat.S_IWUSR)
        elif os.getegid() == sfs.pstat[stat.ST_GID] \
             and (sfs.pstat[stat.ST_MODE] & stat.S_IRGRP) and \
             (sfs.pstat[stat.ST_MODE] & stat.S_IWGRP):
            #Since, we don't need to change the permissions in this case,
            # we should be okay to proceed.
            pass
        elif sfs.pstat[stat.ST_GID] in os.getgroups() \
             and (sfs.pstat[stat.ST_MODE] & stat.S_IRGRP) and \
             (sfs.pstat[stat.ST_MODE] & stat.S_IWGRP):
            #Since, we don't need to change the permissions in this case,
            # we should be okay to proceed.
            pass
        else:
            print_error(e_errors.USERERROR,
                        "Insufficent permissions to move file")
            sys.exit(1)
    except OSError, msg:
        print_error(e_errors.OSERROR,
                    "Unable to set temporary permissions: %s" % str(msg))
        sys.exit(1)

    #Obtain the file family from the directory tag, if the user wants
    # to reset layer 4 file family with it.
    if intf.match_directory_file_family:
        new_dir_sfs = namespace.Tag(os.path.dirname(output_filename))
        directory_file_family = new_dir_sfs.get_file_family()
    else:
        directory_file_family = None

    try:
        #Attempt to rename the file.  This can work if the input and
        # output targets are under the same mount point.
        file_utils.wrapper(os.rename, (input_filename, output_filename))

        #Set the following for "success."
        out_fd = None
        in_fd = None
    except OSError, msg:
        #If the rename is attempted between two different mount points,
        # then the error EXDEV is returned from rename.  This situation
        # is currently called an error, but may need to be implemented
        # in the future.


        #If the rename was between two different pnfs database areas, then we
        # we see the error EPERM.  The following is not done atomically,
        # but there is no other way.  This involves creating a new file
        # entry writing all the metadata then removing the original file.
        if getattr(msg, "errno", None) in [errno.EPERM, errno.EXDEV]:
            try:
                delete_at_exit.register(output_filename)
                #Create for reading and writing.  The default open mode
                # is sufficent for making metadata changes.
                out_fd = file_utils.open_fd(output_filename,
                                            os.O_RDWR | os.O_CREAT | os.O_EXCL)
            except OSError, msg2:
                print_error(e_errors.OSERROR,
                            "Unable to create file %s: %s" %
                            (output_filename, str(msg2)))
                sys.exit(1)
            try:
                #Remeber the original mode so the new file can be set to it.
                in_fd = file_utils.open_fd(input_filename, os.O_RDWR)
                mode = file_utils.get_stat(in_fd)[stat.ST_MODE]
            except OSError, msg2:
                print_error(e_errors.OSERROR,
                            "Unable to access file %s: %s" %
                            (input_filename, str(msg2)))
                sys.exit(1)
        else:
            print_error(e_errors.OSERROR,
                        "Unable to move file %s: %s" %
                        (input_filename, str(msg)))
            sys.exit(1)

    #new_p = pnfs.Pnfs(output_filename)
    new_sfs = namespace.StorageFS(output_filename)

    #List the output file's metadata.
    file_information = ("New File Name: %s" % output_filename,
                        "PNFS/Chimera ID: %s" % new_sfs.get_id())
    Trace.message(5, ("%s\n" * len(file_information) % file_information))
    Trace.log(e_errors.INFO,
              ("%s  " * len(file_information) % file_information))

    #Create new pnfs values.
    new_volume = sfs.volume
    new_location_cookie = sfs.location_cookie
    new_size = sfs.size
    if intf.match_directory_file_family:
        new_file_family = directory_file_family #Changed.
    elif intf.match_volume_file_family:
        vf = volume_info['volume_family']
        new_file_family = volume_family.extract_file_family(vf)  #Changed.
    else:
        new_file_family = sfs.origff
    new_filename = output_filename  #Changed.
    new_volume_filepath = sfs.mapfile
    new_sfs_id = new_sfs.get_id() #Changed.
    new_volume_file = sfs.pnfsid_map
    new_bfid = sfs.bfid
    if sfs.origdrive != namespace.UNKNOWN:
        new_drive = sfs.origdrive
    else:
        new_drive = file_info['drive']  #Added if not present.
    if sfs.crc != namespace.UNKNOWN:
        new_crc = sfs.crc
    else:
        new_crc = file_info['complete_crc']  #Added if not present.

    #Create new file clerk values.
    fc_ticket = {}
    fc_ticket['fc'] = file_info.copy()
    fc_ticket['fc']['pnfsid'] = new_sfs_id
    fc_ticket['fc']['pnfs_name0'] = output_filename
    #fc_ticket['fc']['drive'] = new_drive
    fc_ticket['fc']['uid'] = sfs.pstat[stat.ST_UID]
    fc_ticket['fc']['gid'] = sfs.pstat[stat.ST_GID]

    try:
        #Update file's layer 1 information.
        new_sfs.set_bit_file_id(new_bfid)
    except (OSError, IOError), msg:
        print_error(e_errors.OSERROR,
                    "%s layer 1 update failed: %s" % (sfs.print_id, str(msg)))
        sys.exit(1)

    try:
        #Update file's layer 4 information.
        new_sfs.set_xreference(new_volume, new_location_cookie, new_size,
                               new_file_family, new_filename,
                               new_volume_filepath, new_sfs_id,
                               new_volume_file, new_bfid, new_drive, new_crc)
    except OSError, msg:
        print_error(e_errors.OSERROR,
                    "%s layer 4 update failed: %s" % (sfs.print_id, str(msg)))
        sys.exit(1)

    #Update the layer information that is NOT layer 1 and 4.
    for i in [2, 3, 5, 6, 7]:
        try:
            #If rename() succeded this will not be necessary.
            if layer_info[i] and not new_sfs.readlayer(i):
                tmp_string = ""
                for item in layer_info[i]:
                    #Loop over the lines to build the string.
                    tmp_string = tmp_string + item
                new_sfs.writelayer(i, tmp_string)
        except KeyError:
            #We didn't read in a layer before.  Just skip it.
            pass
        except (OSError, IOError), msg:
            #Ignore EACCESS errors.  That is the error given when a layer is
            # turned off in the pnfs configuration file.
            if msg.args[0] != errno.EACCES:
                print_error(e_errors.OSERROR,
                            "%s layer %s update failed: %s" % \
                            (sfs.print_id, i, str(msg)))
                sys.exit(1)

    if out_fd: #If the rename failed and we did it the hard way.

        #The file size, permissions, last access/modification time and
        # ownership must all be reset.
        #Note: Ownership must be last, otherwise the permissions to set
        # the rest won't be there.

        try:
            new_sfs.set_file_size(file_info['size'])
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "Pnfs filesize update failed: %s" % str(msg))
            sys.exit(1)

        try:
            file_utils.chmod(output_filename, mode)
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "File permissions update failed: %s" % str(msg))
            sys.exit(1)

        try:
            file_utils.utime(output_filename,
                     (sfs.pstat[stat.ST_ATIME], sfs.pstat[stat.ST_MTIME]))
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "File access and modification time update failed: %s" \
                        % str(msg))
            sys.exit(1)

        try:
            file_utils.chown(output_filename,
                     sfs.pstat[stat.ST_UID], sfs.pstat[stat.ST_GID])
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "File ownership update failed: %s" % str(msg))
            sys.exit(1)
    elif not out_fd:

        # If the file was rename()ed we must set the permissions back.
        # They would have been modified if the original file was read-only.

        try:
            if file_utils.get_stat(output_filename)[stat.ST_MODE] \
                   != sfs.pstat[stat.ST_MODE]:
                file_utils.chmod(output_filename, sfs.pstat[stat.ST_MODE])
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "File permissions update failed: %s" % str(msg))
            #We don't wat to fail the transfer over this. Otherwise the
            # pnfs part of the move is completed, but the Enstore DB
            # part won't be.  That confict will prevent encp from reading
            # the file.

    #Update the file clerk information.  This must be last.  If any of the
    # the prevous steps fail in setting the pnfs information, the
    # delete_at_exit cleanup will undo the changes.  After changing the
    # file database there is no going back.


    fc_reply = fcc.set_pnfsid(fc_ticket)
    if not e_errors.is_ok(fc_reply):
        print_error(fc_reply['status'][0],
                    "File clerk update failed: %s" % fc_reply['status'][1])

        sys.stderr.flush()
        sys.exit(1)

    fc_reply = fcc.find_copies(fc_ticket["fc"]["bfid"])
    failed_bfids = []
    if e_errors.is_ok(fc_reply):
        for bfid in fc_reply.get("copies"):
            update_ticket={}
            update_ticket["bfid"] = bfid
            update_ticket["pnfs_name0"] = fc_ticket["fc"]["pnfs_name0"]
            reply = fcc.modify(update_ticket)
            if not e_errors.is_ok(reply):
                failed_bfids.append({"bfid" : bfid, "status" : (reply['status'][0], reply['status'][1])})

    if len(failed_bfids) > 0 :
        map(lambda x : print_error(x["status"][0],"File clerk update of copy {} failed: {}".format(x["bfid"],x["status"][1])),failed_bfids)
        sys.stderr.flush()
        sys.exit(1)

    if in_fd:
        try:
            os.close(in_fd)
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "Error closing %s: %s" % (input_filename, str(msg)))
            sys.stderr.flush()
    if out_fd:
        try:
            os.close(out_fd)
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "Error closing %s: %s" % (output_filename, str(msg)))
            sys.stderr.flush()

    #If we got here then the new file is in place and we need to take
    # this new file off of the list of cleanup deletions.
    delete_at_exit.unregister(output_filename)

    try:
        if in_fd:
            #Remove the orginal file.  Also, this removes the pnfs layers
            # before the actual file is deleted.  If os.remove() were to be
            # used, the layer information is "trashed" and delfile would mark
            # the moved file as deleted.
            sfs.rm()
    except OSError, msg:
        print_error(e_errors.OSERROR,
                    "Unable to remove original file %s: %s" %
                    (input_filename, str(msg)))
        sys.exit(1)

    Trace.message(1, "File successfully moved.")

class EnmvInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):
        self.verbose = 0           # higher the number the more is output
        self.match_directory_file_family = 0  #modify layer 4 file family
        self.match_volume_file_family = 0  #modify layer 4 file family

        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.enmv_options)

    def parse_options(self):
        # normal parsing of options
        option.Interface.parse_options(self)

        #Need a source and destination file name.
        if self.help:
            print description
            self.print_help()
        elif self.usage:
            print description
            self.print_usage()
        self.arglen = len(self.args)
        if self.arglen != 2 and (not self.help and not self.usage):
            if self.arglen < 2:
                message = "%s: not enough arguments specified" % \
                          e_errors.USERERROR
            elif self.arglen > 2:
                message = "%s: too many arguments specified" % \
                          e_errors.USERERROR
            print description
            self.print_usage(message)
            sys.exit(1)

        self.input = [enstore_functions2.fullpath(self.args[0])[1]]
        self.output = [enstore_functions2.fullpath(self.args[1])[1]]

    #  define our specific parameters
    parameters = ["<source file> <destination file>"]

    enmv_options = {
        option.MATCH_DIRECTORY_FILE_FAMILY:{option.HELP_STRING:
                                            "Set layer 4 file family to match"
                                            " the directory file family.",
                                            option.VALUE_USAGE:option.IGNORED,
                                            option.VALUE_TYPE:option.INTEGER,
                                            option.USER_LEVEL:option.USER,},
        option.MATCH_VOLUME_FILE_FAMILY:{option.HELP_STRING:
                                         "Set layer 4 file family to match"
                                         " the volume file family.",
                                         option.VALUE_USAGE:option.IGNORED,
                                         option.VALUE_TYPE:option.INTEGER,
                                         option.USER_LEVEL:option.USER,},
        option.VERBOSE:{option.HELP_STRING:"Print out information.",
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_TYPE:option.INTEGER,
                        option.USER_LEVEL:option.USER,},
        }


def main(intf):

    #Initialize the Trace module.
    Trace.init("ENMV")
    for x in xrange(6, intf.verbose + 1):
        Trace.do_print(x)
    for x in xrange(1, intf.verbose + 1):
        Trace.do_message(x)


    for item in intf.input:
        if not namespace.is_storage_path(item):
            print_error(e_errors.USERERROR,
                        "Source file %s is not a valid pnfs file." % (item,))
            sys.exit(1)

    for item in intf.output:
        if not namespace.is_storage_path(item, check_name_only = 1):
            print_error(e_errors.USERERROR,
                   "Destination file %s is not a valid pnfs file." % (item,))
            sys.exit(1)

    for i in range(len(intf.input)):
        move_file(intf.input[i], intf.output[i], intf)


def do_work(intf):

    try:
        main(intf)
        delete_at_exit.quit(0)
    except SystemExit:
        delete_at_exit.quit(1)
    except:
        exc, msg, tb = sys.exc_info()
        try:
            sys.stderr.write("%s\n" % (tb,))
            sys.stderr.write("%s %s\n" % (exc, msg))
            sys.stderr.flush()
        except IOError:
            pass
        Trace.handle_error(exc, msg, tb)
        del tb  #Avoid resource leak.
        delete_at_exit.quit(1)


if __name__ == '__main__':
    delete_at_exit.setup_signal_handling()

    enmv_intf = EnmvInterface(sys.argv, 0) # zero means admin

    do_work(enmv_intf)
