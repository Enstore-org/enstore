
import sys
import pprint
import os
import re
import string
import errno
import stat

import option
import delete_at_exit
import e_errors
import pnfs
import enstore_functions2
import configuration_client
import file_clerk_client
import volume_clerk_client
import volume_family
import atomic
import charset


def quit(exit_code=1):
    delete_at_exit.quit(exit_code)

def print_error(errcode,errmsg):
    format = str(errcode)+" "+str(errmsg) + '\n'
    format = "ERROR: "+format
    sys.stderr.write(format)
    sys.stderr.flush()

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

def move_file(input_filename, output_filename):

    #Check the input file.
    if not os.path.exists(input_filename):
        print_error(e_errors.USERERROR, "Input file does not exists.")
        sys.exit(1)
    if not os.path.isfile(input_filename):
        print_error(e_errors.USERERROR, "Source file is not a file.")
        sys.exit(1)
        
    #check the output file.
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
    if not charset.is_in_filenamecharset(output_filename):
        print_error(e_errors.USERERROR,
                    "Output filename contains invalid characters.")
        sys.exit(1)

    p = pnfs.Pnfs(input_filename)
    p.get_bit_file_id()
    p.get_xreference()

    if p.bfid != p.bit_file_id:
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

    file_info = fcc.bfid_info(p.bit_file_id)
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
    #Extract this for readability.
    db_volume_family = volume_info['volume_family']


    #Check for inconsistancies.
    if p.volume != file_info['external_label']:
        print_error(e_errors.CONFLICT,
                    "Volume information does not match.")
        sys.exit(1)
    elif not same_cookie(p.location_cookie, file_info['location_cookie']):
        print_error(e_errors.CONFLICT,
                    "Location cookie information does not match.")
        sys.exit(1)
    elif long(p.size) != long(file_info['size']):
        print_error(e_errors.CONFLICT,
                    "File size information does not match.")
        sys.exit(1)
    elif p.origff != volume_family.extract_file_family(db_volume_family):
        print_error(e_errors.CONFLICT,
                    "File family information does not match.")
        sys.exit(1)
    elif p.origname != file_info['pnfs_name0']:
        print_error(e_errors.CONFLICT,
                    "File family information does not match.")
        sys.exit(1)
    #Mapfile is obsolete.
    elif p.pnfsid_file != file_info['pnfsid']:
        print_error(e_errors.CONFLICT,
                    "Pnfs id information does not match.")
        sys.exit(1)
    elif p.bfid != file_info['bfid']:
        print_error(e_errors.CONFLICT,
                    "Bfid information does not match.")
        sys.exit(1)
    #Original drive not always recorded.
    elif p.origdrive != file_info['drive']:
        print_error(e_errors.CONFLICT,
                    "Original drive information does not match.")
        sys.exit(1)
    elif p.crc != pnfs.UNKNOWN and file_info['complete_crc']:
        print_error(e_errors.CONFLICT,
                    "CRC information does not match.")
        sys.exit(1)


    try:
        #Attempt to rename the file.  This can work if the input and
        # output targets are under the same mount point.
        os.rename(input_filename, output_filename)

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
        if getattr(msg, "errno", None) == errno.EPERM:
            try:
                #Create for reading and writing.  The default open mode
                # is sufficent for making metadata changes.
                out_fd = atomic.open(output_filename)
            except OSError, msg2:
                print_error(e_errors.OSERROR,
                            "Unable to create file %s: %s" %
                            (output_filename, str(msg2)))
                sys.exit(1)
            try:
                #Remeber the original mode so the new file can be set to it.
                in_fd = os.open(input_filename, os.O_RDWR)
                mode = os.fstat(in_fd)[stat.ST_MODE]
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

    new_p = pnfs.Pnfs(output_filename)

    #Create new pnfs values.
    new_volume = p.volume
    new_location_cookie = p.location_cookie
    new_size = p.size
    new_file_family = p.origff
    new_filename = output_filename  #Changed.
    new_volume_filepath = p.mapfile
    new_pnfsid = new_p.get_id() #Changed.
    new_volume_file = p.pnfsid_map
    new_bfid = p.bfid
    if p.origdrive != pnfs.UNKNOWN:
        new_drive = p.origdrive
    else:
        new_drive = file_info['drive']  #Added if not present.
    if p.crc != pnfs.UNKNOWN:
        new_crc = p.crc
    else:
        new_crc = file_info['complete_crc']  #Added if not present.

    #Create new file clerk values.
    fc_ticket = {}
    fc_ticket["fc"] = file_info.copy()
    fc_ticket["fc"]["pnfsid"] = new_pnfsid
    fc_ticket["fc"]["pnfs_name0"] = output_filename
    fc_ticket["fc"]["drive"] = new_drive

    try:
        #Update file's layer 1 information.
        new_p.set_bit_file_id(new_bfid)
    except OSError, msg:
        print_error(e_errors.OSERROR,
                    "Pnfs layer 1 update failed: %s" % str(msg))
        sys.stderr.flush()
        sys.exit(1)

    try:
        #Update file's layer 4 information.
        new_p.set_xreference(new_volume, new_location_cookie, new_size,
                             new_file_family, new_filename,
                             new_volume_filepath, new_pnfsid, new_volume_file,
                             new_bfid, new_drive, new_crc)
    except OSError, msg:
        print_error(e_errors.OSERROR,
                    "Pnfs layer 4 update failed: %s" % str(msg))
        sys.stderr.flush()
        sys.exit(1)

    if in_fd: #If the rename failed and we did it the hard way.
        try:
            new_p.set_file_size(file_info['size'])
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "Pnfs filesize update failed: %s" % str(msg))
            sys.stderr.flush()
            sys.exit(1)

        try:
            os.chmod(output_filename, mode)
        except OSError, msg:
            print_error(e_errors.OSERROR,
                        "File permissions update failed: %s" % str(msg))

            sys.stderr.flush()
            sys.exit(1)

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

    if in_fd:
        try:
            os.close(in_fd)
        except OSError:
            print_error(e_errors.OSERROR,
                        "Error closing %s." % input_filename)
            sys.stderr.flush()
    if out_fd:
        try:
            os.close(out_fd)
        except OSError:
            print_error(e_errors.OSERROR,
                        "Error closing %s." % output_filename)
            sys.stderr.flush()

    #If we got here then the new file is in place and we need to take
    # this new file off of the list of cleanup deletions.
    delete_at_exit.unregister(output_filename)

    try:
        if in_fd:
            #Remove the orginal file.  Also, this remove the pnfs layers
            # before the actual file is deleted.  If os.remove() were to be
            # used, the layer information is "trashed" and delfile would mark
            # the moved file as deleted.
            p.rm()
    except OSError:
        print_error(e_errors.OSERROR,
                    "Unable to remove original file %s." % input_filename)
        sys.stderr.flush()
        sys.exit(1)

class EnmvInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):

        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.enmv_options)

    def parse_options(self):
        # normal parsing of options
        option.Interface.parse_options(self)

        #Need a source and destination file name.
        self.arglen = len(self.args)
        if self.arglen < 2:
            print_error(e_errors.USERERROR, "not enough arguments specified")
            self.print_usage()
            sys.exit(1)
        elif self.arglen > 2:
            print_error(e_errors.USERERROR, "too many arguments specified")
            self.print_usage()
            sys.exit(1)
            
        self.input = [enstore_functions2.fullpath(self.args[0])[1]]
        self.output = [enstore_functions2.fullpath(self.args[1])[1]]

    #  define our specific parameters
    parameters = ["<source file> <destination file>"]

    enmv_options = {
        }


def main(intf):

    for item in intf.input:
        if not pnfs.is_pnfs_path(item):
            print_error(e_errors.USERERROR,
                        "Source file %s is not a valid pnfs file." % (item,))
            sys.exit(1)

    for item in intf.output:
        if not pnfs.is_pnfs_path(item, check_name_only = 1):
            print_error(e_errors.USERERROR,
                   "Destination file %s is not a valid pnfs file." % (item,))
            sys.exit(1)

    for i in range(len(intf.input)):
        move_file(intf.input[i], intf.output[i])
    

def do_work(intf):

    try:
        main(intf)
        quit(0)
    except SystemExit:
        quit(1)
    #except:
        #exc, msg, tb = sys.exc_info()
        #sys.stderr.write("%s\n" % (tb,))
        #sys.stderr.write("%s %s\n" % (exc, msg))
        #quit(1)
        

if __name__ == '__main__':
    delete_at_exit.setup_signal_handling()

    intf = EnmvInterface(sys.argv, 0) # zero means admin

    do_work(intf)
