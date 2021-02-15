#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

'''
Update the deleted status in the Enstore DB, from information in the
PNFS trash directory.  Parses /usr/etc/pnfsSetup to obtain the trash
directory.

This is a replacement for $ENSTORE_DIR/sbin/delfile
'''
from __future__ import print_function

# system imports
import os
import string
import sys
import traceback

# enstore modules
import option
import file_clerk_client
import volume_clerk_client
import e_errors
import Trace

PNFS_SETUP = "/usr/etc/pnfsSetup"


def get_trash():

    # Historical note: This function used to look at the 'TRASH_CAN'
    # environmental variable for the value to return.

    # We need to automatically detect if there is a pnfs server configured.
    # If there is, then we need to find the trash value and return it.

    try:
        f_ps = open(PNFS_SETUP)  # f_ps = File Pnfs Setup
        d_ps = f_ps.readlines()  # d_ps = Data Pnfs Setup
        f_ps.close()
    except (OSError, IOError) as detail:
        sys.stderr.write("Unable to access pnfsSetup: %s\n" % (str(detail),))
        return None

    for line in d_ps:
        line = line.strip()
        words = line.split("=")
        if words[0] == "trash":
            return os.path.join(words[1].strip(), "4")

    sys.stderr.write("No trash directory listed in %s.\n" % (PNFS_SETUP,))
    return None


def get_bfid(mf):
    try:
        f = open(mf)
        r = f.readlines()
        f.close()
    except (OSError, IOError):
        return None, None

    if len(r) > 8:
        return string.strip(r[0]), string.strip(r[8])

    return None, None


def main(intf):
    success = True
    vols = []

    if os.geteuid() != 0:
        sys.stderr.write("Must be user root.\n")
        return False

    fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))
    vcc = volume_clerk_client.VolumeClerkClient(fcc.csc)
    trash = get_trash()
    # print trash
    files = os.listdir(trash)

    # Update the files.  And get a list of volumes at the same time.
    for i in files:
        fp = os.path.join(trash, i)
        vol, bfid = get_bfid(fp)
        if bfid:
            if not vol in vols:
                vols.append(vol)
            # delete
            fcc.bfid = bfid
            if fcc.bfid_info().get('active_package_files_count', 1) > 0 and \
                    fcc.bfid_info().get('package_id', None) == bfid:
                Trace.alarm(e_errors.WARNING,
                            'Skipping non-empy package file %s' % (bfid,),
                            fcc.bfid_info().get('pnfs_name0', None))
                print('skipping non-empty package file', bfid, '...')
                continue
            print('deleting', bfid, '...', end=' ')
            result = fcc.set_deleted('yes')
            if result['status'][0] != e_errors.OK:
                print(bfid, result['status'][1])
                success = False
            else:
                print('done')
                try:
                    os.unlink(fp)
                except BaseException:
                    print('can not delete', fp)
                    success = False

    # Touch the list of volumes.
    for i in vols:
        print('touching', i, '...', end=' ')
        result = vcc.touch(i)
        if result['status'][0] == e_errors.OK:
            print('done')
        else:
            print('failed')
            success = False

    if not success:
        return 1
        # this will keep *-output file
        # sys.exit(1)

    return 0


def do_work(intf):

    Trace.init("DELFILE")

    try:
        exit_status = main(intf)
    except (SystemExit, KeyboardInterrupt) as msg:
        Trace.log(e_errors.ERROR, "delfile aborted from: %s" % str(msg))
        sys.exit(1)
    except BaseException:
        # Get the uncaught exception.
        exc, msg, tb = sys.exc_info()
        # Print it to terminal.
        traceback.print_exception(exc, msg, tb)
        # Also, send it to the log file.
        Trace.handle_error(exc, msg, tb)

        del tb  # No cyclic references.
        sys.exit(1)

    sys.exit(exit_status)


class DelfileInterface(option.Interface):
    def valid_dictionaries(self):
        return (self.help_options,)


if __name__ == '__main__':
    intf_of_delfile = DelfileInterface()

    do_work(intf_of_delfile)
