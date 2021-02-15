#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# Given a pnfsid and bfid find the current path to the file using the
# fewest possible resources.
#
###############################################################################

# system imports
from future.utils import raise_
import errno
import sys
import copy
import os
import string
import threading

import bfid_util
import pnfs
import chimera
import namespace
import enstore_functions2
import enstore_functions3
import configuration_client
import enstore_constants
import e_errors
import file_utils
import hostaddr
# info_client or file_clerk_client is imported in get_clerk_client().

#######################################################################

# If true, use the info_server.  If false, use the file_clerk.
USE_INFO_SERVER_DEFAULT = False

search_list = None
search_list_lock = threading.Lock()

# sfs_id is a string representing the uninque internal ID for the storage
#   filesystem.  Current supported storage filesystem types are:
#   PNFS and Chimera.
# bfid is a string consisting of a files unique id in Enstore.
# file_record is file information from the file_clerk's bfid_info() function.
# likely_path is the location that the calling function believes to be
#   the correct return value.
# use_info_server specifies if the info server or the file clerk should be
#   used for bfid_info() calls.


def find_id_path(sfs_id, bfid, file_record=None, likely_path=None,
                 path_type=enstore_constants.BOTH,
                 use_info_server=USE_INFO_SERVER_DEFAULT):
    global search_list

    if not namespace.is_id(sfs_id):
        raise ValueError("Expected storage filesystem id not: %s" % (sfs_id,))
    if not bfid_util.is_bfid(bfid):
        raise ValueError("Expected bfid not: %s" % (bfid,))

    # Remember if a file with matching storage filesystem id (and unmatching
    # bfid) was found after all storage filesystems have been checked.
    possible_reused_sfs_id = 0
    # Remember if a file with found layers 1 and 4 has not had a directory
    # entry found after all storage filesystems have been checked.
    possible_orphaned_file = 0

    # afn = Access File Name   (aka .(access)())
    afn = ""

    # We will need the pnfs database numbers.
    if pnfs.is_pnfsid(sfs_id):
        pnfsid_db = int(sfs_id[:4], 16)
    else:
        pnfsid_db = None

    # If we weren't passed a file_record, then we must get it ourselves.
    if not file_record:
        # Get either the file_clerk_client or the info_client depending
        # on our needs.
        infc = get_clerk_client(use_info_server=use_info_server)
        file_record = infc.bfid_info(bfid)
        if not e_errors.is_ok(file_record):
            #ValueError = punt
            raise ValueError(
                "Unable to obtain file information: %s" %
                (file_record['status'],))
    else:
        infc = None  # Make sure this exists, so we can test for it later.

    # Extract these values.  'pnfs_path' and 'pnfs_id' correlate if the file
    # record comes from the db directly.  'pnfs_name0' and 'pnfsid' are
    # the names via the file clerk bfid_info() function.
    enstoredb_path = file_record.get('pnfs_path',
                                     file_record.get('pnfs_name0', None))
    enstoredb_sfs_id = file_record.get('pnfs_id',
                                       file_record.get('pnfsid', None))

    # Lets try the suggested path first.
    path_list = []
    if likely_path:
        path_list.append(likely_path)
    # Otherwise play some quick search games by reading /etc/mtab.
    if enstoredb_path:
        # get_enstore_mount_point() and get_enstore_admin_mount_point() look
        # at /etc/mtab.  This makes this largely a fast call compared to
        # those that query a storage filesystem.
        if path_type == enstore_constants.FS:
            mount_paths = namespace.get_enstore_admin_mount_point()

        elif path_type == enstore_constants.NONFS:
            mount_paths = namespace.get_enstore_mount_point()
        else:  # both
            mount_paths = namespace.get_enstore_mount_point() + \
                namespace.get_enstore_admin_mount_point()

        for mount_path in mount_paths:
            if enstoredb_path.startswith(mount_path):
                # Need to add one to the length of mount_path to skip
                # passed the leading "/".
                new_path = enstoredb_path
                path_list.append(new_path)
            else:
                if enstoredb_path.find("/pnfs/fs/usr/") == -1:
                    new_path = enstoredb_path.replace(
                        "/pnfs/", "/pnfs/fs/usr/", 1)
                    if new_path not in path_list:
                        path_list.append(new_path)
                else:
                    new_path = enstoredb_path.replace(
                        "/pnfs/fs/usr/", "/pnfs/", 1)
                    if new_path not in path_list:
                        path_list.append(new_path)

            # If the paths begins with something like
            # /pnfs/fnal.gov/usr/... we need to convert and check for
            # this too.  This is most likely necessary when the scan
            # is run on an offline copy that did not have the
            # fnal.gov symbolic link to fs made.
            domain_name = hostaddr.getdomainname()
            if domain_name:
                # Non-admin (/pnfs/xyz) path handling.
                if path_type in [enstore_constants.NONFS,
                                 enstore_constants.BOTH]:
                    new_path = enstoredb_path.replace(
                        "/pnfs/%s/usr/" % (domain_name,), "/pnfs/", 1)
                    if new_path not in path_list:
                        path_list.append(new_path)
                # Admin (/pnfs/fs/usr/xyz) path handling.
                if path_type in [enstore_constants.FS, enstore_constants.BOTH]:
                    new_path = enstoredb_path.replace(
                        "/pnfs/%s/usr/" % (domain_name,), "/pnfs/fs/usr/", 1)
                    if new_path not in path_list:
                        path_list.append(new_path)

    if path_list:
        for try_path in path_list:
            try:
                layer1_bfid = pnfs.get_layer_1(try_path)
            except (OSError, IOError):
                layer1_bfid = None
            if layer1_bfid == bfid:
                return try_path  # We found the currently mounted path!

    # Loop over all found mount points.
    search_list_lock.acquire()
    try:
        if search_list is None:
            search_list = namespace.process_mtab()
    except BaseException:
        search_list_lock.release()
        raise_(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
    search_list_copy = search_list[:]
    search_list_lock.release()
    for search_item in search_list_copy:
        database_info = search_item[namespace.DB_INFO]
        db_num = search_item[namespace.DB_NUMBER]
        mp = search_item[namespace.DB_MOUNT_POINTS][0]

        # If the last db tried is still set to its initial value (-1), we need
        # to skip the the next.  The variable db_num will be None for Chimera.
        if db_num is not None and db_num < 0:
            continue

        # If we want a specific type of pnfs path, ignore the others.
        if path_type == enstore_constants.FS:
            if db_num == 0:
                # If the admin mount point is /pnfs/fs/usr then
                # is_normal_pnfs_path() does the correct thing, but if it is
                # just /pnfs/fs, then we need this if statement to do the
                # correct thing knowning the admin DB number is always zero.
                pass
            elif pnfs.is_normal_pnfs_path(mp):
                continue
            elif chimera.is_normal_chimera_path(mp):
                continue
        elif path_type == enstore_constants.NONFS and pnfs.is_admin_pnfs_path(mp):
            continue

        # This test is to make sure that the pnfs filesystem we are going
        # to query has a database N (where N is pnfsid_db).  Otherwise
        # we hang querying a non-existant database.  If the pnfsid_db
        # matches the last one we tried we skip this test as it has
        # already been done.
        if pnfs.get_last_db()[namespace.DB_INFO] != pnfsid_db:
            try:
                pnfs.N(pnfsid_db, mp).get_databaseN()
            except IOError:
                # This /pnfs/fs doesn't contain a database with the id we
                # are looking for.
                continue

        # We don't need to determine the full path of the file
        # to know if it exists.  The path could be different
        # between two machines anyway.
        # afn =  Access File Name
        afn = pnfs.access_file(mp, sfs_id)

        # Check layer 1 to get the bfid.
        try:
            layer1_bfid = pnfs.get_layer_1(afn)
        except (OSError, IOError):
            layer1_bfid = None
        if layer1_bfid:
            # If we haven't needed the file_clerk_client/info_client yet,
            # get it now.
            if not infc:
                infc = get_clerk_client(use_info_server=use_info_server)
            # Make sure this is the correct file or copy thereof.
            if layer1_bfid == file_record['bfid'] or \
                    layer1_bfid == \
                    infc.find_original(file_record['bfid'])['original']:

                if layer1_bfid != file_record['bfid']:
                    # Must be a multiple copy to get to this point.
                    is_multiple_copy = True
                else:
                    is_multiple_copy = False

                if file_record['deleted'] == 'yes':
                    try:
                        sfs = namespace.StorageFS(mp, shortcut=True)
                        tmp_name_list = sfs.get_path(sfs_id, mp)
                        # Deal with multiple possible matches.
                        if len(tmp_name_list) == 1:
                            # There is one known case where asking for layer
                            # 1 using the pnfsid and seperately using the
                            # filename returned different values.  This
                            # attempts to catch such situations.
                            alt_layer1_bfid = sfs.get_bit_file_id(
                                tmp_name_list[0])
                            if alt_layer1_bfid != layer1_bfid:
                                message = "conflicting layer 1 values " \
                                          "(id %s, name %s)" % \
                                          (layer1_bfid, alt_layer1_bfid)
                                raise OSError(errno.EBADF, message,
                                              tmp_name_list[0])

                            # Remember, that in order to get this far the
                            # file needs to be marked deleted in the file
                            # db, thus both cases are both errors.
                            if not is_multiple_copy:
                                raise OSError(errno.EEXIST,
                                              "storage filesystem entry exists",
                                              tmp_name_list[0])
                            else:
                                raise OSError(errno.EEXIST,
                                              "found original of copy",
                                              tmp_name_list[0])
                        else:
                            raise OSError(errno.EIO, "to many matches",
                                          tmp_name_list)
                    except (OSError, IOError) as detail:
                        if detail.errno in [errno.EBADFD, errno.EIO]:
                            raise OSError(errno.ENOENT, "orphaned file",
                                          sfs_id)
                        else:
                            raise_(sys.exc_info()[0], sys.exc_info()[1],
                                   sys.exc_info()[2])

                else:
                    sfs = namespace.StorageFS(afn)
                    db_a_dirpath = namespace.get_directory_name(afn)

                    #database_path = pnfs.database_file(db_a_dirpath)
                    # Get the database info for the filesystem.
                    db_info = sfs.get_database(db_a_dirpath)

                    # Update the global cache information.
                    if database_info != db_info:

                        # At this point the database that the file is in
                        # doesn't match the one that returned a positive hit.
                        # So, we first check if a known PNFS database is
                        # a match...
                        for item in search_list_copy:
                            if item[namespace.DB_INFO] == db_info:
                                mount_points = item[namespace.DB_MOUNT_POINTS]
                                for mount_point in mount_points:
                                    sfs_path = pnfs.access_file(mount_point,
                                                                enstoredb_sfs_id)
                                    layer1_bfid = sfs.get_bit_file_id(sfs_path)
                                    if layer1_bfid and \
                                            layer1_bfid == file_record['bfid']:
                                        pnfs.set_last_db(copy.copy(item))
                                        sfs_id_mp = mount_point
                                        break
                        else:
                            # ...if it is not a match then we have a database
                            # not in our current cached list.  So we need
                            # to find it.  This is going to be a resource
                            # hog, but once we find it, we won't need to
                            # do so for any more files.

                            # If this sfs.get_path() fails, it is most likely,
                            # because of permission problems of either
                            # /pnfs/fs or /pnfs/fs/usr.  Especially for
                            # new pnfs servers.
                            try:
                                sfs = namespace.StorageFS(mp)
                                sfs_path_list = sfs.get_path(enstoredb_sfs_id,
                                                             mp)
                                # Deal with multiple possible matches.
                                if len(sfs_path_list) == 1:
                                    sfs_path = sfs_path_list[0]
                                else:
                                    raise OSError(errno.EIO, "to many matches",
                                                  sfs_path_list)
                                sfs_id_mp = sfs.get_pnfs_db_directory(sfs_path)
                            except (OSError, IOError) as msg:
                                sfs_path = afn
                                if msg.errno in [errno.ENOENT]:
                                    sfs_id_mp = None
                                    # Using the current mountpoint, we
                                    # were able to obtain layer 1, but the
                                    # file does not exists?  Let's flag this
                                    # as a possible orphan file.
                                    possible_orphaned_file = \
                                        possible_orphaned_file + 1
                                else:
                                    sfs_id_mp = mp  # ???

                            if sfs_id_mp is not None:
                                # This is just some paranoid checking.
                                afn = pnfs.access_file(sfs_id_mp,
                                                       enstoredb_sfs_id)
                                layer1_bfid = pnfs.get_layer_1(afn)
                                if layer1_bfid and \
                                        layer1_bfid == file_record['bfid']:
                                    # last_db_tried = (db_info,
                                    #                 (pnfsid_db, pnfsid_mp))
                                    pnfs.set_last_db((db_info,
                                                      (pnfsid_db, sfs_id_mp)))
                                    pnfs.add_mtab(db_info, pnfsid_db,
                                                  sfs_id_mp)
                            else:
                                pnfs.set_last_db(("", (-1, "")))
                    else:
                        pnfs.set_last_db((db_info, (pnfsid_db, mp)))
                        # We found the file, set the pnfs path.
                        sfs_path = afn
                        sfs_id_mp = mp

                    # pnfs_path and pnfsid_mp needs to be set correctly by
                    # this point.
                    if sfs_id_mp is None:
                        continue
                    break

            else:
                try:
                    sfs = namespace.StorageFS(mp, shortcut=True)
                    tmp_name_list = sfs.get_path(enstoredb_sfs_id, mp)
                    # Deal with multiple possible matches.
                    if len(tmp_name_list) == 1:
                        # There is one known case where asking for layer 1
                        # using the pnfsid and seperately using the
                        # filename to ask for the same layer 1 returned
                        # different values.  This attempts to catch such
                        # situations.
                        if db_num != 0 and pnfsid_db != db_num:
                            continue
                        alt_layer1_bfid = sfs.get_bit_file_id(tmp_name_list[0])
                        if alt_layer1_bfid != layer1_bfid:
                            message = "conflicting layer 1 values " \
                                      "(id %s, name %s) for %s" % \
                                      (layer1_bfid, alt_layer1_bfid, sfs_id)
                            raise OSError(errno.EBADF, message,
                                          tmp_name_list[0])
                        else:
                            # The BFIDs match, so there isn't a problem here.
                            # Keep going.
                            pass

                    else:
                        raise OSError(errno.EIO, "to many matches",
                                      tmp_name_list)
                except (OSError, IOError) as detail:
                    if detail.errno in [errno.EBADFD, errno.EIO]:
                        raise OSError(errno.ENOENT, "orphaned file",
                                      sfs_id)
                    else:
                        raise_(sys.exc_info()[0], sys.exc_info()[1],
                               sys.exc_info()[2])
                except (ValueError) as detail:
                    # If the error is like:
                    # "The pnfs id (0001000000000000008EBF98) is not valid."
                    # we want to keep searching instead of passing this
                    # ValueError along.  The error comes from sfs.get_path().
                    if str(detail).find("is not valid") == -1:
                        raise_(sys.exc_info()[0], sys.exc_info()[1],
                               sys.exc_info()[2])

            # To recap:
            # By this point we found a file with the pnfsid that we are
            # looking for.  However, the bfid in layer 1 didn't match the
            # bfid we are looking for.

            # If we found the right bfid brand, we know the right storage
            # filesystem was found.
            sfs_brand = bfid_util.extract_brand(layer1_bfid)
            filedb_brand = bfid_util.extract_brand(file_record['bfid'])
            if sfs_brand and filedb_brand and sfs_brand == filedb_brand:
                if file_record['deleted'] == 'yes':
                    raise OSError(errno.ENOENT,
                                  replaced_error_string(layer1_bfid, bfid),
                                  tmp_name_list[0])

                possible_reused_sfs_id = possible_reused_sfs_id + 1
                continue

            # If we found a bfid that didn't have the correct id or the
            # brands did not match, go back to the top and try the next
            # storage filesystem.
    else:
        if afn:
            if file_record['deleted'] != 'yes':
                # We don't have a layer 1.  As a last ditch effort check layer 4.
                # There probably won't be anything, but every now and then...
                try:
                    layer4_dict = pnfs.get_layer_4(afn)
                except (OSError, IOError) as msg:
                    if msg.args[0] in [errno.EACCES, errno.EPERM]:
                        raise_(sys.exc_info()[0], sys.exc_info()[1],
                               sys.exc_info()[2])
                    layer4_dict = {}

                # We also need to grab the pnfs id of the file that is there.
                #  This way we can differentiate the cases were the file
                # does not exist and those replaced.
                try:
                    normal = pnfs.get_enstore_pnfs_path(
                        file_record['pnfs_name0'])
                except (OSError, IOError):
                    normal = None
                try:
                    admin = pnfs.get_enstore_fs_path(file_record['pnfs_name0'])
                except (OSError, IOError):
                    admin = None
                # Only put the mount point types that actually exist in the
                # search list.
                s_list = []
                for path_value in [normal, admin]:
                    if path_value:
                        s_list.append(path_value)
                for path in s_list:
                    try:
                        sfs_sfs_id = pnfs.get_pnfsid(path)
                        break
                    except (OSError, IOError):
                        continue
                else:
                    sfs_sfs_id = ""

                # We throw away the (err_l, warn_l, info_l) values becuase we
                # expect them to not be there if layer 1 is not there.  It does
                # not make sense to report what we already know.
                if layer4_dict.get('bfid', None):
                    # If we found a bfid in layer 4, report the error about
                    # layer 1.
                    if possible_reused_sfs_id:
                        # File is not deleted, pnfs id is still valid,
                        # found a different bfid in layer 1.  Lets fall
                        # though and report more useful error message.
                        pass

                        #raise OSError(errno.ENOENT, "reused pnfsid", afn)
                    elif possible_orphaned_file:
                        # File is not deleted, pnfs id is still valid, found
                        # the right layer 1 but the file does not have a
                        # directory entry in PNFS.
                        raise OSError(errno.ENOENT, "orphaned file",
                                      sfs_id)
                    elif layer4_dict.get('bfid', None) != bfid:
                        raise OSError(errno.ENOENT, "found non-matching file",
                                      afn)
                    else:
                        raise OSError(errno.ENOENT, "missing layer 1", afn)
                elif sfs_sfs_id and sfs_sfs_id != file_record['pnfsid']:
                    raise OSError(errno.ENOENT, "has been replaced",
                                  sfs_id)
                else:
                    # Since the file exists, but has no layers, report the
                    # error.
                    raise OSError(errno.ENOENT, os.strerror(errno.ENOENT),
                                  sfs_id)

        layer1_bfid = pnfs.get_layer_1(enstoredb_path)
        if layer1_bfid and layer1_bfid != file_record['bfid']:
            # If this is the case that the bfids don't match,
            # also include this piece of information.
            raise OSError(errno.EEXIST,
                          replaced_error_string(layer1_bfid, bfid),
                          enstoredb_path)

        if possible_reused_sfs_id > 0:
            raise OSError(errno.EEXIST, "reused pnfsid",
                          enstoredb_path)

        raise OSError(errno.ENOENT, os.strerror(errno.ENOENT),
                      enstoredb_path)

    # Since we know if we are using /pnfs/fs or not, we can maniplate the
    # original name to the correct pnfs base path.  This will speed things
    # up when scanning files written to /pnfs/xyz but only having /pnfs/fs
    # mounted for the scan.
    #
    # This won't help for the case of moved or renamed files.  We still
    # will bite the bullet of calling get_path() for those.
    if not pnfs.is_access_name(sfs_path) and \
            not chimera.is_access_name(sfs_path):
        # If we have already had to do a full path lookup to find the
        # correct pnfs database/mountpoint, we don't need to worry about
        # any of this path munging.
        use_name = sfs_path
        use_mp = sfs_id_mp
    elif db_num == 0 and enstoredb_path.find("/pnfs/fs/usr") == -1:
        try:
            use_name = pnfs.get_enstore_fs_path(enstoredb_path)
        except (OSError, IOError):
            # We can get here when the file has been moved/renamed.
            use_name = enstoredb_path
        use_mp = sfs_id_mp.replace("/pnfs/fs", "/pnfs/fs/usr/", 1)
    elif mp.find("/pnfs/fs/usr/") >= 0 and \
            enstoredb_path.find("/pnfs/fs/usr") == -1:
        try:
            use_name = pnfs.get_enstore_fs_path(enstoredb_path)
        except (OSError, IOError):
            # We can get here when the file has been moved/renamed.
            use_name = enstoredb_path
        use_mp = sfs_id_mp
    elif mp.find("/pnfs/fs/usr/") == -1 and \
            enstoredb_path.find("/pnfs/fs/usr") >= 0:
        try:
            use_name = pnfs.get_enstore_pnfs_path(enstoredb_path)
        except (OSError, IOError):
            # We can get here when the file has been moved/renamed.
            use_name = enstoredb_path
        use_mp = sfs_id_mp
    else:
        # Stick the currently valid mount point in place of the original
        # mount point currently recorded in the Enstore DB.
        #
        # This was determine to be correct/necessary when a testing situation
        # on GCCen found that /pnfs/fs/usr/data1 and /pnfs/data1 may not
        # refer to the same metadata.  In the specific case, /pnfs/data1
        # was a Chimera filesystem while /pnfs/fs/usr/data1 was still
        # a PNFS filesystem.
        try:
            use_name = enstoredb_path.replace(
                file_utils.get_mount_point(enstoredb_path),
                sfs_id_mp)
        except (OSError, IOError):
            use_name = enstoredb_path
        use_mp = sfs_id_mp

    use_name = os.path.abspath(use_name)
    use_mp = os.path.abspath(use_mp)

    # Use the sfs_id to initialize the StorageFS class.  This prevents an
    # ENOENT error from being raised if the 'use_name' path guess does
    # not exist.  Files moved between directories need this.
    sfs = namespace.StorageFS(sfs_id)
    try:
        cur_sfs_id = sfs.get_id(use_name)
    except (OSError, IOError) as msg:
        cur_sfs_id = None
    if not cur_sfs_id or cur_sfs_id != enstoredb_sfs_id:
        # Before jumping off the deep end by calling get_path(), lets try
        # one more thing.  Here we try and remove any intermidiate
        # directories that are not present in the current path.
        #  Example: original path begins: /pnfs/sam/dzero/...
        #           current use path begins /pnfs/fs/usr/dzero/...
        #           If we can detect that we need to remove the "/sam/"
        #           part we can save the time of a full get_path() lookup.
        just_sfs_path_part = pnfs.strip_pnfs_mountpoint(enstoredb_path)
        # Skip element zero in dir_list since we already tried it.  No use
        # wasting time trying something we know will fail.
        # Also, there are cases were the directory we should skip, just happens
        # to have another directory with the same name underneath were we
        # are looking.  The false positive is known to slow D0 down a lot
        # if we don't avoid it.
        # Don't check everything... limit this to the second through fifth
        # directory levels for performace, since it is highly unlikely
        # to be found more than one level below the mount point.
        dir_list = just_sfs_path_part.split("/", 5)[1:]
        for i in range(len(dir_list[:-1])):
            single_dir = os.path.join(use_mp, dir_list[i])
            try:
                file_utils.get_stat(single_dir)
                use_name = os.path.join(use_mp,
                                        string.join(dir_list[i:], "/"))
                break
            except (OSError, IOError):
                pass

        # We need to check if it is an orphaned file.  If the sfs ids match
        # then the file has not been moved or renamed since it was written.
        # If they match, then the ".(access)()" filename is passed to
        # check_file(). check_file() should skip its own check of the
        # ".(access)()" filenames, since they have been shone to still be
        # valid.
        #
        # If it doesn't match then:
        # 1) The file is orphaned.  (get_path() gives ENOENT or EIO)
        # 2) The file is moved.  (get_path() gives the new path)
        # 3) The file is renamed. (get_path() gives the new name)
        try:
            cur_sfs_id = pnfs.get_pnfsid(use_name)
        except (OSError, IOError):
            cur_sfs_id = None
        if not cur_sfs_id or cur_sfs_id != enstoredb_sfs_id:
            try:
                sfs = namespace.StorageFS(use_mp, shortcut=True)
                tmp_name_list = sfs.get_path(enstoredb_sfs_id, use_mp)
                # Deal with multiple possible matches.
                if len(tmp_name_list) == 1:
                    tmp_name = tmp_name_list[0]
                else:
                    raise OSError(errno.EIO, "to many matches", tmp_name_list)

                if tmp_name[0] == "/":
                    # Make sure the path is an absolute path.
                    sfs_path = tmp_name
                else:
                    # If the path is not an absolute path we get here.  What
                    # happend is that get_path() was able to find a pnfs
                    # mount point connected to the correct pnfs database,
                    # but not a mount for the correct database.
                    #
                    # The best we can do is use the .(access)() name.
                    pass
            except (OSError, IOError) as detail:
                if detail.errno in [errno.EBADFD, errno.EIO]:
                    raise OSError(errno.EIO, "orphaned file", sfs_id)
                else:
                    raise_(sys.exc_info()[0], sys.exc_info()[1],
                           sys.exc_info()[2])
        else:
            sfs_path = use_name
    else:
        sfs_path = use_name

    return sfs_path


find_chimeraid_path = find_id_path
# We can replace this when we think find_id_path() is really ready.
#find_pnfsid_path = find_id_path


# pnfsid is a string representing the uninque internal ID for the PNFS storage
#   filesystem.
# bfid is a string consisting of a files unique id in Enstore.
# file_record is file information from the file_clerk's bfid_info() function.
# likely_path is the location that the calling function believes to be
#   the correct return value.
# use_info_server specifies if the info server or the file clerk should be
#   used for bfid_info() calls.
def find_pnfsid_path(pnfsid, bfid, file_record=None, likely_path=None,
                     path_type=enstore_constants.BOTH,
                     use_info_server=USE_INFO_SERVER_DEFAULT):
    global search_list

    if not pnfs.is_pnfsid(pnfsid):
        raise ValueError("Expected pnfsid not: %s" % (pnfsid,))
    if not bfid_util.is_bfid(bfid):
        raise ValueError("Expected bfid not: %s" % (bfid,))

    # Remember if a file with matching pnfsid (and unmatching bfid) was found
    # after all pnfs databases have been checked.
    possible_reused_pnfsid = 0
    # Remember if a file with found layers 1 and 4 has not had a directory
    # entry found after all pnfs databases have been checked.
    possible_orphaned_file = 0

    # afn = Access File Name   (aka .(access)())
    afn = ""

    # We will need the pnfs database numbers.
    if pnfs.is_pnfsid(pnfsid):
        pnfsid_db = int(pnfsid[:4], 16)
    else:
        pnfsid_db = None

    # If we weren't passed a file_record, then we must get it ourselves.
    if not file_record:
        # Get either the file_clerk_client or the info_client depending
        # on our needs.
        infc = get_clerk_client(use_info_server=use_info_server)
        file_record = infc.bfid_info(bfid)
        if not e_errors.is_ok(file_record):
            #ValueError = punt
            raise ValueError(
                "Unable to obtain file information: %s" %
                (file_record['status'],))
    else:
        infc = None  # Make sure this exists, so we can test for it later.

    # Extract these values.  'pnfs_path' and 'pnfs_id' correlate if the file
    # record comes from the db directly.  'pnfs_name0' and 'pnfsid' are
    # the names via the file clerk bfid_info() function.
    enstoredb_path = file_record.get('pnfs_path',
                                     file_record.get('pnfs_name0', None))
    enstoredb_pnfsid = file_record.get('pnfs_id',
                                       file_record.get('pnfsid', None))

    # Lets try the suggested path first.
    path_list = []
    if likely_path:
        path_list.append(likely_path)
    # Otherwise play some quick search games by reading /etc/mtab.
    if enstoredb_path:
        # get_enstore_mount_point() and get_enstore_admin_mount_point look
        # at /etc/mtab.  This makes this largely a fast call compared to
        # those that query pnfs.
        if path_type == enstore_constants.FS:
            mount_paths = pnfs.get_enstore_admin_mount_point()
        elif path_type == enstore_constants.NONFS:
            mount_paths = pnfs.get_enstore_mount_point()
        else:  # both
            mount_paths = pnfs.get_enstore_mount_point() + \
                pnfs.get_enstore_admin_mount_point()

        for mount_path in mount_paths:
            if enstoredb_path.startswith(mount_path):
                # Need to add one to the length of mount_path to skip
                # passed the leading "/".
                new_path = enstoredb_path
                path_list.append(new_path)
            else:
                if enstoredb_path.find("/pnfs/fs/usr/") == -1:
                    new_path = enstoredb_path.replace(
                        "/pnfs/", "/pnfs/fs/usr/", 1)
                    path_list.append(new_path)
                else:
                    new_path = enstoredb_path.replace(
                        "/pnfs/fs/usr/", "/pnfs/", 1)
                    path_list.append(new_path)

            # If the paths begins with something like
            # /pnfs/fnal.gov/usr/... we need to convert and check for
            # this too.  This is most likely necessary when the scan
            # is run on an offline copy that did not have the
            # fnal.gov symbolic link to fs made.
            if path_type in [enstore_constants.FS, enstore_constants.BOTH]:
                domain_name = hostaddr.getdomainname()
                if domain_name:
                    new_path = enstoredb_path.replace(
                        "/pnfs/%s/usr/" % (domain_name,), "/pnfs/fs/usr/", 1)
                    if new_path not in path_list:
                        path_list.append(new_path)

    if path_list:
        for try_path in path_list:
            try:
                layer1_bfid = pnfs.get_layer_1(try_path)
            except (OSError, IOError):
                layer1_bfid = None
            if layer1_bfid == bfid:
                return try_path

    # Loop over all found mount points.
    search_list_lock.acquire()
    try:
        if search_list is None:
            search_list = pnfs.process_mtab()
    except BaseException:
        search_list_lock.release()
        raise_(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
    search_list_copy = search_list[:]
    search_list_lock.release()
    for search_item in search_list_copy:
        database_info = search_item[pnfs.DB_INFO]
        db_num = search_item[pnfs.DB_NUMBER]
        mp = search_item[pnfs.DB_MOUNT_POINTS][0]

        # If last_db_tried is still set to its initial value, we need to
        # skip the the next.
        if db_num < 0:
            continue

        # If we want a specific type of pnfs path, ignore the others.
        # Valid types are:
        # FS -> /pnfs/fs or /pnfs/fs/usr
        # NONFS - > /pnfs/xyz
        # BOTH -> /pnfs/xyz, /pnfs/fs or /pnfs/fs/usr
        if path_type == enstore_constants.FS:
            if db_num == 0:
                # If the admin mount point is /pnfs/fs/usr then
                # is_normal_pnfs_path() does the correct thing by returning
                # True, but if it is just /pnfs/fs, then we need this if
                # statement to do the correct thing - return False - knowning
                # the admin DB number is always zero.
                pass
            elif pnfs.is_normal_pnfs_path(mp):
                continue
        elif path_type == enstore_constants.NONFS and pnfs.is_admin_pnfs_path(mp):
            continue

        # This test is to make sure that the pnfs filesystem we are going
        # to query has a database N (where N is pnfsid_db).  Otherwise
        # we hang querying a non-existent database.  If the pnfsid_db
        # matches the last one we tried we skip this test as it has
        # already been done.
        if pnfs.get_last_db()[pnfs.DB_INFO] != pnfsid_db:
            try:
                pnfs.N(pnfsid_db, mp).get_databaseN()
            except IOError:
                # This /pnfs/fs doesn't contain a database with the id we
                # are looking for.
                continue

        # We don't need to determine the full path of the file
        # to know if it exists.  The path could be different
        # between two machines anyway.
        # afn =  Access File Name
        afn = pnfs.access_file(mp, pnfsid)

        # Check layer 1 to get the bfid.
        try:
            layer1_bfid = pnfs.get_layer_1(afn)
        except (OSError, IOError):
            layer1_bfid = None
        if layer1_bfid:
            # If we haven't needed the file_clerk_client/info_client yet,
            # get it now.
            if not infc:
                infc = get_clerk_client(use_info_server=use_info_server)
            # Make sure this is the correct file or copy thereof.
            if layer1_bfid == file_record['bfid'] or \
                    layer1_bfid == \
                    infc.find_original(file_record['bfid'])['original']:

                if layer1_bfid != file_record['bfid']:
                    # Must be a multiple copy to get to this point.
                    is_multiple_copy = True
                else:
                    is_multiple_copy = False

                if file_record['deleted'] == 'yes':
                    try:
                        p = pnfs.Pnfs(shortcut=True)
                        tmp_name_list = p.get_path(pnfsid, mp)
                        # Deal with multiple possible matches.
                        if len(tmp_name_list) == 1:
                            # There is one known case where asking for layer
                            # 1 using the pnfsid and separately using the
                            # filename returned different values.  This
                            # attempts to catch such situations.
                            alt_layer1_bfid = pnfs.get_layer_1(
                                tmp_name_list[0])
                            if alt_layer1_bfid != layer1_bfid:
                                message = "conflicting layer 1 values " \
                                          "(id %s, name %s)" % \
                                          (layer1_bfid, alt_layer1_bfid)
                                raise OSError(errno.EBADF, message,
                                              tmp_name_list[0])

                            # Remember, that in order to get this far the
                            # file needs to be marked deleted in the file
                            # db, thus both cases are both errors.
                            if not is_multiple_copy:
                                raise OSError(errno.EEXIST,
                                              "pnfs entry exists",
                                              tmp_name_list[0])
                            else:
                                raise OSError(errno.EEXIST,
                                              "found original of copy",
                                              tmp_name_list[0])
                        else:
                            raise OSError(errno.EIO, "to many matches",
                                          tmp_name_list)
                    except (OSError, IOError) as detail:
                        if detail.errno in [errno.EBADFD, errno.EIO]:
                            raise OSError(errno.ENOENT, "orphaned file",
                                          pnfsid)
                        else:
                            raise_(sys.exc_info()[0], sys.exc_info()[1],
                                   sys.exc_info()[2])

                else:
                    db_a_dirpath = pnfs.get_directory_name(afn)
                    database_path = pnfs.database_file(db_a_dirpath)
                    # Get the database info for the filesystem.
                    db_info = pnfs.get_database(database_path)

                    # Update the global cache information.
                    if database_info != db_info:

                        # At this point the database that the file is in
                        # doesn't match the one that returned a positive hit.
                        # So, we first check if a known PNFS database is
                        # a match...
                        for item in search_list_copy:
                            if item[pnfs.DB_INFO] == db_info:
                                mount_points = item[pnfs.DB_MOUNT_POINTS]
                                for mount_point in mount_points:
                                    pnfs_path = pnfs.access_file(mount_point,
                                                                 enstoredb_pnfsid)
                                    layer1_bfid = pnfs.get_layer_1(pnfs_path)
                                    if layer1_bfid and \
                                            layer1_bfid == file_record['bfid']:
                                        pnfs.set_last_db(copy.copy(item))
                                        pnfsid_mp = mount_point
                                        break
                        else:
                            # ...if it is not a match then we have a database
                            # not in our current cached list.  So we need
                            # to find it.  This is going to be a resource
                            # hog, but once we find it, we won't need to
                            # do so for any more files once the mount
                            # point is added to the cached list.

                            # If this p.get_path() fails, it is most likely,
                            # because of permission problems of either
                            # /pnfs/fs or /pnfs/fs/usr.  Especially for
                            # new pnfs servers.
                            try:
                                p = pnfs.Pnfs()
                                pnfs_path_list = p.get_path(enstoredb_pnfsid,
                                                            mp)
                                # Deal with multiple possible matches.
                                if len(pnfs_path_list) == 1:
                                    pnfs_path = pnfs_path_list[0]
                                else:
                                    raise OSError(errno.EIO, "to many matches",
                                                  pnfs_path_list)
                                pnfsid_mp = p.get_pnfs_db_directory(pnfs_path)
                            except (OSError, IOError) as msg:
                                pnfs_path = afn
                                if msg.errno in [errno.ENOENT]:
                                    pnfsid_mp = None
                                    # Using the current mountpoint, we
                                    # were able to obtain layer 1, but the
                                    # file does not exist?  Let's flag this
                                    # as a possible orphan file.
                                    possible_orphaned_file = \
                                        possible_orphaned_file + 1
                                else:
                                    pnfsid_mp = mp  # ???

                            if pnfsid_mp is not None:
                                # This is just some paranoid checking.
                                afn = pnfs.access_file(pnfsid_mp,
                                                       enstoredb_pnfsid)
                                layer1_bfid = pnfs.get_layer_1(afn)
                                if layer1_bfid and \
                                        layer1_bfid == file_record['bfid']:
                                    # last_db_tried = (db_info,
                                    #                 (pnfsid_db, pnfsid_mp))
                                    pnfs.set_last_db((db_info,
                                                      (pnfsid_db, pnfsid_mp)))
                                    pnfs.add_mtab(db_info, pnfsid_db,
                                                  pnfsid_mp)
                            else:
                                pnfs.set_last_db(("", (-1, "")))

                    else:
                        pnfs.set_last_db((db_info, (pnfsid_db, mp)))
                        # We found the file, set the pnfs path.
                        pnfs_path = afn
                        pnfsid_mp = mp

                    # pnfs_path and pnfsid_mp needs to be set correctly by
                    # this point.
                    if pnfsid_mp is None:
                        continue
                    break

            else:
                try:
                    p = pnfs.Pnfs(shortcut=True)
                    tmp_name_list = p.get_path(enstoredb_pnfsid, mp)
                    # Deal with multiple possible matches.
                    if len(tmp_name_list) == 1:
                        # There is one known case where asking for layer 1
                        # using the pnfsid and seperately using the
                        # filename to ask for the same layer 1 returned
                        # different values.  This attempts to catch such
                        # situations.
                        if db_num != 0 and pnfsid_db != db_num:
                            continue
                        alt_layer1_bfid = pnfs.get_layer_1(tmp_name_list[0])
                        if alt_layer1_bfid != layer1_bfid:
                            message = "conflicting layer 1 values " \
                                      "(id %s, name %s) for %s" % \
                                      (layer1_bfid, alt_layer1_bfid, pnfsid)
                            raise OSError(errno.EBADF, message,
                                          tmp_name_list[0])
                        else:
                            # The BFIDs match, so there isn't a problem here.
                            # Keep going.
                            pass

                    else:
                        raise OSError(errno.EIO, "to many matches",
                                      tmp_name_list)
                except (OSError, IOError) as detail:
                    if detail.errno in [errno.EBADFD, errno.EIO]:
                        raise OSError(errno.ENOENT, "orphaned file",
                                      pnfsid)
                    else:
                        raise_(sys.exc_info()[0], sys.exc_info()[1],
                               sys.exc_info()[2])

            # To recap:
            # By this point we found a file with the pnfsid that we are
            # looking for.  However, the bfid in layer 1 didn't match the
            # bfid we are looking for.

            # If we found the right bfid brand, we know the right pnfs system
            # was found.
            pnfs_brand = bfid_util.extract_brand(layer1_bfid)
            filedb_brand = bfid_util.extract_brand(file_record['bfid'])
            if pnfs_brand and filedb_brand and pnfs_brand == filedb_brand:
                if file_record['deleted'] == 'yes':
                    raise OSError(errno.ENOENT,
                                  replaced_error_string(layer1_bfid, bfid),
                                  tmp_name_list[0])

                possible_reused_pnfsid = possible_reused_pnfsid + 1
                continue

            # If we found a bfid that didn't have the correct id or the
            # brands did not match, go back to the top and try the next
            # pnfs filesystem.
    else:
        if afn:
            if file_record['deleted'] != 'yes':
                # We don't have a layer 1.  As a last ditch effort check layer 4.
                # There probably won't be anything, but every now and then...
                try:
                    layer4_dict = pnfs.get_layer_4(afn)
                except (OSError, IOError) as msg:
                    if msg.args[0] in [errno.EACCES, errno.EPERM]:
                        raise_(sys.exc_info()[0], sys.exc_info()[1],
                               sys.exc_info()[2])
                    layer4_dict = {}

                # We also need to grab the pnfs id of the file that is there.
                #  This way we can differentiate the cases were the file
                # does not exist and those replaced.
                try:
                    normal = pnfs.get_enstore_pnfs_path(
                        file_record['pnfs_name0'])
                except (OSError, IOError):
                    normal = None
                try:
                    admin = pnfs.get_enstore_fs_path(file_record['pnfs_name0'])
                except (OSError, IOError):
                    admin = None
                # Only put the mount point types that actually exist in the
                # search list.
                s_list = []
                for path_value in [normal, admin]:
                    if path_value:
                        s_list.append(path_value)
                for path in s_list:
                    try:
                        pnfs_pnfsid = pnfs.get_pnfsid(path)
                        break
                    except (OSError, IOError):
                        continue
                else:
                    pnfs_pnfsid = ""

                # We throw away the (err_l, warn_l, info_l) values becuase we
                # expect them to not be there if layer 1 is not there.  It does
                # not make sense to report what we already know.
                if layer4_dict.get('bfid', None):
                    # If we found a bfid in layer 4, report the error about
                    # layer 1.
                    if possible_reused_pnfsid:
                        # File is not deleted, pnfs id is still valid,
                        # found a different bfid in layer 1.  Lets fall
                        # though and report more useful error message.
                        pass

                        #raise OSError(errno.ENOENT, "reused pnfsid", afn)
                    elif possible_orphaned_file:
                        # File is not deleted, pnfs id is still valid, found
                        # the right layer 1 but the file does not have a
                        # directory entry in PNFS.
                        raise OSError(errno.ENOENT, "orphaned file",
                                      pnfsid)
                    elif layer4_dict.get('bfid', None) != bfid:
                        raise OSError(errno.ENOENT, "found non-matching file",
                                      afn)
                    else:
                        raise OSError(errno.ENOENT, "missing layer 1", afn)
                elif pnfs_pnfsid and pnfs_pnfsid != file_record['pnfsid']:
                    raise OSError(errno.ENOENT, "has been replaced",
                                  pnfsid)
                else:
                    # Since the file exists, but has no layers, report the
                    # error.
                    raise OSError(errno.ENOENT, os.strerror(errno.ENOENT),
                                  pnfsid)

        layer1_bfid = pnfs.get_layer_1(enstoredb_path)
        if layer1_bfid and layer1_bfid != file_record['bfid']:
            # If this is the case that the bfids don't match,
            # also include this piece of information.
            raise OSError(errno.EEXIST,
                          replaced_error_string(layer1_bfid, bfid),
                          enstoredb_path)

        if possible_reused_pnfsid > 0:
            raise OSError(errno.EEXIST, "reused pnfsid",
                          enstoredb_path)

        raise OSError(errno.ENOENT, os.strerror(errno.ENOENT),
                      enstoredb_path)

    # Since we know if we are using /pnfs/fs or not, we can maniplate the
    # original name to the correct pnfs base path.  This will speed things
    # up when scanning files written to /pnfs/xyz but only having /pnfs/fs
    # mounted for the scan.
    #
    # This won't help for the case of moved or renamed files.  We still
    # will bite the bullet of calling get_path() for those.
    if not pnfs.is_access_name(pnfs_path):
        # If we have already had to do a full path lookup to find the
        # correct pnfs database/mountpoint, we don't need to worry about
        # any of this path munging.
        use_name = pnfs_path
        use_mp = pnfsid_mp
    elif db_num == 0 and enstoredb_path.find("/pnfs/fs/usr") == -1:
        try:
            use_name = pnfs.get_enstore_fs_path(enstoredb_path)
        except (OSError):
            # We can get here when the file has been moved/renamed.
            use_name = enstoredb_path
        use_mp = pnfsid_mp.replace("/pnfs/fs", "/pnfs/fs/usr/", 1)
    elif mp.find("/pnfs/fs/usr/") >= 0 and \
            enstoredb_path.find("/pnfs/fs/usr") == -1:
        try:
            use_name = pnfs.get_enstore_fs_path(enstoredb_path)
        except (OSError):
            # We can get here when the file has been moved/renamed.
            use_name = enstoredb_path
        use_mp = pnfsid_mp
    elif mp.find("/pnfs/fs/usr/") == -1 and \
            enstoredb_path.find("/pnfs/fs/usr") >= 0:
        try:
            use_name = pnfs.get_enstore_pnfs_path(enstoredb_path)
        except (OSError):
            # We can get here when the file has been moved/renamed.
            use_name = enstoredb_path
        use_mp = pnfsid_mp
    else:
        use_name = enstoredb_path
        use_mp = pnfsid_mp

    use_name = os.path.abspath(use_name)
    use_mp = os.path.abspath(use_mp)

    try:
        cur_pnfsid = pnfs.get_pnfsid(use_name)
    except OSError:
        cur_pnfsid = None
    if not cur_pnfsid or cur_pnfsid != enstoredb_pnfsid:
        # Before jumping off the deep end by calling get_path(), lets try
        # one more thing.  Here we try and remove any intermidiate
        # directories that are not present in the current path.
        #  Example: original path begins: /pnfs/sam/dzero/...
        #           current use path begins /pnfs/fs/usr/dzero/...
        #           If we can detect that we need to remove the "/sam/"
        #           part we can save the time of a full get_path() lookup.
        just_pnfs_path_part = pnfs.strip_pnfs_mountpoint(enstoredb_path)
        # Skip element zero in dir_list since we already tried it.  No use
        # wasting time trying something we know will fail.
        # Also, there are cases were the directory we should skip, just happens
        # to have another directory with the same name underneath were we
        # are looking.  The false positive is known to slow D0 down a lot
        # if we don't avoid it.
        # Don't check everything... limit this to the second through fith
        # directory levels for performace, since it is highly unlikely
        # to be found more than one level below the mount point.
        dir_list = just_pnfs_path_part.split("/", 5)[1:]
        for i in range(len(dir_list[:-1])):
            single_dir = os.path.join(use_mp, dir_list[i])
            try:
                file_utils.get_stat(single_dir)
                use_name = os.path.join(use_mp,
                                        string.join(dir_list[i:], "/"))
                break
            except (OSError, IOError):
                pass

        # We need to check if it is an orphaned file.  If the pnfsids match
        # then the file has not been moved or renamed since it was written.
        # If they match, then the ".(access)()" filename is passed to
        # check_file(). check_file() should skip its own check of the
        # ".(access)()" filenames, since they have been shone to still be
        # valid.
        #
        # If it doesn't match then:
        # 1) The file is orphaned.  (get_path() gives ENOENT or EIO)
        # 2) The file is moved.  (get_path() gives the new path)
        # 3) The file is renamed. (get_path() gives the new name)
        try:
            cur_pnfsid = pnfs.get_pnfsid(use_name)
        except OSError:
            cur_pnfsid = None
        if not cur_pnfsid or cur_pnfsid != enstoredb_pnfsid:
            try:
                tmp_name_list = pnfs.Pnfs(shortcut=True).get_path(
                    enstoredb_pnfsid, use_mp)
                # Deal with multiple possible matches.
                if len(tmp_name_list) == 1:
                    tmp_name = tmp_name_list[0]
                else:
                    raise OSError(errno.EIO, "to many matches", tmp_name_list)

                if tmp_name[0] == "/":
                    # Make sure the path is an absolute path.
                    pnfs_path = tmp_name
                else:
                    # If the path is not an absolute path we get here.  What
                    # happend is that get_path() was able to find a pnfs
                    # mount point connected to the correct pnfs database,
                    # but not a mount for the correct database.
                    #
                    # The best we can do is use the .(access)() name.
                    pass
            except (OSError, IOError) as detail:
                if detail.errno in [errno.EBADFD, errno.EIO]:
                    raise OSError(errno.EIO, "orphaned file", pnfsid)
                else:
                    raise_(sys.exc_info()[0], sys.exc_info()[1],
                           sys.exc_info()[2])
        else:
            pnfs_path = use_name
    else:
        pnfs_path = use_name

    return pnfs_path


# Return the specific 'replaced with %s file" we want.
def replaced_error_string(layer1_bfid, bfid):
    # If this is the case that the bfids don't match,
    # also include this piece of information.
    try:
        layer1_time = int(bfid_util.strip_brand(layer1_bfid)[:-5])
        bfid_time = int(bfid_util.strip_brand(bfid)[:-5])
    except (KeyError, ValueError, AttributeError, TypeError):
        return "found different file"

    if bfid_time < layer1_time:
        return "replaced with newer file"
    else:
        return "replaced with another file"


def get_clerk_client(use_info_server=USE_INFO_SERVER_DEFAULT):
    # Get the configuration server and file info server clients.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM
    if use_info_server:
        # PNFS scans should use the info server.
        import info_client
        infc = info_client.infoClient(csc, flags=flags)
    else:
        import file_clerk_client
        # Things like encp and migration should not depend on the info_server.
        infc = file_clerk_client.FileClient(csc, flags=flags)

    return infc
