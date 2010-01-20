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
import errno
import sys
import copy
import os
import string

import pnfs
import enstore_functions2
import enstore_functions3
import configuration_client
import info_client
import file_clerk_client
import enstore_constants
import e_errors
import file_utils

#######################################################################

BOTH="BOTH"
FS="FS"
NONFS="NONFS"

#pnfsid is a string consisting of a files unique id in PNFS.
#bfid is a string consisting of a files unique id in Enstore.
#file_record is file information from the file_clerk's bfid_info() function.
#use_info_server specifies if the info server or the file clerk should be
#   used for bfid_info() calls.
def find_pnfsid_path(pnfsid, bfid, file_record = None, likely_path = None,
                     path_type = BOTH, use_info_server=False):

    if not pnfs.is_pnfsid(pnfsid):
        raise ValueError("Expected pnfsid not: %s" % (pnfsid,))
    if not enstore_functions3.is_bfid(bfid):
        raise ValueError("Expected bfid not: %s" % (bfid,))

    #Remember if a file with matching pnfsid (and unmatching bfid) was found
    # after all pnfs databases have been checked.
    possible_reused_pnfsid = 0
    #Remember if a file with found layers 1 and 4 has not had a directory
    # entry found after all pnfs databases have been checked.
    possible_orphaned_file = 0

    #afn = Access File Name   (aka .(access)())
    afn = ""
    
    #We will need the pnfs database numbers.
    if pnfs.is_pnfsid(pnfsid):
        pnfsid_db = int(pnfsid[:4], 16)
    else:
        pnfsid_db = None

    #Get the configuration server and file info server clients.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM
    if use_info_server:
        #PNFS scans should use the info server.
        infc = info_client.infoClient(csc, flags=flags)
    else:
        #Things like encp and migration should not depend on the info_server.
        infc = file_clerk_client.FileClient(csc, flags=flags)

    if not file_record:
        file_record = infc.bfid_info(bfid)
        if not e_errors.is_ok(file_record):
            #ValueError = punt
            raise ValueError("Unable to obtain file information.")

    #Extract these values.  'pnfs_path' and 'pnfs_id' correlate if the file
    # record comes from the db directly.  'pnfs_name0' and 'pnfsid' are
    # the names via the file clerk bfid_info() function.
    enstoredb_path = file_record.get('pnfs_path',
                                     file_record.get('pnfs_name0', None))
    enstoredb_pnfsid = file_record.get('pnfs_id',
                                       file_record.get('pnfsid', None))

    #Lets try the suggested path first.
    path_list = []
    if likely_path:
        path_list.append(likely_path)
    #Otherwise play some quick search games by reading /etc/mtab.
    if enstoredb_path:
        # get_enstore_mount_point() and get_enstore_admin_mount_point look
        # at /etc/mtab.  This makes this largely a fast call compared to
        # those that query pnfs.
        if path_type == FS:
            mount_paths = pnfs.get_enstore_admin_mount_point()
        elif path_type == NONFS:
            mount_paths = pnfs.get_enstore_mount_point()
        else:  #both
            mount_paths = pnfs.get_enstore_mount_point() + \
                          pnfs.get_enstore_admin_mount_point()

        for mount_path in mount_paths:
            if enstoredb_path.startswith(mount_path):
                #Need to add one to the length of mount_path to skip
                # passed the leading "/".
                new_path = os.path.join(mount_path,
                                        enstoredb_path[len(mount_path)+1:])
                path_list.append(new_path)

    if path_list:
        for try_path in path_list:
            try:
                layer1_bfid = pnfs.get_layer_1(try_path)
            except (OSError, IOError):
                layer1_bfid = None
            if layer1_bfid == bfid:
                return try_path

    #Loop over all found mount points.
    search_list = pnfs.process_mtab()
    for database_info, (db_num, mp)  in search_list:

        #If last_db_tried is still set to its initial value, we need to
        # skip the the next.
        if db_num < 0:
            continue

        #If we want a specific type of pnfs path, ignore the others.
        if path_type == FS:
            if db_num == 0:
                #If the admin mount point is /pnfs/fs/usr then
                # is_normal_pnfs_path() does the correct thing, but if it is
                # just /pnfs/fs, then we need this if statement to do the
                # correct thing knowning the admin DB number is always zero.
                pass
            elif pnfs.is_normal_pnfs_path(mp):
                continue
        elif path_type == NONFS and pnfs.is_admin_pnfs_path(mp):
            continue
        
        #This test is to make sure that the pnfs filesystem we are going
        # to query has a database N (where N is pnfsid_db).  Otherwise
        # we hang querying a non-existant database.  If the pnfsid_db
        # matches the last one we tried we skip this test as it has
        # already been done.
        if pnfs.get_last_db()[0] != pnfsid_db:
            try:
                pnfs.N(pnfsid_db, mp).get_databaseN()
            except IOError:
                #This /pnfs/fs doesn't contain a database with the id we
                # are looking for.
                continue
        
        #We don't need to determine the full path of the file
        # to know if it exists.  The path could be different
        # between two machines anyway.
        #afn =  Access File Name
        afn = pnfs.access_file(mp, pnfsid)

        #Check layer 1 to get the bfid.
        try:
            layer1_bfid = pnfs.get_layer_1(afn)
        except (OSError, IOError):
            layer1_bfid = None
        if layer1_bfid:
            #Make sure this is the correct file or copy thereof.
            if layer1_bfid == file_record['bfid'] or \
                   layer1_bfid == \
                   infc.find_original(file_record['bfid'])['original']:
                
                if layer1_bfid != file_record['bfid']:
                    #Must be a multiple copy to get to this point.
                    is_multiple_copy = True
                else:
                    is_multiple_copy = False

                if file_record['deleted'] == 'yes':
                    try:
                        tmp_name_list = pnfs.Pnfs(shortcut = True).get_path(pnfsid, mp)
                        #Deal with multiple possible matches.
                        if len(tmp_name_list) == 1:
                            #There is one known case where asking for layer
                            # 1 using the pnfsid and seperately using the
                            # filename returned different values.  This
                            # attempts to catch such situations.
                            alt_layer1_bfid = pnfs.get_layer_1(tmp_name_list[0])
                            if alt_layer1_bfid != layer1_bfid:
                                message = "conflicting layer 1 values " \
                                          "(id %s, name %s)" % \
                                          (layer1_bfid, alt_layer1_bfid)
                                raise OSError(errno.EBADF, message,
                                              tmp_name_list[0])

                            #Remember, that in order to get this far the
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
                    except (OSError, IOError), detail:
                        if detail.errno in [errno.EBADFD, errno.EIO]:
                            raise OSError(errno.ENOENT, "orphaned file",
                                          pnfsid)
                        else:
                            raise sys.exc_info()[0], sys.exc_info()[1], \
                                  sys.exc_info()[2]
                        
                else:
                    db_a_dirpath = pnfs.get_directory_name(afn)
                    database_path = pnfs.database_file(db_a_dirpath)
                    #Get the database info for the filesystem.
                    db_info = pnfs.get_database(database_path)

                    #Update the global cache information.
                    if database_info != db_info:

                        #At this point the database that the file is in
                        # doesn't match the one that returned a positive hit.
                        # So, we first check if a known PNFS database is
                        # a match...
                        for item in search_list:
                            if item[0] == db_info:
                                pnfs_path = pnfs.access_file(item[1][1],
                                                             enstoredb_pnfsid)
                                layer1_bfid = pnfs.get_layer_1(pnfs_path)
                                if layer1_bfid and \
                                       layer1_bfid == file_record['bfid']:
                                    #last_db_tried = copy.copy(item)
                                    pnfs.set_last_db(copy.copy(item))
                                    pnfsid_mp = item[1][1]
                                    break
                        else:
                            #...if it is not a match then we have a database
                            # not it our current cached list.  So we need
                            # to find it.  This is going to be a resource
                            # hog, but once we find it, we won't need to
                            # do so for any more files.
                            
                            #If this p.get_path() fails, it is most likely,
                            # because of permission problems of either
                            # /pnfs/fs or /pnfs/fs/usr.  Especially for
                            # new pnfs servers.
                            try:
                                p = pnfs.Pnfs()
                                pnfs_path_list = p.get_path(enstoredb_pnfsid,
                                                            mp)
                                #Deal with multiple possible matches.
                                if len(pnfs_path_list) == 1:
                                    pnfs_path = pnfs_path_list[0]
                                else:
                                    raise OSError(errno.EIO, "to many matches",
                                                  pnfs_path_list)
                                pnfsid_mp = p.get_pnfs_db_directory(pnfs_path)
                            except (OSError, IOError), msg:
                                pnfs_path = afn
                                if msg.errno in [errno.ENOENT]:
                                    pnfsid_mp = None
                                    #Using the current mountpoint, we
                                    # were able to obtain layer 1, but the
                                    # file does not exists?  Let's flag this
                                    # as a possible orphan file. 
                                    possible_orphaned_file = \
                                                   possible_orphaned_file + 1
                                else:
                                    pnfsid_mp = mp #???

                            if pnfsid_mp != None:
                                #This is just some paranoid checking.
                                afn = pnfs.access_file(pnfsid_mp,
                                                       enstoredb_pnfsid)
                                layer1_bfid = pnfs.get_layer_1(afn)
                                if layer1_bfid and \
                                       layer1_bfid == file_record['bfid']:
                                    #last_db_tried = (db_info,
                                    #                 (pnfsid_db, pnfsid_mp))
                                    pnfs.set_last_db((db_info,
                                                     (pnfsid_db, pnfsid_mp)))
                                    pnfs.add_mtab(db_info, pnfsid_db,
                                                  pnfsid_mp)
                            else:
                                pnfs.set_last_db(("", (-1, "")))

                    else:
                        pnfs.set_last_db((db_info, (pnfsid_db, mp)))
                        #We found the file, set the pnfs path.
                        pnfs_path = afn
                        pnfsid_mp = mp

                    #pnfs_path and pnfsid_mp needs to be set correctly by
                    # this point.
                    if pnfsid_mp == None:
                        continue
                    break
            
            else:
                try:
                    p =  pnfs.Pnfs(shortcut = True)
                    tmp_name_list = p.get_path(enstoredb_pnfsid, mp)
                    #Deal with multiple possible matches.
                    if len(tmp_name_list) == 1:
                        #There is one known case where asking for layer 1
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
                            #The BFIDs match, so there isn't a problem here.
                            # Keep going.
                            pass

                    else:
                        raise OSError(errno.EIO, "to many matches",
                                      tmp_name_list)
                except (OSError, IOError), detail:
                    if detail.errno in [errno.EBADFD, errno.EIO]:
                        raise OSError(errno.ENOENT, "orphaned file",
                                      pnfsid)
                    else:
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]


            #To recap:
            # By this point we found a file with the pnfsid that we are
            # looking for.  However, the bfid in layer 1 didn't match the
            # bfid we are looking for.


            #If we found the right bfid brand, we know the right pnfs system
            # was found.
            pnfs_brand = enstore_functions3.extract_brand(layer1_bfid)
            filedb_brand = enstore_functions3.extract_brand(file_record['bfid'])
            if pnfs_brand and filedb_brand and pnfs_brand == filedb_brand:
                if file_record['deleted'] == 'yes':
                    raise OSError(errno.ENOENT,
                                  replaced_error_string(layer1_bfid, bfid),
                                  tmp_name_list[0])

                possible_reused_pnfsid = possible_reused_pnfsid + 1
                continue

            #If we found a bfid that didn't have the correct id or the
            # brands did not match, go back to the top and try the next
            # pnfs filesystem.
            
    else:
        if afn:
            if file_record['deleted'] != 'yes':
                #We don't have a layer 1.  As a last ditch effort check layer 4.
                # There probably won't be anything, but every now and then...
                try:
                    layer4_dict = pnfs.get_layer_4(afn)
                except (OSError, IOError), msg:
                    if msg.args[0] in [errno.EACCES, errno.EPERM]:
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]
                    layer4_dict = {}

                #We also need to grab the pnfs id of the file that is there.
                #  This way we can differentiate the cases were the file
                # does not exist and those replaced.
                s_list = [pnfs.get_enstore_pnfs_path(file_record['pnfs_name0']),
                          pnfs.get_enstore_fs_path(file_record['pnfs_name0'])]
                for path in s_list:
                    try:
                        pnfs_pnfsid = pnfs.get_pnfsid(path)
                        break
                    except (OSError, IOError):
                        continue
                else:
                    pnfs_pnfsid = ""

                #We through away the (err_l, warn_l, info_l) values becuase we
                # expect them to not be there if layer 1 is not there.  It does
                # not make sense to report what we already know.
                if layer4_dict.get('bfid', None):
                     #If we found a bfid in layer 4, report the error about
                     # layer 1.
                     if possible_reused_pnfsid:
                         #File is not deleted, pnfs id is still valid,
                         #found a different bfid in layer 1.  Lets fall
                         # though and report more useful error message.
                         pass
                     
                         #raise OSError(errno.ENOENT, "reused pnfsid", afn)
                     elif possible_orphaned_file:
                         #File is not deleted, pnfs id is still valid, found
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
                    #Since the file exists, but has no layers, report the error.
                    raise OSError(errno.ENOENT, os.strerror(errno.ENOENT),
                                  pnfsid)

        layer1_bfid = pnfs.get_layer_1(enstoredb_path)
        if layer1_bfid and layer1_bfid != file_record['bfid']:
            #If this is the case that the bfids don't match,
            # also include this piece of information.
            raise OSError(errno.EEXIST,
                          replaced_error_string(layer1_bfid, bfid),
                          enstoredb_path)

        if possible_reused_pnfsid > 0:
            raise OSError(errno.EEXIST, "reused pnfsid",
                          enstoredb_path)

        raise OSError(errno.ENOENT, os.strerror(errno.ENOENT),
                      enstoredb_path)

    #Since we know if we are using /pnfs/fs or not, we can maniplate the
    # original name to the correct pnfs base path.  This will speed things
    # up when scanning files written to /pnfs/xyz but only having /pnfs/fs
    # mounted for the scan.
    #
    #This won't help for the case of moved or renamed files.  We still
    # will bite the bullet of calling get_path() for those.
    if not pnfs.is_access_name(pnfs_path):
        #If we have already had to do a full path lookup to find the
        # correct pnfs database/mountpoint, we don't need to worry about
        # any of this path munging.
        use_name = pnfs_path
        use_mp = pnfsid_mp
    elif db_num == 0 and enstoredb_path.find("/pnfs/fs/usr") == -1:
        try:
            use_name = pnfs.get_enstore_fs_path(enstoredb_path)
        except (OSError):
            #We can get here when the file has been moved/renamed.
            use_name = enstoredb_path
        use_mp = pnfsid_mp.replace("/pnfs/fs", "/pnfs/fs/usr/", 1)
    elif mp.find("/pnfs/fs/usr/") >= 0 and \
             enstoredb_path.find("/pnfs/fs/usr") == -1:
        try:
            use_name = pnfs.get_enstore_fs_path(enstoredb_path)
        except (OSError):
            #We can get here when the file has been moved/renamed.
            use_name = enstoredb_path
        use_mp = pnfsid_mp
    elif mp.find("/pnfs/fs/usr/") == -1 and \
             enstoredb_path.find("/pnfs/fs/usr") >= 0:
        try:
            use_name = pnfs.get_enstore_pnfs_path(enstoredb_path)
        except (OSError):
            #We can get here when the file has been moved/renamed.
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
        #Before jumping off the deep end by calling get_path(), lets try
        # one more thing.  Here we try and remove any intermidiate
        # directories that are not present in the current path.
        #  Example: original path begins: /pnfs/sam/dzero/...
        #           current use path begins /pnfs/fs/usr/dzero/...
        #           If we can detect that we need to remove the "/sam/"
        #           part we can save the time of a full get_path() lookup.
        just_pnfs_path_part = pnfs.strip_pnfs_mountpoint(enstoredb_path)
        #Skip element zero in dir_list since we already tried it.  No use
        # wasting time trying something we know will fail.
        #Also, there are cases were the directory we should skip, just happens
        # to have another directory with the same name underneath were we
        # are looking.  The false positive is known to slow D0 down a lot
        # if we don't avoid it.
        #Don't check everything... limit this to the first 5 minux 1
        # directories.
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

        #We need to check if it is an orphaned file.  If the pnfsids match
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
                tmp_name_list = pnfs.Pnfs(shortcut = True).get_path(
                    enstoredb_pnfsid, use_mp)
                #Deal with multiple possible matches.
                if len(tmp_name_list) == 1:
                    tmp_name = tmp_name_list[0]
                else:
                    raise OSError(errno.EIO, "to many matches", tmp_name_list)
                
                if tmp_name[0] == "/":
                    #Make sure the path is an absolute path.
                    pnfs_path = tmp_name
                else:
                    #If the path is not an absolute path we get here.  What
                    # happend is that get_path() was able to find a pnfs
                    # mount point connected to the correct pnfs database,
                    # but not a mount for the correct database.
                    #
                    #The best we can do is use the .(access)() name.
                    pass
            except (OSError, IOError), detail:
                if detail.errno  in [errno.EBADFD, errno.EIO]:
                    raise OSError(errno.EIO, "orphaned file", pnfsid)
                else:
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
        else:
            pnfs_path = use_name
    else:
        pnfs_path = use_name

    return pnfs_path


#Return the specific 'replaced with %s file" we want.
def replaced_error_string(layer1_bfid, bfid):
    #If this is the case that the bfids don't match,
    # also include this piece of information.
    try:
        layer1_time = int(enstore_functions3.strip_brand(layer1_bfid)[:-5])
        bfid_time = int(enstore_functions3.strip_brand(bfid)[:-5])
    except (KeyError, ValueError, AttributeError, TypeError):
        return "found different file"

    if bfid_time < layer1_time:
        return "replaced with newer file"
    else:
        return "replaced with another file"
