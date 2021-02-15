#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
from __future__ import print_function
from future.utils import raise_
import string
import errno
import sys
import socket
import os
import types

# enstore imports
import generic_client
import option
import backup_client
import hostaddr
import Trace
import e_errors
import pprint
import volume_clerk_client
import volume_family
import chimera
import info_client
import enstore_constants
import file_utils
from en_eval import en_eval

MY_NAME = enstore_constants.FILE_CLERK_CLIENT  # "FILE_C_CLIENT"
MY_SERVER = enstore_constants.FILE_CLERK  # "file_clerk"
RCV_TIMEOUT = 10
RCV_TRIES = 5

# union(list_of_sets)


def union(s):
    res = []
    for i in s:
        for j in i:
            if not j in res:
                res.append(j)
    return res


class FileClient(info_client.fileInfoMethods,  # generic_client.GenericClient,
                 backup_client.BackupClient):

    def __init__(self, csc, bfid=0, server_address=None, flags=0, logc=None,
                 alarmc=None, rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES):

        info_client.fileInfoMethods.__init__(self, csc, MY_NAME, server_address,
                                             flags=flags, logc=logc,
                                             alarmc=alarmc,
                                             rcv_timeout=rcv_timeout,
                                             rcv_tries=rcv_tries,
                                             server_name=MY_SERVER)

        self.bfid = bfid

    # create a bit file using complete metadata -- bypassing all
    def create_bit_file(self, file, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        # file is a structure without bfid
        ticket = {"fc": {}}
        ticket["fc"]["external_label"] = str(file["external_label"])
        ticket["fc"]["location_cookie"] = str(file["location_cookie"])
        ticket["fc"]["size"] = long(file["size"])
        ticket["fc"]["sanity_cookie"] = file["sanity_cookie"]
        ticket["fc"]["complete_crc"] = long(file["complete_crc"])
        ticket["fc"]["pnfsid"] = str(file["pnfsid"])
        ticket["fc"]["pnfs_name0"] = str(file["pnfs_name0"])
        ticket["fc"]["drive"] = str(file["drive"])
        # handle uid and gid
        if "uid" in file:
            ticket["fc"]["uid"] = file["uid"]
        if "gid" in file:
            ticket["fc"]["gid"] = file["gid"]
        ticket = self.new_bit_file(ticket)
        if ticket["status"][0] == e_errors.OK:
            ticket = self.set_pnfsid(ticket, timeout=timeout, retry=retry)
        return ticket

    def set_cache_status(self, arguments, timeout=0, retry=0):
        #
        # arguments look like a dictionary or a list of
        # dictionaries with keys
        # "bfid", "cache_status","archive_status","cache_location"
        ticket = {}
        ticket["bfids"] = []
        if isinstance(arguments, list):
            ticket["bfids"] = arguments[:]
        elif isinstance(arguments, dict):
            ticket["bfids"].append(arguments)
        else:
            raise_(
                TypeError,
                "Expect dictionary or list of dictionaries, not %s" %
                (type(arguments)))
        ticket["work"] = "set_cache_status"
        r = self.send(ticket, rcv_timeout=timeout, tries=retry)
        return r

    def open_bitfile(self, bfid, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        r = self.send({"work": "open_bitfile", "bfid": bfid},
                      rcv_timeout=timeout, tries=retry)
        return r

    def open_bitfile_for_package(
            self, bfid, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        r = self.send({"work": "open_bitfile_for_package",
                       "bfid": bfid}, rcv_timeout=timeout, tries=retry)
        return r

    def set_children(self, ticket, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        ticket["work"] = "set_children"
        r = self.send(ticket, rcv_timeout=timeout, tries=retry)
        return r

    def new_bit_file(self,
                     ticket,
                     timeout=RCV_TIMEOUT,
                     retry=RCV_TRIES):
        ticket['work'] = "new_bit_file"
        r = self.send(ticket, rcv_timeout=timeout, tries=retry)
        return r

    def show_state(self, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        return self.send({'work': 'show_state'},
                         rcv_timeout=timeout, tries=retry)

    def replay(self, all=None, timeout=1200, retry=1):
        ticket = {'work': 'replay',
                  'func': 'replay_cache_written_events'}
        if all:
            ticket['args'] = all
        return self.send(ticket, rcv_timeout=timeout, tries=retry)

    def set_pnfsid(self, ticket, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        ticket['work'] = "set_pnfsid"
        r = self.send(ticket, rcv_timeout=timeout, tries=retry)
        return r

    def get_brand(self, timeout=0, retry=0):
        ticket = {'work': 'get_brand'}
        r = self.send(ticket, rcv_timeout=timeout, tries=retry)
        if r['status'][0] == e_errors.OK:
            return r['brand']
        else:
            return None

    # find_copies(bfid) -- find the first generation of copies
    def find_copies(self, bfid, timeout=0, retry=0):
        ticket = {'work': 'find_copies',
                  'bfid': bfid}
        return self.send(ticket, rcv_timeout=timeout, tries=retry)

    # find_all_copies(bfid) -- find all copies from this file
    # This is done on the client side
    def find_all_copies(self, bfid, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        res = self.find_copies(bfid, timeout, retry)
        if res["status"][0] == e_errors.OK:
            copies = union([[bfid], res["copies"]])
            for i in res["copies"]:
                res2 = self.find_all_copies(i)
                if res2["status"][0] == e_errors.OK:
                    copies = union([copies, res2["copies"]])
                else:
                    return res2
            res["copies"] = copies
        return res

    # find_original(bfid) -- find the immidiate original
    def find_original(self, bfid, timeout=0, retry=0):
        ticket = {'work': 'find_original',
                  'bfid': bfid}
        if bfid:
            ticket = self.send(ticket, rcv_timeout=timeout, tries=retry)
        else:
            ticket['status'] = (e_errors.OK, None)
        return ticket

    # find_the_original(bfid) -- find the altimate original of this file
    # This is done on the client side
    def find_the_original(self, bfid, timeout=0, retry=0):
        res = self.find_original(bfid, timeout, retry)
        if res['status'][0] == e_errors.OK:
            if res['original']:
                res2 = self.find_the_original(res['original'], timeout, retry)
                return res2
            # this is actually the else part
            res['original'] = bfid
        return res

    # find_duplicates(bfid) -- find all original/copies of this file
    # This is done on the client side
    def find_duplicates(self, bfid, timeout=0, retry=0):
        res = self.find_the_original(bfid, timeout, retry)
        if res['status'][0] == e_errors.OK:
            return self.find_all_copies(res['original'])
        return res

    # get all pairs of bfids relating to migration/duplication of
    # the specified bfid
    def find_migrated(self, bfid, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        r = self.send({"work": "find_migrated", "bfid": bfid},
                      rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # report any information if this file has been involved in migration
    # or duplication
    def find_migration_info(self, bfid, find_src=1, find_dst=1,
                            order_by="copied", timeout=0, retry=0):
        # Map the possible arguments to something more simple.
        if not find_src:
            use_find_src = 0
        else:
            use_find_src = 1
        if not find_dst:
            use_find_dst = 0
        else:
            use_find_dst = 1

        # Make sure a valid order by value is given.
        if order_by not in ("copied", "swapped", "checked", "closed"):
            return {'status': (e_errors.WRONGPARAMETER,
                               "Expected migration state, not %s"
                               % (str(order_by),)),
                    'work': "find_migration_info",
                    'bfid': bfid,
                    }

        r = self.send({'work': "find_migration_info",
                       'bfid': bfid,
                       'find_src': use_find_src,
                       'find_dst': use_find_dst,
                       'order_by': order_by,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # Set the migration copied state for the bfid pair.
    def set_copied(self, src_bfid, dst_bfid, timeout=0, retry=0):
        r = self.send({'work': "set_copied",
                       'src_bfid': src_bfid,
                       'dst_bfid': dst_bfid,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # Clear the migration copied state for the bfid pair.
    def unset_copied(self, src_bfid, dst_bfid, timeout=0, retry=0):
        r = self.send({'work': "unset_copied",
                       'src_bfid': src_bfid,
                       'dst_bfid': dst_bfid,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # Set the migration swapped state for the bfid pair.
    def set_swapped(self, src_bfid, dst_bfid, timeout=0, retry=0):
        r = self.send({'work': "set_swapped",
                       'src_bfid': src_bfid,
                       'dst_bfid': dst_bfid,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # Clear the migration swapped state for the bfid pair.
    def unset_swapped(self, src_bfid, dst_bfid, timeout=0, retry=0):
        r = self.send({'work': "unset_swapped",
                       'src_bfid': src_bfid,
                       'dst_bfid': dst_bfid,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # Set the migration checked state for the bfid pair.
    def set_checked(self, src_bfid, dst_bfid, timeout=0, retry=0):
        r = self.send({'work': "set_checked",
                       'src_bfid': src_bfid,
                       'dst_bfid': dst_bfid,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # Clear the migration checked state for the bfid pair.
    def unset_checked(self, src_bfid, dst_bfid, timeout=0, retry=0):
        r = self.send({'work': "unset_checked",
                       'src_bfid': src_bfid,
                       'dst_bfid': dst_bfid,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # Set the migration closed state for the bfid pair.
    def set_closed(self, src_bfid, dst_bfid, timeout=0, retry=0):
        r = self.send({'work': "set_closed",
                       'src_bfid': src_bfid,
                       'dst_bfid': dst_bfid,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']
        return r

    # Clear the migration closed state for the bfid pair.
    def unset_closed(self, src_bfid, dst_bfid, timeout=0, retry=0):
        r = self.send({'work': "unset_closed",
                       'src_bfid': src_bfid,
                       'dst_bfid': dst_bfid,
                       }, rcv_timeout=timeout, tries=retry)
        if 'work' in r:
            del r['work']

    # def set_delete(self, ticket):
    #     #Is this really set_deleted or set_delete?
    #     ticket['work'] = "set_deleted"
    #     r = self.send(ticket)
    #     return r

    def mark_bad(self, path, specified_bfid=None,
                 timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        # get the full absolute path
        a_path = os.path.abspath(path)
        dirname, filename = os.path.split(a_path)

        # does it exist?
        if not os.access(path, os.F_OK):
            msg = "%s does not exist!" % (path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # check premission
        if not os.access(dirname, os.W_OK):
            msg = "not enough privilege to rename %s" % (path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # get bfid
        bfid_file = os.path.join(dirname, '.(use)(1)(%s)' % (filename))
        f = open(bfid_file)
        bfid = string.strip(f.readline())
        f.close()

        # Detect if the suplied bfid is a multiple copy of the primary bfid.
        is_multiple_copy = False
        if specified_bfid:
            copy_dict = self.find_all_copies(bfid, timeout, retry)
            if e_errors.is_ok(copy_dict):
                copy_bfids = copy_dict['copies']
            else:
                return copy_dict
            try:
                # Remove the primary bfid from the list.  file_copies()
                # can miss copies of copies, so we don't want to use that.
                del copy_bfids[copy_bfids.index(bfid)]
            except IndexError:
                pass
            # If the bfid is in the list, we have a valid mupltiple copy.
            if specified_bfid in copy_bfids:
                bfid = specified_bfid
                is_multiple_copy = True
            else:
                msg = "%s bfid is not a copy of %s" % (specified_bfid, path)
                return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        if len(bfid) < 12:
            msg = "can not find bfid for %s" % (path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        record = self.bfid_info(bfid, timeout, retry)
        if record['status'][0] != e_errors.OK:
            return record

        if is_multiple_copy:
            bad_file = path
        else:
            bad_file = os.path.join(dirname, ".bad." + filename)
            if os.path.exists(bad_file):
                msg = "Refuse to set file bad because there is already .bad. file {} present ".format(
                    bad_file)
                return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        ticket = {'work': 'mark_bad', 'bfid': bfid, 'path': bad_file}
        ticket = self.send(ticket, rcv_timeout=timeout, tries=retry)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        if not is_multiple_copy:
            try:
                os.rename(a_path, bad_file)
            except BaseException:
                msg = "failed to rename %s to %s" % (a_path, bad_file)
                ticket = {'work': 'unmark_bad', 'bfid': bfid, 'path': bad_file}
                ticket = self.send(ticket, rcv_timeout=timeout, tries=retry)
                if ticket['status'][0] != e_errors.OK:
                    msg += '(Failed to umark the file bad: ' + \
                        ticket['status'][1] + ')'
                return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        print(bfid, a_path, "->", bad_file)
        return ticket

    def unmark_bad(self, path, specified_bfid=None,
                   timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        # get the full absolute path
        a_path = os.path.abspath(path)
        dirname, filename = os.path.split(a_path)

        # does it exist?
        if not os.access(path, os.F_OK):
            msg = "%s does not exist!" % (path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # check premission
        if not os.access(dirname, os.W_OK):
            msg = "not enough privilege to rename %s" % (path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # get bfid
        bfid_file = os.path.join(dirname, '.(use)(1)(%s)' % (filename))
        f = open(bfid_file)
        bfid = string.strip(f.readline())
        f.close()
        if len(bfid) < 12:
            msg = "can not find bfid for %s" % (path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # Detect if the suplied bfid is a multiple copy of the primary bfid.
        is_multiple_copy = False
        if specified_bfid:
            copy_dict = self.find_all_copies(bfid, timeout, retry)
            if e_errors.is_ok(copy_dict):
                copy_bfids = copy_dict['copies']
            else:
                return copy_dict
            try:
                # Remove the primary bfid from the list.  file_copies()
                # can miss copies of copies, so we don't want to use that.
                del copy_bfids[copy_bfids.index(bfid)]
            except IndexError:
                pass
            if specified_bfid in copy_bfids:
                bfid = specified_bfid
                is_multiple_copy = True
            else:
                msg = "%s bfid is not a copy of %s" % (specified_bfid, path)
                return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # is it a "bad" file?
        if filename[:5] != ".bad." and not is_multiple_copy:
            msg = "%s is not officially a bad file" % (path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        record = self.bfid_info(bfid, timeout, retry)
        if record['status'][0] != e_errors.OK:
            return record

        if is_multiple_copy:
            good_file = path
        else:
            good_file = os.path.join(dirname, filename[5:])
            # rename it
            try:
                os.rename(a_path, good_file)
            except BaseException:
                msg = "failed to rename %s to %s" % (a_path, good_file)
                return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # log it
        ticket = {'work': 'unmark_bad', 'bfid': bfid}
        ticket = self.send(ticket, rcv_timeout=timeout, tries=retry)
        if ticket['status'][0] == e_errors.OK:
            print(bfid, a_path, "->", good_file)
        return ticket

    def bfid_info(self, bfid=None, timeout=0, retry=0):
        if not bfid:
            bfid = self.bfid
        r = self.send({"work": "bfid_info",
                       "bfid": bfid}, rcv_timeout=timeout, tries=retry)

        if "work" in r:
            del r['work']

        return r

    # This is only to be used internally
    def exist_bfids(self, bfids=[], timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        if bfids is None:
            bfids = self.bfid
        r = self.send({"work": "exist_bfids",
                       "bfids": bfids}, rcv_timeout=timeout, tries=retry)
        return r['result']

    # This is a retrofit for bfid
    def set_deleted(self, deleted, restore_dir="no", bfid=None,
                    timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        deleted = string.lower(deleted)
        if deleted not in enstore_constants.FILE_DELETED_FLAGS:
            message = "Unsupported delete flag \"%s\", supported flags are " % (
                deleted,)
            for f in enstore_constants.FILE_DELETED_FLAGS:
                message = message + "\"" + f + "\","
            message = message[:-1]
            return {'status': (e_errors.FILE_CLERK_ERROR, message)}
        if bfid is None:
            bfid = self.bfid
        r = self.send({"work": "set_deleted",
                       "bfid": bfid,
                       "deleted": deleted,
                       "restore_dir": restore_dir}, rcv_timeout=timeout, tries=retry)
        return r

    def get_crcs(self, bfid, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        r = self.send({"work": "get_crcs",
                       "bfid": bfid}, rcv_timeout=timeout, tries=retry)
        return r

    def set_crcs(self, bfid, sanity_cookie, complete_crc,
                 timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        r = self.send({"work": "set_crcs",
                       "bfid": bfid,
                       "sanity_cookie": sanity_cookie,
                       "complete_crc": complete_crc}, rcv_timeout=timeout, tries=retry)
        return r

    # delete a volume

    def delete_volume(self, vol, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        r = self.send({"work": "delete_volume",
                       "external_label": vol}, rcv_timeout=timeout, tries=retry)
        return r

    # erase a volume

    def erase_volume(self, vol, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        r = self.send({"work": "erase_volume",
                       "external_label": vol}, rcv_timeout=timeout, tries=retry)
        return r

    # does the volume contain any undeleted file?

    def has_undeleted_file(self, vol, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        r = self.send({"work": "has_undeleted_file",
                       "external_label": vol}, rcv_timeout=timeout, tries=retry)
        return r

    def restore(self, bfid, uid=None, gid=None,
                force=None, timeout=0, retry=0):
        # get the file information from the file clerk
        bit_file = self.bfid_info(bfid, timeout, retry)
        if bit_file['status'][0] != e_errors.OK:
            return bit_file
        del bit_file['status']

        # take care of uid and gid
        if not uid:
            uid = bit_file['uid']
        if not gid:
            gid = bit_file['gid']

        """
	# try its best to set uid and gid
        try:
            os.setregid(gid, gid)
            os.setreuid(uid, uid)
        except:
            pass
        """

        # check if the volume is deleted
        if bit_file["external_label"][-8:] == '.deleted':
            message = "volume %s is deleted" % (bit_file["external_label"],)
            return {'status': (e_errors.FILE_CLERK_ERROR, message)}

        # make sure the file has to be deleted (if --force was specified,
        # allow for the restore to update the file)
        if bit_file['deleted'] != 'yes' and force is None:
            message = "%s is not deleted" % (bfid,)
            return {'status': (e_errors.FILE_CLERK_ERROR, message)}

        # find out file_family
        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        vol = vcc.inquire_vol(bit_file['external_label'])
        if vol['status'][0] != e_errors.OK:
            return vol
        file_family = volume_family.extract_file_family(vol['volume_family'])
        del vcc

        # check if the path is a valid pnfs path
        if not bit_file['pnfs_name0']:
            # We get here if there is no path information.
            message = "no path information found for %s" % (bfid,)
            return {'status': (e_errors.FILE_CLERK_ERROR, message)}
        if not chimera.is_chimera_path(
                bit_file['pnfs_name0'], check_name_only=1):
            message = "%s is not a valid chimera/pnfs path" % (
                bit_file['pnfs_name0'],)
            return {'status': (e_errors.FILE_CLERK_ERROR, message)}

        # its directory has to exist
        p_p, p_f = os.path.split(bit_file['pnfs_name0'])
        p_stat = file_utils.get_stat(p_p)
        rtn_code2 = file_utils.e_access_cmp(p_stat, os.F_OK)
        if not rtn_code2:
            message = "can not write in directory %s" % (p_p,)
            return {'status': (e_errors.FILE_CLERK_ERROR, message)}

        # check if the file has already existed (if --force was specified,
        # allow for the restore to update the file)
        rtn_code = file_utils.e_access(bit_file['pnfs_name0'], os.F_OK)
        if rtn_code and force is None:  # file exists
            message = "%s exists" % (bit_file['pnfs_name0'],)
            return {'status': (e_errors.FILE_CLERK_ERROR, message)}
        if rtn_code and force is not None:
            # check if any file has the same pnfs_id
            pnfs_id = ""
            if chimera.is_chimera_path(
                    bit_file['pnfs_name0'], check_name_only=1):
                pnfs_id = chimera.get_pnfsid(bit_file['pnfs_name0'])
            else:
                message = "file %s is not chimera nor pnfs"\
                          % (bit_file['pnfs_name0'])
                return {'status': (e_errors.FILE_CLERK_ERROR, message)}
            if pnfs_id != bit_file['pnfsid']:
                message = "file pnfs id (%s) does not match database pnfs id (%s)"\
                          % (bit_file['pnfs_name0'], pnfs_id)
                return {'status': (e_errors.FILE_CLERK_ERROR, message)}

        # Setup the File class to do the update.
        bit_file['file_family'] = file_family
        pf = None
        if chimera.is_chimera_path(bit_file['pnfs_name0'], check_name_only=1):
            pf = chimera.File(bit_file)
        else:
            message = "%s is not chimera not pnfs file" % (
                bit_file['pnfs_name0'],)
            return {'status': (e_errors.FILE_CLERK_ERROR, message)}

        # Now create/update it; catch any error
        if not rtn_code:  # DOES NOT EXIST
            # Has it already existed?
            if pf.exists() and force is None:
                message = "%s already exists" % (bit_file['pnfs_name0'],)
                return {'status': (e_errors.FILE_CLERK_ERROR, message)}

            if not pf.exists():
                # We need to wrap this code (when uid == 0) to set the euid and
                # egid to the owner of the directory.  This will allow root
                # to create files in non-admin and non-trusted filesystems.
                #print "os.geteuid():", os.geteuid(), p_p
                file_utils.match_euid_egid(p_p)
                #print "os.geteuid():", os.geteuid(), p_p
                try:
                    pf.create()
                except (OSError, IOError) as msg:
                    message = "can not create: %s" % (str(msg),)
                    return {'status': (e_errors.PNFS_ERROR, message)}
                except BaseException:
                    message = "can not create: %s: %s" % (str(sys.exc_info()[0]),
                                                          str(sys.exc_info()[1]))
                    return {'status': (e_errors.PNFS_ERROR, message)}
                file_utils.set_euid_egid(0, 0)
                file_utils.release_lock_euid_egid()

                # Now that we are back to root, we can change the ownership
                # of the file.
                file_utils.chown(bit_file['pnfs_name0'], uid, gid)

        else:  # DOES EXIST
            file_utils.match_euid_egid(bit_file['pnfs_name0'])
            try:
                pf.update()
                message = ""
            except BaseException:
                message = "can not update %s: %s" % (pf.path,
                                                     sys.exc_info()[1])

            file_utils.set_euid_egid(0, 0)
            file_utils.release_lock_euid_egid()

            if message:
                return {'status': (e_errors.FILE_CLERK_ERROR, message)}

        pnfs_id = pf.get_pnfs_id()
        if pnfs_id != pf.pnfs_id or bit_file['deleted'] != "no":
            # update file record
            return self.modify({'bfid': bfid, 'pnfsid': pnfs_id,
                                'deleted': 'no'})

        return {'status': (e_errors.OK, None)}

    # rebuild pnfs file entry

    def rebuild_pnfs_file(self, bfid, file_family=None,
                          timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        ticket = {"work": "restore_file2",
                  "bfid": bfid,
                  "check": 0}
        if file_family:
            ticket['file_family'] = file_family
        return self.send(ticket, rcv_timeout=timeout, tries=retry)

    # get volume map name for given bfid
    def get_volmap_name(self, bfid=None, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        if not bfid:
            bfid = self.bfid
        r = self.send({"work": "get_volmap_name",
                       "bfid": bfid}, rcv_timeout=timeout, tries=retry)
        return r

    # delete bitfile
    def del_bfid(self, bfid=None, timeout=RCV_TIMEOUT, retry=RCV_TRIES):
        if not bfid:
            bfid = self.bfid
        r = self.send({"work": "del_bfid",
                       "bfid": bfid}, rcv_timeout=timeout, tries=retry)
        return r

    # create file record
    def add(self, ticket, timeout=0, retry=0):
        ticket['work'] = 'add_file_record'
        return self.send(ticket, rcv_timeout=timeout, tries=retry)

    # modify file record
    def modify(self, ticket, timeout=0, retry=0):
        if isinstance(ticket, dict):
            ticket['work'] = 'modify_file_record'
            return self.send(ticket, rcv_timeout=timeout, tries=retry)
        elif isinstance(ticket, list):
            rticket = {}
            rticket["work"] = 'modify_file_records'
            rticket["list"] = ticket
            return self.send(rticket)
        else:
            raise_(
                TypeError,
                "Expect dictionary or list of dictionaries, not %s" %
                (type(ticket)))

    # swap parents for children
    def swap_package(self, ticket, timeout=600, retry=1):
        ticket['work'] = 'swap_package'
        return self.send(ticket, rcv_timeout=timeout, tries=retry)

    def made_copy(self, bfid, timeout=0, retry=0):
        r = self.send({"work": "made_copy", "bfid": bfid},
                      rcv_timeout=timeout, tries=retry)
        return r


class FileClerkClientInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.list = None
        self.bfid = 0
        self.bfids = None
        self.children = None
        self.field = None
        self.backup = 0
        self.deleted = 0
        self.restore = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.get_crcs = None
        self.set_crcs = None
        self.all = 1
        self.ls_active = None
        self.mark_bad = None
        self.unmark_bad = None
        self.show_bad = 0
        self.add = None
        self.modify = None
        self.show_state = None
        self.erase = None
        self.find_copies = None
        self.find_all_copies = None
        self.find_original = None
        self.find_the_original = None
        self.find_duplicates = None
        self.force = None  # use real clerks (True); use info server (False)
        self.package = None  # print package files
        self.replay = None
        self.pkginfo = None

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.file_options)

    file_options = {
        option.ADD: {option.HELP_STRING:
                     "add file record (dangerous! don't try this at home)",
                     option.VALUE_TYPE: option.STRING,
                     option.VALUE_USAGE: option.REQUIRED,
                     option.VALUE_LABEL: "bfid",
                     option.USER_LEVEL: option.ADMIN},
        option.BACKUP: {option.HELP_STRING:
                        "backup file journal -- part of database backup",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_TYPE: option.INTEGER,
                        option.VALUE_USAGE: option.IGNORED,
                        option.USER_LEVEL: option.ADMIN},
        option.BFID: {option.HELP_STRING: "get info of a file",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.USER_LEVEL: option.USER},
        option.BFIDS: {option.HELP_STRING: "list all bfids on a volume",
                       option.VALUE_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.VALUE_LABEL: "volume_name",
                       option.USER_LEVEL: option.ADMIN},
        option.DELETED: {option.HELP_STRING: "used with --bfid to mark the file as deleted",
                         option.DEFAULT_TYPE: option.STRING,
                         option.VALUE_USAGE: option.REQUIRED,
                         option.VALUE_LABEL: "yes/no",
                         option.USER_LEVEL: option.ADMIN},
        option.ERASE: {option.HELP_STRING: "permenantly erase a file",
                       option.VALUE_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.VALUE_LABEL: "bfid",
                       option.USER_LEVEL: option.HIDDEN},
        option.FIND_ALL_COPIES: {option.HELP_STRING: "find all copies of this file",
                                 option.VALUE_TYPE: option.STRING,
                                 option.VALUE_USAGE: option.REQUIRED,
                                 option.VALUE_LABEL: "bfid",
                                 option.USER_LEVEL: option.ADMIN},
        option.GET_CHILDREN: {option.HELP_STRING: "find all children of the package file",
                              option.VALUE_TYPE: option.STRING,
                              option.VALUE_USAGE: option.REQUIRED,
                              option.VALUE_LABEL: "bfid",
                              option.USER_LEVEL: option.ADMIN},
        option.FIELD: {option.HELP_STRING: "used with --children to extract only a particular file record field",
                       option.DEFAULT_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.USER_LEVEL: option.ADMIN},
        option.REPLAY: {option.HELP_STRING: "replay cache written events. If REPLAY presents and is > 1, replay all",
                        option.VALUE_TYPE: option.INTEGER,
                        option.USER_LEVEL: option.ADMIN,
                        option.EXTRA_VALUES: [{
                            option.VALUE_LABEL: "replay",
                            option.VALUE_TYPE: option.INTEGER,
                            option.VALUE_USAGE: option.OPTIONAL,
                            option.DEFAULT_TYPE: option.INTEGER,
                            option.DEFAULT_VALUE: 1
                        }]
                        },

        option.FIND_COPIES: {option.HELP_STRING: "find the immediate copies of this file",
                             option.VALUE_TYPE: option.STRING,
                             option.VALUE_USAGE: option.REQUIRED,
                             option.VALUE_LABEL: "bfid",
                             option.USER_LEVEL: option.ADMIN},
        option.FIND_DUPLICATES: {option.HELP_STRING: "find all duplicates related to this file",
                                 option.VALUE_TYPE: option.STRING,
                                 option.VALUE_USAGE: option.REQUIRED,
                                 option.VALUE_LABEL: "bfid",
                                 option.USER_LEVEL: option.ADMIN},
        option.FIND_ORIGINAL: {option.HELP_STRING: "find the immediate original of this file",
                               option.VALUE_TYPE: option.STRING,
                               option.VALUE_USAGE: option.REQUIRED,
                               option.VALUE_LABEL: "bfid",
                               option.USER_LEVEL: option.ADMIN},
        option.FIND_THE_ORIGINAL: {option.HELP_STRING: "find the very first original of this file",
                                   option.VALUE_TYPE: option.STRING,
                                   option.VALUE_USAGE: option.REQUIRED,
                                   option.VALUE_LABEL: "bfid",
                                   option.USER_LEVEL: option.ADMIN},
        # Additionally, --force can be used to talk to the file clerk and
        # not the info srver.
        option.FORCE: {option.HELP_STRING:
                       "Force restore of file from DB that still exists"
                       " (in some capacity) in PNFS.",
                       option.VALUE_USAGE: option.IGNORED,
                       option.VALUE_TYPE: option.INTEGER,
                       option.USER_LEVEL: option.HIDDEN},
        option.PACKAGE: {option.HELP_STRING:
                         "Force printing package files and non-packaged files",
                         option.VALUE_USAGE: option.IGNORED,
                         option.VALUE_TYPE: option.INTEGER,
                         option.USER_LEVEL: option.ADMIN},
        option.PACKAGE_INFO: {option.HELP_STRING:
                              "Force printing information about package_id archive/cache status",
                              option.VALUE_USAGE: option.IGNORED,
                              option.VALUE_TYPE: option.INTEGER,
                              option.USER_LEVEL: option.ADMIN},
        option.GET_CRCS: {option.HELP_STRING: "get crc of a file",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.VALUE_LABEL: "bfid",
                          option.USER_LEVEL: option.ADMIN},
        option.LIST: {option.HELP_STRING: "list the files in a volume",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.VALUE_LABEL: "volume_name",
                      option.USER_LEVEL: option.USER,
                      },
        option.LS_ACTIVE: {option.HELP_STRING: "list active files in a volume",
                           option.VALUE_TYPE: option.STRING,
                           option.VALUE_USAGE: option.REQUIRED,
                           option.VALUE_LABEL: "volume_name",
                           option.USER_LEVEL: option.USER},
        option.MARK_BAD: {option.HELP_STRING: "Mark the file with the given "
                          "filename as bad.  Include the bfid only if the "
                          "file is a multiple copy file.",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.VALUE_LABEL: "path",
                          option.USER_LEVEL: option.ADMIN,
                          option.EXTRA_VALUES: [{
                              option.VALUE_NAME: "bfid",
                              option.VALUE_LABEL: "bfid",
                              option.VALUE_TYPE: option.STRING,
                              option.VALUE_USAGE: option.OPTIONAL,
                              option.DEFAULT_TYPE: None,
                              option.DEFAULT_VALUE: None,
                          }]
                          },
        option.MODIFY: {option.HELP_STRING:
                        "modify file record (dangerous!)",
                        option.VALUE_TYPE: option.STRING,
                        option.VALUE_USAGE: option.REQUIRED,
                        option.VALUE_LABEL: "bfid",
                        option.USER_LEVEL: option.ADMIN},
        option.RECURSIVE: {option.HELP_STRING: "restore directory",
                           option.DEFAULT_NAME: "restore_dir",
                           option.DEFAULT_VALUE: option.DEFAULT,
                           option.DEFAULT_TYPE: option.INTEGER,
                           option.VALUE_USAGE: option.IGNORED,
                           option.USER_LEVEL: option.ADMIN},
        option.RESTORE: {option.HELP_STRING: "restore a deleted file with optional uid:gid",
                         option.VALUE_TYPE: option.STRING,
                         option.VALUE_USAGE: option.REQUIRED,
                         option.VALUE_LABEL: "bfid",
                         option.USER_LEVEL: option.ADMIN,
                         option.EXTRA_VALUES: [{
                             option.VALUE_NAME: "owner",
                             option.VALUE_LABEL: "uid[:gid]",
                             option.VALUE_TYPE: option.STRING,
                             option.VALUE_USAGE: option.OPTIONAL,
                             option.DEFAULT_TYPE: None,
                             option.DEFAULT_VALUE: None
                         }]
                         },
        option.SET_CRCS: {option.HELP_STRING: "set CRC of a file",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.USER_LEVEL: option.ADMIN},
        option.SHOW_BAD: {option.HELP_STRING: "list all bad files",
                          option.DEFAULT_VALUE: option.DEFAULT,
                          option.DEFAULT_TYPE: option.INTEGER,
                          option.VALUE_USAGE: option.IGNORED,
                          option.USER_LEVEL: option.USER},
        option.SHOW_STATE: {option.HELP_STRING:
                            "show internal state of the server",
                            option.DEFAULT_VALUE: option.DEFAULT,
                            option.DEFAULT_TYPE: option.INTEGER,
                            option.VALUE_USAGE: option.IGNORED,
                            option.USER_LEVEL: option.ADMIN},
        option.UNMARK_BAD: {option.HELP_STRING: "Unmark the file with the given "
                            "filename as bad.  Include the bfid only if the "
                            "file is a multiple copy file.",
                            option.VALUE_TYPE: option.STRING,
                            option.VALUE_USAGE: option.REQUIRED,
                            option.VALUE_LABEL: "path",
                            option.USER_LEVEL: option.ADMIN,
                            option.EXTRA_VALUES: [{
                                option.VALUE_NAME: "bfid",
                                option.VALUE_LABEL: "bfid",
                                option.VALUE_TYPE: option.STRING,
                                option.VALUE_USAGE: option.OPTIONAL,
                                option.DEFAULT_TYPE: None,
                                option.DEFAULT_VALUE: None,
                            }]},
    }


def do_work(intf):
    # now get a file clerk client
    fcc = FileClient(
        (intf.config_host,
         intf.config_port),
        intf.bfid,
        None,
        intf.alive_rcv_timeout,
        intf.alive_retries)
    Trace.init(fcc.get_name(MY_NAME))

    ifc = info_client.infoClient(fcc.csc)

    ticket = fcc.handle_generic_commands(MY_SERVER, intf)
    if ticket:
        pass

    elif intf.backup:
        ticket = fcc.start_backup()
        ticket = fcc.backup()
        ticket = fcc.stop_backup()

    elif intf.show_state:
        ticket = fcc.show_state()
        w = 0
        for i in ticket['state'].keys():
            if len(i) > w:
                w = len(i)
        fmt = "%%%ds = %%s" % (w)
        for i in ticket['state'].keys():
            print(fmt % (i, ticket['state'][i]))

    elif intf.deleted and intf.bfid:
        try:
            if intf.restore_dir:
                do_dir = "yes"
        except AttributeError:
            do_dir = "no"
        ticket = fcc.set_deleted(intf.deleted, do_dir)
        Trace.trace(13, str(ticket))

    elif intf.list:
        if intf.force:
            ticket = fcc.tape_list(intf.list, all_files=(not intf.package))
        else:
            ticket = ifc.tape_list(intf.list, all_files=(not intf.package))
        ifc.print_volume_files(intf.list, ticket, intf.package, intf.pkginfo)
    elif intf.mark_bad:
        ticket = fcc.mark_bad(intf.mark_bad, intf.bfid)

    elif intf.unmark_bad:
        ticket = fcc.unmark_bad(intf.unmark_bad, intf.bfid)

    elif intf.show_bad:
        if intf.force:
            ticket = fcc.show_bad()
        else:
            ticket = ifc.show_bad()
        if ticket['status'][0] == e_errors.OK:
            for f in ticket['bad_files']:
                print(f['label'], f['bfid'], f['size'], f['path'])

    elif intf.ls_active:
        if intf.force:
            ticket = fcc.list_active(intf.ls_active)
        else:
            ticket = ifc.list_active(intf.ls_active)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['active_list']:
                print(i)
    elif intf.children:
        ticket = fcc.get_children(intf.children, intf.field)
        if ticket['status'][0] == e_errors.OK:
            def printer(x): return pprint.pprint(x) if isinstance(
                x, dict) else sys.stdout.write(str(x) + "\n")
            map(printer, ticket["children"])
    elif intf.replay:
        if intf.replay > 1:
            ticket = fcc.replay(all=True)
        else:
            ticket = fcc.replay()
        if ticket['status'][0] == e_errors.OK:
            print("Successfully replayed cache written events")
    elif intf.bfids:
        if intf.force:
            ticket = fcc.get_bfids(intf.bfids)
        else:
            ticket = ifc.get_bfids(intf.bfids)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['bfids']:
                print(i)
            # print `ticket['bfids']`
    elif intf.bfid:
        if intf.force:
            ticket = fcc.bfid_info(intf.bfid)
        else:
            ticket = ifc.bfid_info(intf.bfid)
        if ticket['status'][0] == e_errors.OK:
            # print ticket['fc'] #old encp-file clerk format
            #print ticket['vc']
            status = ticket['status']
            del ticket['status']
            pprint.pprint(ticket)
            ticket['status'] = status
    elif intf.restore:
        uid = None
        gid = None
        if intf.owner:
            owner = string.split(intf.owner, ':')
            uid = int(owner[0])
            if len(owner) > 1:
                gid = int(owner[1])
        ticket = fcc.restore(intf.restore, uid=uid, gid=gid,
                             force=intf.force)

    elif intf.add:
        d = {}
        for s in intf.args:
            k, v = string.split(s, '=')
            try:
                v = en_eval(v)  # numeric args
            except BaseException:
                pass  # yuk...
            d[k] = v
        if intf.add != "None":
            d['bfid'] = intf.add  # bfid
        ticket = fcc.add(d)
        print("bfid =", ticket['bfid'])
    elif intf.modify:
        d = {}
        for s in intf.args:
            k, v = string.split(s, '=')
            if k != 'bfid':  # nice try, can not modify bfid
                try:
                    v = en_eval(v)  # numeric args
                except BaseException:
                    pass  # yuk...
                d[k] = v
        d['bfid'] = intf.modify
        ticket = fcc.modify(d)
        if ticket['status'][0] == e_errors.OK:
            print("bfid =", ticket['bfid'])
    elif intf.find_copies:
        if intf.force:
            ticket = fcc.find_copies(intf.find_copies)
        else:
            ticket = ifc.find_copies(intf.find_copies)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['copies']:
                print(i)
    elif intf.find_all_copies:
        if intf.force:
            ticket = fcc.find_all_copies(intf.find_all_copies)
        else:
            ticket = ifc.find_all_copies(intf.find_all_copies)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['copies']:
                print(i)
    elif intf.find_original:
        if intf.force:
            ticket = fcc.find_original(intf.find_original)
        else:
            ticket = ifc.find_original(intf.find_original)
        if ticket['status'][0] == e_errors.OK:
            print(ticket['original'])
    elif intf.find_the_original:
        if intf.force:
            ticket = fcc.find_the_original(intf.find_the_original)
        else:
            ticket = ifc.find_the_original(intf.find_the_original)
        if ticket['status'][0] == e_errors.OK:
            print(ticket['original'])
    elif intf.find_duplicates:
        if intf.force:
            ticket = fcc.find_duplicates(intf.find_duplicates)
        else:
            ticket = ifc.find_duplicates(intf.find_duplicates)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['copies']:
                print(i)
    elif intf.erase:
        # Make this a hidden option -- this is too dangerous otherwise
        ALLOW_ERASE = True
        if ALLOW_ERASE:
            ticket = fcc.del_bfid(intf.erase)
        else:
            ticket = {}
            ticket['status'] = (e_errors.NOT_SUPPORTED, None)
    elif intf.get_crcs:
        bfid = intf.get_crcs
        ticket = fcc.get_crcs(bfid)
        if ticket['status'][0] == e_errors.OK:
            print("bfid %s: sanity_cookie %s, complete_crc %s" % (repr(bfid), ticket["sanity_cookie"],
                                                                  repr(ticket["complete_crc"])))  # keep L suffix
    elif intf.set_crcs:
        bfid, sanity_size, sanity_crc, complete_crc = string.split(
            intf.set_crcs, ',')
        sanity_crc = en_eval(sanity_crc)
        sanity_size = en_eval(sanity_size)
        complete_crc = en_eval(complete_crc)
        sanity_cookie = (sanity_size, sanity_crc)
        ticket = fcc.set_crcs(bfid, sanity_cookie, complete_crc)
        sanity_cookie = ticket['sanity_cookie']
        complete_crc = ticket['complete_crc']
        print("bfid %s: sanity_cookie %s, complete_crc %s" % (repr(bfid), ticket["sanity_cookie"],
                                                              repr(ticket["complete_crc"])))  # keep L suffix

    else:
        intf.print_help()
        sys.exit(0)

    fcc.check_ticket(ticket)


if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace(6, "fcc called with args %s" % (sys.argv,))

    # fill in interface
    intf = FileClerkClientInterface(user_mode=0)

    do_work(intf)
