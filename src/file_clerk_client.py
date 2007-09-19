#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import string
import errno
import sys
import socket
import select
import os

import rexec
_rexec = rexec.RExec()

def eval(stuff):
    return _rexec.r_eval(stuff)

# enstore imports
import generic_client
import option
import backup_client
import callback
import hostaddr
import Trace
import e_errors
import pprint
import volume_clerk_client
import volume_family
import pnfs
import info_client
import enstore_constants

MY_NAME = enstore_constants.FILE_CLERK_CLIENT   #"FILE_C_CLIENT"
MY_SERVER = enstore_constants.FILE_CLERK        #"file_clerk"
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

class FileClient(generic_client.GenericClient, 
                      backup_client.BackupClient):

    def __init__( self, csc, bfid=0, server_address=None, flags=0, logc=None,
                  alarmc=None, rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES,
                  #Timeout and tries are for backward compatibility.
                  timeout=None, tries=None):
        ###For backward compatibility.
        if timeout != None:
            rcv_timeout = timeout
        if tries != None:
            rcv_tries = tries
        ###
            
        generic_client.GenericClient.__init__(self,csc,MY_NAME,server_address,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              server_name = MY_SERVER)
	self.bfid = bfid
	#if self.server_address == None:
        #    self.server_address = self.get_server_address(
        #        MY_SERVER, rcv_timeout, rcv_tries)

    # create a bit file using complete metadata -- bypassing all
    def create_bit_file(self, file):
        # file is a structure without bfid
        ticket = {"fc":{}}
        ticket["fc"]["external_label"] = str(file["external_label"])
        ticket["fc"]["location_cookie"] = str(file["location_cookie"])
        ticket["fc"]["size"] = long(file["size"])
        ticket["fc"]["sanity_cookie"] = file["sanity_cookie"]
        ticket["fc"]["complete_crc"]  = long(file["complete_crc"])
        ticket["fc"]["pnfsid"] = str(file["pnfsid"])
        ticket["fc"]["pnfs_name0"] = str(file["pnfs_name0"])
        ticket["fc"]["drive"] = str(file["drive"])
        # handle uid and gid
        if file.has_key("uid"):
            ticket["fc"]["uid"] = file["uid"]
        if file.has_key("gid"):
            ticket["fc"]["gid"] = file["gid"]
        ticket = self.new_bit_file(ticket)
        if ticket["status"][0] == e_errors.OK:
            ticket = self.set_pnfsid(ticket)
        return ticket

    def new_bit_file(self, ticket):
        ticket['work'] = "new_bit_file"
        r = self.send(ticket)
        return r

    def show_state(self):
        return self.send({'work':'show_state'})

    def set_pnfsid(self, ticket):
        ticket['work'] = "set_pnfsid"
        r = self.send(ticket)
        return r

    def get_brand(self, timeout=0, retry=0):
        ticket = {'work': 'get_brand'}
        r = self.send(ticket, timeout, retry)
        if r['status'][0] == e_errors.OK:
            return r['brand']
        else:
            return None

    # find_copies(bfid) -- find the first generation of copies
    def find_copies(self, bfid, timeout=0, retry=0):
        ticket = {'work': 'find_copies',
                  'bfid': bfid}
        return self.send(ticket, timeout, retry)

    # find_all_copies(bfid) -- find all copies from this file
    # This is done on the client side
    def find_all_copies(self, bfid):
        res = self.find_copies(bfid)
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
            ticket = self.send(ticket, timeout, retry)
        else:
            ticket['status'] = (e_errors.OK, None)
        return ticket

    # find_the_original(bfid) -- find the altimate original of this file
    # This is done on the client side
    def find_the_original(self, bfid):
        res = self.find_original(bfid)
        if res['status'][0] == e_errors.OK:
            if res['original']:
                res2 = self.find_the_original(res['original'])
                return res2
            # this is actually the else part
            res['original'] = bfid
        return res

    # find_duplicates(bfid) -- find all original/copies of this file
    # This is done on the client side
    def find_duplicates(self, bfid):
        res = self.find_the_original(bfid)
        if res['status'][0] == e_errors.OK:
            return self.find_all_copies(res['original'])
        return res

    # def set_delete(self, ticket):
    #     #Is this really set_deleted or set_delete?
    #     ticket['work'] = "set_deleted"
    #     r = self.send(ticket)
    #     return r

    def get_bfids(self,external_label):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"          : "get_bfids",
                  "callback_addr" : (host, port),
                  "external_label": external_label}
        # send the work ticket to the file clerk
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r, w, x = select.select([listen_socket], [], [], 60)
        if not r:
            listen_socket.close()
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
        control_socket, address = listen_socket.accept()
        if not hostaddr.allow(address):
            listen_socket.close()
            control_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj(control_socket)
        listen_socket.close()
        
        if ticket["status"][0] != e_errors.OK:
            return ticket
        
        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['file_clerk_callback_addr'])
  
        ticket= callback.read_tcp_obj(data_path_socket)
        list = callback.read_tcp_obj_new(data_path_socket)
        ticket['bfids'] = list
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        return ticket

    def list_active(self,external_label):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"          : "list_active2",
                  "callback_addr" : (host, port),
                  "external_label": external_label}
        # send the work ticket to the file clerk
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r, w, x = select.select([listen_socket], [], [], 60)
        if not r:
            listen_socket.close()
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
        control_socket, address = listen_socket.accept()
        if not hostaddr.allow(address):
            listen_socket.close()
            control_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj(control_socket)
        listen_socket.close()
        
        if ticket["status"][0] != e_errors.OK:
            return ticket
        
        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['file_clerk_callback_addr'])
  
        ticket= callback.read_tcp_obj(data_path_socket)
        list = callback.read_tcp_obj_new(data_path_socket)
        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        ticket['active_list'] = []
        for i in list:
            ticket['active_list'].append(i[0])
        data_path_socket.close()

        return ticket

    def tape_list(self,external_label):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"          : "tape_list2",
                  "callback_addr" : (host, port),
                  "external_label": external_label}
        # send the work ticket to the file clerk
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r, w, x = select.select([listen_socket], [], [], 60)
        if not r:
            listen_socket.close()
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
        control_socket, address = listen_socket.accept()
        if not hostaddr.allow(address):
            listen_socket.close()
            control_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj(control_socket)
        listen_socket.close()
        
        if ticket["status"][0] != e_errors.OK:
            return ticket
        
        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['file_clerk_callback_addr'])
  
        ticket= callback.read_tcp_obj(data_path_socket)
        vol = callback.read_tcp_obj_new(data_path_socket)
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        # convert to external format
        ticket['tape_list'] = []
        for s in vol:
            if s['deleted'] == 'y':
                deleted = 'yes'
            elif s['deleted'] == 'n':
                deleted = 'no'
            else:
                deleted = 'unknown'

            if s['sanity_size'] == -1:
                sanity_size = None
            else:
                sanity_size = s['sanity_size']

            if s['sanity_crc'] == -1:
                sanity_crc = None
            else:
                sanity_crc = s['sanity_crc']

            if s['crc'] == -1:
                crc = None
            else:
                crc = s['crc']

            record = {
                'bfid': s['bfid'],
                'complete_crc': crc,
                'deleted': deleted,
                'drive': s['drive'],
                'external_label': s['label'],
                'location_cookie': s['location_cookie'],
                'pnfs_name0': s['pnfs_path'],
                'pnfsid': s['pnfs_id'],
                'sanity_cookie': (sanity_size, sanity_crc),
                'size': s['size']
            }

            if s.has_key('uid'):
                record['uid'] = s['uid']
            if s.has_key('gid'):
                record['gid'] = s['gid']
            ticket['tape_list'].append(record)

        return ticket

    def mark_bad(self, path):
        # get the full absolute path
        a_path = os.path.abspath(path)
        dir, file = os.path.split(a_path)

	# does it exist?
        if not os.access(path, os.F_OK):
            msg = "%s does not exist!"%(path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # check premission
        if not os.access(dir, os.W_OK):
            msg = "not enough privilege to rename %s"%(path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # get bfid
        bfid_file = os.path.join(dir, '.(use)(1)(%s)'%(file))
        f = open(bfid_file)
        bfid = string.strip(f.readline())
        f.close()

        if len(bfid) < 12:
            msg = "can not find bfid for %s"%(path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        record = self.bfid_info(bfid)
        if record['status'][0] != e_errors.OK:
            return record

        bad_file = os.path.join(dir, ".bad."+file)
	# rename it
        try:
            os.rename(a_path, bad_file)
        except:
            msg = "failed to rename %s to %s"%(a_path, bad_file)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # log it
        ticket = {'work': 'mark_bad', 'bfid': bfid, 'path': bad_file};
        ticket = self.send(ticket)
        if ticket['status'][0] == e_errors.OK:
            print bfid, a_path, "->", bad_file
        return ticket

    def unmark_bad(self, path):
        # get the full absolute path
        a_path = os.path.abspath(path)
        dir, file = os.path.split(a_path)

        # is it a "bad" file?
	if file[:5] != ".bad.":
            msg = "%s is not officially a bad file"%(path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

	# does it exist?
        if not os.access(path, os.F_OK):
            msg = "%s does not exist!"%(path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # check premission
        if not os.access(dir, os.W_OK):
            msg = "not enough privilege to rename %s"%(path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # get bfid
        bfid_file = os.path.join(dir, '.(use)(1)(%s)'%(file))
        f = open(bfid_file)
        bfid = string.strip(f.readline())
        f.close()
        if len(bfid) < 12:
            msg = "can not find bfid for %s"%(path)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        record = self.bfid_info(bfid)
        if record['status'][0] != e_errors.OK:
            return record

        good_file = os.path.join(dir, file[5:])
	# rename it
        try:
            os.rename(a_path, good_file)
        except:
            msg = "failed to rename %s to %s"%(a_path, good_file)
            return {'status': (e_errors.FILE_CLERK_ERROR, msg)}

        # log it
        ticket = {'work': 'unmark_bad', 'bfid': bfid}
        ticket = self.send(ticket)
        if ticket['status'][0] == e_errors.OK:
            print bfid, a_path, "->", good_file
        return ticket


    def show_bad(self):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"          : "show_bad",
                  "callback_addr" : (host, port)}
        # send the work ticket to the file clerk
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r, w, x = select.select([listen_socket], [], [], 60)
        if not r:
            listen_socket.close()
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for file clerk callback"
        control_socket, address = listen_socket.accept()
        if not hostaddr.allow(address):
            listen_socket.close()
            control_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj(control_socket)
        listen_socket.close()
        
        if ticket["status"][0] != e_errors.OK:
            return ticket
        
        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['file_clerk_callback_addr'])
  
        ticket= callback.read_tcp_obj(data_path_socket)
        bad_files = callback.read_tcp_obj_new(data_path_socket)
        ticket['bad_files'] = bad_files
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        return ticket

    def bfid_info(self, bfid = None, timeout=0, retry=0):
        if not bfid:
            bfid = self.bfid
        r = self.send({"work" : "bfid_info",
                       "bfid" : bfid }, timeout, retry)

        if r.has_key("work"):
            del r['work']

        return r

    # This is only to be used internally
    def exist_bfids(self, bfids = []):
        if bfids == None:
            bfids = self.bfid
        r = self.send({"work" : "exist_bfids",
                       "bfids": bfids} )
        return r['result']

    # This is a retrofit for bfid
    def set_deleted(self, deleted, restore_dir="no", bfid = None):
        if bfid == None:
            bfid = self.bfid
        r = self.send({"work"        : "set_deleted",
                       "bfid"        : bfid,
                       "deleted"     : deleted,
		       "restore_dir" : restore_dir } )
        return r


    def get_crcs(self, bfid):
        r = self.send({"work"        : "get_crcs",
                       "bfid"        : bfid})
        return r

    def set_crcs(self, bfid, sanity_cookie, complete_crc):
        r = self.send({"work"        : "set_crcs",
                       "bfid"        : bfid,
                       "sanity_cookie": sanity_cookie,
                       "complete_crc": complete_crc})
        return r
        
    # delete a volume

    def delete_volume(self, vol):
        r = self.send({"work"           : "delete_volume",
		       "external_label" : vol } )
	return r

    # erase a volume

    def erase_volume(self, vol):
        r = self.send({"work"           : "erase_volume",
		       "external_label" : vol } )
	return r

    # does the volume contain any undeleted file?

    def has_undeleted_file(self, vol):
        r = self.send({"work"           : "has_undeleted_file",
		       "external_label" : vol } )
	return r

    def restore(self, bfid, uid = None, gid = None):
        bit_file = self.bfid_info(bfid)
        if bit_file['status'][0] != e_errors.OK:
            return bit_file
        del bit_file['status']

        # take care of uid and gid
        if not uid:
            uid = bit_file['uid']
        if not gid:
            gid = bit_file['gid']

	# try its best to set uid and gid
        try:
            os.setregid(gid, gid)
            os.setreuid(uid, uid)
        except:
            pass

        # check if the volume is deleted
        if bit_file["external_label"][-8:] == '.deleted':
            return {'status': (e_errors.FILE_CLERK_ERROR, "volume %s is deleted"%(bit_file["external_label"]))}

        # make sure the file has to be deleted
        if bit_file['deleted'] != 'yes':
            return {'status': (e_errors.FILE_CLERK_ERROR, "%s is not deleted"%(bfid))}

        # check if the path is a valid pnfs path
        if bit_file['pnfs_name0'][:5] != '/pnfs':
            return {'status': (e_errors.FILE_CLERK_ERROR, "%s is not a valid pnfs path"%(bit_file['pnfs_name0']))}

        # check if the file has already existed
        if os.access(bit_file['pnfs_name0'], os.F_OK): # file exists
            return {'status': (e_errors.FILE_CLERK_ERROR, "%s exists"%(bit_file['pnfs_name0']))}

        # its path has to exist
        pp, pf = os.path.split(bit_file['pnfs_name0'])
        if not os.access(pp, os.W_OK):
            return {'status': (e_errors.FILE_CLERK_ERROR, "can not write in directory %s"%(pp))}

        # find out file_family
        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        vol = vcc.inquire_vol(bit_file['external_label'])
        if vol['status'][0] != e_errors.OK:
            return vol
        file_family = volume_family.extract_file_family(vol['volume_family'])
        del vcc

        bit_file['file_family'] = file_family
        pf = pnfs.File(bit_file)
        # pf.show()

        # Has it already existed?
        if pf.exists():
            return {'status': (e_errors.FILE_CLERK_ERROR, "%s already exists"%(bit_file['pnfs_name0']))}

        # To Do: check if any file has the same pnfs_id

        # Now create it; catch any error
        try:
            pf.create()
        except:
            return {'status': (e_errors.FILE_CLERK_ERROR, "can not create %s"%(pf.path))}

        pnfs_id = pf.get_pnfs_id()
        if pnfs_id != pf.pnfs_id:
            # update file record
            return self.modify({'bfid': bfid, 'pnfsid':pnfs_id, 'deleted':'no'})

        return {'status':(e_errors.OK, None)}

            

    # rebuild pnfs file entry
    def rebuild_pnfs_file(self, bfid, file_family = None):
        ticket = {"work": "restore_file2",
                  "bfid": bfid,
                  "check": 0}
        if file_family:
            ticket['file_family'] = file_family
        return self.send(ticket)

    # get volume map name for given bfid
    def get_volmap_name(self, bfid = None):
        if not bfid:
            bfid = self.bfid
        r = self.send({"work"           : "get_volmap_name",
                       "bfid"           : bfid} )
	return r

    # delete bitfile
    def del_bfid(self, bfid = None):
        if not bfid:
            bfid = self.bfid
        r = self.send({"work"           : "del_bfid",
                       "bfid"           : bfid} )
	return r

    # create file record
    def add(self, ticket):
        ticket['work'] = 'add_file_record'
        return self.send(ticket)

    # modify file record
    def modify(self, ticket):
        ticket['work'] = 'modify_file_record'
        return self.send(ticket)

class FileClerkClientInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.list =None 
        self.bfid = 0
        self.bfids = None
        self.backup = 0
        self.deleted = 0
	self.restore = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.get_crcs=None
        self.set_crcs=None
	self.all = 0
        self.ls_active = None
        self.mark_bad = None
        self.unmark_bad = None
        self.show_bad = 0
        self.add = None
        self.modify = None
        self.show_state = None
        self.dont_try_this_at_home_erase = None
        self.find_copies = None
        self.find_all_copies = None
        self.find_original = None
        self.find_the_original = None
        self.find_duplicates = None

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)


    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.file_options)

    file_options = {
        option.ADD:{option.HELP_STRING:
                    "add file record (dangerous! don't try this at home)",
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"bfid",
                    option.USER_LEVEL:option.ADMIN},
        option.BACKUP:{option.HELP_STRING:
                       "backup file journal -- part of database backup",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        option.BFID:{option.HELP_STRING:"get info of a file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.USER_LEVEL:option.USER},
        option.BFIDS:{option.HELP_STRING:"list all bfids on a volume",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
        option.DELETED:{option.HELP_STRING:"used with --bfid to mark the file as deleted",
                        option.DEFAULT_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"yes/no",
                        option.USER_LEVEL:option.ADMIN},
        option.GET_CRCS:{option.HELP_STRING:"get crc of a file",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"bfid",
                         option.USER_LEVEL:option.ADMIN},
        option.FIND_COPIES:{option.HELP_STRING:"find the immediate copies of this file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"file",
                     option.USER_LEVEL:option.ADMIN},
        option.FIND_ALL_COPIES:{option.HELP_STRING:"find all copies of this file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"file",
                     option.USER_LEVEL:option.ADMIN},
        option.FIND_ORIGINAL:{option.HELP_STRING:"find the immediate original of this file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"file",
                     option.USER_LEVEL:option.ADMIN},
        option.FIND_THE_ORIGINAL:{option.HELP_STRING:"find the very first original of this file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"file",
                     option.USER_LEVEL:option.ADMIN},
        option.FIND_DUPLICATES:{option.HELP_STRING:"find all duplicates related to this file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"file",
                     option.USER_LEVEL:option.ADMIN},
        option.LIST:{option.HELP_STRING:"list the files in a volume",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"volume_name",
                     option.USER_LEVEL:option.USER},
        option.MARK_BAD:{option.HELP_STRING:"mark the file bad",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"path",
                     option.USER_LEVEL:option.ADMIN},
        option.UNMARK_BAD:{option.HELP_STRING:"unmark a bad file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"path",
                     option.USER_LEVEL:option.ADMIN},
        option.SHOW_BAD:{option.HELP_STRING:"list all bad files",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.USER},
        option.SHOW_STATE:{option.HELP_STRING:
                       "show internal state of the server",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        option.LS_ACTIVE:{option.HELP_STRING:"list active files in a volume",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"volume_name",
                          option.USER_LEVEL:option.USER},
        option.MODIFY:{option.HELP_STRING:
                    "modify file record (dangerous!)",
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"bfid",
                    option.USER_LEVEL:option.ADMIN},
        option.RECURSIVE:{option.HELP_STRING:"restore directory",
                          option.DEFAULT_NAME:"restore_dir",
                          option.DEFAULT_VALUE:option.DEFAULT,
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.VALUE_USAGE:option.IGNORED,
                          option.USER_LEVEL:option.ADMIN},
        option.RESTORE:{option.HELP_STRING:"restore a deleted file with optional uid:gid",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"bfid",
                     option.USER_LEVEL:option.ADMIN,
                     option.EXTRA_VALUES:[{
                         option.VALUE_NAME:"owner",
                         option.VALUE_LABEL:"uid[:gid]",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.OPTIONAL,
                         option.DEFAULT_TYPE:None,
                         option.DEFAULT_VALUE:None
                         }]
                     },
        option.SET_CRCS:{option.HELP_STRING:"set CRC of a file",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.USER_LEVEL:option.ADMIN},
        }


def do_work(intf):
    # now get a file clerk client
    fcc = FileClient((intf.config_host, intf.config_port), intf.bfid, None, intf.alive_rcv_timeout, intf.alive_retries)
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
        fmt = "%%%ds = %%s"%(w)
	for i in ticket['state'].keys():
            print fmt%(i, ticket['state'][i])

    elif intf.deleted and intf.bfid:
	try:
	    if intf.restore_dir: dir ="yes"
	except AttributeError:
	    dir = "no"
        ticket = fcc.set_deleted(intf.deleted, dir)
        Trace.trace(13, str(ticket))

    elif intf.list:
        ticket = ifc.tape_list(intf.list)
        if ticket['status'][0] == e_errors.OK:
            format = "%%-%ds %%-20s %%10s %%-22s %%-7s %%s"%(len(intf.list))
            # print "%-8s %-16s %10s %-22s %-7s %s\n"%(
            #    "label", "bfid", "size", "location_cookie", "delflag", "original_name")
            print format%("label", "bfid", "size", "location_cookie", "delflag", "original_name")
            print
            tape = ticket['tape_list']
            for record in tape:
                if record['deleted'] == 'yes':
                    deleted = 'deleted'
                elif record['deleted'] == 'no':
                    deleted = 'active'
                else:
                    deleted = 'unknown'
                # print "%-8s %-16s %10i %-22s %-7s %s" % (intf.list,
                print format % (intf.list,
                    record['bfid'], record['size'],
                    record['location_cookie'], deleted,
                    record['pnfs_name0'])

    elif intf.mark_bad:
        ticket = fcc.mark_bad(intf.mark_bad)

    elif intf.unmark_bad:
        ticket = fcc.unmark_bad(intf.unmark_bad)

    elif intf.show_bad:
        ticket = ifc.show_bad()
        if ticket['status'][0] == e_errors.OK:
            for f in ticket['bad_files']:
                print f['label'], f['bfid'], f['size'], f['path']

    elif intf.ls_active:
        ticket = ifc.list_active(intf.ls_active)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['active_list']:
                print i
    elif intf.bfids:
        ticket  = ifc.get_bfids(intf.bfids)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['bfids']:
                print i
            # print `ticket['bfids']`
    elif intf.bfid:
        ticket = ifc.bfid_info(intf.bfid)
	if ticket['status'][0] ==  e_errors.OK:
	    #print ticket['fc'] #old encp-file clerk format
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
        ticket = fcc.restore(intf.restore, uid=uid, gid=gid)

    elif intf.add:
        d={}
        for s in intf.args:
            k,v=string.split(s,'=')
            try:
                v=eval(v) #numeric args
            except:
                pass #yuk...
            d[k]=v
        if intf.add != "None":
            d['bfid']=intf.add # bfid
        ticket = fcc.add(d)
        print "bfid =", ticket['bfid']
    elif intf.modify:
        d={}
        for s in intf.args:
            k,v=string.split(s,'=')
            if k != 'bfid': # nice try, can not modify bfid
                try:
                    v=eval(v) #numeric args
                except:
                    pass #yuk...
                d[k]=v
        d['bfid']=intf.modify
        ticket = fcc.modify(d)
        if ticket['status'][0] == e_errors.OK:
            print "bfid =", ticket['bfid']
    elif intf.find_copies:
        ticket = fcc.find_copies(intf.find_copies)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['copies']:
                print i
    elif intf.find_all_copies:
        ticket = fcc.find_all_copies(intf.find_all_copies)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['copies']:
                print i
    elif intf.find_original:
        ticket = fcc.find_original(intf.find_original)
        if ticket['status'][0] == e_errors.OK:
            print ticket['original']
    elif intf.find_the_original:
        ticket = fcc.find_the_original(intf.find_the_original)
        if ticket['status'][0] == e_errors.OK:
            print ticket['original']
    elif intf.find_duplicates:
        ticket = fcc.find_duplicates(intf.find_duplicates)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['copies']:
                print i
    elif intf.dont_try_this_at_home_erase:
        # Comment out -- this is too dangerous
        # ticket = fcc.del_bfid(intf.dont_try_this_at_home_erase)
        ticket = {}
        ticket['status'] = (e_errors.OK, None)
    elif intf.get_crcs:
        bfid=intf.get_crcs
        ticket = fcc.get_crcs(bfid)
        print "bfid %s: sanity_cookie %s, complete_crc %s"%(`bfid`,ticket["sanity_cookie"],
                                                 `ticket["complete_crc"]`) #keep L suffix
    elif intf.set_crcs:
        bfid,sanity_size,sanity_crc,complete_crc=string.split(intf.set_crcs,',')
        sanity_crc=eval(sanity_crc)
        sanity_size=eval(sanity_size)
        complete_crc=eval(complete_crc)
        sanity_cookie=(sanity_size,sanity_crc)
        ticket=fcc.set_crcs(bfid,sanity_cookie,complete_crc)
        sanity_cookie = ticket['sanity_cookie']
        complete_crc = ticket['complete_crc']
        print "bfid %s: sanity_cookie %s, complete_crc %s"%(`bfid`,ticket["sanity_cookie"],
                                                            `ticket["complete_crc"]`) #keep L suffix
        
    else:
	intf.print_help()
        sys.exit(0)

    fcc.check_ticket(ticket)


if __name__ == "__main__" :
    Trace.init(MY_NAME)
    Trace.trace(6,"fcc called with args %s"%(sys.argv,))

    # fill in interface
    intf = FileClerkClientInterface(user_mode=0)

    do_work(intf)
