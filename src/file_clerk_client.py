###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import string
import errno
import sys
import socket
import select

import rexec
_rexec = rexec.RExec()

def eval(stuff):
    return _rexec.r_eval(stuff)

# enstore imports
import setpath
import generic_client
import option
import backup_client
import udp_client
import callback
import hostaddr
import Trace
import e_errors
import pprint

MY_NAME = "FILE_C_CLIENT"
MY_SERVER = "file_clerk"
RCV_TIMEOUT = 10
RCV_TRIES = 5

class FileClient(generic_client.GenericClient, 
                      backup_client.BackupClient):

    def __init__( self, csc, bfid=0, server_address=None, timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        generic_client.GenericClient.__init__(self, csc, MY_NAME, server_address)
	self.bfid = bfid
	if self.server_address == None:
            self.server_address = self.get_server_address(MY_SERVER,
                                                          timeout, tries)

    def new_bit_file(self, ticket):
        ticket['work'] = "new_bit_file"
        r = self.send(ticket)
        return r

    def set_pnfsid(self, ticket):
        ticket['work'] = "set_pnfsid"
        r = self.send(ticket)
        return r

    def get_brand(self):
        ticket = {'work': 'get_brand'}
        r = self.send(ticket)
        if r['status'][0] == e_errors.OK:
            return r['brand']
        else:
            return None

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

        r, w, x = select.select([listen_socket], [], [], 15)
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
        ticket = {"work"          : "list_active",
                  "callback_addr" : (host, port),
                  "external_label": external_label}
        # send the work ticket to the file clerk
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r, w, x = select.select([listen_socket], [], [], 15)
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
        ticket['active_list'] = list
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        return ticket

    def tape_list(self,external_label):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"          : "tape_list",
                  "callback_addr" : (host, port),
                  "external_label": external_label}
        # send the work ticket to the file clerk
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r, w, x = select.select([listen_socket], [], [], 15)
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
        ticket['tape_list'] = vol
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        return ticket


    def bfid_info(self, bfid = None):
        if not bfid:
            bfid = self.bfid
        r = self.send({"work" : "bfid_info",
                       "bfid" : bfid } )
        del r['work']
        return r

    # This is only to be used internally
    def exist_bfids(self, bfids = []):
        if bfids == None:
            bfids = self.bfid
        r = self.send({"work" : "exist_bfids",
                       "bfids": bfids} )
        return r['result']

    def set_deleted(self, deleted, restore_dir="no"):
        r = self.send({"work"        : "set_deleted",
                       "bfid"        : self.bfid,
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
        
    # rename a volume

    def rename_volume(self, old, new):
        r = self.send({"work"           : "rename_volume",
		       "external_label" : old,
		       "new_external_label" : new } )
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

    # restore a volume

    def restore_volume(self, vol):
        r = self.send({"work"           : "restore_volume",
		       "external_label" : vol } )
	return r

    # does the volume contain any undeleted file?

    def has_undeleted_file(self, vol):
        r = self.send({"work"           : "has_undeleted_file",
		       "external_label" : vol } )
	return r

    # rename volume and volume map
    def restore2(self, file_name, restore_dir="no"):
        r = self.send({"work"           : "restore_file",
                       "file_name"      : file_name,
		       "restore_dir"    : restore_dir } )
	return r

    # restore a deleted file
    def restore(self, bfid, file_family = None):
        ticket = {"work": "restore_file2",
                  "bfid": bfid}
        if file_family:
            ticket['file_family'] = file_family
        r = self.send(ticket)
	return r

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
        self.add = None
        self.modify = None
        self.dont_try_this_at_home_erase = None
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
        option.LIST:{option.HELP_STRING:"list the files in a volume",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"volume_name",
                     option.USER_LEVEL:option.USER},
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
        option.RESTORE:{option.HELP_STRING:"restore a deleted file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"bfid",
                     option.USER_LEVEL:option.ADMIN,
                     option.EXTRA_VALUES:[{
                         option.VALUE_NAME:"file_family",
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

    ticket = fcc.handle_generic_commands(MY_SERVER, intf)
    if ticket:
        pass

    elif intf.backup:
        ticket = fcc.start_backup()
        ticket = fcc.backup()
        ticket = fcc.stop_backup()

    elif intf.deleted and intf.bfid:
	try:
	    if intf.restore_dir: dir ="yes"
	except AttributeError:
	    dir = "no"
        ticket = fcc.set_deleted(intf.deleted, dir)
        Trace.trace(13, str(ticket))

    elif intf.list:
        ticket = fcc.tape_list(intf.list)
        if ticket['status'][0] == e_errors.OK:
            
            print "     label           bfid       size        location_cookie delflag original_name\n"
            tape = ticket['tape_list']
            # sort it accordinbg to location cookies
            klist = []
            for key in tape.keys():
                klist.append((tape[key]['location_cookie'], tape[key]['bfid']))
            klist.sort()
            for k in klist:
                key = k[1]
                record = tape[key]
                deleted = 'unknown'
                if record.has_key('deleted'):
                    if record['deleted'] == 'yes':
                        deleted = 'deleted'
                    else:
                        deleted = 'active'
                #else:
                #    print `record`
                print "%10s %s %10i %22s %7s %s" % (intf.list,
                    record['bfid'], record['size'],
                    record['location_cookie'], deleted,
                    record['pnfs_name0'])

    elif intf.ls_active:
        ticket = fcc.list_active(intf.ls_active)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['active_list']:
                print i
    elif intf.bfids:
        ticket  = fcc.get_bfids(intf.bfids)
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['bfids']:
                print i
            # print `ticket['bfids']`
    elif intf.bfid:
        ticket = fcc.bfid_info()
	if ticket['status'][0] ==  e_errors.OK:
	    #print ticket['fc'] #old encp-file clerk format
	    #print ticket['vc']
            status = ticket['status']
            del ticket['status']
	    pprint.pprint(ticket)
            ticket['status'] = status
    elif intf.restore:
        if intf.file_family:
            ticket = fcc.restore(intf.restore, intf.file_family)
        else:
            ticket = fcc.restore(intf.restore)
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
        print "bfid =", ticket['bfid']
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
