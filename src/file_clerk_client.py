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
import backup_client
import udp_client
import callback
import hostaddr
import Trace
import e_errors

MY_NAME = "FILE_C_CLIENT"
MY_SERVER = "file_clerk"

class FileClient(generic_client.GenericClient, 
                      backup_client.BackupClient):

    def __init__( self, csc, bfid=0, server_address=None ):
        generic_client.GenericClient.__init__(self, csc, MY_NAME)
        self.u = udp_client.UDPClient()
	self.bfid = bfid
	if server_address != None:
            self.server_address = server_address
	else:
            self.server_address = self.get_server_address(MY_SERVER)

    def new_bit_file(self, ticket):
        ticket['work'] = "new_bit_file"
        r = self.send(ticket)
        return r

    def set_pnfsid(self, ticket):
        ticket['work'] = "set_pnfsid"
        r = self.send(ticket)
        return r

    def set_delete(self, ticket):
        #Is this really set_deleted or set_delete?
        ticket['work'] = "set_deleted"
        r = self.send(ticket)
        return r

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
        list =''
        while 1:
            msg=callback.read_tcp_raw(data_path_socket)
            if not msg: break
            if list:
                list = list + msg
            else: list = msg
        ticket['bfids'] = string.split(list)
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
        tape_list=''
        while 1:
            msg=callback.read_tcp_raw(data_path_socket)
            if not msg: break
            if tape_list: tape_list=tape_list+msg
            else: tape_list=msg
        ticket['tape_list'] = tape_list
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        return ticket


    def bfid_info(self):
        r = self.send({"work" : "bfid_info",
                       "bfid" : self.bfid } )
        return r

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
        
    # rename volume and volume map
    def rename_volume(self, bfid, external_label, 
		      set_deleted, restore_vm, restore_dir):
        r = self.send({"work"           : "rename_volume",
                       "bfid"           : bfid,
		       "external_label" : external_label,
		       "set_deleted"    : set_deleted,
		       "restore"        : restore_vm,
		       "restore_dir"    : restore_dir } )
	return r

    # rename volume and volume map
    def restore(self, file_name, restore_dir="no"):
        r = self.send({"work"           : "restore_file",
                       "file_name"      : file_name,
		       "restore_dir"    : restore_dir } )
	return r

    # get volume map name for given bfid
    def get_volmap_name(self):
        r = self.send({"work"           : "get_volmap_name",
                       "bfid"           : self.bfid} )
	return r

    # delete bitfile
    def del_bfid(self):
        r = self.send({"work"           : "del_bfid",
                       "bfid"           : self.bfid} )
	return r

class FileClerkClientInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
        # fill in the defaults for the possible options
        self.do_parse = flag
        self.restricted_opts = opts
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
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options()+[
                "bfid=","deleted=","list=","backup",
                "get-crcs=","set-crcs=",
                "restore=", "recursive", "bfids="]
            
def do_work(intf):
    # now get a file clerk client
    fcc = FileClient((intf.config_host, intf.config_port), intf.bfid)
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
            print ticket['tape_list']
    elif intf.bfids:
        ticket  = fcc.get_bfids(intf.bfids)
        if ticket['status'][0] == e_errors.OK:
            print `ticket['bfids']`
    elif intf.bfid:
        ticket = fcc.bfid_info()
	if ticket['status'][0] ==  e_errors.OK:
	    print ticket['fc']
	    print ticket['vc']
    elif intf.restore:
	try:
	    if intf.restore_dir: dir="yes"
	except AttributeError:
	    dir = "no"
	print "file",intf.restore
        ticket = fcc.restore(intf.restore, dir)
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
    intf = FileClerkClientInterface()

    do_work(intf)
