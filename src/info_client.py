#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system import
import os
import sys
import string
import pprint
import pwd
import socket
import errno
import copy
import select

# enstore import
import generic_client
import option
import time
import callback
import hostaddr
import e_errors
import enstore_constants
import enstore_functions3

MY_NAME = enstore_constants.INFO_CLIENT     #"info_client"
MY_SERVER = enstore_constants.INFO_SERVER   #"info_server"
RCV_TIMEOUT = 10
RCV_TRIES = 1

# union(list_of_sets)
def union(s):
    res = []
    for i in s:
        for j in i:
            if not j in res:
                res.append(j)
    return res

# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time 
def timestamp2time(s):
    if s == '1969-12-31 17:59:59':
        return -1
    else:
        # take care of daylight saving time
        tt = list(time.strptime(s, "%Y-%m-%d %H:%M:%S"))
        tt[-1] = -1
        return time.mktime(tuple(tt))

#turn byte count into a nicely formatted string
def capacity_str(x,mode="GB"):
    if mode == "GB":
        z = x/1024./1024./1024. # GB
        return "%7.2fGB"%(z,)

    x=1.0*x	## make x floating-point
    neg=x<0	## remember the sign of x
    x=abs(x)   ##  make x positive so that "<" comparisons work

    for suffix in ('B ', 'KB', 'MB', 'GB', 'TB', 'PB'):
        if x <= 1024:
            break
        x=x/1024
    if neg:	## if x was negative coming in, restore the - sign  
        x = -x
    return "%6.2f%s"%(x,suffix)

def show_volume_header():
	print "%-16s %9s   %-41s   %-16s %-36s %-12s"%(
		"label", "avail.", "system_inhibit", "library", "volume_family", "comment")

def show_volume(v):
    # pprint.pprint(v)
    si0t = ''
    si1t = ''
    si_time = (timestamp2time(v['si_time_0']), timestamp2time(v['si_time_1']))
    if si_time[0] > 0:
        si0t = time.strftime("%m%d-%H%M",
                             time.localtime(si_time[0]))
    if si_time[1] > 0:
        si1t = time.strftime("%m%d-%H%M",
                             time.localtime(si_time[1]))
    print "%-16s %9s   (%-10s %9s %-8s %9s)   %-16s %-36s"%(
        v['label'], capacity_str(v['remaining_bytes']),
        v['system_inhibit_0'], si0t,
        v['system_inhibit_1'], si1t,
        # v['user_inhibit_0'],v['user_inhibit_1'],
        v['library'],
        v['storage_group']+'.'+v['file_family']+'.'+v['wrapper']),
    if v['comment']:
        print v['comment']
    else:
        print

# bfid, storage_group, library, media_type, volume,
# location_cookie, size, crc, pnfsid, pnfs_path
show_file_format = "%20s %8s %8s %8s %8s %22s %7s %12d %12d %20s %s"
def show_file(f, verbose=0):
    print show_file_format % (
        f['bfid'],
        f['storage_group'],
        f['library'],
        f['media_type'],
        f['external_label'],
        f['location_cookie'],
        f['deleted'],
        f['size'],
        f['complete_crc'],
        f['pnfsid'],
        f['pnfs_name0'])

class fileInfoMethods(generic_client.GenericClient):
    def __init__(self, csc, name, server_address=None, flags=0, logc=None,
                 alarmc=None, server_name=None,
                 rcv_timeout = generic_client.DEFAULT_TIMEOUT,
                 rcv_tries = generic_client.DEFAULT_TRIES):
        generic_client.GenericClient.__init__(self, csc, name,
                                              server_address = server_address,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc,
                                              server_name = server_name,
                                              rcv_timeout = rcv_timeout,
                                              rcv_tries = rcv_tries)

    #Use an active protocol model for receiving large amounts of information.
    #This function uses UDP and TCP.
    #
    #There is an issue with this method.  If the initial server response over
    # UDP is dropped, the server will wait (by default 1 minute) for
    # the tcp negotiation to complete.  The server will queue up requests
    # (including retries from the dropped request) and will hopefully and
    # eventually fulfill them all.  But this is not optimal.
    def send_with_long_answer(self, ticket):

        # send the work ticket to the server
        ticket = self.send(ticket, 60, 1)
        if not e_errors.is_ok(ticket) and \
               ticket['status'][0] not in [e_errors.TOO_MANY_FILES,
                                           e_errors.TOO_MANY_VOLUMES]:
            return ticket

        try:
            #This should be made non-blocking to control the timeout period.
            #That would allow for a better retry mechinism.
            control_socket = socket.socket(socket.AF_INET,
                                           socket.SOCK_STREAM)
            control_socket.connect(ticket['callback_addr'])
        except (socket.error), msg:
            message = "failed to establish control socket: %s" % (str(msg),)
            raise callback.TCPError(msg.errno, message)

        #Read the data.
        try:
            ticket = callback.read_tcp_obj_new(control_socket)
        except (socket.error, callback.TCPError):
            control_socket.close()
            raise sys.exc_info()[0], sys.exc_info()[1], \
                  sys.exc_info()[2]  

        #Socket cleanup.
        control_socket.close()

        return ticket

    ###################################################################
    ## Begin file clerk functions.
    
    def bfid_info(self, bfid, timeout=0, retry=0):
        r = self.send({"work" : "bfid_info", "bfid" : bfid}, timeout, retry)
        if r.has_key('work'):
            del r['work']
        return r

    # find_copies(bfid) -- find the first generation of copies
    def find_copies(self, bfid, timeout=0, retry=0):
        ticket = {'work': 'find_copies', 'bfid': bfid}
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

    # get all pairs of bfids relating to migration/duplication of
    # the specified bfid
    def find_migrated(self, bfid):
        r = self.send({"work" : "find_migrated", "bfid" : bfid})
        if r.has_key('work'):
            del r['work']
        return r

    def get_bfids(self, external_label):
        ticket = {"work"          : "get_bfids2",
                  #"callback_addr" : (host, port),
                  "external_label": external_label}
        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.get_bfids_old(external_label)
            return done_ticket #Avoid duplicate "convert to external format"
        if not e_errors.is_ok(done_ticket):
            return done_ticket

        return done_ticket

    ### For backward compatiblility with old servers.  (2-19-2009)
    def get_bfids_old(self, external_label):
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
        data_path_socket.connect(ticket['info_clerk_callback_addr'])

        ticket= callback.read_tcp_obj(data_path_socket)
        bfid_list = callback.read_tcp_obj_new(data_path_socket)
        ticket['bfids'] = bfid_list
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        return ticket

    def list_active(self, external_label):
        ticket = {"work"           : "list_active3",
                  #"callback_addr"  : (host, port),
                  "external_label" : external_label}

        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.list_active_old(external_label)
            return done_ticket #Avoid duplicate "convert to external format"
        if not e_errors.is_ok(done_ticket):
            return done_ticket
        
        # convert to external format
        active_list = copy.copy(done_ticket['active_list'])
        done_ticket['active_list'] = []
        for i in active_list:
            done_ticket['active_list'].append(i[0])

        return done_ticket

    ### For backward compatiblility with old servers.  (2-19-2009)
    def list_active_old(self, external_label):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"           : "list_active2",
                  "callback_addr"  : (host, port),
                  "external_label" : external_label}
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
        data_path_socket.connect(ticket['info_clerk_callback_addr'])

        ticket= callback.read_tcp_obj(data_path_socket)
        active_list = callback.read_tcp_obj_new(data_path_socket)
        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        # convert to external format
        ticket['active_list'] = []
        for i in active_list:
            ticket['active_list'].append(i[0])
        data_path_socket.close()

        return ticket

    def tape_list(self, external_label):
        ticket = {"work"           : "tape_list3",
                  #"callback_addr"  : (host, port),
                  "external_label" : external_label}
        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.tape_list_old(external_label)
            return done_ticket #Avoid duplicate "convert to external format"
        if not e_errors.is_ok(done_ticket):
            return done_ticket
        
        return done_ticket


        # convert to external format
        vol = copy.copy(done_ticket['tape_list'])
        done_ticket['tape_list'] = []
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
            done_ticket['tape_list'].append(record)

        return done_ticket

    ### For backward compatiblility with old servers.  (2-19-2009)
    def tape_list_old(self, external_label):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"           : "tape_list2",
                  "callback_addr"  : (host, port),
                  "external_label" : external_label}
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
        data_path_socket.connect(ticket['info_clerk_callback_addr'])

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

    def show_bad(self):
        ticket = {"work"          : "show_bad2",
                  #"callback_addr" : (host, port),
                  }
        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.show_bad_old()
        if not e_errors.is_ok(done_ticket):
            return done_ticket
        
        return done_ticket

    ### For backward compatiblility with old servers.  (2-19-2009)
    def show_bad_old(self):
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
        data_path_socket.connect(ticket['info_clerk_callback_addr'])

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

    ## End file clerk functions.
    ###################################################################

class volumeInfoMethods(generic_client.GenericClient):
    def __init__(self, csc, name, server_address=None, flags=0, logc=None,
                 alarmc=None, server_name = None,
                 rcv_timeout = generic_client.DEFAULT_TIMEOUT,
                 rcv_tries = generic_client.DEFAULT_TRIES):
        generic_client.GenericClient.__init__(self, csc, name,
                                              server_address = server_address,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc,
                                              server_name = server_name,
                                              rcv_timeout = rcv_timeout,
                                              rcv_tries = rcv_tries)

    #Use an active protocol model for receiving large amounts of information.
    #This function uses UDP and TCP.
    #
    #There is an issue with this method.  If the initial server response over
    # UDP is dropped, the server will wait (by default 1 minute) for
    # the tcp negotiation to complete.  The server will queue up requests
    # (including retries from the dropped request) and will hopefully and
    # eventually fulfill them all.  But this is not optimal.
    def send_with_long_answer(self, ticket):

        # send the work ticket to the server
        ticket = self.send(ticket, 60, 1)
        if not e_errors.is_ok(ticket) and \
               ticket['status'][0] not in [e_errors.TOO_MANY_FILES,
                                           e_errors.TOO_MANY_VOLUMES]:
            return ticket

        try:
            #This should be made non-blocking to control the timeout period.
            #That would allow for a better retry mechinism.
            control_socket = socket.socket(socket.AF_INET,
                                           socket.SOCK_STREAM)
            control_socket.connect(ticket['callback_addr'])
        except (socket.error), msg:
            message = "failed to establish control socket: %s" % (str(msg),)
            raise callback.TCPError(msg.errno, message)

        #Read the data.
        try:
            ticket = callback.read_tcp_obj_new(control_socket)
        except (socket.error, callback.TCPError):
            control_socket.close()
            raise sys.exc_info()[0], sys.exc_info()[1], \
                  sys.exc_info()[2]  

        #Socket cleanup.
        control_socket.close()

        return ticket

    ###################################################################
    ## Begin volume clerk functions.

    def inquire_vol(self, external_label, timeout=60, retry=10):
        ticket= { 'work': 'inquire_vol',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # get a list of all volumes
    def get_vols(self, key=None, state=None, not_cond=None):
        ticket = {"work"          : "get_vols3",
                  #"callback_addr" : (host, port),
                  "key"           : key,
                  "in_state"      : state,
                  "not"	          : not_cond,
                  }

        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.get_vols_old()
        if not e_errors.is_ok(done_ticket):
            return done_ticket

        return done_ticket

    # get a list of all volumes
    ### For backward compatiblility with old servers.  (2-19-2009)
    def get_vols_old(self, key=None, state=None, not_cond=None):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"		  : "get_vols2",
                  "callback_addr" : (host, port),
                  "key"		   : key,
                  "in_state"	  : state,
                  "not"		   : not_cond}

        # send the work ticket to the library manager
        ticket = self.send(ticket, 60, 1)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r,w,x = select.select([listen_socket], [], [], 60)
        if not r:
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for info clerk callback"

        control_socket, address = listen_socket.accept()

        if not hostaddr.allow(address):
            control_socket.close()
            listen_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj(control_socket)

        listen_socket.close()

        if ticket["status"][0] != e_errors.OK:
            return ticket

        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['info_clerk_callback_addr'])
        ticket= callback.read_tcp_obj(data_path_socket)
        volumes = callback.read_tcp_obj_new(data_path_socket)
        data_path_socket.close()

        """
        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        if volumes.has_key("header"):		# full info
            if print_list:
                show_volume_header()
                print
                for v in volumes["volumes"]:
                    show_volume(v)
        else:
            vlist = ''
            for v in volumes.get("volumes",[]):
                vlist = vlist+v['label']+" "
            if print_list:
                print vlist
        """

        ticket['volumes'] = volumes.get('volumes',[])
        ticket['header'] = volumes.get('header', None)
        return ticket

    # get a list of all problem volumes
    def get_pvols(self):
        ticket = {"work"          : "get_pvols2",
                  #"callback_addr" : (host, port),
                  }

        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.get_pvols_old()
        if not e_errors.is_ok(done_ticket):
            return done_ticket

        return done_ticket

    # get a list of all problem volumes
    ### For backward compatiblility with old servers.  (2-19-2009)
    def get_pvols_old(self):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"		: "get_pvols",
                  "callback_addr"	: (host, port)}

        # send the work ticket to the library manager
        ticket = self.send(ticket, 60, 1)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r,w,x = select.select([listen_socket], [], [], 60)
        if not r:
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for info clerk callback"

        control_socket, address = listen_socket.accept()

        if not hostaddr.allow(address):
            control_socket.close()
            listen_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj(control_socket)

        listen_socket.close()

        if ticket["status"][0] != e_errors.OK:
            return ticket

        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['info_clerk_callback_addr'])
        ticket= callback.read_tcp_obj(data_path_socket)
        volumes = callback.read_tcp_obj_new(data_path_socket)
        data_path_socket.close()


        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        ticket['volumes'] = volumes.get('volumes',[])
        return ticket

    def get_sg_count(self, lib, sg, timeout=60, retry=10):
        ticket = {'work':'get_sg_count',
                  'library': lib,
                  'storage_group': sg}
        return(self.send(ticket,timeout,retry))

    # get a list of all volumes
    def list_sg_count(self):
        ticket = {"work"          : "list_sg_count2",
                  #"callback_addr" : (host, port),
                  }

        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.list_sg_count_old()
        if not e_errors.is_ok(done_ticket):
            return done_ticket

        return done_ticket

    # list all sg counts
    ### For backward compatiblility with old servers.  (2-19-2009)
    def list_sg_count_old(self):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"		  : "list_sg_count",
                          "callback_addr" : (host, port)}

        ticket = self.send(ticket,60,1)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r,w,x = select.select([listen_socket], [], [], 60)
        if not r:
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for volume clerk callback"

        control_socket, address = listen_socket.accept()

        if not hostaddr.allow(address):
            control_socket.close()
            listen_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj(control_socket)

        listen_socket.close()

        if ticket["status"][0] != e_errors.OK:
            return ticket

        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['info_clerk_callback_addr'])
        ticket= callback.read_tcp_obj(data_path_socket)
        sgcnt = callback.read_tcp_obj_new(data_path_socket)
        data_path_socket.close()

        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        ticket['sgcnt'] = sgcnt
        return ticket

    # get a list of all volumes
    def get_vol_list(self):
        ticket = {"work"          : "get_vol_list2",
                  #"callback_addr" : (host, port),
                  }

        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.get_vol_list_old()
        if not e_errors.is_ok(done_ticket):
            return done_ticket

        return done_ticket

    # get a list of all volumes
    ### For backward compatiblility with old servers.  (2-19-2009)
    def get_vol_list_old(self):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"	  : "get_vol_list",
                  "callback_addr" : (host, port)}

        # send the work ticket to the library manager
        ticket = self.send(ticket,60,1)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r,w,x = select.select([listen_socket], [], [], 60)
        if not r:
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for volume clerk callback"

        control_socket, address = listen_socket.accept()

        if not hostaddr.allow(address):
            control_socket.close()
            listen_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj(control_socket)

        listen_socket.close()

        if ticket["status"][0] != e_errors.OK:
            return ticket

        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['info_clerk_callback_addr'])
        ticket= callback.read_tcp_obj(data_path_socket)
        volumes = callback.read_tcp_obj_new(data_path_socket)
        data_path_socket.close()

        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        ticket['volumes'] = volumes
        return ticket

    # show_history
    def show_history(self, vol):
        ticket = {"work"          : "history2",
                  "external_label" : vol,
                  #"callback_addr" : (host, port),
                  }

        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.show_history_old(vol)
        if not e_errors.is_ok(done_ticket):
            return done_ticket

        return done_ticket

    # show_history
    ### For backward compatiblility with old servers.  (2-19-2009)
    def show_history_old(self, vol):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"           : "history",
                  "external_label" : vol,
                  "callback_addr"  : (host, port)}

        # send the work ticket to volume clerk
        ticket = self.send(ticket, 10, 1)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r, w, x = select.select([listen_socket], [], [], 60)
        if not r:
            listen_socket.close()
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for volume clerk callback"

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
        data_path_socket.connect(ticket['info_clerk_callback_addr'])
        ticket = callback.read_tcp_obj(data_path_socket)
        res = callback.read_tcp_obj_new(data_path_socket)
        ticket['history'] = res

        data_path_socket.close()

        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        return ticket

    def write_protect_status(self, vol):
        ticket = {"work"           : "write_protect_status",
                  "external_label" : vol}
        return self.send(ticket)

    ## End volume clerk functions.
    ###################################################################

class infoClient(fileInfoMethods, volumeInfoMethods):
    def __init__(self, csc, logname='UNKNOWN', rcv_timeout = RCV_TIMEOUT,
                 rcv_tries = RCV_TRIES, flags=0, logc=None, alarmc=None,
                 server_address = None):

        self.logname = logname
        self.node = os.uname()[1]
        self.pid = os.getpid()
        #generic_client.GenericClient.__init__(self, csc, MY_NAME,
        #                                      server_address,
        #                                      flags=flags, logc=logc,
        #                                      alarmc=alarmc,
        #                                      server_name = MY_SERVER)
        fileInfoMethods.__init__(self, csc, MY_NAME,
                                 server_address = server_address,
                                 flags=flags, logc=logc,
                                 alarmc=alarmc,
                                 server_name = MY_SERVER)
        volumeInfoMethods.__init__(self, csc, MY_NAME,
                                   server_address = server_address,
                                   flags=flags, logc=logc,
                                   alarmc=alarmc,
                                   server_name = MY_SERVER)
        
        try:
            self.uid = pwd.getpwuid(os.getuid())[0]
        except:
            self.uid = "unknown"
        self.rcv_timeout = rcv_timeout
        self.rcv_tries = rcv_tries

    # send_no_wait
    def send2(self, ticket):
        if not self.server_address: return
        self.u.send_no_wait(ticket, self.server_address)

    # generic test
    def hello(self):
        if not self.server_address: return
        ticket = {'work': 'hello'}
        return self.send(ticket, 30, 1)

    # generic test for send_no_wait
    def hello2(self):
        if not self.server_address: return
        ticket = {'work': 'hello'}
        return self.send2(ticket)

    def debug(self, level = 0):
        ticket = {
            'work': 'debugging',
            'level': level}
        self.send2(ticket)

    def debug_on(self):
        self.debug(1)

    def debug_off(self):
        self.debug(0)

    ###################################################################
    ## Begin info server functions.

    def file_info(self, bfid):
        ticket = {"work" : "file_info",
                  "bfid" : bfid}
        r = self.send(ticket)
        return r
    
    def find_file_by_path(self, pnfs_name0):
        ticket = {"work" : "find_file_by_path2",
                  "pnfs_name0" : pnfs_name0}
        r = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if r['status'][0] == e_errors.KEYERROR and \
               r['status'][1].startswith("cannot find requested function"):
            r = self.find_file_by_path_old(pnfs_name0)
        
        if r.has_key('work'):
            del r['work']
        return r

    ### For backward compatiblility with old servers.  (2-19-2009)
    def find_file_by_path_old(self, pnfs_name0):
        ticket = {"work" : "find_file_by_path",
                  "pnfs_name0" : pnfs_name0}
        r = self.send(ticket)
        if r.has_key('work'):
            del r['work']
        return r

    def find_file_by_pnfsid(self, pnfsid):
        ticket = {"work" : "find_file_by_pnfsid2", "pnfsid" : pnfsid}
        r = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if r['status'][0] == e_errors.KEYERROR and \
               r['status'][1].startswith("cannot find requested function"):
            r = self.find_file_by_pnfsid_old(pnfsid)
        
        if r.has_key('work'):
            del r['work']
        return r

    ### For backward compatiblility with old servers.  (2-19-2009)
    def find_file_by_pnfsid_old(self, pnfsid):
        ticket = {"work" : "find_file_by_pnfsid",
                  "pnfsid" : pnfsid}
        r = self.send(ticket)
        if r.has_key('work'):
            del r['work']
        return r

    def find_file_by_location(self, vol, loc):
        ticket = {"work" : "find_file_by_location2",
                  "external_label" : vol, "location_cookie" : loc}
        r = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if r['status'][0] == e_errors.KEYERROR and \
               r['status'][1].startswith("cannot find requested function"):
            r = self.find_file_by_location_old(vol, loc)
        
        if r.has_key('work'):
            del r['work']
        return r

    ### For backward compatiblility with old servers.  (2-19-2009)
    def find_file_by_location_old(self, vol, loc):
        ticket = {"work" : "find_file_by_location",
                  "external_label" : vol,
                  "location_cookie" : loc}
        r = self.send(ticket)
        if r.has_key('work'):
            del r['work']
        return r

    def find_same_file(self, bfid):
        ticket = {"work": "find_same_file2",
                  "bfid": bfid}
        done_ticket = self.send_with_long_answer(ticket)

        #Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
               done_ticket['status'][1].startswith("cannot find requested function"):
            return self.find_same_file_old(bfid)
            
        return done_ticket

    ### For backward compatiblility with old servers.  (2-19-2009)
    def find_same_file_old(self, bfid):
        return self.send({"work": "find_same_file", "bfid": bfid})

    def query_db(self, q):
        ticket = {"work"          : "query_db2",
                  "query"         : q,
                  #"callback_addr" : (host, port),
                  }
        return self.send_with_long_answer(ticket)
        

    ### For backward compatiblility with old servers.  (2-19-2009)
    def query_db_old(self, q):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"          : "query_db",
                  "query"         : q,
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
        data_path_socket.connect(ticket['info_clerk_callback_addr'])

        ticket= callback.read_tcp_obj(data_path_socket)
        result = callback.read_tcp_obj_new(data_path_socket)
        ticket['result'] = result
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket
        return ticket

    ## End info server functions.
    ###################################################################

def show_query_result(result):
    width = []
    w = len(result['fields'])
    for i in range(w):
        width.append(len(result['fields'][i]))

    for r in result['result']:
        for i in range(w):
            l1 = len(str(r[i]))
            if l1 > width[i]:
                width[i] = l1

    output_format = []
    for i in range(w):
        output_format.append("%%%ds "%(width[i]))

    ll = 0
    for i in range(w):
        ll = ll + width[i]
    ll = ll + 2*(w - 1)

    for i in range(w):
        print output_format[i]%(result['fields'][i]),
    print
    print "-"*ll
    for r in result['result']:
        for i in range(w):
            print output_format[i]%(r[i]),
        print

class InfoClientInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.list =None 
        self.bfid = 0
        self.bfids = None
        self.check = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.ls_active = None
        self.vols = 0
        self.pvols = None
        self.gvol = None
        self.ls_sg_count = 0
        self.get_sg_count = None
        self.vol = None
        self.ls_sg_count = 0
        self.just = None
        self.labels = None
        self.history = None
        self.write_protect_status = None
        self.show_bad = 0
        self.query = ''
        self.find_same_file = None
        self.file = None
        self.show_file = None
        self.show_copies = None
        
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)
        
        
    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.info_options)

    info_options = {
            option.BFID:{option.HELP_STRING:"get info of a file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.USER_LEVEL:option.USER},
             option.BFIDS:{option.HELP_STRING:"list all bfids on a volume",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"volume_name",
                            option.USER_LEVEL:option.ADMIN},
            option.CHECK:{option.HELP_STRING:"check a volume",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"volume_name",
                            option.USER_LEVEL:option.ADMIN},
            option.FIND_SAME_FILE:{option.HELP_STRING:"find a file of the same size and crc",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_LABEL: "bfid",
                            option.VALUE_USAGE:option.REQUIRED,
                            option.USER_LEVEL:option.ADMIN},
            option.FILE:{option.HELP_STRING:"get info of a file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"path|pnfsid|bfid|vol:loc",
                            option.USER_LEVEL:option.USER},
            option.GET_SG_COUNT:{
                            option.HELP_STRING: 'check allocated count for lib,sg',
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"library",
                            option.USER_LEVEL:option.ADMIN,
                            option.EXTRA_VALUES:[{
                                    option.VALUE_NAME:"storage_group",
                                    option.VALUE_LABEL:"storage_group",
                                    option.VALUE_TYPE:option.STRING,
                                    option.VALUE_USAGE:option.REQUIRED}]},
            option.GVOL:{option.HELP_STRING:"get info of a volume in human readable time format",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"volume_name",
                            option.USER_LEVEL:option.USER},
            option.HISTORY:{option.HELP_STRING:"show state change history of volume",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"volume_name",
                            option.USER_LEVEL:option.ADMIN},
            option.JUST:{option.HELP_STRING:"used with --pvols to list problem",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.USER},
            option.LABELS:{
                            option.HELP_STRING:"list all volume labels",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.ADMIN},
            option.LIST:{option.HELP_STRING:"list the files in a volume",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"volume_name",
                            option.USER_LEVEL:option.USER},
            option.LIST_SG_COUNT:{
                            option.HELP_STRING:"list all sg counts",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.USER},
            option.LS_ACTIVE:{option.HELP_STRING:"list active files in a volume",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"volume_name",
                            option.USER_LEVEL:option.USER},
            option.QUERY:{option.HELP_STRING:"query database",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"query",
                            option.USER_LEVEL:option.ADMIN},
            option.PVOLS:{option.HELP_STRING:"list all problem volumes",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.USER},
            option.SHOW_BAD:{option.HELP_STRING:"list all bad files",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.USER},
            option.SHOW_COPIES:{option.HELP_STRING:"all copies of a file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"bfid",
                            option.USER_LEVEL:option.USER},
            option.SHOW_FILE:{option.HELP_STRING:"show info of a file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"bfid",
                            option.USER_LEVEL:option.USER},
            option.VOL:{option.HELP_STRING:"get info of a volume",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"volume_name",
                            option.USER_LEVEL:option.USER},
            
            option.VOLS:{option.HELP_STRING:"list all volumes",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.USER},
            option.WRITE_PROTECT_STATUS:{option.HELP_STRING:"show write protect status",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"volume_name",
                            option.USER_LEVEL:option.ADMIN},
            
            }

def do_work(intf):
    # now get a info client
    ifc = infoClient((intf.config_host, intf.config_port), None, intf.alive_rcv_timeout, intf.alive_retries)

    ticket = ifc.handle_generic_commands(MY_SERVER, intf)
    if ticket:
        pass

    elif intf.list:
        ticket = ifc.tape_list(intf.list)
        if ticket['status'][0] == e_errors.OK:
            output_format = "%%-%ds %%-20s %%10s %%-22s %%-7s %%s"%(len(intf.list))
            print output_format%("label", "bfid", "size", "location_cookie", "delflag", "original_name")
            print
            tape = ticket['tape_list']
            for record in tape:
                if record['deleted'] == 'yes':
                    deleted = 'deleted'
                elif record['deleted'] == 'no':
                    deleted = 'active'
                else:
                    deleted = 'unknown'
                print output_format % (intf.list,
                                       record['bfid'], record['size'],
                                       record['location_cookie'], deleted,
                                       record['pnfs_name0'])
    elif intf.file:
        # is it vol:loc?
        try:
            vol, loc = string.split(intf.file, ":")
        except ValueError:
            vol, loc = "", "" #no colon found
        if enstore_functions3.is_volume(vol) and \
           enstore_functions3.is_location_cookie(loc):
            ticket = ifc.find_file_by_location(vol, loc)
        elif enstore_functions3.is_pnfsid(intf.file):
            #Make sure that pnfsids go before bfids.  A non-branded
            # bfid can look like a pnfsid.  The obvious difference
            # is the length (pnfs ids are longer), but since the
            # length of bfids change over time it is harder to
            # use length in regular expressions.
            ticket = ifc.find_file_by_pnfsid(intf.file)
        elif enstore_functions3.is_bfid(intf.file):
            ticket = ifc.bfid_info(intf.file)
        else:
            ticket = ifc.find_file_by_path(intf.file)
        if ticket['status'][0] ==  e_errors.OK:
            status = ticket['status']
            del ticket['status']
            pprint.pprint(ticket)
            ticket['status'] = status
        if ticket['status'][0] == e_errors.TOO_MANY_FILES:
            for sub_ticket in ticket['file_list']:
                pprint.pprint(sub_ticket)

    elif intf.show_file:
        ticket = ifc.file_info(intf.show_file)
        if e_errors.is_ok(ticket):
            show_file(ticket['file_info'])

    elif intf.show_copies:
        ticket = ifc.find_all_copies(intf.show_copies)
        for i in ticket["copies"]:
            ticket = ifc.file_info(i)
            if e_errors.is_ok(ticket):
                show_file(ticket['file_info'])

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
            status = ticket['status']
            del ticket['status']
            pprint.pprint(ticket)
            ticket['status'] = status
    elif intf.find_same_file:
        ticket = ifc.find_same_file(intf.find_same_file)
        if ticket['status'][0] ==  e_errors.OK:

            print "%10s %20s %10s %22s %7s %s" % (
            "label", "bfid", "size", "location_cookie", "delflag", "original path")
            for record in ticket['files']:
                deleted = 'unknown'
                if record.has_key('deleted'):
                    if record['deleted'] == 'yes':
                        deleted = 'deleted'
                    elif record['deleted'] == 'no':
                        deleted = 'active'

                print "%10s %20s %10d %22s %7s %s" % (
                        record['external_label'],
                        record['bfid'], record['size'],
                        record['location_cookie'], deleted,
                        record['pnfs_name0'])

    elif intf.check:
        ticket = ifc.inquire_vol(intf.check)
        # guard against error
        if ticket['status'][0] == e_errors.OK:
            print "%-10s  %s %s %s" % (ticket['external_label'],
                                       capacity_str(ticket['remaining_bytes']),
                                       ticket['system_inhibit'],
                                       ticket['user_inhibit'])
    elif intf.history:
        ticket = ifc.show_history(intf.history)
        if ticket['status'][0] == e_errors.OK and len(ticket['history']):
            for state in ticket['history']:
                state_type = state['type']
                if state['type'] == 'system_inhibit_0':
                    state_type = 'system_inhibit[0]'
                elif state['type'] == 'system_inhibit_1':
                    state_type = 'system_inhibit[1]'
                elif state['type'] == 'user_inhibit_0':
                    state_type = 'user_inhibit[0]'
                elif state['type'] == 'user_inhibit_1':
                    state_type = 'user_inhibit[1]'
                print "%-28s %-20s %s"%(state['time'], state_type, state['value'])
    elif intf.write_protect_status:
        ticket = ifc.write_protect_status(intf.write_protect_status)
        if ticket['status'][0] == e_errors.OK:
            print intf.write_protect_status, "write-protect", ticket['status'][1]
    elif intf.vols:
        # optional argument
        nargs = len(intf.args)
        not_cond = None
        if nargs:
            if nargs == 3:
                key = intf.args[0]	 
                in_state=intf.args[1]
                not_cond = intf.args[2]
            elif nargs == 2:
                key = intf.args[0]	 
                in_state=intf.args[1]
            elif nargs == 1:
                key = None
                in_state=intf.args[0]
            else:
                print "Wrong number of arguments"
                print "usage: --vols"
                print "	   --vols state (will match system_inhibit)"
                print "	   --vols key state"
                print "	   --vols key state not (not in state)"
                return
        else:
            key = None
            in_state = None

        # get the information from the server
        ticket = ifc.get_vols(key, in_state, not_cond)
        
        # print out the answer
        if ticket.has_key("header"):		# full info
            show_volume_header()
            print
            for v in ticket["volumes"]:
                show_volume(v)
        else:
            vlist = ''
            for v in ticket.get("volumes",[]):
                vlist = vlist + v['label'] + " "
            print vlist

    elif intf.pvols:
        # get the information from the server
        ticket = ifc.get_pvols()

        # pull out just the answers we are looking for
        
        problem_vol = {}
        for i in ticket['volumes']:
            if i['system_inhibit_0'] != 'none':
                if problem_vol.has_key(i['system_inhibit_0']):
                    problem_vol[i['system_inhibit_0']].append(i)
                else:
                    problem_vol[i['system_inhibit_0']] = [i]
            if i['system_inhibit_1'] != 'none':
                if problem_vol.has_key(i['system_inhibit_1']):
                    problem_vol[i['system_inhibit_1']].append(i)
                else:
                    problem_vol[i['system_inhibit_1']] = [i]

        if intf.just:
            interested = intf.args
        else:
            interested = problem_vol.keys()

        # print out the answer
        for k in problem_vol.keys():
            if k in interested:
                print '====', k
                for v in problem_vol[k]:
                    show_volume(v)
                print
    elif intf.labels:
        ticket = ifc.get_vol_list()
        if e_errors.is_ok(ticket):
            for i in ticket['volumes']:
                print i
    elif intf.show_bad:
        ticket = ifc.show_bad()
        if e_errors.is_ok(ticket):
            for f in ticket['bad_files']:
                print f['label'], f['bfid'], f['size'], f['path']
    elif intf.query:
        ticket = ifc.query_db(intf.query)
        #Try the old protocol if the server doesn't know about this one.
        if ticket['status'][0] == e_errors.KEYERROR and \
               ticket['status'][1].startswith("cannot find requested function"):
            ticket = ifc.query_db_old(intf.query)
            if ticket['status'][0] == e_errors.OK:
                if ticket['result']['status'][0] != e_errors.OK:
                    ticket['status'] = ticket['result']['status']
                else:
                    show_query_result(ticket['result'])
        else:
            #Do the new way.
            if e_errors.is_ok(ticket):
                show_query_result(ticket)
        
    elif intf.ls_sg_count:
        ticket = ifc.list_sg_count()
        sgcnt = ticket['sgcnt']
        sk = sgcnt.keys()
        sk.sort()
        print "%12s %16s %10s"%('library', 'storage group', 'allocated')
        print '='*40
        for i in sk:
            lib, sg = string.split(i, ".")
            print "%12s %16s %10d"%(lib, sg, sgcnt[i])
    elif intf.get_sg_count:
        ticket = ifc.get_sg_count(intf.get_sg_count, intf.storage_group)
        print "%12s %16s %10d"%(ticket['library'], ticket['storage_group'], ticket['count'])
    elif intf.vol:
        ticket = ifc.inquire_vol(intf.vol)
        if ticket['status'][0] == e_errors.OK:
            status = ticket['status']
            del ticket['status']
            # do not show non_del_files
            del ticket['non_del_files']
            pprint.pprint(ticket)
            ticket['status'] = status
    elif intf.gvol:
        ticket = ifc.inquire_vol(intf.gvol)
        if ticket['status'][0] == e_errors.OK:
            status = ticket['status']
            del ticket['status']
            # do not show non_del_files
            del ticket['non_del_files']
            ticket['declared'] = time.ctime(ticket['declared'])
            ticket['first_access'] = time.ctime(ticket['first_access'])
            ticket['last_access'] = time.ctime(ticket['last_access'])
            if ticket.has_key('si_time'):
                    ticket['si_time'] = (time.ctime(ticket['si_time'][0]),
                                                             time.ctime(ticket['si_time'][1]))
            pprint.pprint(ticket)
            ticket['status'] = status
    else:
        intf.print_help()
        sys.exit(0)
    ifc.check_ticket(ticket)

if __name__ == '__main__':
    intf = InfoClientInterface(user_mode=0)
    do_work(intf)
