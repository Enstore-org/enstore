#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
# system imports
import sys
import string
import time
import errno
import socket
import re
#import select
import pprint

# enstore imports
#import callback
import hostaddr
import option
import generic_client
import backup_client
#import udp_client
import Trace
import e_errors
import file_clerk_client
import cPickle
import info_client
import enstore_constants
from en_eval import en_eval

MY_NAME = enstore_constants.VOLUME_CLERK_CLIENT  #"VOLUME_C_CLIENT"
MY_SERVER = enstore_constants.VOLUME_CLERK       #"volume_clerk"
RCV_TIMEOUT = 10
RCV_TRIES = 5

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

    x=1.0*x    ## make x floating-point
    neg=x<0    ## remember the sign of x
    x=abs(x)   ##  make x positive so that "<" comparisons work

    for suffix in ('B ', 'KB', 'MB', 'GB', 'TB', 'PB'):
        if x <= 1024:
            break
        x=x/1024
    if neg:    ## if x was negative coming in, restore the - sign
        x = -x
    return "%6.2f%s"%(x,suffix)


KiB=1<<10
MiB=1<<20
GiB=1<<30
TiB=1<<40

KB=1000
MB=1000000
GB=1000000000
TB=1000000000000

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

re_split_string = re.compile("[a-zA-Z]+|[0-9]+")

mult_d = {'l':1, 'b':1, 'm':MB, 'k':KB, 'g':GB, 't':TB}
mult_b = {'m':MiB,'k':KiB, 'g':GiB, 't':TiB}

def my_atol(s):
	s_orig = s
	if not s:
		raise ValueError, s_orig
	array = re_split_string.findall(s.lower())
	length = len(array)
	if length<1 or length>2:
		raise ValueError, s_orig
	if length == 1 :
		return long(float(array[0]))
	value = array[0]
	unit  = array[1]
	mult  = 1
	if unit in ["kib", "mib", "gib", "tib"]:
		mult=mult_b[unit[0]]
	elif unit in ["l","b","k","m","g","t","lb","kb","mb","gb","tb"]:
		try:
			mult=mult_d[unit[0]]
		except:
			raise ValueError, s_orig
        else:
            raise ValueError, s_orig
	x = float(value)*mult
        return(long(x))

# to sum up an object -- as an integrity assurance
#
# currently it only deal with numerical, string, list and dictionary
#
def sumup(a):
	# symple type?
	if type(a) == type(1) or type(a) == type(1L) or \
		type(a) == type(1.0):	# number
		return a
	elif type(a) == type("a"):	# string or character
		if len(a) == 1:		# character
			return ord(a)
		else:			# string
			sumation = 0
			for i in a:
				sumation = sumation + ord(i)
			return sumation
	elif type(a) == type([]):	# list
		sumation = 0
		for i in a:
			sumation = sumation + sumup(i)
		return sumation
	elif type(a) == type({}):	# dictionary
		sumation = 0
		for i in a.keys():
			sumation = sumation + sumup(i) + sumup(a[i])
		return sumation

	return 0

# simple syntax check for volume labels
#
# Characters        Constraint
# 1-2               must be alphabetic in upper case
# 3-4               alphanumerical
# 5-6               numerical
# 7-8               either L1 or blank
# length must be 6 or 8.
#
# check_label() returns 0 if the label conforms above rule, otherwise
# none zero is returned

def check_label(label):
    # cehck the length and suffix 'L1'
    if len(label) == 8:
        if label[-2:] != 'L1' and label[-2:] != 'L2':
            return 1
    elif len(label) != 6:
            return 1

    # now check the rest of the rules
    if  not label[0] in string.uppercase or \
        not label[1] in string.uppercase or \
        not label[2] in string.uppercase+string.digits or \
        not label[3] in string.uppercase+string.digits or \
        not label[4] in string.digits or \
        not label[5] in string.digits:
        return 1

    return 0



# a function to extract values from enstore vol --vols

def extract_volume(v):    # v is a string
    system_inhibit = ["", ""]
    si_time = (0, 0)
    t = string.split(v, '(')
    t1 = t[0]
    label, avail = string.split(t1)
    tt = string.split(t[1], ')')
    ttt = string.split(tt[1])
    library = ttt[0]
    volume_family = ttt[1]
    if len(ttt) == 3:
        comment = ttt[2]
    else:
        comment = ""
    t = string.split(tt[0])
    system_inhibit = [t[0], ""]
    if len(t) == 2:
        system_inhibit[1] = t[1]
        si_time = (0,0)
    elif len(t) == 3:
        if t[1][0] in string.digits:    # time stamp
            si_time = (t[1], '')
            system_inhibit[1] = t[2]
        else:
            system_inhibit[1] = t[1]
            si_time = ('', t[2])
    elif len(t) == 4:
        system_inhibit[1] = t[2]
        si_time = (t[1], t[3])

    return {'label': label,
            'avail': avail,
            'system_inhibit': system_inhibit,
            'si_time': si_time,
            'library': library,
            'volume_family': volume_family,
            'comment': comment}

class VolumeClerkClient(info_client.volumeInfoMethods, #generic_client.GenericClient,
                        backup_client.BackupClient):

    def __init__( self, csc, server_address=None, flags=0, logc=None,
                  alarmc=None, rcv_timeout = RCV_TIMEOUT,
                  rcv_tries = RCV_TRIES):
        #generic_client.GenericClient.__init__(self,csc,MY_NAME,server_address,
        #                                      flags=flags, logc=logc,
        #                                      alarmc=alarmc,
        #                                      rcv_timeout=rcv_timeout,
        #                                      rcv_tries=rcv_tries,
        #                                      server_name = MY_SERVER)
	info_client.volumeInfoMethods.__init__(self,csc,MY_NAME,
					       server_address = server_address,
					       flags=flags, logc=logc,
					       alarmc=alarmc,
					       rcv_timeout=rcv_timeout,
					       rcv_tries=rcv_tries,
					       server_name = MY_SERVER)
        #if self.server_address == None:
        #    self.server_address = self.get_server_address(
        #        MY_SERVER, rcv_timeout=rcv_timeout, tries=rcv_tries)

    # add a volume to the stockpile
    def add(self,
            library,               # name of library media is in
            file_family,           # volume family the media is in
            storage_group,         # storage group for this volume
            media_type,            # media
            external_label,        # label as known to the system
            capacity_bytes,        #
            eod_cookie  = "none",  # code for seeking to eod
            user_inhibit  = ["none","none"],# 0:"none" | "readonly" | "NOACCESS"
            error_inhibit = "none",# "none" | "readonly" | "NOACCESS" | "writing"
                                   # lesser access is specified as
                                   #       we find media errors,
                                   # writing means that a mover is
                                   #       appending or that a mover
                                   #       crashed while writing
            last_access = -1,      # last accessed time
            first_access = -1,     # first accessed time
            declared = -1,         # time volume was declared to system
            sum_wr_err = 0,        # total number of write errors
            sum_rd_err = 0,        # total number of read errors
            sum_wr_access = 0,     # total number of write mounts
            sum_rd_access = 0,     # total number of read mounts
            wrapper = "cpio_odc",  # kind of wrapper for volume
            blocksize = -1,        # blocksize (-1 =  media type specifies)
            non_del_files = 0,     # non-deleted files
            system_inhibit = ["none","none"], # 0:"none" | "writing??" | "NOACCESS", "DELETED
                                             # 1:"none" | "readonly" | "full"
            remaining_bytes = None,
            timeout=60, retry=1
            ):
        Trace.log(e_errors.INFO, 'add label=%s'%(external_label,))
        if storage_group == 'none':
            # the rest must be 'none' only
            file_family = 'none'
        if file_family == 'none':
            # the rest must be 'none' only
            wrapper = 'none'
        ticket = { 'work'            : 'addvol',
                   'library'         : library,
                   'storage_group'   : storage_group,
                   'file_family'     : file_family,
                   'media_type'      : media_type,
                   'external_label'  : external_label,
                   'capacity_bytes'  : capacity_bytes,
                   'eod_cookie'      : eod_cookie,
                   'user_inhibit'    : user_inhibit,
                   'error_inhibit'   : error_inhibit,
                   'last_access'     : last_access,
                   'first_access'    : first_access,
                   'declared'        : declared,
                   'sum_wr_err'      : sum_wr_err,
                   'sum_rd_err'      : sum_rd_err,
                   'sum_wr_access'   : sum_wr_access,
                   'sum_rd_access'   : sum_rd_access,
                   'wrapper'         : wrapper,
                   'blocksize'       : blocksize,
                   'non_del_files'   : non_del_files,
                   'system_inhibit'  : system_inhibit
                   }
        if remaining_bytes != None:
            ticket['remaining_bytes'] = remaining_bytes
        # no '.' are allowed in storage_group, file_family and wrapper
        for item in ('storage_group', 'file_family', 'wrapper'):
            if '.' in ticket[item]:
                print "No '.' allowed in %s"%(item,)
                break
        else:
            return self.send(ticket,timeout,retry)
        return {'status':(e_errors.NOTALLOWED, "No '.' allowed in %s"%(item,))}

    def show_state(self):
        return self.send({'work':'show_state'})

    def modify(self,ticket, timeout=60, retry=5):
        ticket['work']='modifyvol'
        return self.send(ticket,timeout,retry)

    # remove a volume entry in volume database
    def rmvolent(self, external_label, timeout=300, retry=1):
        ticket= { 'work'           : 'rmvolent',
                  'external_label' : external_label}
        return self.send(ticket,timeout,retry)

    # delete a volume from the stockpile
    def restore(self, external_label, restore=0, timeout=300, retry=1):
        if restore: restore_vm = "yes"
        else: restore_vm = "no"
        ticket= { 'work'           : 'restorevol',
                  'external_label' : external_label,
                  "restore"         : restore_vm}
        return self.send(ticket,timeout,retry)

    # rebuild sg scounts
    def rebuild_sg_count(self, timeout=300, retry=1):
        return(self.send({'work':'rebuild_sg_count'},timeout,retry))

    # set sg count
    def set_sg_count(self, lib, sg, count=0, timeout=60, retry=5):
        ticket = {'work':'set_sg_count',
                  'library': lib,
                  'storage_group': sg,
                  'count': count}
        return(self.send(ticket,timeout,retry))

    # get sg count
    def get_sg_count(self, lib, sg, timeout=60, retry=10):
        ticket = {'work':'get_sg_count',
                  'library': lib,
                  'storage_group': sg}
        return(self.send(ticket,timeout,retry))

    # what is the current status of a specified volume?
    def inquire_vol(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'inquire_vol',
                  'external_label' : external_label }
	return self.send(ticket,timeout,retry)

    # update the last_access time
    def touch(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'touch',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # trim obsolete fields if necessary
    def check_record(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'check_record',
                  'external_label' : external_label }
        return self.send(ticket,timeout, retry)

    def write_protect_on(self, vol):
        ticket = {"work"            : "write_protect_on",
                  "external_label"  : vol}
        return self.send(ticket)

    def write_protect_off(self, vol):
        ticket = {"work"            : "write_protect_off",
                  "external_label"  : vol}
        return self.send(ticket)

    def write_protect_status(self, vol):
        ticket = {"work"            : "write_protect_status",
                  "external_label"  : vol}
        return self.send(ticket)

    # show_quota() -- show quota
    def show_quota(self, timeout=60, retry=1):
        ticket = {'work': 'show_quota'}
	return self.send(ticket, timeout, retry)

    # move a volume to a new library
    def new_library(self, external_label,new_library, timeout=60, retry=10):
        ticket= { 'work'           : 'new_library',
                  'external_label' : external_label,
                  'new_library'    : new_library}
        return self.send(ticket,timeout,retry)

    # we are using the volume
    def set_writing(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_writing',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # we are using the volume
    def set_system_readonly(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_readonly',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as not allowed
    def set_system_notallowed(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_notallowed',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as noaccess
    def set_system_noaccess(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_noaccess',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as full
    def set_system_full(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_full',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as migrated
    def set_system_migrated(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_migrated',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as in progress for migration
    def set_system_migrating(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_migrating',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as duplicated
    def set_system_duplicated(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_duplicated',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as in progress for duplication
    def set_system_duplicating(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_duplicating',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as cloned
    def set_system_cloned(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_cloned',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # mark volume as in progress for cloning
    def set_system_cloning(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_cloning',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # set system inhibit to none
    def set_system_none(self, external_label, timeout=60, retry=10):
        ticket= { 'work'           : 'set_system_none',
                  'external_label' : external_label }
        return self.send(ticket,timeout,retry)

    # clear any inhibits on the volume
    def clr_system_inhibit(self,external_label,what=None, pos=0, timeout=60, retry=10):
        ticket= { 'work'           : 'clr_system_inhibit',
                  'external_label' : external_label,
                  'inhibit'        : what,
                  'position'       : pos}
        return self.send(ticket,timeout,retry)

    # decrement the file count on a tape
    def decr_file_count(self,external_label, count=1, timeout=300, retry=1):
        ticket= { 'work'           : 'decr_file_count',
                  'external_label' : external_label,
                  'count'          : count }
        return self.send(ticket,timeout,retry)

    # this many bytes left - read the database
    def get_remaining_bytes(self, external_label, timeout=60, retry=10):
        ticket= { 'work'            : 'get_remaining_bytes',
                  'external_label'  : external_label }
        return self.send(ticket,timeout,retry)

    # this many bytes left - update database
    def set_remaining_bytes(self, external_label,remaining_bytes,eod_cookie,bfid=None, timeout=0, retry=0):
        # Note - bfid should be set if we added a new file
        ticket= { 'work'            : 'set_remaining_bytes',
                  'external_label'  : external_label,
                  'remaining_bytes' : remaining_bytes,
                  'eod_cookie'      : eod_cookie,
                  'bfid'            : bfid }
        return self.send(ticket,timeout,retry)

    # update the counts in the database
    def update_counts(self, external_label, wr_err=0, rd_err=0,wr_access=0,rd_access=0,mounts=0, timeout=300, retry=1):
        ticket= { 'work'            : 'update_counts',
                  'external_label'  : external_label,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access,
                  'mounts'          : mounts
                  }
        return self.send(ticket,timeout,retry)

    # Check if volume is available
    def is_vol_available(self, work, external_label, family=None, size=0, timeout=60, retry=10):
        ticket = { 'work'                : 'is_vol_available',
                   'action'              : work,
                   'volume_family'       : family,
                   'file_size'           : size,
                   'external_label'      : external_label
                   }
        return self.send(ticket,timeout,retry)


    # which volume can we use for this library, bytes and file family and ...
    def next_write_volume (self, library, min_remaining_bytes,
                           volume_family, vol_veto_list,first_found, mover={}, exact_match=0, timeout=300, retry=1):
        if not mover:
             mover_type = 'Mover'
        else:
            mover_type = mover.get('mover_type','Mover')
        if mover_type == 'DiskMover':
            exact_match = 1
        ticket = { 'work'                : 'next_write_volume',
                   'library'             : library,
                   'min_remaining_bytes' : min_remaining_bytes,
                   'volume_family'       : volume_family,
                   'vol_veto_list'       : `vol_veto_list`,
                   'first_found'         : first_found,
                   'mover'               : mover,
                   'use_exact_match'     : exact_match}
        Trace.trace(22, "next_write_volume:sending")
        r=self.send(ticket,timeout,retry)
        Trace.trace(22, "next_write_volume:rc=%s"%(r,))


        #return self.send(ticket,timeout,retry)
        return r

    # check if specific volume can be used for write
    def can_write_volume (self, library, min_remaining_bytes,
                           volume_family, external_label, timeout=300, retry=1):
        ticket = { 'work'                : 'can_write_volume',
                   'library'             : library,
                   'min_remaining_bytes' : min_remaining_bytes,
                   'volume_family'         : volume_family,
                   'external_label'       : external_label }

        return self.send(ticket,timeout,retry)

    # clear the pause flag for the LM and all LMs that relate to the Media Changer
    def clear_lm_pause(self, library_manager, timeout=60, retry=10):
        ticket = { 'work'    :'clear_lm_pause',
                   'library' : library_manager
                   }
        return  self.send(ticket,timeout,retry)

    def rename_volume(self, old, new, timeout=300, retry=1):
        ticket = {'work': 'rename_volume',
                  'old' : old,
                  'new' : new }
        return self.send(ticket,timeout,retry)

    def delete_volume(self, vol, check_state=None, timeout=60, retry=10):
        ticket = {'work': 'delete_volume',
                  'external_label': vol}
        if check_state != None:
            ticket['check_state'] = check_state
        return self.send(ticket,timeout,retry)

    def erase_volume(self, vol, timeout=60, retry=1):
        ticket = {'work': 'erase_volume',
                  'external_label': vol}
        return self.send(ticket,timeout,retry)

    def restore_volume(self, vol, timeout=60, retry=1):
        ticket = {'work': 'restore_volume',
                  'external_label': vol}
        return self.send(ticket,timeout,retry)

    def recycle_volume(self, vol, clear_sg = False, reset_declared = True, timeout=300, retry=1):
        ticket = {'work': 'recycle_volume',
                  'external_label': vol,
                  'reset_declared': reset_declared}
        if clear_sg:
            ticket['clear_sg'] = True
        return self.send(ticket,timeout,retry)

    def set_ignored_sg(self, sg, timeout=60, retry=1):
        ticket = {'work': 'set_ignored_sg',
                  'sg': sg}
        return self.send(ticket,timeout,retry)

    def clear_ignored_sg(self, sg, timeout=60, retry=1):
        ticket = {'work': 'clear_ignored_sg',
                  'sg': sg}
        return self.send(ticket,timeout,retry)

    def clear_all_ignored_sg(self, timeout=60, retry=1):
        ticket = {'work': 'clear_all_ignored_sg'}
        return self.send(ticket,timeout,retry)

    def list_ignored_sg(self, timeout=60, retry=1):
        ticket = {'work': 'list_ignored_sg'}
        return self.send(ticket,timeout,retry)

    def set_comment(self, vol, comment, timeout=60, retry=1):
        ticket = {'work': 'set_comment',
                  'vol': vol,
                  'comment': comment}
        return self.send(ticket,timeout,retry)

    def assign_sg(self, vol, sg, timeout=60, retry=1):
        ticket = {'work': 'reassign_sg',
                  'external_label': vol,
                  'storage_group': sg}
        return self.send(ticket,timeout,retry)

    def list_migrated_files(self, src_vol, dst_vol,
                            timeout = 0, retry = 0):
        r = self.send({'work' : "list_migrated_files",
                       'src_vol' : src_vol,
                       'dst_vol' : dst_vol,
                       }, timeout, retry)
        if r.has_key('work'):
            del r['work']
        return r

    def list_duplicated_files(self, src_vol, dst_vol,
                              timeout = 0, retry = 0):
        r = self.send({'work' : "list_duplicated_files",
                       'src_vol' : src_vol,
                       'dst_vol' : dst_vol,
                       }, timeout, retry)
        if r.has_key('work'):
            del r['work']
        return r

    def set_migration_history(self, src_vol, dst_vol, timeout = 0, retry = 0):
        r = self.send({'work' : "set_migration_history",
                       'src_vol' : src_vol,
                       'dst_vol' : dst_vol,
                       }, timeout, retry)
        if r.has_key('work'):
            del r['work']
        return r

    def set_migration_history_closed(self, src_vol, dst_vol,
				     timeout = 0, retry = 0):
        r = self.send({'work' : "set_migration_history_closed",
                       'src_vol' : src_vol,
                       'dst_vol' : dst_vol,
                       }, timeout, retry)
        if r.has_key('work'):
            del r['work']
        return r

class VolumeClerkClientInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.clear = ""
        self.backup = 0
        self.vols = 0
        self.pvols = 0
        self.just = 0
        self.labels = 0
	self.full = None
        self.in_state = 0
        self.next = 0
        self.vol = ""
        self.gvol = ""
        self.check = ""
        self.history = None
        self.add = ""
        self.erase = None
        self.modify = ""
        self.delete = ""
        self.restore = ""
        self.all = 0
        self.new_library = ""
        self.read_only = ""
        self.no_access = ""
        self.migrated = None
	self.duplicated = None
        self.not_allowed = None
        self.decr_file_count = 0
        self.rmvol = 0
        self.vol1ok = 0
        self.lm_to_clear = ""
        self.list = None
        self.ls_active = None
        self.recycle = None
        self.export = None
        self.show_state = None
        self._import = None
        self.ignore_storage_group = None
        self.forget_ignored_storage_group = None
        self.forget_all_ignored_storage_groups = 0
        self.show_ignored_storage_groups = 0
        self.remaining_bytes = None
        self.set_comment = None
        self.volume = None
        self.assign_sg = None
        self.clear_sg = False
        self.touch = None
	self.trim_obsolete = None
        self.show_quota = 0
        self.bypass_label_check = 0
        self.ls_sg_count = 0
        self.rebuild_sg_count = 0
        self.set_sg_count = None
        self.get_sg_count = None
        self.write_protect_on = None
        self.write_protect_off = None
        self.write_protect_status = None
        self.keep_declaration_time = False
	self.force = False #use real clerks (True); use info server (False)
	self.package=None
	self.pkginfo=None

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.volume_options)

    volume_options = {
	    option.PACKAGE:{option.HELP_STRING:
			    "Force printing package files and non-packaged files",
			    option.VALUE_USAGE:option.IGNORED,
			    option.VALUE_TYPE:option.INTEGER,
			    option.USER_LEVEL:option.ADMIN},
	    option.PACKAGE_INFO:{option.HELP_STRING:
				 "Force printing information about package_id archive/cache status",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.ADMIN},
	    option.ADD:{option.HELP_STRING:"declare a new volume",
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"volume_name",
                    option.USER_LEVEL:option.ADMIN,
                    option.EXTRA_VALUES:[{option.VALUE_NAME:"library",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.REQUIRED,},
                                         {option.VALUE_NAME:"storage_group",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.REQUIRED,},
                                         {option.VALUE_NAME:"file_family",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.REQUIRED,},
                                         {option.VALUE_NAME:"wrapper",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.REQUIRED,},
                                         {option.VALUE_NAME:"media_type",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.REQUIRED,},
                                     {option.VALUE_NAME:"volume_byte_capacity",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.REQUIRED,},
                                         {option.VALUE_NAME:"remaining_bytes",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,
                                          option.DEFAULT_TYPE:None,
                                          option.DEFAULT_VALUE:None,}
                                         ]},
        option.ALL:{option.HELP_STRING:"used with --restore to restore all",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        option.ASSIGN_SG:{
                    option.HELP_STRING: 'reassign to new storage group',
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"storage_group",
                    option.USER_LEVEL:option.ADMIN,
                    option.EXTRA_VALUES:[{
                        option.VALUE_NAME:"volume",
                        option.VALUE_LABEL:"volume_name",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED}]},
        option.BACKUP:{option.HELP_STRING:
                       "backup voume journal -- part of database backup",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
	option.BYPASS_LABEL_CHECK:{
                       option.HELP_STRING:
                       "skip syntatical label check when adding new volumes",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        option.CHECK:{option.HELP_STRING:"check a volume",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
	option.CLEAR:{option.HELP_STRING:"clear a volume",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
        option.CLEAR_SG:{option.HELP_STRING:"used with recycle to clear storage group",
                      option.VALUE_TYPE:option.INTEGER,
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.VALUE_USAGE:option.IGNORED,
                      option.USER_LEVEL:option.ADMIN},
        option.DECR_FILE_COUNT:{option.HELP_STRING:
                                "decreases file count of a volume",
                                option.VALUE_TYPE:option.INTEGER,
                                option.VALUE_USAGE:option.REQUIRED,
                                option.VALUE_LABEL:"count",
                                option.USER_LEVEL:option.ADMIN},
        option.DELETE:{option.HELP_STRING:"delete a volume",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
	option.DUPLICATED:{option.HELP_STRING:"set volume to DUPLICATED",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"volume_name",
                          option.USER_LEVEL:option.ADMIN},
        option.ERASE:{option.HELP_STRING:"erase a volume",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
        option.EXPORT:{option.HELP_STRING:
                       "export a volume",
                       option.DEFAULT_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_LABEL:"volume_name",
                       option.USER_LEVEL:option.ADMIN},
	option.FORCE:{option.HELP_STRING:"use real cleark, not info_server",
                      option.VALUE_TYPE:option.INTEGER,
                      option.VALUE_USAGE:option.IGNORED,
                      option.VALUE_LABEL:"force",
                      option.USER_LEVEL:option.HIDDEN},
	option.FORGET_IGNORED_STORAGE_GROUP:{option.HELP_STRING:
                      "clear a ignored storage group",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"storage_group",
                      option.USER_LEVEL:option.ADMIN},
        option.FORGET_ALL_IGNORED_STORAGE_GROUPS:{option.HELP_STRING:
                      "clear all ignored storage groups",
                      option.VALUE_TYPE:option.INTEGER,
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.USER_LEVEL:option.ADMIN},
	option.FULL:{option.HELP_STRING:"set volume to full",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"volume_name",
                          option.USER_LEVEL:option.ADMIN},
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
        option.IGNORE_STORAGE_GROUP:{option.HELP_STRING:
                      'ignore a storage group. The format is "<library>.<storage_group>"',
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"storage_group",
                      option.USER_LEVEL:option.ADMIN},
        option.IMPORT:{option.HELP_STRING:
                       'import an exported volume object. The file name is of the format "vol.<volume_name>.obj"',
                       option.DEFAULT_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_NAME:"_import",
                       option.VALUE_LABEL:"exported_volume_object",
                       option.USER_LEVEL:option.ADMIN},
        option.JUST:{option.HELP_STRING:"used with --pvols to list problem",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.USER},
	option.KEEP_DECLARATION_TIME:{option.HELP_STRING:
                      "keep declared time when recycling",
                      option.VALUE_TYPE:option.INTEGER,
                      option.VALUE_USAGE:option.IGNORED,
                      option.USER_LEVEL:option.ADMIN},
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
        option.MIGRATED:{option.HELP_STRING:"set volume to MIGRATED",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"volume_name",
                          option.USER_LEVEL:option.ADMIN},
        option.MODIFY:{option.HELP_STRING:
                       "modify a volume record -- extremely dangerous",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"volume_name",
                        option.USER_LEVEL:option.ADMIN},
        option.NEW_LIBRARY:{option.HELP_STRING:"set new library",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"library",
                            option.USER_LEVEL:option.ADMIN,
			    option.EXTRA_VALUES:[{option.VALUE_NAME:"volume",
						  option.VALUE_LABEL:"volume_name",
						  option.VALUE_TYPE:option.STRING,
						  option.VALUE_USAGE:option.REQUIRED}]},
	option.NO_ACCESS:{option.HELP_STRING:"set volume to NOTALLOWED",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"volume_name",
                          option.USER_LEVEL:option.ADMIN},
        option.NOT_ALLOWED:{option.HELP_STRING:"set volume to NOTALLOWED",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"volume_name",
                          option.USER_LEVEL:option.ADMIN},
        option.PVOLS:{option.HELP_STRING:"list all problem volumes",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.USER},
        option.READ_ONLY:{option.HELP_STRING:"set volume to readonly",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"volume_name",
                          option.USER_LEVEL:option.ADMIN},
        option.REBUILD_SG_COUNT:{
                     option.HELP_STRING:"rebuild sg count db",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.RECYCLE:{option.HELP_STRING:"recycle a volume",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
        option.RESET_LIB:{option.HELP_STRING:"reset library manager",
                          option.VALUE_NAME:"lm_to_clear",
                          option.VALUE_LABEL:"library",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.USER_LEVEL:option.ADMIN},
        option.RESTORE:{option.HELP_STRING:"restore a volume",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"volume_name",
                        option.USER_LEVEL:option.ADMIN},
	option.SET_COMMENT:{
                        option.HELP_STRING:"set comment for a volume",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"comment",
                        option.USER_LEVEL:option.ADMIN,
                        option.EXTRA_VALUES:[{option.VALUE_NAME:"volume",
                                          option.VALUE_LABEL:"volume_name",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.REQUIRED}]},
        option.SHOW_IGNORED_STORAGE_GROUPS:{option.HELP_STRING:
                      "show all ignored storage group",
                      option.VALUE_TYPE:option.INTEGER,
                      option.VALUE_USAGE:option.IGNORED,
                      option.USER_LEVEL:option.ADMIN},
	option.SET_SG_COUNT:{
                    option.HELP_STRING: 'set allocated count of lib,sg',
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"library",
                    option.USER_LEVEL:option.ADMIN,
                    option.EXTRA_VALUES:[{
                        option.VALUE_NAME:"storage_group",
                        option.VALUE_LABEL:"storage_group",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED},
                       {option.VALUE_NAME:"count",
                        option.VALUE_LABEL:"count",
                        option.VALUE_TYPE:option.INTEGER,
                        option.VALUE_USAGE:option.REQUIRED},
                        ]},
	option.SHOW_STATE:{option.HELP_STRING:
                       "show internal state of the server",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        option.SHOW_QUOTA:{option.HELP_STRING:
                       "show quota information",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
	option.TOUCH:{option.HELP_STRING:"set last_access time as now",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"volume_name",
                          option.USER_LEVEL:option.ADMIN},
        option.TRIM_OBSOLETE:{option.HELP_STRING:"trim obsolete fields",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
        option.WRITE_PROTECT_ON:{option.HELP_STRING:"set write protect on",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
        option.WRITE_PROTECT_OFF:{option.HELP_STRING:"set write protect off",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
        option.WRITE_PROTECT_STATUS:{option.HELP_STRING:"show write protect status",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"volume_name",
                      option.USER_LEVEL:option.ADMIN},
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
        option.VOL1OK:{option.HELP_STRING:
                       "reset cookie to '0000_000000000_0000001'",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.DEFAULT_NAME:"vol1ok",
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        }

def do_work(intf):
    # get a volume clerk client
    vcc = VolumeClerkClient((intf.config_host, intf.config_port), None,  intf.alive_rcv_timeout, intf.alive_retries)
    Trace.init(vcc.get_name(MY_NAME))

    ifc = info_client.infoClient(vcc.csc)

    ticket = vcc.handle_generic_commands(MY_SERVER, intf)
    if ticket:
        pass

    elif intf.backup:
        ticket = vcc.start_backup()
        ticket = vcc.backup()
        ticket = vcc.stop_backup()
    elif intf.show_state:
        ticket = vcc.show_state()
        w = 0
        for i in ticket['state'].keys():
            if len(i) > w:
                w = len(i)
        fmt = "%%%ds = %%s"%(w)
	for i in ticket['state'].keys():
            print fmt%(i, ticket['state'][i])
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
                print "       --vols state (will match system_inhibit)"
                print "       --vols key state"
                print "       --vols key state not (not in state)"
                return
        else:
            key = None
            in_state = None
	if intf.force:
		ticket = vcc.get_vols(key, in_state, not_cond)
	else:
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
        if intf.force:
	    ticket = vcc.get_pvols()
	else:
	    ticket = ifc.get_pvols()
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
        for k in problem_vol.keys():
            if k in interested:
                print '====', k
                for v in problem_vol[k]:
                    show_volume(v)
                print
    elif intf.labels:
        if intf.force:
	    ticket = vcc.get_vol_list()
	else:
	    ticket = ifc.get_vol_list()
        if ticket['status'][0] == e_errors.OK:
            for i in ticket['volumes']:
                print i
    elif intf.next:
        ticket = vcc.next_write_volume(intf.args[0], #library
                                       long(intf.args[1]), #min_remaining_byte
                                       intf.args[2], #volume_family
                                            [], #vol_veto_list
                                             1) #first_found
    elif intf.assign_sg and intf.volume:
        ticket = vcc.assign_sg(intf.volume, intf.assign_sg)
    elif intf.rebuild_sg_count:
        ticket = vcc.rebuild_sg_count()
    elif intf.ls_sg_count:
        if intf.force:
	    ticket = vcc.list_sg_count()
	else:
	    ticket = ifc.list_sg_count()
	if ticket['status'][0] == e_errors.OK:
	    sgcnt = ticket['sgcnt']
	    sk = sgcnt.keys()
	    sk.sort()
	    print "%12s %16s %10s"%('library', 'storage group', 'allocated')
	    print '='*40
	    for i in sk:
	        lib, sg = string.split(i, ".")
		print "%12s %16s %10d"%(lib, sg, sgcnt[i])
    elif intf.get_sg_count:
        if intf.force:
	    ticket = vcc.get_sg_count(intf.get_sg_count, intf.storage_group)
	else:
            ticket = ifc.get_sg_count(intf.get_sg_count, intf.storage_group)
        print "%12s %16s %10d"%(ticket['library'], ticket['storage_group'], ticket['count'])
    elif intf.set_sg_count:
        ticket = vcc.set_sg_count(intf.set_sg_count, intf.storage_group, intf.count)
	if ticket['status'][0] == e_errors.OK:
	    print "%12s %16s %10d"%(ticket['library'], ticket['storage_group'], ticket['count'])

    elif intf.touch:
        ticket = vcc.touch(intf.touch)
    elif intf.trim_obsolete:
        ticket = vcc.check_record(intf.trim_obsolete)
    elif intf.show_quota:
        ticket = vcc.show_quota()
	pprint.pprint(ticket['quota'])
    elif intf.vol:
        if intf.force:
	    ticket = vcc.inquire_vol(intf.vol)
	else:
	    ticket = ifc.inquire_vol(intf.vol)
        if ticket['status'][0] == e_errors.OK:
            status = ticket['status']
            del ticket['status']
            # do not show non_del_files
            del ticket['non_del_files']
            pprint.pprint(ticket)
            ticket['status'] = status
    elif intf.gvol:
        if intf.force:
            ticket = vcc.inquire_vol(intf.gvol)
	else:
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
	    if ticket.has_key('modification_time'):
		    if ticket['modification_time'] != -1:
			    ticket['modification_time'] = time.ctime(ticket['modification_time'])
            pprint.pprint(ticket)
            ticket['status'] = status
    elif intf.check:
        if intf.force:
	    ticket = vcc.inquire_vol(intf.check)
	else:
            ticket = ifc.inquire_vol(intf.check)
        ##pprint.pprint(ticket)
        # guard against error
        if ticket['status'][0] == e_errors.OK:
            print "%-10s  %s %s %s" % (ticket['external_label'],
                                   capacity_str(ticket['remaining_bytes']),
                                   ticket['system_inhibit'],
                                   ticket['user_inhibit'])
    elif intf.history:
        if intf.force:
	    ticket = vcc.show_history(intf.history)
	else:
	    ticket = ifc.show_history(intf.history)
        if ticket['status'][0] == e_errors.OK and len(ticket['history']):
            for state in ticket['history']:
                stype = state['type']
                if state['type'] == 'system_inhibit_0':
                    stype = 'system_inhibit[0]'
                elif state['type'] == 'system_inhibit_1':
                    stype = 'system_inhibit[1]'
                elif state['type'] == 'user_inhibit_0':
                    stype = 'user_inhibit[0]'
                elif state['type'] == 'user_inhibit_1':
                    stype = 'user_inhibit[1]'
                print "%-28s %-20s %s"%(state['time'], stype, state['value'])
    elif intf.write_protect_on:
        ticket = vcc.write_protect_on(intf.write_protect_on)
    elif intf.write_protect_off:
        ticket = vcc.write_protect_off(intf.write_protect_off)
    elif intf.write_protect_status:
        if intf.force:
	    ticket = vcc.write_protect_status(intf.write_protect_status)
	else:
            ticket = ifc.write_protect_status(intf.write_protect_status)
        if ticket['status'][0] == e_errors.OK:
            print intf.write_protect_status, "write-protect", ticket['status'][1]
    elif intf.set_comment: # set comment of vol
        if len(intf.argv) != 4:
            print "Error! usage: enstore %s --set-comment=<comment> <vol>"%(sys.argv[0])
            sys.exit(1)
        ticket = vcc.set_comment(intf.volume, intf.set_comment)
    elif intf.export: # volume export
        # check for correct syntax
        if len(sys.argv) != 3:	# wrong number of arguments
            print "Error! usage: enstore %s --export <vol>"%(sys.argv[0])
            sys.exit(1)
        # get volume info first
        volume = {}
        volume['vol_name'] = intf.export
        volume['files'] = {}
        ticket = vcc.inquire_vol(intf.export)
        if ticket['status'][0] == e_errors.OK:
            del ticket['status']
            volume['vol'] = ticket
            # get all bfids
            fcc = file_clerk_client.FileClient(vcc.csc)
            ticket = fcc.get_bfids(intf.export)
            if ticket['status'][0] == e_errors.OK:
                bfids = ticket['bfids']
                status = (e_errors.OK, None)
                for i in bfids:
                    ticket = fcc.bfid_info(i)
                    if ticket['status'][0] == e_errors.OK:
                        status = ticket['status']
                        del ticket['status']
                        # deal with brain damaged backward compatibility
                        if ticket.has_key('fc'):
                            del ticket['fc']
                        if ticket.has_key('vc'):
                            del ticket['vc']
                        volume['files'][i] = ticket
                    else:
                        break
                # is the status still ok?
                if status[0] == e_errors.OK:
                    # !!! need to generate a key to prevent fake import
                    # dump it now
                    volume['key'] = 0
                    volume['key'] = sumup(volume) * -1
                    f = open('vol.'+intf.export+'.obj', 'w')
                    cPickle.dump(volume, f)
                    f.close()
                    Trace.log(e_errors.INFO, "volume %s exported"%(intf.export))
                else:
                    Trace.log(e_errors.ERROR, "failed to export volume "+intf.export)
                ticket={'status':status}
    elif intf._import: # volume import

        # Just to be paranoid, a lot of checking here

        # check for correct syntax
        if len(sys.argv) != 3:	# wrong number of arguments
            print "Error! usage: enstore %s --import vol.<vol>.obj"%(sys.argv[0])
            sys.exit(1)

        # the import file name must be vol.<vol>.obj
        fname = string.split(intf._import, '.')
        if len(fname) < 3 or fname[0] != 'vol' or fname[-1] != 'obj':
            print "Error!", intf._import, "is of wrong type of name"
            sys.exit(1)

        vname = fname[1]

        # load it up
        try:
            f = open(intf._import, 'r')
        except:
            print "Error! can not open", intf._import
            sys.exit(1)
        try:
            volume = cPickle.load(f)
        except:
            print "Error! can not load", intf._import
            sys.exit(1)

        # check if it is a real exported volume object
        #if sumup(volume) != 0:
        #    print "Error!", intf._import, "is a counterfeit"
        #    sys.exit(1)

        # check if volume contains all necessary information
        for i in ['vol', 'files', 'vol_name', 'key']:
            if not volume.has_key(i):
                print 'Error! missing key "'+i+'"'
                sys.exit(1)
        # check if file name match the volume name
        if volume['vol']['external_label'] != vname:
            print "Error!", intf._import, "does not match external_label"
            sys.exit(1)

        # check if all files match the external_label
        bfids = []
        for i in volume['files'].keys():
            f = volume['files'][i]
            if f['external_label'] != vname:
                print "Error!", intf._import, "is corrupted"
                sys.exit(1)
            bfids.append(f['bfid'])    # collect all bfids

        # get a fcc here
        fcc = file_clerk_client.FileClient(vcc.csc)

        # check for file existence
        result = fcc.exist_bfids(bfids)
        err = 0
        j = 0
        for i in result:
            if i:
                print "Error! file %s exists"%(volume['files'].values()[j]['bfid'])
                err = 1
            j = j + 1
        if err:
            sys.exit(1)

        # check if volume exists
        v = vcc.inquire_vol(volume['vol']['external_label'])
        if v['status'][0] == e_errors.OK:	# it exists
            print "Error! volume %s exists"%(volume['vol']['external_label'])
            sys.exit(1)

        # now we are getting serious

        # get the file family from volume record
        try:
            sg, ff, wr = string.split(volume['vol']['volume_family'], '.')
        except:
            print "Invalid volume_family:", `volume['vol']['volume_family']`
            sys.exit(1)

        # insert the file records first
        for i in volume['files'].keys():
            f = volume['files'][i]
            ticket = fcc.add(f)
            # handle errors
            if ticket['status'][0] != e_errors.OK:
                print "Error! failed to insert file record:", `f`
                # ignore the error, if serious, make it up later
                # sys.exit(1)

        # insert the volume record

        ticket = vcc.add(
                    library = volume['vol']['library'],
                    file_family = ff,
                    storage_group = sg,
                    media_type = volume['vol']['media_type'],
                    external_label = volume['vol']['external_label'],
                    capacity_bytes = volume['vol']['capacity_bytes'],
                    eod_cookie = volume['vol']['eod_cookie'],
                    user_inhibit = volume['vol']['user_inhibit'],
                    # error_inhibit = volume['vol']['error_inhibit'],
                    last_access = volume['vol']['last_access'],
                    first_access = volume['vol']['first_access'],
                    declared = volume['vol']['declared'],
                    sum_wr_err = volume['vol']['sum_wr_err'],
                    sum_rd_err = volume['vol']['sum_rd_err'],
                    sum_wr_access = volume['vol']['sum_wr_access'],
                    sum_rd_access = volume['vol']['sum_rd_access'],
                    wrapper = volume['vol']['wrapper'],
                    blocksize = volume['vol']['blocksize'],
                    non_del_files = volume['vol']['non_del_files'],
                    system_inhibit = volume['vol']['system_inhibit'],
                    remaining_bytes = volume['vol']['remaining_bytes'])
    elif intf.ignore_storage_group:
        ticket = vcc.set_ignored_sg(intf.ignore_storage_group)
        if ticket['status'][0] == e_errors.OK:
            pprint.pprint(ticket['status'][1])
    elif intf.forget_ignored_storage_group:
        ticket = vcc.clear_ignored_sg(intf.forget_ignored_storage_group)
        if ticket['status'][0] == e_errors.OK:
            pprint.pprint(ticket['status'][1])
    elif intf.forget_all_ignored_storage_groups:
        ticket = vcc.clear_all_ignored_sg()
        if ticket['status'][0] == e_errors.OK:
            pprint.pprint(ticket['status'][1])
    elif intf.show_ignored_storage_groups:
        ticket = vcc.list_ignored_sg()
        if ticket['status'][0] == e_errors.OK:
            pprint.pprint(ticket['status'][1])
    elif intf.add:
        if not intf.bypass_label_check:
            if check_label(intf.add):
                print 'Error: "%s" is not a valid label of format "AAXX99{L1|L2}" '%(intf.add)
                sys.exit(1)

        #print intf.add, repr(intf.args)
        cookie = 'none'
        if intf.vol1ok:
            cookie = '0000_000000000_0000001'
        #library, storage_group, file_family, wrapper, media_type, capacity = intf.args[:6]
        capacity = my_atol(intf.volume_byte_capacity)
        if intf.remaining_bytes != None:
            remaining = my_atol(intf.remaining_bytes)
        else:
            remaining = None
        # if wrapper is empty create a default one
        if not intf.wrapper:
            if intf.media_type == 'null': #media type
                intf.wrapper = "null"
            else:
                intf.wrapper = "cpio_odc"
        ticket = vcc.add(intf.library,
                         intf.file_family,
                         intf.storage_group,
                         intf.media_type,
                         intf.add,                  # name of this volume
                         capacity,
                         wrapper=intf.wrapper,
                         eod_cookie=cookie,            # rem cap'y of volume
                         remaining_bytes = remaining)
    elif intf.modify:
        d={}
        for s in intf.args:
            k,v=string.split(s,'=')
            try:
                v=en_eval(v) #numeric args
            except:
                pass #yuk...
            d[k]=v
        d['external_label']=intf.modify # name of this volume
        ticket = vcc.modify(d)


    elif intf.new_library:
        ticket = vcc.new_library(intf.volume,          # volume name
				 intf.new_library)     # new library name
    elif intf.delete:
        # ticket = vcc.delete(intf.delete,intf.force)   # name of this volume
        ticket = vcc.delete_volume(intf.delete)   # name of this volume
    elif intf.erase:
        ticket = vcc.erase_volume(intf.erase)
    elif intf.restore:
        # ticket = vcc.restore(intf.restore, intf.all)  # name of volume
        ticket = vcc.restore_volume(intf.restore)  # name of volume
    elif intf.recycle:
        reset_declared = not intf.keep_declaration_time
        if intf.clear_sg:
            ticket = vcc.recycle_volume(intf.recycle, reset_declared = reset_declared, clear_sg = True)
        else:
            ticket = vcc.recycle_volume(intf.recycle, reset_declared = reset_declared)
    elif intf.clear_sg:    # This is wrong
        print "Error: --clear-sg must be used with --recycle"
    elif intf.clear:
        nargs = len(intf.args)
        try:
            ipos = int(intf.args[1])-1
        except:
            ipos = -1
        if nargs == 0:
            ticket = vcc.clr_system_inhibit(intf.clear)
        elif nargs == 2 and (intf.args[0] in ["system_inhibit",
            "system-inhibit", "user_inhibit", "user-inhibit"]) and \
            (ipos == 0 or ipos == 1):
                ticket = vcc.clr_system_inhibit(intf.clear, string.replace(intf.args[0], '-', '_'), ipos)
        else:
            print "usage: enstore vol --clear <vol> system_inhibit|user_inhibit 1|2"
            ticket = {'status':(e_errors.OK, None)}
    elif intf.decr_file_count:
        print `type(intf.decr_file_count)`
        ticket = vcc.decr_file_count(intf.args[0],int(intf.decr_file_count))
        Trace.trace(12, repr(ticket))
    elif intf.read_only:
        ticket = vcc.set_system_readonly(intf.read_only)  # name of this volume
    elif intf.full:
        ticket = vcc.set_system_full(intf.full) # name of this volume
    elif intf.migrated:
        ticket = vcc.set_system_migrated(intf.migrated) # name of this volume
    elif intf.duplicated:
        ticket = vcc.set_system_duplicated(intf.duplicated) # name of this volume
    elif intf.no_access:
        ticket = vcc.set_system_notallowed(intf.no_access)  # name of this volume
    elif intf.not_allowed:
        ticket = vcc.set_system_notallowed(intf.not_allowed)  # name of this volume
    elif intf.lm_to_clear:
        ticket = vcc.clear_lm_pause(intf.lm_to_clear)
    elif intf.list:
        if intf.force:
	    #ticket = vcc.tape_list(intf.list)
	    ticket = {'status' : "--force not supported"}
	else:
            ticket = ifc.tape_list(intf.list)
	ifc.print_volume_files(intf.list,ticket,intf.package,intf.pkginfo)
    elif intf.ls_active:
        if intf.force:
            #ticket = vcc.list_active(intf.ls_active)
	    ticket = {'status' : "--force not supported"}
	else:
	    ticket = ifc.list_active(intf.ls_active)
        active_list = ticket['active_list']
        for i in active_list:
            print i
    else:
        intf.print_help()
        sys.exit(0)

    vcc.check_ticket(ticket)

if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace( 6, 'vcc called with args: %s'%(sys.argv,) )

    # fill in the interface
    intf = VolumeClerkClientInterface(user_mode=0)

    do_work(intf)


