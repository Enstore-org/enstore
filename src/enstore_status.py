#
# system import
import sys
import time
import string
import os
import stat
import types

import rexec
_rexec = rexec.RExec()
def eval(stuff):
    return _rexec.r_eval(stuff)

# enstore imports
import Trace
import e_errors
import enstore_constants
import enstore_functions2
import mover_constants

FILE_FAMILY = 'file_family'
FILE_FAMILY_WIDTH = 'file_family_width'
SEEN = 'seen'
VOLUME = 'volume'
STORAGE_GROUP = 'storage_group'
STORAGE_GROUP_LIMIT = 'storage_group_limit'
QUESTION = "?"

# locate and pull out the dictionaries in the text message. assume that if
# there is more than one dict, they are of the form -
#
#                 dict1 , dict2 , dict3 ...
#
# with only a comma and whitespace between them
def get_dict(text):
    dicts = []
    start = string.find(text, "{")
    if not start == -1:
        end = string.rfind(text, "}")
        if not end == -1:
            # we have a start and an end curly brace, assume that all inbetween
            # are part of the dictionaries
            try:
                dicts = eval(text[start:end+1])
		if type(dicts) == types.DictType:
                    # dicts is a dictionary, we want to return a list
                    dicts = [dicts,]
            except SyntaxError:
                # the text was not in the right format so ignore it
                pass
    return dicts

# add commas in the appropriate places in the number passed as a string
def add_commas(str):
    l = len(str)
    new_str = ""
    j = 0
    # the string might have a 'L' at the end to show it was a long int. 
    # avoid it
    if str[l-1] == "L":
        end = l-2
    else:
        end = l-1

    # count backwards from the end of the string to the beginning
    for i in range(end, -1, -1):
        if j == 3: 
            j = 0
            new_str = ",%s"%(new_str,)
        new_str = "%s%s"%(str[i], new_str)
        j = j + 1
    return new_str


# given a list of media changers and a log file message, see if any of the
# media changers are mentioned in the log file message
def mc_in_list(msg, mcs):
    for msgDict in msg:
        for mc in mcs:
            if mc == msgDict.get("media_changer", ""):
                return 1
    else:
        return 0

class EncpLine:

    def bytes_to_mbytes(self, bytes):
        return "%.2f"%(bytes/1024.0/1024.0,)

    def __init__(self, line):
        self.line = line

        # these fields are the same for a success message or a failure message
        self.time = line[0]
        self.node = line[1]
        self.pid = line[2]
        self.user = line[3]

        # determine if this is an encp success message or an encp error message
        if len(line) == 27:
            # this is a success encp
            self.success = 1
            self.infile = line[4]
            self.outfile = line[5]
            self.bytes = line[6]
            self.direction = line[7]  # either 'r' or 'w'
            self.overall_rate = self.bytes_to_mbytes(line[8])
            self.network_rate = self.bytes_to_mbytes(line[9])   # was data transfer rate
            self.drive_rate = self.bytes_to_mbytes(line[10])
            self.volume = line[11]
            self.mover = line[12]
            self.drive_id = line[13]
            self.drive_sn = line[14]
            self.elapsed = line[15]
            self.mc = line[16]
            self.interface = line[17]
            self.driver = line[18]
            self.storage_group = line[19]
            self.encp_ip = line[20]
            self.encp_id = line[21]
            self.disk_rate = self.bytes_to_mbytes(line[22])
            self.transfer_rate = self.bytes_to_mbytes(line[23])   # was user rate
            self.encp_version = line[24]
            self.file_family = line[25]
            self.wrapper =  line[26]
            self.type = None    # this is only valid for encp error lines
            self.error = None   # this is only valid for encp error lines
        else:
            # this is an error encp
            self.success = 0
            self.encp_id = line[4]
            self.encp_version = line[5]
            self.type = line[6]
            self.error = line[7]
            self.infile = line[8]
            self.outfile = line[9]
            self.bytes = line[10]
            self.storage_group = line[11]
            self.wrapper =  line[12]
            self.file_family = line[13]
            self.mover = line[14]
            self.drive_id = line[15]
            self.drive_sn = line[16]
            self.direction = line[17]  # either 'r' or 'w'
            self.volume = line[18]
            self.overall_rate = None     # this is only valid for success lines
            self.network_rate = None     # this is only valid for success lines
            self.drive_rate = None       # this is only valid for success lines
            self.elapsed = None          # this is only valid for success lines
            self.mc = None               # this is only valid for success lines
            self.interface = None        # this is only valid for success lines
            self.driver = None           # this is only valid for success lines
            self.encp_ip = None          # this is only valid for success lines
            self.disk_rate = None        # this is only valid for success lines
            self.transfer_rate = None    # this is only valid for success lines

        # direction should be 'from' or 'to'
        if self.direction == 'r':
            self.direction = 'from'
        else:
            self.direction = 'to'

class SgLine:

    def __init__(self, line):
	self.line = line
        [self.time, self.node, self.pid, self.user, self.status, self.server, 
         self.text] = string.split(line, None, 6)
	if not string.find(self.text, enstore_constants.PENDING) == -1:
	    # this is an add to the pending queue
	    self.pending = 1
	else:
	    self.pending = None
	# get the storage group
	dummy, sg = string.split(self.text, ":")
	self.sg = string.strip(sg)

class EnStatus:

    # remove all single quotes
    def unquote(self, s):
        return string.replace(s,"'","")

    def get_common_q_info(self, mover, worktype, key, writekey, readkey, dict):
	dict[enstore_constants.ID] = mover['unique_id']
	dict[enstore_constants.PORT] = mover['callback_addr'][1]
	if mover['work'] == 'write_to_hsm':
	    self.text[key][writekey] = self.text[key][writekey] + 1
	    dict[enstore_constants.WORK] = enstore_constants.WRITE
	else:
	    self.text[key][readkey] = self.text[key][readkey] + 1
	    dict[enstore_constants.WORK] = enstore_constants.READ

	encp = mover['encp']
	dict[enstore_constants.CURRENT] = repr(encp['curpri'])
	dict[enstore_constants.BASE] = repr(encp['basepri'])
	dict[enstore_constants.DELTA] = repr(encp['delpri'])
	dict[enstore_constants.AGETIME] = repr(encp['agetime'])

	# always try to get the users file name
	if dict[enstore_constants.WORK] == enstore_constants.READ:
	    dict[enstore_constants.FILE] = mover[enstore_constants.OUTFILE]
	else:
	    dict[enstore_constants.FILE] = mover.get(enstore_constants.INFILE, "")
		
	wrapper = mover['wrapper']
	dict[enstore_constants.BYTES] = add_commas(str(wrapper['size_bytes']))

	# 'mtime' not found in reads
	if wrapper.has_key('mtime'):
	    dict[enstore_constants.MODIFICATION] = \
				       enstore_functions2.format_time(wrapper['mtime'])
	machine = wrapper['machine']
	dict[enstore_constants.NODE] = self.unquote(machine[1])
	dict[enstore_constants.USERNAME] = wrapper['uname']

	times = mover['times']
	dict[enstore_constants.SUBMITTED] = enstore_functions2.format_time(times['t0'])

	vc = mover['vc']
	# 'file_family' is not present in a read, use volume family instead
	if vc.has_key('volume_family'):
	    dict[enstore_constants.VOLUME_FAMILY] = vc['volume_family']
	if vc.has_key('file_family'):
	    dict[enstore_constants.FILE_FAMILY] = vc['file_family']
	    dict[enstore_constants.FILE_FAMILY_WIDTH] = \
						 repr(vc.get('file_family_width', ""))
	fc = mover.get('fc', "")
	if fc:
	    if fc.has_key('external_label'):
		if not (worktype is enstore_constants.PENDING and \
			dict[enstore_constants.WORK] is enstore_constants.WRITE):
		    dict[enstore_constants.DEVICE] = fc['external_label']
	    dict[enstore_constants.LOCATION_COOKIE] = fc.get(enstore_constants.LOCATION_COOKIE,
							     None)

    def get_pend_dict(self, mover, key, write_key, read_key):
	# 'mover' not found in pending work
	dict = {enstore_constants.MOVER : enstore_constants.NOMOVER}
	self.get_common_q_info(mover, enstore_constants.PENDING, key, write_key, 
			       read_key, dict)
	if mover.has_key(enstore_constants.REJECT_REASON):
	    dict[enstore_constants.REJECT_REASON] = \
					    mover[enstore_constants.REJECT_REASON][0]
	return dict

    # information we want and put it in a dictionary
    def parse_lm_pend_queues(self, work, key, writekey, readkey):
	self.text[key][enstore_constants.PENDING] = {enstore_constants.READ : [],
						     enstore_constants.WRITE: []}
	# first the read queue, preserve the order sent from the lm
	for mover in work['admin_queue']:
	    dict = self.get_pend_dict(mover, key, writekey, readkey)
	    if dict[enstore_constants.WORK] == enstore_constants.WRITE:
		self.text[key][enstore_constants.PENDING][enstore_constants.WRITE].append(dict)
	    else:
		self.text[key][enstore_constants.PENDING][enstore_constants.READ].append(dict)
	    
	for mover in work['read_queue']:
	    dict = self.get_pend_dict(mover, key, writekey, readkey)
	    self.text[key][enstore_constants.PENDING][enstore_constants.READ].append(dict)
	for mover in work['write_queue']:
	    dict = self.get_pend_dict(mover, key, writekey, readkey)
	    self.text[key][enstore_constants.PENDING][enstore_constants.WRITE].append(dict)

    # information we want and put it in a dictionary
    def parse_lm_wam_queues(self, work, key, writekey, readkey):
        self.text[key][enstore_constants.WORK] = []
        for mover in work:
            dict = {enstore_constants.MOVER : mover['mover']}
	    self.get_common_q_info(mover, enstore_constants.WORK, key, writekey, 
				   readkey, dict)
 	    dict[enstore_constants.DEQUEUED] = \
				enstore_functions2.format_time(mover['times']['lm_dequeued'])
            self.text[key][enstore_constants.WORK].append(dict)

    def format_host(self, host):
        fhost = self.unquote(host)
        return enstore_functions2.strip_node(fhost)

    # output the passed alive status
    def output_alive(self, host, state, time, key):
        if not self.text.has_key(key):
            self.text[key] = {}
        self.text[key][enstore_constants.STATUS] = [state, 
                                                    self.format_host(host), 
                                                    enstore_functions2.format_time(time)]

    # output the passed alive status
    def output_error(self, host, state, time, key):
	self.output_alive(host, "ERROR: %s"%(state,), time, key)

    # output the timeout error
    def output_etimedout(self, host, state, time, key, last_time=0):
        if last_time == -1:
            ltime = enstore_constants.NO_INFO
        else:
            ltime = enstore_functions2.format_time(last_time)
        if not self.text.has_key(key):
            self.text[key] = {}
        self.text[key][enstore_constants.STATUS] = [state, self.format_host(host),
                                               enstore_functions2.format_time(time), ltime]

    # output the library manager suspect volume list
    def output_suspect_vols(self, ticket, key):
        sus_vols = ticket[enstore_constants.SUSPECT_VOLUMES]
        if not self.text.has_key(key):
            self.text[key] = {}
        if sus_vols:
            self.text[key][enstore_constants.SUSPECT_VOLS] = []
            for svol in sus_vols:
		self.text[key][enstore_constants.SUSPECT_VOLS].append(\
		                              [svol['external_label'], svol['movers']])
        else:
            self.text[key][enstore_constants.SUSPECT_VOLS] = ["None"]

    # output the active volumes list
    def output_lmactive_volumes(self, active_volumes, key):
        if not self.text.has_key(key):
            self.text[key] = {}
        self.text[key][enstore_constants.ACTIVE_VOLUMES] = active_volumes

    # output the state of the library manager
    def output_lmstate(self, ticket, key):
        if not self.text.has_key(key):
            self.text[key] = {}
        self.text[key][enstore_constants.LMSTATE] = ticket['state']

    # output the library manager queues
    def output_lmqueues(self, ticket, key):
        work = ticket[enstore_constants.ATMOVERS]
        if not self.text.has_key(key):
            self.text[key] = {}
        self.text[key][enstore_constants.TOTALPXFERS] = 0
        self.text[key][enstore_constants.READPXFERS] = 0
        self.text[key][enstore_constants.WRITEPXFERS] = 0
        self.text[key][enstore_constants.TOTALONXFERS] = 0
        self.text[key][enstore_constants.READONXFERS] = 0
        self.text[key][enstore_constants.WRITEONXFERS] = 0
        if work:
            self.parse_lm_wam_queues(work, key, enstore_constants.WRITEONXFERS,
				     enstore_constants.READONXFERS)
            self.text[key][enstore_constants.TOTALONXFERS] = self.text[key][enstore_constants.READONXFERS] + self.text[key][enstore_constants.WRITEONXFERS]
        else:
            self.text[key][enstore_constants.WORK] = enstore_constants.NO_WORK
        pending_work = ticket[enstore_constants.PENDING_WORKS]
        if pending_work:
            self.parse_lm_pend_queues(pending_work, key, enstore_constants.WRITEPXFERS,
				      enstore_constants.READPXFERS)
            self.text[key][enstore_constants.TOTALPXFERS] = self.text[key][enstore_constants.READPXFERS] + self.text[key][enstore_constants.WRITEPXFERS]
        else:
            self.text[key][enstore_constants.PENDING] = enstore_constants.NO_PENDING

    # output the mover status
    def output_moverstatus(self, ticket, key):
        # clean out all the old info but save the status
        self.text[key] = {enstore_constants.STATUS : self.text[key][enstore_constants.STATUS]}
        self.text[key][enstore_constants.COMPLETED] = self.unquote(repr(ticket[enstore_constants.TRANSFERS_COMPLETED]))
        self.text[key][enstore_constants.FAILED] = self.unquote(repr(ticket[enstore_constants.TRANSFERS_FAILED]))
        # these are the states where the information  in the ticket refers to a current transfer
	lcl_state = ticket[enstore_constants.STATE]
        if lcl_state in (mover_constants.ACTIVE, mover_constants.MOUNT_WAIT,
			 mover_constants.DISMOUNT_WAIT):
            self.text[key][enstore_constants.CUR_READ] = add_commas(str(ticket[enstore_constants.BYTES_READ]))
            self.text[key][enstore_constants.CUR_WRITE] = add_commas(str(ticket[enstore_constants.BYTES_WRITTEN]))
            self.text[key][enstore_constants.FILES] = ["%s -->"%(ticket[enstore_constants.FILES][0],)]
            self.text[key][enstore_constants.FILES].append(ticket[enstore_constants.FILES][1])
            self.text[key][enstore_constants.VOLUME] = ticket[enstore_constants.CURRENT_VOLUME]
            if ticket[enstore_constants.STATE] == mover_constants.MOUNT_WAIT:
                self.text[key][enstore_constants.STATE] = "busy mounting volume %s"%\
                                                          (ticket[enstore_constants.CURRENT_VOLUME],)
            elif ticket[enstore_constants.STATE] == mover_constants.DISMOUNT_WAIT:
                self.text[key][enstore_constants.STATE] = "busy dismounting volume %s"%\
                                                          (ticket[enstore_constants.CURRENT_VOLUME],)
            # in the following 2 tests the mover state must be 'ACTIVE'
            elif ticket["mode"] == mover_constants.WRITE:
                self.text[key][enstore_constants.STATE] = "busy writing %s bytes to %s"%\
                                                          (add_commas(str(ticket[enstore_constants.BYTES_TO_TRANSFER])),
                                                           ticket[enstore_constants.CURRENT_VOLUME])
            else:
                self.text[key][enstore_constants.STATE] = "busy reading %s bytes from %s"%\
                                                          (add_commas(str(ticket[enstore_constants.BYTES_TO_TRANSFER])),
                                                           ticket[enstore_constants.CURRENT_VOLUME])
            if ticket["mode"] == mover_constants.WRITE:
                self.text[key][enstore_constants.EOD_COOKIE] = ticket[enstore_constants.CURRENT_LOCATION]
            else:
                self.text[key][enstore_constants.LOCATION_COOKIE] = ticket[enstore_constants.CURRENT_LOCATION]
        # these states imply the ticket information refers to the last transfer
        elif lcl_state in (mover_constants.IDLE, mover_constants.HAVE_BOUND,
			   mover_constants.DRAINING, mover_constants.OFFLINE,
			   mover_constants.CLEANING):
            self.text[key][enstore_constants.LAST_READ] = add_commas(str(ticket[enstore_constants.BYTES_READ]))
            self.text[key][enstore_constants.LAST_WRITE] = add_commas(str(ticket[enstore_constants.BYTES_WRITTEN]))
            if lcl_state == mover_constants.HAVE_BOUND:
                self.text[key][enstore_constants.STATE] = "HAVE BOUND volume (%s) - IDLE"%(ticket[enstore_constants.CURRENT_VOLUME],)
            else:
                self.text[key][enstore_constants.STATE] = "%s"%(lcl_state,)
            if ticket[enstore_constants.TRANSFERS_COMPLETED] > 0:
                self.text[key][enstore_constants.VOLUME] = ticket[enstore_constants.LAST_VOLUME]
                self.text[key][enstore_constants.FILES] = ["%s -->"%(ticket[enstore_constants.FILES][0],)]
                self.text[key][enstore_constants.FILES].append(ticket[enstore_constants.FILES][1])
                if ticket['mode'] == mover_constants.WRITE:
                    self.text[key][enstore_constants.EOD_COOKIE] = ticket[enstore_constants.LAST_LOCATION]
                else:
                    self.text[key][enstore_constants.LOCATION_COOKIE] = ticket[enstore_constants.LAST_LOCATION]
        # this state is an error state, we don't know if the information is valid, so do not output it
        elif lcl_state in (mover_constants.ERROR,):
            self.text[key][enstore_constants.STATE] = "ERROR - %s"%(ticket["status"],)
        # unknown state
        else:
	    if not self.text[key][enstore_constants.STATUS]:
		self.text[key][enstore_constants.STATUS] = enstore_constants.UNKNOWN_S
            self.text[key][enstore_constants.STATE] = "%s"%(ticket[enstore_constants.STATE],)
