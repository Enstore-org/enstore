#
# system import
import sys
import time
import string
import os
import stat

# enstore imports
import Trace
import alarm
import e_errors
import enstore_constants
import enstore_functions
import mover_constants

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
                if len(dicts) == 1:
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

    def __init__(self, line):
	self.line = line
	[self.time, self.node, self.pid, self.user, self.status, self.server, 
	 self.text] = string.split(line, None, 6)
	# parse all success messages and pull out the interesting information
	if self.status == e_errors.sevdict[e_errors.INFO]:
	    try:
		# split out the message type from the rest of the message text
		[self.msg_type, self.text] = string.splitfields(self.text, None, 1)
		[tmp1, tmp2] = string.splitfields(self.text, ": ", 1)
		# get the file names (tmp_list[2] = "->" so ignore it)
		tmp_list = string.splitfields(tmp1, None)
		self.work = tmp_list[0]
		self.infile = tmp_list[1]
		self.outfile = tmp_list[3]
		# get the total data transfer rate
		[tmp1, tmp2] = string.splitfields(tmp2, "(", 1)
		[self.xfer_rate, tmp2] = string.splitfields(tmp2, " ",1)
		# pull out the name of the media changer
		self.mc = get_dict(tmp2)
		tmp_list = string.splitfields(tmp1, " ")
		self.bytes = tmp_list[0]
		self.direction = tmp_list[3]
		self.volume = tmp_list[4]
		self.user_rate = tmp_list[6]
		self.valid = 1
	    except ValueError:
		# we do not handle this formatting
		self.valid = 0
	else:
	    # get rid of the MSG_TYPE=xxx information at the end of the line
	    aList = string.splitfields(self.text, Trace.MSG_TYPE)
	    # some of the lines  do not have MSG_TYPE in them (??? hmmm) so we cannot count on
	    # aList being any more than 1 element long.
	    self.text = aList[0]
	    self.valid = 1

class EnStatus:

    # remove all single quotes
    def unquote(self, s):
	return string.replace(s,"'","")

    # parse the library manager queues returned from "getwork". pull out the
    # information we want and put it in a dictionary
    def parse_lm_queues(self, work, key, worktype, writekey, readkey):
	self.text[key][worktype] = []
	for mover in work:
	    # 'mover' not found in pending work
	    dict = {enstore_constants.MOVER : mover.get('mover', " ")}
	    dict[enstore_constants.ID] = mover['unique_id']
	    if mover.has_key(enstore_constants.REJECT_REASON):
		dict[enstore_constants.REJECT_REASON] = mover[enstore_constants.REJECT_REASON][0]
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

	    wrapper = mover['wrapper']
	    dict[enstore_constants.FILE] = wrapper['fullname']
	    dict[enstore_constants.BYTES] = add_commas(str(wrapper['size_bytes']))

	    # 'mtime' not found in reads
	    if wrapper.has_key('mtime'):
		dict[enstore_constants.MODIFICATION] = enstore_functions.format_time(wrapper['mtime'])

	    machine = wrapper['machine']
	    dict[enstore_constants.NODE] = self.unquote(machine[1])

	    times = mover['times']
	    dict[enstore_constants.SUBMITTED] = enstore_functions.format_time(times['t0'])
	    # 'lm_dequeued' not found in pending work
	    if times.has_key('lm_dequeued'):
		dict[enstore_constants.DEQUEUED] = enstore_functions.format_time(times['lm_dequeued'])

	    vc = mover['vc']
	    # 'file_family' is not present in a read, use volume family instead
	    if vc.has_key('file_family'):
		dict[enstore_constants.FILE_FAMILY] = vc['file_family']
		dict[enstore_constants.FILE_FAMILY_WIDTH] = repr(vc.get('file_family_width', ""))
	    elif vc.has_key('volume_family'):
		dict[enstore_constants.VOLUME_FAMILY] = vc['volume_family']

	    # 'fc' not found in pending work
	    fc = mover.get('fc', "")
	    # 'external_label' not found in pending work
	    if fc:
                if fc.has_key('external_label'):
                    dict[enstore_constants.DEVICE] = fc['external_label']
	    self.text[key][worktype].append(dict)

    # output the passed alive status
    def output_alive(self, host, port, state, time, key):
	if not self.text.has_key(key):
	    self.text[key] = {}
	self.text[key][enstore_constants.STATUS] = [state, 
						    self.unquote(host), 
						    repr(port), 
						    enstore_functions.format_time(time)]

    # output the timeout error
    def output_etimedout(self, host, port, state, time, key, last_time=0):
	if last_time == -1:
	    ltime = enstore_constants.NO_INFO
	else:
	    ltime = enstore_functions.format_time(last_time)
	if not self.text.has_key(key):
	    self.text[key] = {}
	self.text[key][enstore_constants.STATUS] = [state, self.unquote(host),
						    repr(port), 
						    enstore_functions.format_time(time), ltime]

    # output timeout error when trying to get config dict from config server
    def output_noconfigdict(self, state, time, key):
	if not self.text.has_key(key):
	    self.text[key] = {}
	self.text[key][enstore_constants.STATUS] = [ state, 
						     enstore_constants.NO_INFO,
						     enstore_constants.NO_INFO,
						     enstore_functions.format_time(time)]

    # output a line stating that we do not support this server
    def output_nofunc(self, key):
	if not self.text.has_key(key):
	    self.text[key] = {}
	self.text[key][enstore_constants.STATUS] = ["NO SUPPORT IN INQ", 
						    enstore_constants.NO_INFO,
						    enstore_constants.NO_INFO, 
						    enstore_constants.NO_INFO]

    # output the library manager suspect volume list
    def output_suspect_vols(self, ticket, key):
	sus_vols = ticket['suspect_volumes']
	if not self.text.has_key(key):
	    self.text[key] = {}
	if sus_vols:
	    self.text[key][enstore_constants.SUSPECT_VOLS] = []
	    for svol in sus_vols:
	        str = svol['external_label']+" - "
	        movers = svol['movers']
	        if movers:
	            not_first_one = 0
	            for mover in movers:
	                if not_first_one:
	                    str=str+", "
	                str = str+mover
	                not_first_one = 1
	        self.text[key][enstore_constants.SUSPECT_VOLS].append(str)
	else:
	    self.text[key][enstore_constants.SUSPECT_VOLS] = ["None"]

    # output the state of the library manager
    def output_lmstate(self, ticket, key):
	if not self.text.has_key(key):
	    self.text[key] = {}
	self.text[key][enstore_constants.LMSTATE] = ticket['state']

    # output the library manager queues
    def output_lmqueues(self, ticket, key):
	work = ticket['at movers']
	if not self.text.has_key(key):
	    self.text[key] = {}
	self.text[key][enstore_constants.TOTALPXFERS] = 0
	self.text[key][enstore_constants.READPXFERS] = 0
	self.text[key][enstore_constants.WRITEPXFERS] = 0
	self.text[key][enstore_constants.TOTALONXFERS] = 0
	self.text[key][enstore_constants.READONXFERS] = 0
	self.text[key][enstore_constants.WRITEONXFERS] = 0
	if work:
	    self.parse_lm_queues(work, key, enstore_constants.WORK, 
				 enstore_constants.WRITEONXFERS,
				 enstore_constants.READONXFERS)
	    self.text[key][enstore_constants.TOTALONXFERS] = self.text[key][enstore_constants.READONXFERS] + self.text[key][enstore_constants.WRITEONXFERS]
	else:
	    self.text[key][enstore_constants.WORK] = enstore_constants.NO_WORK
	pending_work = ticket['pending_work']
	if pending_work:
	    self.parse_lm_queues(pending_work, key, enstore_constants.PENDING,
				 enstore_constants.WRITEPXFERS,
				 enstore_constants.READPXFERS)
	    self.text[key][enstore_constants.TOTALPXFERS] = self.text[key][enstore_constants.READPXFERS] + self.text[key][enstore_constants.WRITEPXFERS]
	else:
	    self.text[key][enstore_constants.PENDING] = enstore_constants.NO_PENDING

    # output the mover status
    def output_moverstatus(self, ticket, key):
	# clean out all the old info but save the status
	self.text[key] = {enstore_constants.STATUS : self.text[key][enstore_constants.STATUS]}
	self.text[key][enstore_constants.COMPLETED] = repr(ticket["transfers_completed"])
	self.text[key][enstore_constants.FAILED] = repr(ticket["transfers_failed"])
	# these are the states where the information  in the ticket refers to a current transfer
       	if ticket["state"] in (mover_constants.ACTIVE, mover_constants.MOUNT_WAIT,
			       mover_constants.DISMOUNT_WAIT):
	    self.text[key][enstore_constants.CUR_READ] = add_commas(str(ticket["bytes_read"]))
	    self.text[key][enstore_constants.CUR_WRITE] = add_commas(str(ticket["bytes_written"]))
	    self.text[key][enstore_constants.FILES] = ["%s -->"%(ticket['files'][0],)]
	    self.text[key][enstore_constants.FILES].append(ticket['files'][1])
	    self.text[key][enstore_constants.VOLUME] = ticket['current_volume']
	    if ticket["state"] == mover_constants.MOUNT_WAIT:
		self.text[key][enstore_constants.STATE] = "busy mounting volume %s"%\
							  (ticket['current_volume'],)
	    elif ticket["state"] == mover_constants.DISMOUNT_WAIT:
		self.text[key][enstore_constants.STATE] = "busy dismounting volume %s"%\
							  (ticket['current_volume'],)
	    # in the following 2 tests the mover state must be 'ACTIVE'
	    elif ticket["mode"] == mover_constants.WRITE:
		self.text[key][enstore_constants.STATE] = "busy writing %s bytes to Enstore"%\
							  (add_commas(str(ticket["bytes_to_transfer"])),)
	    else:
		self.text[key][enstore_constants.STATE] = "busy reading %s bytes from Enstore"%\
							  (add_commas(str(ticket["bytes_to_transfer"])),)
	    if ticket["mode"] == mover_constants.WRITE:
		self.text[key][enstore_constants.EOD_COOKIE] = ticket["current_location"]
	    else:
		self.text[key][enstore_constants.LOCATION_COOKIE] = ticket["current_location"]
	# these states imply the ticket information refers to the last transfer
	elif ticket["state"] in (mover_constants.IDLE, mover_constants.HAVE_BOUND,
				 mover_constants.DRAINING, mover_constants.OFFLINE,
				 mover_constants.CLEANING):
	    self.text[key][enstore_constants.LAST_READ] = add_commas(str(ticket["bytes_read"]))
	    self.text[key][enstore_constants.LAST_WRITE] = add_commas(str(ticket["bytes_written"]))
	    if ticket['state'] == mover_constants.HAVE_BOUND:
		self.text[key][enstore_constants.STATE] = "IDLE - have bound volume"
	    else:
		self.text[key][enstore_constants.STATE] = ticket['state']
	    if ticket['transfers_completed'] > 0:
		self.text[key][enstore_constants.VOLUME] = ticket['last_volume']
		self.text[key][enstore_constants.FILES] = ["%s -->"%(ticket['files'][0],)]
		self.text[key][enstore_constants.FILES].append(ticket['files'][1])
		if ticket['mode'] == mover_constants.WRITE:
		    self.text[key][enstore_constants.EOD_COOKIE] = ticket["last_location"]
		else:
		    self.text[key][enstore_constants.LOCATION_COOKIE] = ticket["last_location"]
	# this state is an error state, we don't know if the information is valid, so do not output it
        elif ticket["state"] in (mover_constants.ERROR,):
	    self.text[key][enstore_constants.STATE] = "ERROR - %s"%(ticket["status"],)
	# unknown state
	else:
	    self.text[key][enstore_constants.STATE] = ticket["state"]
