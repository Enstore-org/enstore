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

# strip off anything before the '/'
def strip_file_dir(str):
    ind = string.rfind(str, "/")
    if not ind == -1:
	str = str[(ind+1):]

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

# parse the encp line
def parse_encp_line(line):
    [etime, enode, etmp, euser, estatus, etmp2, erest] = string.split(line, None, 6)
    if estatus == e_errors.sevdict[e_errors.INFO]:
	try:
	    [etype, erest] = string.split(erest, None, 1)
	    [erest2, erest3] = string.splitfields(erest, ": ", 1)
	    # erest2 has the file name info which we do not need, get the 
	    # total data transfer rate from the end of erest3
	    [erest2, tt] = string.splitfields(erest3, "(", 1)
	    # check if this is a read from enstore or a write to enstore
	    [tt, etmp] = string.splitfields(tt, ")",1)
	    # pull out the name of the media changer
	    mc = get_dict(etmp)
	    [tt, etmp] = string.splitfields(tt, " ",1)
	    erate = string.splitfields(erest2, " ")
	except ValueError:
	    # we do not handle this formatting
	    return []
	return [etime, enode, euser, estatus, tt, erate[0], "%s %s"%(erate[3], erate[4]), erate[6], mc, erate[3]]
    else:
	return [etime, enode, euser, estatus, erest]
	

# given a list of media changers and a log file message, see if any of the
# media changers are mentioned in the log file message
def mc_in_list(msg, mcs):
    for msgDict in msg:
        for mc in mcs:
            if mc == msgDict.get("media_changer", ""):
                return 1
    else:
        return 0

class EnStatus:

    # remove all single quotes
    def unquote(self, s):
	return string.replace(s,"'","")

    # get the eod_cookie from the ticket
    def get_eod_cookie(self, ticket, key):
        vi = ticket.get("vol_info", {})
        if vi:
	    if not self.text.has_key(key):
		self.text[key] = {}
	    self.text[key][enstore_constants.EOD_COOKIE] = vi.get("eod_cookie", "")

    # get the location cookie from the ticket
    def get_location_cookie(self, ticket, key):
        fc = ticket['work_ticket'].get('fc', {})
        if fc:
	    if not self.text.has_key(key):
		self.text[key] = {}
	    self.text[key][enstore_constants.LOCATION_COOKIE] = fc.get("location_cookie", " ")
    
    # parse the library manager queues returned from "getwork". pull out the
    # information we want and put it in a dictionary
    def parse_lm_queues(self, work, key, worktype):
	self.text[key][worktype] = []
	for mover in work:
	    # 'mover' not found in pending work
	    dict = {enstore_constants.MOVER : mover.get('mover', " ")}
	    dict[enstore_constants.ID] = mover['unique_id']
	    if mover.has_key(enstore_constants.REJECT_REASON):
		dict[enstore_constants.REJECT_REASON] = mover[enstore_constants.REJECT_REASON][0]
	    dict[enstore_constants.PORT] = mover['callback_addr'][1]
	    if mover['work'] == 'write_to_hsm':
		dict[enstore_constants.WORK] = enstore_constants.WRITE
	    else:
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
	    dict[enstore_constants.FILE_FAMILY] = vc['file_family']
	    # 'file_family_width not found in reads
	    if vc.has_key('file_family_width'):
		dict[enstore_constants.FILE_FAMILY_WIDTH] = repr(vc['file_family_width'])

	    # 'fc' not found in pending work
	    fc = mover.get('fc', "")
	    # 'external_label' not found in pending work
	    if fc:
                if fc.has_key('external_label'):
                    dict[enstore_constants.DEVICE] = fc['external_label']
	    self.text[key][worktype].append(dict)

    # output the blocksize info
    def output_blocksizes(self, info):
	if not self.text.has_key(enstore_constants.BLOCKSIZES):
	    self.text[enstore_constants.BLOCKSIZES] = {}
	for a_key in info.keys():
	    if a_key != 'status':
		self.text[enstore_constants.BLOCKSIZES][a_key] = info[a_key]

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
	if len(sus_vols) != 0:
	    self.text[key][enstore_constants.SUSPECT_VOLS] = []
	    for svol in sus_vols:
	        str = svol['external_label']+" - "
	        movers = svol['movers']
	        if len(movers) != 0:
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
	if len(work) != 0:
	    self.parse_lm_queues(work, key, enstore_constants.WORK)
	else:
	    self.text[key][enstore_constants.WORK] = enstore_constants.NO_WORK
	pending_work = ticket['pending_work']
	if len(pending_work) != 0:
	    self.parse_lm_queues(pending_work, key, enstore_constants.PENDING)
	else:
	    self.text[key][enstore_constants.PENDING] = enstore_constants.NO_PENDING

    # output the mover status
    def output_moverstatus(self, ticket, key):
	# we need to clear out the dict as keywords come and go depending on the
	# state of the mover.  save the status which is in there first.
	status = self.text[key][enstore_constants.STATUS]
	self.text[key] = {}
	self.text[key][enstore_constants.STATUS] = status
	self.text[key][enstore_constants.COMPLETED] = repr(ticket["no_xfers"])
       	if ticket["state"] == "busy":
	    self.text[key][enstore_constants.CUR_READ] = add_commas(str(ticket["rd_bytes"]))
	    self.text[key][enstore_constants.CUR_WRITE] = add_commas(str(ticket["wr_bytes"]))
	    if ticket["mode"] == "r":
	        self.text[key][enstore_constants.STATE] = "%s reading %s bytes from Enstore"%\
					(ticket["state"], 
				     add_commas(str(ticket["bytes_to_xfer"])))
                self.get_location_cookie(ticket, key)
		self.text[key][enstore_constants.FILES] = []
		self.text[key][enstore_constants.FILES].append("%s -->"%(ticket['files'][1],))
		self.text[key][enstore_constants.FILES].append(ticket['files'][0])
		self.text[key][enstore_constants.VOLUME] = ticket['tape']
	    elif ticket["mode"] == "w":
	        self.text[key][enstore_constants.STATE] = "%s writing %s bytes to Enstore"%\
					(ticket["state"], 
				     add_commas(str(ticket["bytes_to_xfer"])))
                self.get_eod_cookie(ticket, key)
		self.text[key][enstore_constants.FILES] = []
		self.text[key][enstore_constants.FILES].append("%s -->"%(ticket['files'][0],))
		self.text[key][enstore_constants.FILES].append(ticket['files'][1])
		self.text[key][enstore_constants.VOLUME] = ticket['tape']
            elif ticket["mode"] == "u":
                self.text[key][enstore_constants.STATE] = "%s dismounting volume %s"%\
					(ticket["state"], ticket['tape'])
            else:
                self.text[key][enstore_constants.STATE] = "%s??"%(ticket["state"],)

	elif ticket["state"] == "idle":
	    self.text[key][enstore_constants.LAST_READ] = add_commas(str(ticket["rd_bytes"]))
	    self.text[key][enstore_constants.LAST_WRITE] = add_commas(str(ticket["wr_bytes"]))
	    if ticket['mode'] == 'w' or ticket['mode'] == 'r':
		self.text[key][enstore_constants.STATE] = "idle - have bound volume"
	    else:
		self.text[key][enstore_constants.STATE] = "idle"
            if ticket['no_xfers'] > 0:
                work = ticket['work_ticket'].get('work', "")
                if string.find(work, "read") != -1:
                    self.get_location_cookie(ticket, key)
		    self.text[key][enstore_constants.VOLUME] = ticket['tape']
		    self.text[key][enstore_constants.FILES] = []
		    self.text[key][enstore_constants.FILES].append("%s -->"%(ticket['files'][1],))
		    self.text[key][enstore_constants.FILES].append(ticket['files'][0])
                elif string.find(work, "write") != -1:
                    self.get_eod_cookie(ticket, key)
		    self.text[key][enstore_constants.VOLUME] = ticket['tape']
		    self.text[key][enstore_constants.FILES] = []
		    self.text[key][enstore_constants.FILES].append("%s -->"%(ticket['files'][1],))
		    self.text[key][enstore_constants.FILES].append(ticket['files'][0])
        else:
	    self.text[key][enstore_constants.STATE] = ticket["state"]

    # output the library manager mover list
    def output_lmmoverlist(self, ticket, key):
	work = ticket['moverlist']
	if not self.text.has_key(key):
	    self.text[key] = {}
	self.text[key][enstore_constants.MOVERS] = []
	self.text[key][enstore_constants.KNOWN_MOVERS] = []
	if len(work) != 0:
	    for mover in work:
		self.text[key][enstore_constants.KNOWN_MOVERS].append([mover['mover'], 
						     mover['address'][1],
						     mover['state'], 
					    enstore_functions.format_time(mover['last_checked']),
						     mover['summon_try_cnt']])
		# keep a  separate list of the mover names so can easily
		# find if a mover belongs to this library_manager
		self.text[key][enstore_constants.MOVERS].append(mover['mover'])
