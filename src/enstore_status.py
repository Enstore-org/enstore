#
# system import
import sys
import time
import copy
import errno
import regsub
import string
import os
import stat

# enstore imports
import traceback
import Trace
import e_errors
import generic_cs
import log_client

# define file types
ascii_file = 0
html_file = 1
force = 1

html_header1 = "<title>Inquisitor</title>\n"+\
              "<meta http-equiv=\"Refresh\" content=\""
html_header2 = "\">\n"+\
              "<body bgcolor=\"FFFFFF\">\n<pre>\n"

class EnstoreStatus:

    def __init__(self, file, fileType, hname, max_ascii_size, verbose=0):
        Trace.trace(10,'{__init__ essfile '+file+"  "+repr(fileType))
        self.file_name = file 
	self.file_type = fileType
	self.refresh = 8
	self.max_ascii_size = max_ascii_size
	self.set_header()
	self.text = {}
        Trace.trace(10,'}__init__')

    # set the header
    def set_header(self):
	self.header = html_header1+repr(self.refresh)+html_header2

    # write the header to the file
    def write_header(self):
        Trace.trace(12,"{write_header "+self.header)
	self.file.write(self.header)
        Trace.trace(12,"}write_header ")

    # open the file
    def open(self, verbose=0):
        Trace.trace(12,"{open "+self.file_name)
	generic_cs.enprint("opening " + self.file_name, generic_cs.SERVER, \
	                    verbose)
        # try to open status file for append
        try:
            self.file = open(self.file_name, 'a')
            generic_cs.enprint("opened for append", generic_cs.SERVER, verbose)
        except:
            self.file = open(self.file_name, 'w')
            generic_cs.enprint("opened for write", generic_cs.SERVER, verbose)
        Trace.trace(12,"}open")

    # close the file
    def close(self):
        Trace.trace(12,"{close "+self.file_name)
	if self.file_type == html_file:
	    self.file.write("</pre></body>\n")
	self.file.close()
        Trace.trace(12,"}close")

    # flush everything to the file
    def flush(self):
        Trace.trace(10,'{flush')
	# well, nothing has really been written to the file, it is all stored
	# in a hash.  so we must write it all now
	self.file.write("\nENSTORE SYSTEM STATUS\n")
	keys = self.text.keys()
	keys.sort()
	for key in keys:
	    self.file.write(self.text[key])

	self.file.flush()
        Trace.trace(10,'}flush')

    # remove something from the text hash that will be written to the files
    def remove_key(self, key):
	if self.text.has_key(key):
	    del self.text[key]

    # remove all single quotes
    def unquote(self, string):
        Trace.trace(12,"{unquote "+repr(string))
	return regsub.gsub("\'", "", string)
        Trace.trace(12,"}unquote ")

    # move the file to a timestamped backup copy
    def timestamp(self, really=0):
	Trace.trace(11,"{timestamp "+self.file_name)
	s = os.stat(self.file_name)
	if (self.max_ascii_size > 0) or (really == 1):
	    if (s[stat.ST_SIZE] >= self.max_ascii_size) or (really == 1):
	        self.close()
	        ts = regsub.gsub(" ", "_", self.format_time(time.time()))
	        os.system("mv "+self.file_name+" "+self.file_name+"."+ts)
	        self.open()
        Trace.trace(11,"}timestamp ")

    # translate time.time output to a person readable format.
    # strip off the day and reorganize things a little
    def format_time(self, theTime):
        Trace.trace(12,"{format_time ")
	ftime = time.strftime("%c", time.localtime(theTime))
	(dow, mon, day, tod, year) = string.split(ftime)
	ntime = "%s-%s-%s %s" % (year, mon, day, tod)
        Trace.trace(12,"}format_time ")
	return ntime

    # format the status, just use the first element
    def format_status(self, status):
        Trace.trace(12,"{format_status "+repr(status))
	return self.unquote(status[0])
        Trace.trace(12,"}format_status ")

    # format the ip address - replace the ip address with the actual host name
    def format_ip_address(self, host, address):
        Trace.trace(12,"{format_ip_address "+repr(host)+" "+repr(address))
	return "("+self.unquote(host)+", "+repr(address[1])+")"
        Trace.trace(12,"}format_ip_address ")

    # parse the library manager queues returned from "getwork"
    def parse_lm_queues(self, work, spacing, prefix):
	Trace.trace(13,"{parse_lm_queues")
	string = prefix
	first_line_spacing = ""
	for mover in work:
	    callback_addr = mover['callback_addr']
	    encp = mover['encp']
	    # not found in pending work
	    try:
	        fc = mover['fc']
	    except:
	        pass
	    times = mover['times']
	    vc = mover['vc']
	    wrapper = mover['wrapper']
	    machine = wrapper['machine']

	    string = string+first_line_spacing

	    # not found in pending work
	    try:
	        string = string+mover['mover']+", "
	    except:
	        pass
	    string = string+"from NODE: "+self.unquote(machine[1])+" ("+\
	             self.unquote(machine[0])+"),  PORT: "+\
	             repr(callback_addr[1])
	    if mover['work'] == 'write_to_hsm':
	        string = string+spacing+"WRITE to: "
	    else:
		string = string+spacing+"READ to: "
	    string = string+wrapper['fullname']+",  BYTES: "+\
	             repr(wrapper['size_bytes'])
	    # not found in pending work
	    try:
	        string = string+spacing+"DEVICE LABEL: "+fc['external_label']+\
	             ",  "
	    except:
	        string = string+spacing
	    string = string+"FILE FAMILY: "+vc['file_family']
	    # not found in reads
	    try:
	        string = string+",  FILE FAMILY WIDTH: "+\
	                 repr(vc['file_family_width'])
	    except:
	        pass
	    string = string+spacing+"PRIORITIES:  CURRENT "+\
	             repr(encp['curpri'])+",  BASE "+repr(encp['basepri'])+\
	             ",  DELTA "+repr(encp['delpri'])+"  and  AGETIME: "+\
	             repr(encp['agetime'])
	    string = string+spacing+"JOB SUBMITTED: "+\
	             self.format_time(times['t0'])
	    # not found in pending work
	    try:
	        string = string+",  DEQUEUED: "+\
	                 self.format_time(times['lm_dequeued'])
	    except:
	        pass
	    # not found in reads
	    try:
	        string = string+spacing+"FILE MODIFIED: "+\
	                 self.format_time(wrapper['mtime'])
	    except:
	        pass
	    string = string+"\n"

	    # reset this to prepare for another queued element
	    first_line_spacing = spacing
	string = string+"\n"
	Trace.trace(13,"}parse_lm_queues")
	return string

    # format the library manager work queues for output
    def format_lm_queues(self, ticket):
        Trace.trace(12,"{format_lm_queues "+repr(ticket))
	string = "    Work for: "
	spacing = "\n          "
	work = ticket['at movers']
	if len(work) != 0:
	    string = self.parse_lm_queues(work, spacing, string)
	else:
	    string = "    No work at movers\n"
	pending_work = ticket['pending_work']
	if len(pending_work) != 0:
	    string = string+"\n    Pending work: "
	    string = self.parse_lm_queues(pending_work, spacing, string)
	else:
	    string = string+"    No pending work\n\n"

        Trace.trace(12,"}format_lm_queues ")
	return string

    def format_lm_suspect_vols(self, ticket):
        Trace.trace(12,"{format_lm_suspect_vols "+repr(ticket))
	str =     "\n    SUSPECT VOLUMES : "
	spacing = "\n                      "
	sus_vols = ticket['suspect_volumes']
	if len(sus_vols) != 0:
	    for svol in sus_vols:
	        str = str+svol['external_label']+" - "
	        movers = svol['movers']
	        if len(movers) != 0:
	            not_first_one = 0
	            for mover in movers:
	                if not_first_one:
	                    str=str+", "
	                str = str+mover
	                not_first_one = 1
	        str = str+spacing
	else:
	    str = str + "NONE"
        Trace.trace(12,"}format_lm_suspect_vols ")
	return str+"\n\n"

    # parse the library manager moverlist ticket
    def parse_lm_moverlist(self, work):
        Trace.trace(13,"{parse_lm_moverlist")
	string = "    KNOWN MOVER           PORT    STATE         LAST SUMMONED        TRY COUNT\n"
	for mover in work:
	    (address, port) = mover['address']
	    time = self.format_time(mover['last_checked'])
	    string = string+"    %(m)-18.18s    %(p)-4.4d    %(s)-10.10s    %(lc)-20.20s    %(tc)-3d\n" % {'m':mover['mover'], 'p':port, 's':mover['state'], 'lc':time, 'tc':mover['summon_try_cnt']}

	string = string+"\n"
        Trace.trace(13,"}parse_lm_moverlist")
	return string

    # format the library manager mover list for output
    def format_lm_moverlist(self, ticket):
        Trace.trace(12,"{format_lm_moverlist "+repr(ticket))
	string = "    Known Movers: "
	spacing = "\n                 "
	work = ticket['moverlist']
	if len(work) != 0:
	    string = self.parse_lm_moverlist(work)
	else:
	    string = "    No moverlist\n"

        Trace.trace(12,"}format_lm_moverlist ")
	return string

    # format the mover status information
    def format_moverstatus(self, ticket):
        Trace.trace(12,"{format_moverstatus "+repr(ticket))
	spacing = "\n    "
	string = spacing+"Completed Transfers : "+repr(ticket["no_xfers"])
	if ticket["state"] == "busy":
	    p = "Current Transfer : "
	    if ticket["mode"] == "r":
	        m = " reading from HSM"
	    else:
	        m = " writing to HSM"
	elif ticket["state"] == "idle":
	    p = "Last Transfer : "
	    m = " "

	string = string+",  Current State : "+ticket["state"]+m
	string = string+spacing+p+" Read "+\
	             repr(ticket["rd_bytes"])+" bytes,  Wrote "+\
	             repr(ticket["wr_bytes"])+" bytes\n\n" 
        Trace.trace(12,"}format_moverstatus ")
	return string

    # format the encp info taken from the log file
    def format_encp(self, lines, key):
	Trace.trace(13,"{format_encp ")
	prefix =  "\n                     "
	str = key+"            : "
	spacing = ""
	# break up each line into it's component parts, format it and save it
	for line in lines:
	    [etime, enode, etmp, euser, estatus, etmp2, erest] = \
	                          string.split(line, None, 6)
	    str = str+spacing+etime+" on "+enode+" by "+euser
	    spacing = "       "
	    if estatus == log_client.sevdict[log_client.INFO]:
	        [etmp3, ctime]=string.splitfields(line,"cumt=")
	        # remove final cr from ctime
	        otime = string.splitfields(ctime)
	        str = str+" (total time : "+otime[0]+" secs)"
	        [str1, str2, erest2] = string.splitfields(erest,":", 2)
	        str = str+prefix+string.replace(str2, "requester","")+"\n"
	    else:
	        # there was an error or warning
	        [str1, str2, erest2] = string.splitfields(erest, ":", 2)
	        str = str+prefix+str1+" : "+str2+prefix+erest2
	Trace.trace(13,"}format_encp ")
	return str

    # output the encp info
    def output_encp(self, lines, key, verbose):
	Trace.trace(12,"{output_encp ")
	if lines != []:
	    str = self.format_encp(lines, key)
	else:
	    str = "encp            : NONE"
	generic_cs.enprint(str, generic_cs.SERVER|generic_cs.PRETTY_PRINT,\
	                   verbose)
	self.text[key] = str+"\n"
	Trace.trace(12,"}output_encp ")

    # output the blocksize info
    def output_blocksizes(self, info, prefix, key):
        Trace.trace(12,"{output_blocksizes ")
	prefix2 = "                  "
	str = prefix
	ctr = 0
	for a_key in info.keys():
	    if a_key != 'status':
	        if ctr == 3:
	            str = str+",\n"+prefix2
	            ctr = 0
	        elif ctr > 0:
	            str = str+",  "
	        ctr = ctr+1
	        str = str+a_key+" : "+repr(info[a_key])
	self.text[key] = str+"\n"
        Trace.trace(12,"}output_blocksizes ")

    # output the passed alive status
    def output_alive(self, host, tag, status, time, key):
        Trace.trace(12,"{output_alive "+repr(tag)+" "+repr(host))
	ftime = self.format_time(time)
	str = self.unquote(tag)+self.unquote(status['work'])+" on "+\
	      self.format_ip_address(host, status['address'])+" at "+\
	      ftime+"\n"
	self.text[key] = str
        Trace.trace(12,"}output_alive")

    # output the timeout error
    def output_etimedout(self, address, tag, time, key):
        Trace.trace(12,"{output_etimedout "+repr(tag)+" "+repr(address))
	ftime = self.format_time(time)
	str = tag + "timed out on "+self.unquote(repr(address))+" at "+\
	       ftime+"\n"
	self.text[key] = str
        Trace.trace(12,"}output_etimedout")

    # output timeout error when trying to get config dict from config server
    def output_noconfigdict(self, tag, time, key):
        Trace.trace(12,"{output_noconfigdict "+repr(tag))
	ftime = self.format_time(time)
	str = tag + "timed out while getting config dict at "+ftime+"\n"
	self.text[key] = str
        Trace.trace(12,"}output_noconfigdict")

    # output a line stating that we do not support this server
    def output_nofunc(self, key):
        Trace.trace(12,"}output_nofunc"+key)
	str = key+" : NOT SUPPORTED IN INQUISITOR\n"
	self.text[key] = str
        Trace.trace(12,"}output_nofunc")

    # output the library manager suspect volume list
    def output_suspect_vols(self, ticket, key, verbose):
        Trace.trace(12,"{output_suspect_vols "+repr(ticket))
	sm = self.format_lm_suspect_vols(ticket)
	generic_cs.enprint(sm, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                   verbose)
	self.text[key] = self.text[key]+sm
        Trace.trace(12,"}output_suspect_vols")

    # output the library manager queues
    def output_lmqueues(self, ticket, key, verbose):
        Trace.trace(12,"{output_lmqueues "+repr(ticket))
	fq = self.format_lm_queues(ticket)
	generic_cs.enprint(fq, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                   verbose)
	self.text[key] = self.text[key]+fq
        Trace.trace(12,"}output_lmqueues ")

    # output the library manager queues
    def output_moverstatus(self, ticket, key, verbose):
        Trace.trace(12,"{output_moverstatus "+repr(ticket))
	fs = self.format_moverstatus(ticket)
	generic_cs.enprint(fs, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                   verbose)
	self.text[key] = self.text[key]+fs
        Trace.trace(12,"}output_moverstatus")

    # output the library manager mover list
    def output_lmmoverlist(self, ticket, key, verbose):
        Trace.trace(12,"{output_lmmoverlist "+repr(ticket))
	fq = self.format_lm_moverlist(ticket)
	generic_cs.enprint(fq, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                   verbose)
	self.text[key] = self.text[key]+fq
        Trace.trace(12,"}output_lmmoverlist ")

    # set a new timestamp value
    def set_max_ascii_size(self, value):
	self.max_ascii_size = value

    # get the timestamp value
    def get_max_ascii_size(self):
	return self.max_ascii_size

    # reset the refresh
    def set_refresh(self, value):
	self.refresh = value
	self.set_header()

    # return the current refresh value
    def get_refresh(self):
	return self.refresh
