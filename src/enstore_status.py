#
# system import
import sys
import time
import regsub
import string
import os
import stat
import errno

# enstore imports
import Trace
import generic_cs
import log_client

# used to force timestamping
FORCE = 1

# ENCP line pieces from log file
ETIME = 0
ENODE = 1
EUSER = 2
ESTATUS = 3
EXRATE = 4
EBYTES = 5
EDEV = 6
EURATE = 7

# different MOUNT line pieces from log file
MDEV = 4
MSTART = 5

# message is either a mount request or an actual mount
MREQUEST = 0
MMOUNT = 1

TRUE = 1
FALSE = 0
START_TIME = "start_time"
STOP_TIME = "stop_time"
LOG_PREFIX = "LOG-"
bg_color = "FFFFFF"
tdata = "<TD NOSAVE>"
trow = "<TR NOSAVE>\n"
tdata_end = "</TD>\n"

html_header1 = "<title>Enstore Status</title>\n"+\
               "<meta http-equiv=\"Refresh\" content=\""
html_header2 = "\">\n"+\
               "<body bgcolor=\""+bg_color+"\">\n"
html_header3 = "<pre>\n"

# translate time.time output to a person readable format.
# strip off the day and reorganize things a little
def format_time(theTime, sep=" "):
    Trace.trace(12,"{format_time '"+sep+"'")
    ftime = time.strftime("%Y-%b-%d"+sep+"%H:%M:%S", time.localtime(theTime))
    Trace.trace(12,"}format_time ")
    return ftime

# format the timestamp value
def get_ts():
    return format_time(time.time(), "_")

# strip off anything before the '/'
def strip_file_dir(str):
    Trace.trace(10,"{strip_file_dir "+str)
    ind = string.rfind(str, "/")
    if not ind == -1:
	str = str[(ind+1):]
    Trace.trace(10,"}strip_file_dir ")

# parse the encp line
def parse_encp_line(line):
    Trace.trace(12,"{parse_encp_line "+repr(line))
    [etime, enode, etmp, euser, estatus, etmp2, erest] = \
                                                   string.split(line, None, 6)
    if 0: print etmp,etmp2 # quiet lint
    if estatus == e_errors.sevdict[e_errors.INFO]:
        [erest2, erest3] = string.splitfields(erest, ":", 1)
        # erest2 has the file name info which we do not need, get the 
        # total data transfer rate from the end of erest3
        [erest2, tt] = string.splitfields(erest3, "(", 1)
        [tt, etmp] = string.splitfields(tt, ")",1)
        [tt, etmp] = string.splitfields(tt, " ",1)
	erate = string.splitfields(erest2, " ")
    else:
        # there was an error or warning
        try:
            [str1, str2, erest2] = string.splitfields(erest, ":", 2)
	    Trace.trace(12,"}parse_encp_line ")
	    return [etime, enode, euser, estatus, str1, str2, erest2]
        except:
            # the leftover text was formatted funny, just output it
	    Trace.trace(12,"}parse_encp_line ")
	    return [etime, enode, euser, estatus, erest]
    Trace.trace(12,"}parse_encp_line ")
    return [etime, enode, euser, estatus, tt, erate[1], erate[5], erate[7]]

class EnStatus:

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
	ftime = format_time(time)
	str = self.unquote(tag)+self.unquote(status['work'])+" on "+\
	      self.format_ip_address(host, status['address'])+" at "+\
	      ftime+"\n"
	self.text[key] = str
        Trace.trace(12,"}output_alive")

    # output the timeout error
    def output_etimedout(self, address, tag, time, key, last_time=0):
        Trace.trace(12,"{output_etimedout "+repr(tag)+" "+repr(address))
	ftime = format_time(time)
	str = tag + "timed out on "+self.unquote(repr(address))+" at "+\
	       ftime+"\n"
	if not last_time == 0:
	    i = len(tag)
	    if not last_time == -1:
	        ltime = format_time(last_time)
	    else:
		ltime = "----"
	    str = str+i*" "+"last alive at "+ltime+"\n"
	self.text[key] = str
        Trace.trace(12,"}output_etimedout")

    # output timeout error when trying to get config dict from config server
    def output_noconfigdict(self, tag, time, key):
        Trace.trace(12,"{output_noconfigdict "+repr(tag))
	ftime = format_time(time)
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

    # remove all single quotes
    def unquote(self, string):
        if 0: print self # quiet lint
        Trace.trace(12,"{unquote "+repr(string))
	return regsub.gsub("\'", "", string)
        Trace.trace(12,"}unquote ")

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
	             format_time(times['t0'])
	    # not found in pending work
	    try:
	        string = string+",  DEQUEUED: "+\
	                 format_time(times['lm_dequeued'])
	    except:
	        pass
	    # not found in reads
	    try:
	        string = string+spacing+"FILE MODIFIED: "+\
	                 format_time(wrapper['mtime'])
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
        if 0: print self # quiet lint
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
        if 0: print self # quiet lint
        Trace.trace(13,"{parse_lm_moverlist")
	string = "    KNOWN MOVER           PORT    STATE         LAST SUMMONED        TRY COUNT\n"
	for mover in work:
	    (address, port) = mover['address']
	    time = format_time(mover['last_checked'])
	    string = string+"    %(m)-18.18s    %(p)-4.4d    %(s)-10.10s    %(lc)-20.20s    %(tc)-3d\n" % {'m':mover['mover'], 'p':port, 's':mover['state'], 'lc':time, 'tc':mover['summon_try_cnt']}

	string = string+"\n"
        if 0: print address # quiet lint
        Trace.trace(13,"}parse_lm_moverlist")
	return string

    # format the library manager mover list for output
    def format_lm_moverlist(self, ticket):
        Trace.trace(12,"{format_lm_moverlist "+repr(ticket))
	string = "    Known Movers: "
	work = ticket['moverlist']
	if len(work) != 0:
	    string = self.parse_lm_moverlist(work)
	else:
	    string = "    No moverlist\n"

        Trace.trace(12,"}format_lm_moverlist ")
	return string

    # format the mover status information
    def format_moverstatus(self, ticket):
        if 0: print self # quiet lint
        Trace.trace(12,"{format_moverstatus "+repr(ticket))
	spacing = "\n    "
	string = spacing+"Completed Transfers : "+repr(ticket["no_xfers"])
	if ticket["state"] == "busy":
	    p = "Current Transfer : "
	    if ticket["mode"] == "r":
	        m = " reading "+repr(ticket["bytes_to_xfer"])+" bytes from the HSM"
	    else:
	        m = " writing "+repr(ticket["bytes_to_xfer"])+" bytes to the HSM"
	elif ticket["state"] == "idle":
	    p = "Last Transfer : "
	    m = " "

	string = string+",  Current State : "+ticket["state"]+m
	string = string+spacing+p+" Read "+\
	             repr(ticket["rd_bytes"])+" bytes,  Wrote "+\
	             repr(ticket["wr_bytes"])+" bytes\n\n"
        Trace.trace(12,"}format_moverstatus ")
	return string

class EnFile:

    def __init__(self, file):
        Trace.trace(10,'{__init__ enfile '+file)
        self.file_name = file 
	self.filedes = 0
        Trace.trace(10,'}__init__')

    def open(self, mode='w'):
	Trace.trace(10,"{enfile open "+self.file_name)
	try:
            self.filedes = open(self.file_name, mode)
            Trace.trace(10,"}enfile open ")
        except IOError:
            self.filedes = 0
            Trace.trace(10,"}enfile not open")

    def close(self):
	Trace.trace(10,"{enfile close "+self.file_name)
	if not self.filedes == 0:
	    self.filedes.close()
	    self.filedes = 0
	Trace.trace(10,"}enfile close ")

class EnStatusFile(EnFile):

    def __init__(self, file):
        Trace.trace(10,'{__init__ essfile '+file)
	EnFile.__init__(self, file)
	self.text = {}
        Trace.trace(10,'}__init__')

    # open the file
    def open(self, verbose=0):
        Trace.trace(12,"{open "+self.file_name)
	generic_cs.enprint("opening " + self.file_name, generic_cs.SERVER, \
	                    verbose)
        # try to open status file for append
        try:
            self.filedes = open(self.file_name, 'a')
            generic_cs.enprint("opened for append", generic_cs.SERVER, verbose)
        except IOError:
            self.filedes = 0
	    Trace.trace(12, "}enStatusFile not open")
        except:
            self.filedes = open(self.file_name, 'w')
            generic_cs.enprint("opened for write", generic_cs.SERVER, verbose)
        Trace.trace(12,"}open")

    # flush everything to the file
    def flush(self):
        Trace.trace(10,'{flush')
	# well, nothing has really been written to the file, it is all stored
	# in a hash.  so we must write it all now
        if not self.filedes == 0:
            self.filedes.write("\nENSTORE SYSTEM STATUS\n")
            keys = self.text.keys()
            keys.sort()
            for key in keys:
                self.filedes.write(self.text[key])

            self.filedes.flush()
        Trace.trace(10,'}flush')

    # remove something from the text hash that will be written to the files
    def remove_key(self, key):
	Trace.trace(10,"{remove_key "+repr(key))
	if self.text.has_key(key):
	    del self.text[key]
	Trace.trace(10,"}remove_key ")

class EnHTMLFile:

    def __init__(self, refresh):
        Trace.trace(10,'{__init__ enhtmlfile ')
	if refresh == -1:
	    self.refresh = 120
	else:
	    self.refresh = refresh
	self.set_header()
        Trace.trace(10,'}__init__')

    # close the file
    def close(self):
        Trace.trace(12,"{close "+self.file_name)
        if not self.filedes == 0:
            self.filedes.write(self.trailer)
            self.filedes.close()
        Trace.trace(12,"}close")

    # reset the header, the refresh has changed
    def set_header(self):
	Trace.trace(10,"{set_header ")
	self.header = html_header1+repr(self.refresh)+html_header2
	Trace.trace(10,"}set_header ")

    # reset the refresh
    def set_refresh(self, value):
	Trace.trace(10,"{set_refresh ")
	self.refresh = value
	self.set_header()
	Trace.trace(10,"}set_refresh ")

    # return the current refresh value
    def get_refresh(self):
	Trace.trace(10,"{}get_refresh ")
	return self.refresh

class EncpFile:

    # output the encp info
    def output_encp(self, lines, key):
	Trace.trace(12,"{output_html_encp ")
	if lines != []:
	    str = self.format_encp(lines, key)
	else:
	    str = self.format_no_encp()
	self.text[key] = str+"\n"
	Trace.trace(12,"}output_html_encp ")

    # format the line saying there have been no encp requests
    def format_no_encp(self):
        if 0: print self # quiet lint
	Trace.trace(10,"{}format_no_encp ")
	return "\nencp            : NONE\n"

class HTMLStatusFile(EnHTMLFile, EnStatusFile, EnStatus):

    def __init__(self, file, refresh):
        Trace.trace(10,'{__init__ htmlstatusfile ')
	EnStatusFile.__init__(self, file)
	EnHTMLFile.__init__(self, refresh)
	self.header2 = html_header3
	self.trailer = "</pre></body>\n"
        Trace.trace(10,'}__init__')

    # open the file and write the header to the file
    def open(self, verbose=0):
        Trace.trace(12,"{open "+self.header)
	EnStatusFile.open(self, verbose)
        if not self.filedes == 0:
            self.filedes.write(self.header)
            self.filedes.write(self.header2)
        Trace.trace(12,"}write_header ")

class AsciiStatusFile(EncpFile, EnStatusFile, EnStatus):

    def __init__(self, file, max_ascii_size):
        Trace.trace(10,'{__init__ asciifile ')
	self.max_ascii_size = max_ascii_size
	EnStatusFile.__init__(self, file)
        Trace.trace(10,'}__init__')

    # format the encp info taken from the log file
    def format_encp(self, lines, key):
        if 0: print self # quiet lint
	Trace.trace(13,"{format_encp ")
	prefix =  "\n                     "
	str = key+"            : "
	spacing = ""
	# break up each line into it's component parts, format it and save it
	for line in lines:
	    einfo = parse_encp_line(line)
	    str = str+spacing+einfo[ETIME]+" on "+einfo[ENODE]+" by "+\
	          einfo[EUSER]
	    spacing = "                  "
	    if einfo[ESTATUS] == e_errors.sevdict[e_errors.INFO]:
	        str = str+" (Data Transfer Rate : "+einfo[EXRATE]+" MB/S)"
	        # what's left in erest2 is what we want, but make it clearer
	        # that the rate in this line is the user rate
	        str = str+prefix+einfo[EBYTES]+" bytes copied to "+\
	              einfo[EDEV]+" at a user rate of "+einfo[EURATE]+" MB/S\n"
	    else:
	        # there was an error or warning
	        if len(einfo) == 7:
	            str = str+prefix+einfo[4]+" : "+einfo[5]+prefix+einfo[6]
	        else:
	            # the leftover text was formatted funny, just output it
	            str = str+prefix+einfo[4]
	Trace.trace(13,"}format_encp ")
	return str

    # move the file to a timestamped backup copy
    def timestamp(self, really=0):
	Trace.trace(11,"{timestamp "+self.file_name)
	s = os.stat(self.file_name)
	if (self.max_ascii_size > 0) or (really == FORCE):
	    if (s[stat.ST_SIZE] >= self.max_ascii_size) or (really == 1):
	        self.close()
	        os.system("mv "+self.file_name+" "+self.file_name+"."+\
	                  get_ts())
	        self.open()
        Trace.trace(11,"}timestamp ")

    # set a new max_ascii_size value
    def set_max_ascii_size(self, value):
	Trace.trace(10,"{set_max_ascii_size "+repr(value))
	self.max_ascii_size = value
	Trace.trace(10,"}set_max_ascii_size ")

    # get the max_ascii_size value
    def get_max_ascii_size(self):
	Trace.trace(10,"{get_max_ascii_size ")
	return self.max_ascii_size
	Trace.trace(10,"}get_max_ascii_size ")

class EncpStatusFile(EncpFile, EnHTMLFile, EnStatusFile):

    def __init__(self, file, refresh):
        Trace.trace(10,'{__init__ encpstatusfile ')
	EnStatusFile.__init__(self, file)
	EnHTMLFile.__init__(self, refresh)
	self.trailer = "</body>\n"
        Trace.trace(10,'}__init__')

    # open the file and write the header to the file
    def open(self, verbose=0):
        Trace.trace(12,"{open "+self.header)
	EnStatusFile.open(self, verbose)
        if not self.filedes == 0:
            self.filedes.write(self.header)
        Trace.trace(12,"}write_header ")

    # format the line saying there have been no encp requests
    def format_no_encp(self):
	Trace.trace(10,"{}format_no_encp ")
	return "<br><pre>\n\n"+EncpFile.format_no_encp(self)+"</pre>"

    # format the encp info taken from the log file
    def format_encp(self, lines, key):
        if 0: print self,key # quiet lint
	Trace.trace(13,"{format_encp ")
	str = "<P>\n<CENTER><TABLE BORDER COLS=7 WIDTH=\"100%\" NOSAVE>\n"+ \
	      "<TH COLSPAN=7 VALIGN=CENTER>History of ENCP Commands</TH>\n"+ \
	      "<TR VALIGN=CENTER NOSAVE>\n<TD NOSAVE><B>TIME</B></TD>\n"+ \
	      "<TD NOSAVE><B>NODE</B></TD>\n<TD NOSAVE><B>USER</B></TD>\n"+ \
	      "<TD NOSAVE><B>BYTES</B></TD>\n<TD NOSAVE><B>VOLUME</B></TD>\n"+\
	      "<TD NOSAVE><B>DATA TRANSFER RATE (MB/S)</B></TD>\n"+ \
	      "<TD NOSAVE><B>USER RATE (MB/S)</B></TD>\n</TR>\n"
	str2 = "<P><PRE>\n"
	# break up each line into it's component parts, format it and save it
	for line in lines:
	    einfo = parse_encp_line(line)
	    if einfo[ESTATUS] == e_errors.sevdict[e_errors.INFO]:
	        str = str+trow+tdata+einfo[ETIME]+tdata_end+ \
	                       tdata+einfo[ENODE]+tdata_end+ \
	                       tdata+einfo[EUSER]+tdata_end+ \
	                       tdata+einfo[EBYTES]+tdata_end+ \
	                       tdata+einfo[EDEV]+tdata_end+ \
	                       tdata+einfo[EXRATE]+tdata_end+ \
	                       tdata+einfo[EURATE]+tdata_end+"</TR>\n"
	    else:
	        str2 = str2+einfo[ETIME]+" on "+einfo[ENODE]+" by "+einfo[EUSER]
 	        # there was an error or warning
	        if len(einfo) == 7:
	            str2 = str2+"\n  "+einfo[4]+" : "+einfo[5]+"\n  "+\
	                   einfo[6]+"\n"
	        else:
	            # the leftover text was formatted funny, just output it
	            str2 = str2+"\n  "+einfo[4]+"\n"
	else:
	    str = str+"</TABLE></CENTER>\n"
	str = str+str2+"</PRE>\n"
	Trace.trace(13,"}format_encp ")
	return str

class EnDataFile(EnFile):

    # make the data file by grepping the inFile.  fproc is any further
    # processing that must be done to the data before it is written to
    # the ofile.
    def __init__(self, inFile, oFile, text, indir="", fproc=""):
        Trace.trace(10,'{__init__ endatafile '+oFile)
	EnFile.__init__(self, oFile)
	self.lines = []
	self.data = []
	if indir == "":
	    cdcmd = " "
	else:
	    cdcmd = "cd "+indir+";"
	try:
	    os.system(cdcmd+"fgrep '"+text+"' "+inFile+fproc+"> "+oFile)
	    Trace.trace(10,'}__init__')
	except:
	    self.file_name = ""
	    format = str(sys.argv)+" "+\
	             str(sys.exc_info()[0])+" "+\
	             str(sys.exc_info()[1])+" "+\
	             "inquisitor plot system error"
	    Trace.trace(5,"}__init__ "+format)

    def read(self, max_lines):
	Trace.trace(10,"{read "+repr(max_lines))
	i = 0
	if not self.filedes == 0:
            while i < max_lines:
                l = self.filedes.readline()
                if l:
                    self.lines.append(l)
                    i = i + 1
                else:
                    break
	Trace.trace(10,"}read ")
	return self.lines

    # read in the given file and return a list of lines that are between a
    # given start and end time
    def timed_read(self, ticket):
	Trace.trace(10,"{timed_read "+repr(ticket))
	do_all = FALSE
	if ticket.has_key(START_TIME):
	    start_time = ticket[START_TIME]
	else:
	    start_time = ""
	if ticket.has_key(STOP_TIME):
	    stop_time = ticket[STOP_TIME]
	else:
	    stop_time = ""
	    if start_time == "":
	        do_all = TRUE
	# read it in.  only save the lines that match the desired time frame
        if not self.filedes == 0:
            try:
                while TRUE:
                    line = self.filedes.readline()
                    if not line:
                        break
                    else:
                        if do_all or \
                           self.check_line(line, start_time, stop_time):
                            self.lines.append(line)
            except:
                pass
	Trace.trace(10,"}timed_read ")
	return self.lines

    # check the line to see if the date and timestamp on the beginning of it
    # is between the given start and end values
    def check_line(self, line, start_time, stop_time):
        if 0: print self # quiet lint
	Trace.trace(10,"{check_line ")
	# split the line into the date/time and all the rest
	[datetime, rest] = string.split(line, None, 1)
        if 0: print rest # quiet lint
	# remove the beginning LOG_PREFIX
	l = regsub.gsub(LOG_PREFIX, "", datetime)
	# now see if the date/time is between the start time and the end time
	time_ok = TRUE
	if not start_time == "":
	    if l < start_time:
	        time_ok = FALSE
	if time_ok and (not stop_time == ""):
	    if l > stop_time:
	        time_ok = FALSE
	Trace.trace(10,"}check_line ")
	return time_ok

class EnMountDataFile(EnDataFile):

    # parse the mount line
    def parse_line(self, line):
        if 0: print self # quiet lint
	Trace.trace(12,"{parse_line "+repr(line))
	[etime, enode, etmp, euser, estatus, dev, erest] = \
                                                   string.split(line, None, 6)
        if 0: print etmp #quiet lint
	if erest[0:10] == "Requesting":
	    # this is the request for the mount
	    start = MREQUEST
	else:
	    start = MMOUNT
	# parse out the file directory , a remnant from the grep in the time 
	# field
	strip_file_dir(etime)

	Trace.trace(12,"}parse_line ")
	return [etime, enode, euser, estatus, dev, start]

    # pull out the plottable data from each line
    def parse_data(self):
	Trace.trace(10,"{parse_data ")
	for line in self.lines:
	    minfo = self.parse_line(line)
	    self.data.append([minfo[MDEV], string.replace(minfo[ETIME], \
	                LOG_PREFIX, ""), minfo[MSTART]])
	Trace.trace(10,"}parse_data ")

class EnEncpDataFile(EnDataFile):

    # parse the encp line
    def parse_line(self, line):
        if 0: print self # quiet lint
	Trace.trace(12,"{parse_line "+repr(line))
	einfo = parse_encp_line(line)
	if einfo[ESTATUS] == e_errors.sevdict[e_errors.INFO]:
	    # the time info may contain the file directory which we must
	    # strip off
	    strip_file_dir(einfo[ETIME])
	    Trace.trace(12,"}parse_line  - info status")
	    return [einfo[ESTATUS], einfo[ETIME], einfo[EBYTES]]
	else:
	    Trace.trace(12,"}parse_line - error status")
	    return [einfo[ESTATUS]]

    # pull out the plottable data from each line
    def parse_data(self):
	Trace.trace(10,"{parse_data ")
	for line in self.lines:
	    einfo = self.parse_line(line)
	    if einfo[0] == e_errors.sevdict[e_errors.INFO]:
	        self.data.append([string.replace(einfo[1], LOG_PREFIX, ""), \
	                         einfo[2]])
	Trace.trace(10,"}parse_data ")



