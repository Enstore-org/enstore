#
# system import
import sys
import time
import string
import os
import stat
import string

# enstore imports
import Trace
import alarm
import enstore_html
import enstore_status
import e_errors

TRUE = 1
FALSE = 0
TMP = ".tmp"
START_TIME = "start_time"
STOP_TIME = "stop_time"
LOG_PREFIX = "LOG-"

# ENCP line pieces from log file
ETIME = 0
ENODE = 1
EUSER = 2
ESTATUS = 3
EXRATE = 4
EBYTES = 5
EDEV = 6
EURATE = 7
EDICTS = 8

# message is either a mount request or an actual mount
MREQUEST = 0
MMOUNT = 1

# different MOUNT line pieces from log file
MDEV = 4
MSTART = 5
MDICTS = 6

default_dir = "./"

def inq_file_name():
    return "enstore_system.html"

def default_inq_file():
    return "%s%s"%(default_dir, inq_file_name())

def encp_html_file_name():
    return "encp_%s"%(inq_file_name(),)

def default_encp_html_file():
    return "%s%s"%(default_dir, encp_html_file_name())

def config_html_file_name():
    return "config_%s"%(inq_file_name(),)

def default_config_html_file():
    return "%s%s"%(default_dir, config_html_file_name())

def misc_html_file_name():
    return "misc_%s"%(inq_file_name(),)

def default_misc_html_file():
    return "%s%s"%(default_dir, misc_html_file_name())

def status_html_file_name():
    return "status_%s"%(inq_file_name(),)

def default_status_html_file():
    return "%s%s"%(default_dir, status_html_file_name())

class EnFile:

    def __init__(self, file):
        self.file_name = file 
	self.filedes = 0

    def open(self, mode='w'):
	try:
            self.filedes = open(self.file_name, mode)
            Trace.trace(10,"%s open "%(self.file_name,))
        except IOError:
            self.filedes = 0
            Trace.log(e_errors.WARNING,
                      "%s not openable for %s"%(self.file_name, mode))

    def close(self):
	Trace.trace(10,"enfile close %s"%(self.file_name,))
	if self.filedes:
	    self.filedes.close()
	    self.filedes = 0

    # remove the file
    def cleanup(self, keep, pts_dir):
        if not keep:
            # delete the data file
            os.remove(self.file_name)
        else:
            if pts_dir:
                # move these files somewhere, do a copy and remove in case we
                # are moving across disks
                os.system("cp %s %s"%(self.file_name, pts_dir))
                os.remove(self.file_name)

class EnStatusFile(EnFile):

    def __init__(self, file):
     	EnFile.__init__(self, file)
	self.text = {}

    # open the file
    def open(self):
        Trace.trace(12,"open %s"%(self.file_name,))
        # try to open status file for append
        EnFile.open(self, 'a')
        if not self.filedes:
            # could not open for append, try to create it
            EnFile.open(self, 'w')

    # remove something from the text hash that will be written to the files
    def remove_key(self, key):
	if self.text.has_key(key):
	    del self.text[key]
    
    def set_refresh(self, refresh):
	self.refresh = refresh

    def get_refresh(self):
	return self.refresh

class HTMLStatusFile(EnStatusFile, enstore_status.EnStatus):

    def __init__(self, file, refresh):
	EnStatusFile.__init__(self, file)
	self.refresh = refresh

    # write the status info to the file
    def write(self):
        if self.filedes:
	    doc = enstore_html.EnSysStatusPage(self.refresh)
	    doc.body(self.text)
            self.filedes.write(str(doc))

class HTMLEncpStatusFile(EnStatusFile):

    def __init__(self, file, refresh):
	EnStatusFile.__init__(self, file)
	self.refresh = refresh

    # output the encp info
    def write(self, lines):
        if self.filedes:
	    # break up each line into it's component parts and format it
	    eline = []
	    for line in lines:
		einfo = enstore_status.parse_encp_line(line)
		if len(einfo) and einfo[ESTATUS] == e_errors.sevdict[e_errors.INFO]:
		    eline.append([einfo[ETIME], einfo[ENODE], einfo[EUSER], 
				  einfo[EBYTES], einfo[EDEV], einfo[EXRATE], 
				  einfo[EURATE]])
	    else:
		doc = enstore_html.EnEncpStatusPage(self.refresh)
		doc.body(eline)
            self.filedes.write(str(doc))

class HTMLLogFile(EnFile):

    # format the log files and write them to the file, include a link to the
    # page to search the log files
    def write(self, http_path, logfiles, user_logs, host):
        if self.filedes:
	    doc = enstore_html.EnLogPage()
	    doc.body(http_path, logfiles, user_logs, host)
            self.filedes.write(str(doc))

class HTMLConfigFile(EnFile):

    # format the config entry and write it to the file
    def write(self, cdict):
        if self.filedes:
	    doc = enstore_html.EnConfigurationPage()
	    doc.body(cdict)
            self.filedes.write(str(doc))

class HTMLMiscFile(EnFile):

    # format the file name and write it to the file
    def write(self, data):
        if self.filedes:
	    doc = enstore_html.EnMiscPage()
	    doc.body(data)
            self.filedes.write(str(doc))

class EnDataFile(EnFile):

    # make the data file by grepping the inFile.  fproc is any further
    # processing that must be done to the data before it is written to
    # the ofile.
    def __init__(self, inFile, oFile, text, indir="", fproc=""):
	EnFile.__init__(self, oFile)
	self.lines = []
	self.data = []
	if not indir:
	    cdcmd = " "
	else:
	    cdcmd = "cd %s;"%(indir,)
	try:
	    os.system(cdcmd+"grep "+text+" "+inFile+fproc+"> "+oFile)
	except:
	    self.file_name = ""
            exc, msg, tb=sys.exc_info()
	    format = "%s: inquisitor plot system error: %s" % (sys.argv,msg)
	    Trace.trace(9,"__init__ %s"%(format,))

    def read(self, max_lines):
	i = 0
	if self.filedes:
            while i < max_lines:
                l = self.filedes.readline()
                if l:
                    self.lines.append(l)
                    i = i + 1
                else:
                    break
	return self.lines

    # read in the given file and return a list of lines that are between a
    # given start and end time
    def timed_read(self, ticket):
	do_all = FALSE
        start_time = ticket.get(START_TIME, "")
        stop_time = ticket.get(STOP_TIME, "")
        if not stop_time and not start_time:
            do_all = TRUE
	# read it in.  only save the lines that match the desired time frame
        if self.filedes:
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
	return self.lines

    # check the line to see if the date and timestamp on the beginning of it
    # is between the given start and end values
    def check_line(self, line, start_time, stop_time):
	# split the line into the date/time and all the rest
	datetime, rest = string.split(line, None, 1)
	# remove the beginning LOG_PREFIX
	l = string.replace(datetime, LOG_PREFIX,"")
	# now see if the date/time is between the start time and the end time
	time_ok = TRUE
	if start_time:
	    if l < start_time:
	        time_ok = FALSE
	if time_ok and stop_time:
	    if l > stop_time:
	        time_ok = FALSE
	return time_ok

class EnMountDataFile(EnDataFile):

    # parse the mount line
    def parse_line(self, line):
	[etime, enode, etmp, euser, estatus, dev, type, erest] = \
                                                   string.split(line, None, 7)
	if type == string.rstrip(Trace.MSG_MC_LOAD_REQ) :
	    # this is the request for the mount
	    start = MREQUEST
	else:
	    start = MMOUNT

	# parse out the file directory , a remnant from the grep in the time 
	# field
	enstore_status.strip_file_dir(etime)

        # pull out any dictionaries from the rest of the message
        msg_dicts = enstore_status.get_dict(erest)

	return [etime, enode, euser, estatus, dev, start, msg_dicts]

    # pull out the plottable data from each line that is from one of the
    # specified movers
    def parse_data(self, mcs):
	for line in self.lines:
	    minfo = self.parse_line(line)
            if not mcs or mc_in_list(minfo[MDICTS], mcs):
                self.data.append([minfo[MDEV], string.replace(minfo[ETIME],
                                                              LOG_PREFIX, ""),
                                  minfo[MSTART]])

class EnEncpDataFile(EnDataFile):

    # parse the encp line
    def parse_line(self, line):
	einfo = enstore_status.parse_encp_line(line)
        if not len(einfo):
            # nothing was returned skip this line
            return []
        # the time info may contain the file directory which we must
        # strip off
        enstore_status.strip_file_dir(einfo[ETIME])
        return [einfo[ETIME], einfo[EBYTES], einfo[EDICTS]]

    # pull out the plottable data from each line
    def parse_data(self, mcs):
	for line in self.lines:
	    einfo = self.parse_line(line)
	    if einfo and (not mcs or mc_in_list(einfo[2], mcs)):
	        self.data.append([string.replace(einfo[0], LOG_PREFIX, ""), \
	                         einfo[1]])

class HtmlAlarmFile(EnFile):

    # we need to save both the file name passed to us and the one we will
    # write to.  we will create the temp one and then move it to the real
    # one.
    def __init__(self, name):
        EnFile.__init__(self, name+TMP)
        self.real_file_name = name

    # we need to close the open file and move it to the real file name
    def close(self):
        EnFile.close(self)
        os.rename(self.file_name, self.real_file_name)

    # format the file name and write it to the file
    def write(self, data):
        if self.filedes:
	    doc = enstore_html.EnAlarmPage()
	    doc.body(data)
            self.filedes.write(str(doc))

class HTMLPatrolFile(EnFile):

    # we need to save both the file name passed to us and the one we will
    # write to.  we will create the temp one and then move it to the real
    # one.
    def __init__(self, name):
        EnFile.__init__(self, name+TMP)
        self.real_file_name = name

    # we need to close the open file and move it to the real file name
    def close(self):
        EnFile.close(self)
        os.rename(self.file_name, self.real_file_name)

    # format the file name and write it to the file
    def write(self, data):
        if self.filedes:
	    doc = enstore_html.EnPatrolPage()
	    doc.body(data)
            self.filedes.write(str(doc))

class EnAlarmFile(EnFile):

    # open the file, if no mode is passed in, try opening for append and
    # then write
    def open(self, mode=""):
        if mode:
            EnFile.open(self, mode)
        else:
            EnFile.open(self, "a")
            if not self.filedes:
                # the open for append did not work, now try write
                EnFile.open(self, "w")

    # read lines from the file
    def read(self):
        enAlarms = {}
        if self.filedes:
            try:
                while TRUE:
                    line = self.filedes.readline()
                    if not line:
                        break
                    else:
                        theAlarm = alarm.AsciiAlarm(line)
                        enAlarms[theAlarm.timedate] = theAlarm
            except IOError:
                pass
        return enAlarms
                
    # write the alarm to the file
    def write(self, alarm):
        if self.filedes:
            line = repr(alarm)+"\n"
            try:
                self.filedes.write(line)
            except IOError:
                pass

class EnPatrolFile(EnFile):

    # we need to save both the file name passed to us and the one we will
    # write to.  we will create the temp one and then move it to the real
    # one.
    def __init__(self, name):
        EnFile.__init__(self, name+TMP)
        self.real_file_name = name
        self.lines = []

    # we need to close the open file and move it to the real file name
    def close(self):
        EnFile.close(self)
        os.rename(self.file_name, self.real_file_name)

    # write out the alarm
    def write(self, alarm):
        if self.filedes:
            # tell the alarm that this is going to patrol so the alarm
            # can add the patrol expected header
            self.filedes.write(alarm.prepr())

    # rm the file
    def remove(self):
        try:
            if self.real_file_name and os.path.exists(self.real_file_name):
                os.remove(self.real_file_name)
        except IOError:
            # file does not exist
            pass
