#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system import
import sys
import time
import string
import os
import stat
import types

# enstore imports
import Trace
import alarm
import enstore_html
import enstore_functions
import enstore_functions2
import enstore_status
import enstore_constants
import e_errors
import hostaddr
import www_server

TRUE = 1
FALSE = 0
TMP = ".tmp"
START_TIME = "start_time"
STOP_TIME = "stop_time"

NO_THRESHOLD = 0
NO_THRESHOLDS = [NO_THRESHOLD, NO_THRESHOLD, NO_THRESHOLD]
SERVER_PAGE = 0
FULL_PAGE = 1
LM_PAGE = 2

# message is either a mount request or an actual mount
MREQUEST = 0
MMOUNT = 1

# different MOUNT line pieces from log file
MDEV = 4
MPID = 5
MVOLUME = 6
MSTART = 7
MDRIVEID = 8
MDICTS = 9

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

def plot_html_file_name():
    return "plot_%s"%(inq_file_name(),)

def default_plot_html_file():
    return "%s%s"%(default_dir, plot_html_file_name())

def generated_web_page_html_file_name():
    return "generated_web_pages.html"

def default_generated_web_page_html_file():
    return "%s%s"%(default_dir, generated_web_page_html_file_name())

def status_html_file_name():
    return "status_%s"%(inq_file_name(),)

def default_status_html_file():
    return "%s%s"%(default_dir, status_html_file_name())

class EnFile:

    def set_filename(self, file):
        self.file_name = file
        self.real_file_name = file
	self.lockfile = "%s.lock"%(file,)

    def __init__(self, file, system_tag=""):
	self.set_filename(file)
        self.openfile = 0
	self.opened = 0
        self.system_tag = system_tag
	self.nav_link = ""

    def get_lock(self):
	# first check if we can get a lockfile for this
	try:
	    for attempt in [1,2,3,4,5]:
		os.stat(self.lockfile)
		# oops file exists, wait awhile then try again
		time.sleep(1)
	    else:
		return None
	except OSError:
	    # file does not exist, create it
	    os.system("touch %s"%(self.lockfile,))
	    return 1

    def open(self, mode='w'):
        try:
            self.openfile = open(self.file_name, mode)
	    self.opened = 1
            enstore_functions.inqTrace(enstore_constants.INQFILEDBG,"%s open "%(self.file_name,))
        except IOError:
            self.openfile = 0
	    self.opened = 0
            Trace.log(e_errors.WARNING,
                      "%s not openable for %s"%(self.file_name, mode))

    def do_write(self, data, filename=None):
	if filename is None:
	    filename = self.file_name
	try:
	    self.openfile.write(data)
	except IOError, detail:
	    msg = "Error writing %s (%s)"%(filename, detail)
	    Trace.log(e_errors.ERROR, msg, e_errors.IOERROR)


    # write it to the file
    def write(self, data):
        if self.openfile:
	    self.do_write(str(data))

    def close(self):
	enstore_functions.inqTrace(enstore_constants.INQFILEDBG,"enfile close %s"%(self.file_name,))
	if self.openfile:
	    self.openfile.close()
	    self.openfile = 0

    def install(self):
	# move the file we created to the real file name
        enstore_functions.inqTrace(enstore_constants.INQFILEDBG,
		    "Tmp file: %s, real file: %s"%(self.file_name, self.real_file_name))
        if (not self.real_file_name == self.file_name) and os.path.exists(self.file_name):
	    os.system("mv %s %s"%(self.file_name, self.real_file_name))

    def remove(self):
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

    # remove the file
    def cleanup(self, keep, pts_dir):
        if not keep:
            # delete the data file
            if os.path.exists(self.file_name):
                os.remove(self.file_name)
        else:
            if pts_dir:
                # move these files somewhere, do a copy and remove in case we
                # are moving across disks
                if os.path.exists(self.file_name):
                    os.system("cp %s %s"%(self.file_name, pts_dir))
                    os.remove(self.file_name)

    def exists(self):
        if os.path.exists(self.file_name):
            return 1
        else:
            return 0

class EnStatusFile(EnFile):

    def __init__(self, file, system_tag=""):
        EnFile.__init__(self, file, system_tag)
        self.text = {}

    # open the file
    def open(self):
        enstore_functions.inqTrace(enstore_constants.INQFILEDBG,
				   "open %s"%(self.file_name,))
        # try to open status file for append
        EnFile.open(self, 'a')
        if not self.openfile:
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

class HTMLFile(EnStatusFile, enstore_status.EnStatus):


    def __init__(self, file, refresh, system_tag=""):
        EnStatusFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
        self.refresh = refresh

class HTMLExtraPages:

    def write(self, doc):
	if doc.extra_queue_pages:
	    for extra_page_key in doc.extra_queue_pages.keys():
		filename = "%s/%s"%(self.html_dir, doc.extra_queue_pages[extra_page_key][1],)
		extra_file = HTMLFile(filename,
			      doc.extra_queue_pages[extra_page_key][0].refresh,
			      doc.extra_queue_pages[extra_page_key][0].system_tag)
		extra_file.open()
		try:
		    extra_file.write(str(doc.extra_queue_pages[extra_page_key][0]))
		except IOError, detail:
		    msg = "Error writing %s (%s)"%(filename, detail)
		    Trace.log(e_errors.ERROR, msg, e_errors.IOERROR)
		extra_file.close()
		extra_file.install()

class HTMLLmFullStatusFile(EnStatusFile, HTMLExtraPages):

    def __init__(self, file, refresh, system_tag=""):
        EnStatusFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
        self.refresh = refresh


class HTMLLmStatusFile(EnStatusFile):

    def __init__(self, file, refresh, system_tag=""):
        EnStatusFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
        self.refresh = refresh

class HTMLMoverStatusFile(EnStatusFile):

    def __init__(self, file, refresh, system_tag=""):
        EnStatusFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
        self.refresh = refresh

class HTMLMigratorStatusFile(EnStatusFile):

    def __init__(self, file, refresh, system_tag=""):
        EnStatusFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
        self.refresh = refresh

class HTMLFileListFile(EnStatusFile):

    def __init__(self, file, refresh, system_tag=""):
        EnStatusFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
        self.refresh = refresh


class HTMLStatusFile(EnStatusFile, HTMLExtraPages, enstore_status.EnStatus):

    def __init__(self, file, refresh, system_tag="", page_thresholds=NO_THRESHOLDS):
        EnStatusFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
	self.html_dir = enstore_functions2.get_dir(file)
        self.refresh = refresh
	self.page_thresholds = page_thresholds
	self.mover_file = HTMLMoverStatusFile("%s/enstore_movers.html"%(self.html_dir,),
					      refresh, system_tag)
	self.migrator_file = HTMLMoverStatusFile("%s/enstore_migrators.html"%(self.html_dir,),
					      refresh, system_tag)
	self.filelist_file = HTMLFileListFile("%s/enstore_files.html"%(self.html_dir,),
					      refresh, system_tag)

	self.filelist = []
	self.docs_to_install = []

    def remove_server(self, key):
        if self.text.has_key(key):
            del self.text[key]

    def dont_monitor(self, key, host):
        self.text[key] = {}
        self.text[key][enstore_constants.STATUS] = [enstore_constants.NOT_MONITORING,
					 self.format_host(host),
                                         enstore_functions2.format_time(time.time())]

    def set_alive_error_status(self, key):
        try:
            self.text[key][enstore_constants.STATUS][0] = "error"
        except KeyError:
            # the 'update_commands' for example does not have a STATUS
            pass

    def get_file_list(self, lm, name):
	# we may have gotten an error while trying to get the info,
	# so check for a piece of it first
	if lm.has_key(enstore_constants.LMSTATE) and \
	   not lm[enstore_constants.LMSTATE] == enstore_constants.UNKNOWN_S:
	    wam_q = lm.get(enstore_constants.WORK, None)
	    if wam_q and not wam_q is enstore_constants.NO_WORK:
		for wam_elem in wam_q:
		    if wam_elem[enstore_constants.WORK] == enstore_constants.READ:
			self.filelist.append([wam_elem[enstore_constants.NODE],
					      wam_elem[enstore_constants.FILE], name,
					      wam_elem[enstore_constants.DEVICE]])
		    else:
			ff = wam_elem.get(enstore_constants.FILE_FAMILY, None)
			self.filelist.append([wam_elem[enstore_constants.NODE],
					      wam_elem[enstore_constants.FILE], name,
					      ff])
	    pend_q = lm.get(enstore_constants.PENDING, None)
	    if pend_q:
		if type(pend_q) == types.DictionaryType:
		    pread_q = pend_q[enstore_constants.READ]
		else:
		    pread_q = []
		if pread_q:
		    for pread_elem in pread_q:
			self.filelist.append([pread_elem[enstore_constants.NODE],
					      pread_elem[enstore_constants.FILE], name,
					      pread_elem[enstore_constants.DEVICE]])
		if type(pend_q) == types.DictionaryType:
		    pwrite_q = pend_q[enstore_constants.WRITE]
		else:
		    pwrite_q = []
		if pwrite_q:
		    for pwrite_elem in pwrite_q:
			# instead of a volume we will include a file family
			ff = pwrite_elem.get(enstore_constants.FILE_FAMILY, None)
			self.filelist.append([pwrite_elem[enstore_constants.NODE],
					      pwrite_elem[enstore_constants.FILE], name,
					      ff])

    # write the status info to the files
    def write(self):
        if self.openfile:
	    self.docs_to_install = []
            doc = enstore_html.EnSysStatusPage(self.refresh, self.system_tag,
                                               self.page_thresholds)
            doc.body(self.text)
	    self.do_write(str(doc))
	    HTMLExtraPages.write(self, doc)

	    # now make the individual library manager pages
	    status_keys = self.text.keys()
	    self.filelist = []
	    for key in status_keys:
		if enstore_functions2.is_library_manager(key) and \
		   not self.text[key][enstore_constants.STATUS][0] == \
		                 enstore_constants.NOT_MONITORING:
		    doc = enstore_html.EnLmFullStatusPage(key, self.refresh,
							  self.system_tag,
							  self.page_thresholds.get(key,
									 NO_THRESHOLDS)[FULL_PAGE])
		    doc.body(self.text[key])
		    # save the file info for the filelist page
		    self.get_file_list(self.text[key], key)
		    # this is the page with the full lm queue on it (old one)
		    lm_q_file = HTMLLmFullStatusFile("%s/%s-full.html"%(self.html_dir, key),
						     self.refresh, self.system_tag)
		    lm_q_file.open()
		    lm_q_file.write(doc)
		    lm_q_file.close()
		    self.docs_to_install.append(lm_q_file)
		    HTMLExtraPages.write(self, doc)
		    doc = enstore_html.EnLmStatusPage(key, self.refresh,
						      self.system_tag,
						      self.page_thresholds.get(key,
								NO_THRESHOLDS)[LM_PAGE])
		    doc.body(self.text[key])
		    lm_file = HTMLLmStatusFile("%s/%s.html"%(self.html_dir, key),
					       self.refresh, self.system_tag)
		    lm_file.open()
		    lm_file.write(doc)
		    lm_file.close()
		    self.docs_to_install.append(lm_file)
		    HTMLExtraPages.write(self, doc)
	    # now make the mover page
	    doc = enstore_html.EnMoverStatusPage(self.refresh, self.system_tag)
	    doc.body(self.text)
	    self.mover_file.open()
	    self.mover_file.write(doc)
	    self.mover_file.close()
	    self.docs_to_install.append(self.mover_file)
	    # now make the migrator page
	    doc = enstore_html.EnMigratorStatusPage(self.refresh, self.system_tag)
	    doc.body(self.text)
	    self.migrator_file.open()
	    self.migrator_file.write(doc)
	    self.migrator_file.close()
	    self.docs_to_install.append(self.migrator_file)
	    # now make the file list page
	    doc = enstore_html.EnFileListPage(self.refresh, self.system_tag,
					      self.page_thresholds[enstore_constants.FILE_LIST])
	    doc.body(self.filelist)
	    self.filelist_file.open()
	    self.filelist_file.write(doc)
	    self.filelist_file.close()
	    self.docs_to_install.append(self.filelist_file)
	    HTMLExtraPages.write(self, doc)

    def install(self):
	EnStatusFile.install(self)
	for doc in self.docs_to_install:
	    doc.install()


class HTMLEncpStatusFile(EnStatusFile):

    def __init__(self, file, refresh, system_tag=""):
        EnStatusFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
        self.refresh = refresh

    def get_lines(self, lines, formatted_lines):
        for line in lines:
            encp_line = enstore_status.EncpLine(line)
            if encp_line.storage_group:
                user = "%s/%s"%(encp_line.user, encp_line.storage_group)
            else:
                user = encp_line.user
            if encp_line.encp_ip:
                node = enstore_functions2.strip_node(hostaddr.address_to_name(\
                    encp_line.encp_ip))
            else:
                node = encp_line.node
            # if no error is listed use the type field
            if encp_line.error:
               error = encp_line.error
            else:
               error = encp_line.type
            if encp_line.success:
                formatted_lines.append([encp_line.time,
                                        node, user, encp_line.bytes,
                                        "%s %s"%(encp_line.direction,
                                                 encp_line.volume),
                                        encp_line.network_rate, encp_line.transfer_rate,
                                        encp_line.infile, encp_line.outfile,
                                        encp_line.interface,
                                        encp_line.overall_rate,
                                        encp_line.drive_rate,
                                        encp_line.disk_rate])
            else:
                formatted_lines.append([encp_line.time,
                                        node, user, "%s : (SRC: %s) (DST: %s)"%(error,
                                                                                encp_line.infile,
                                                                                encp_line.outfile)])

    # output the encp info
    def write(self, lines):
        if self.openfile:
            # break up each line into it's component parts and format it
            eline = []
            self.get_lines(lines, eline)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
                                       "update_encp - parsed out %s lines"%(len(eline),))

            doc = enstore_html.EnEncpStatusPage(refresh=self.refresh,
                                                system_tag=self.system_tag)
            doc.body(eline)
	    self.do_write(str(doc))

class HTMLLogFile(EnFile):

    def __init__(self, dir, file, cp_dir, system_tag=""):
        filedir = "%s/%s.new"%(dir, file)
        EnFile.__init__(self, filedir, system_tag)
        self.real_file_name = "%s/%s"%(cp_dir, file)

    # format the log files and write them to the file, include a link to the
    # page to search the log files
    def write(self, http_path, logfiles, user_logs, host):
        if self.openfile:
            doc = enstore_html.EnLogPage(system_tag=self.system_tag)
            doc.body(http_path, logfiles, user_logs, host)
	    self.do_write(str(doc))

    def copy(self):
        # copy the file we created to the real file name in the new dir.
        # this will work across disks
        if (not self.real_file_name == self.file_name) and \
           os.path.exists(self.file_name):
            os.system("cp %s %s"%(self.file_name, self.real_file_name))

class HTMLConfigFile(EnFile):

    def __init__(self, file, system_tag=""):
        EnFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)

    # format the config entry and write it to the file
    def write(self, cdict):
        if self.openfile:
            doc = enstore_html.EnConfigurationPage(system_tag=self.system_tag)
            doc.body(cdict)
	    self.do_write(str(doc))

class HTMLPlotFile(EnFile):

    def __init__(self, file, system_tag="", nav_link="", url_gif_dir = ""):
        EnFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
	self.nav_link = nav_link
        self.url_gif_dir = url_gif_dir

    # format the config entry and write it to the file
    def write(self, jpgs, stamps, pss, mount_label, links_l=None):
        if self.openfile:
            doc = enstore_html.EnPlotPage(system_tag=self.system_tag,
					  mount_label=mount_label,
					  links_l=links_l,
					  nav_link=self.nav_link,
                                          url_gif_dir=self.url_gif_dir)
            doc.body(jpgs, stamps, pss)
	    self.do_write(str(doc))

class HTMLGeneratedFile(EnFile):

    def __init__(self, file, system_tag="", nav_link="", url_gif_dir = ""):
        EnFile.__init__(self, file, system_tag)
        self.file_name = "%s.new"%(file,)
	self.nav_link = nav_link
        self.url_gif_dir = url_gif_dir

    # format the config entry and write it to the file
    def write(self, directory_list, mount_label, links_l=None):
        if self.openfile:
            doc = enstore_html.EnGeneratedWebPage(system_tag=self.system_tag,
					  mount_label=mount_label,
					  links_l=links_l,
					  nav_link=self.nav_link,
                                          url_gif_dir=self.url_gif_dir)
            doc.body(directory_list)
	    self.do_write(str(doc))

class HTMLMiscFile(EnFile):

    # format the file name and write it to the file
    def write(self, data):
        if self.openfile:
            doc = enstore_html.EnMiscPage(system_tag=self.system_tag)
            doc.body(data)
	    self.do_write(str(doc))

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
        os.system(cdcmd+"grep "+text+" "+inFile+fproc+"> "+oFile)
        tmp = enstore_functions2.strip_file_dir(inFile)
        self.date = string.replace(tmp, enstore_constants.LOG_PREFIX, "")

    def read(self, max_lines):
        i = 0
        if self.openfile:
            while i < max_lines:
                l = self.openfile.readline()
		if l:
		    # this is a gross hsck
		    if string.find(l, "set_route(") == -1:
			self.lines.append(l)
			i = i + 1
                else:
                    break
        return self.date, self.lines

    # check the line to see if the date and timestamp on the beginning of it
    # is between the given start and end values
    def check_line(self, line, start_time, stop_time, prefix):
        # split the line into the date/time and all the rest
	fields = string.split(line, None, 1)
	datetime = string.strip(fields[0])
        # remove the beginning file prefix if there is one
	if prefix:
	    l = string.replace(datetime, prefix,"")
	else:
	    l = datetime
        # now see if the date/time is between the start time and the end time
        time_ok = TRUE
        if start_time:
            if l < start_time:
                time_ok = FALSE
        if time_ok and stop_time:
            if l > stop_time:
                time_ok = FALSE
        return time_ok

    # read in the given file and return a list of lines that are between a
    # given start and end time
    def timed_read(self, start_time, stop_time, prefix=enstore_constants.LOG_PREFIX):
        do_all = FALSE
        if stop_time is None and start_time is None:
            do_all = TRUE
        # read it in.  only save the lines that match the desired time frame
        if self.openfile:
            try:
                while TRUE:
                    line = self.openfile.readline()
                    if not line:
                        break
                    else:
                        if do_all or \
                           self.check_line(line, start_time, stop_time, prefix):
                            self.lines.append(line)
            except:
                pass
        return self.lines

class EnMountDataFile(EnDataFile):

    def __init__(self, inFile, oFile, text, indir="", fproc="", config={}):
	EnDataFile.__init__(self, inFile, oFile, text, indir, fproc)
	self.config = config
	self.servers = self.config.keys()

    # find out if this is a null mover
    def is_null_mover(self, mover_log_name):
	for server in self.servers:
	    if self.config[server].has_key('logname') and \
	       self.config[server]['logname'] == mover_log_name:
		if self.config[server]['driver'] == 'NullDriver':
		    # this is a null mover
		    rtn = 1
		    break
		else:
		    # this is not a null mover
		    rtn = 0
		    break
	else:
	    # assume this is not a null mover
	    rtn = 0
	return rtn

    # parse the mount line
    def parse_line(self, line):
	try:
            field_l = string.split(line, None)
            l = len(field_l)
            if l == 9:
                # old format
                [etime, enode, epid, euser, estatus, mover, type, request, volume] = field_l
                drive_id = ""
            elif l == 10:
                [etime, enode, epid, euser, estatus, mover, type, request, volume, drive_id] = field_l
            elif l == 11:
                [etime, enode, epid, euser, estatus, mover, type, request, volume,
                 drive_id, dummy] = field_l
            else:
                return []
	except ValueError:
	    # this line has a bad syntax, ignore it
	    Trace.trace(enstore_constants.INQPLOTDBG, "Plot line has evil syntax (%s)"%(line,))
	    return []
	# if this is a null mover, ignore it
	if not self.is_null_mover(mover):
	    if type == string.rstrip(Trace.MSG_MC_LOAD_REQ) :
		# this is the request for the mount
		start = MREQUEST
	    else:
		start = MMOUNT

	    # parse out the file directory , a remnant from the grep in the time
	    # field
	    etime = enstore_functions2.strip_file_dir(etime)

	    # pull out any dictionaries from the rest of the message
	    #msg_dicts = enstore_status.get_dict(erest)
            msg_dicts = {}     # to be fixed NOTE: efb
	    return [etime, enode, euser, estatus, mover, epid, volume, start, drive_id, msg_dicts]
	else:
	    return []

    # pull out the plottable data from each line that is from one of the
    # specified movers
    def parse_data(self, mcs, prefix):
        for line in self.lines:
            minfo = self.parse_line(line)
            if minfo and (not mcs or enstore_status.mc_in_list(minfo[MDICTS], mcs)):
                self.data.append([minfo[MDEV], minfo[MPID], minfo[MVOLUME],
                                  string.replace(minfo[0], prefix, ""), minfo[MSTART],
                                  minfo[MDRIVEID]])

class EnEncpDataFile(EnDataFile):
    # this method of reading in all the data and then parsing it is not
    # scalable.  this is causing the encp plots to fail on cdf.  this will
    # eventually be replaced by getting the information from the accounting
    # database.  i do not want to put alot of work into it.  i will fix
    # this case and not worry about duplicating code at the moment.

    # pull out the plottable data from each line
    def parse_data(self, line, mcs, prefix):
        encp_line = enstore_status.EncpLine(line)
        if encp_line.valid:
            if not mcs or enstore_status.mc_in_list(encp_line.mc, mcs):
                # remove the directory and the log prefix to get just the
                # date and time
                etime = enstore_functions2.strip_file_dir(encp_line.time)
                self.data.append([string.replace(etime, prefix, ""),
                                  encp_line.bytes, encp_line.direction,
                                  encp_line.mover, encp_line.drive_id,
                                  encp_line.storage_group])
        del encp_line

    def read_and_parse(self, start_time, stop_time, prefix, media_changer):
        do_all = FALSE
        if stop_time is None and start_time is None:
            do_all = TRUE
        # read it in.  only save the lines that match the desired time frame
        if self.openfile:
            try:
                while TRUE:
                    line = self.openfile.readline()
                    if not line:
                        break
                    else:
                        if do_all or \
                           self.check_line(line, start_time, stop_time, prefix):
                            self.parse_data(line, media_changer, prefix)
                        del line
            except:
                pass


class EnSgDataFile(EnDataFile):

    # pull out the plottable data from each line
    def parse_data(self, prefix):
	self.data = {}
        for line in self.lines:
	    sg_line = enstore_status.SgLine(line)
	    # remove the directory and the log prefix to get just the
	    # date and time
	    etime = enstore_functions2.strip_file_dir(sg_line.time)
	    etime = string.replace(etime, prefix, "")
	    if self.data.has_key(sg_line.sg):
		self.data[sg_line.sg].append([etime, sg_line.pending])
	    else:
		self.data[sg_line.sg] = [[etime, sg_line.pending],]

class HtmlAlarmFile(EnFile):

    # we need to save both the file name passed to us and the one we will
    # write to.  we will create the temp one and then move it to the real
    # one.
    def __init__(self, name, system_tag=""):
        EnFile.__init__(self, name+TMP, system_tag)
        self.real_file_name = name

    # we need to close the open file and move it to the real file name
    def close(self):
        EnFile.close(self)
        os.rename(self.file_name, self.real_file_name)

    # format the file name and write it to the file
    def write(self, data, www_host):
        if self.openfile:
            doc = enstore_html.EnAlarmPage(system_tag=self.system_tag)
            doc.body(data, www_host)
	    self.do_write(str(doc))

class EnAlarmFile(EnFile):

    # open the file, if no mode is passed in, try opening for append and
    # then write
    def open(self, mode=""):
        if not os.path.exists(self.file_name):
            d = os.path.dirname(self.file_name)
            if not os.path.exists(d):
                os.makedirs(d)
            EnFile.open(self, "w")
            EnFile.close(self)
        if mode:
            EnFile.open(self, mode)
        else:
            EnFile.open(self, "a")
            if not self.openfile:
                # the open for append did not work, now try write
                EnFile.open(self, "w")

    # read lines from the file
    def read(self):
        enAlarms = {}
        if self.openfile:
            try:
                while TRUE:
                    line = self.openfile.readline()
                    if not line:
                        break
                    else:
                        theAlarm = alarm.AsciiAlarm(line)
			if not theAlarm.id == 0:
			    enAlarms[theAlarm.id] = theAlarm
            except IOError:
                pass
        return enAlarms

    # write the alarm to the file
    def write(self, alarm):
        if self.openfile:
            line = repr(alarm)+"\n"
	    self.do_write(line)

class HtmlSaagFile(EnFile):

    # we need to save both the file name passed to us and the one we will
    # write to.  we will create the temp one and then move it to the real
    # one.
    def __init__(self, name, system_tag=""):
        EnFile.__init__(self, name+TMP, system_tag)
        self.real_file_name = name
	self.enstore_ball = ""

    def write(self, enstore_contents, other_contents, media_contents, alarm_contents,
	      node_contents, outage, offline, status_file_name):
        if self.openfile:
            doc = enstore_html.EnSaagPage(system_tag=self.system_tag)
            media = enstore_functions.get_media()
            doc.body(enstore_contents, other_contents, media_contents, alarm_contents,
		     node_contents, outage, offline, media, status_file_name)
	    # save the status of the enstore ball
	    self.enstore_ball = enstore_contents[enstore_constants.ENSTORE]
	    self.do_write(str(doc))


class HtmlSaagNetworkFile(EnFile):

    # we need to save both the file name passed to us and the one we will
    # write to.  we will create the temp one and then move it to the real
    # one.
    def __init__(self, name, system_tag=""):
        EnFile.__init__(self, name+TMP, system_tag)
        self.real_file_name = name
	self.enstore_ball = ""

    def write(self, network_contents, outage, offline):
        if self.openfile:
            doc = enstore_html.EnSaagNetworkPage(system_tag=self.system_tag)
            doc.body(network_contents, outage, offline)
	    self.do_write(str(doc))


class ScheduleFile(EnFile):

    def __init__(self, dir, name):
        self.html_dir = dir
        EnFile.__init__(self, "%s/%s"%(dir, name))

    def open(self, mode='w'):
	if self.get_lock():
	    EnFile.open(self, mode)
	else:
	    Trace.log(e_errors.ERROR,
		      "Could not create outage file lock file (%s)"%(self.lockfile,))

    def close(self):
	EnFile.close(self)
	# get rid of the lock file
	os.system("rm -f %s"%(self.lockfile,))

    def read(self):
        try:
            EnFile.open(self, 'r')
            if self.openfile:
                code=string.join(self.openfile.readlines(),'')
                exec(code)
            else:
                outage = {}
                offline = {}
                override = {}
            try:
                outage_d = outage
            except UnboundLocalError:
                outage_d = {}
            try:
                offline_d = offline
            except UnboundLocalError:
                offline_d = {}
            try:
                override_d = override
            except UnboundLocalError:
                override_d = {}
        except:
            # can't find the module
            outage_d = {}
            offline_d = {}
            override_d = {}
        EnFile.close(self)
        return outage_d, offline_d, override_d

    # turn the dictionary into python code to be written out to the file
    def write(self, dict1, dict2, dict3):
        # open the file for writing
        self.open()
        # write out the dictionary
        if self.openfile:
	    self.do_write("outage = %s\n"%(dict1,))
	    self.do_write("offline = %s\n"%(dict2,))
	    self.do_write("override = %s\n"%(dict3,))
            rtn = 1
            # close the file
            self.close()
        else:
            # could not open the file
            rtn = 0
        return rtn


class SeenDownFile(EnFile):

    def __init__(self, dir, name):
        self.html_dir = dir
        EnFile.__init__(self, "%s/%s"%(dir, name))

    def open(self, mode='w'):
	# first check if we can get a lockfile for this
	try:
	    for attempt in [1,2,3,4,5]:
		os.stat(self.lockfile)
		# oops file exists, wait awhile then try again
		time.sleep(1)
	    else:
		Trace.log(e_errors.ERROR,
			"Could not create seen-down file lock file (%s)"%(self.lockfile,))
	except OSError:
	    # file does not exist, create it
	    os.system("touch %s"%(self.lockfile,))
	    EnFile.open(self, mode)

    def close(self):
        EnFile.close(self)
        # get rid of the lock file
        os.system("rm -f %s"%(self.lockfile,))

    def read(self):
        try:
            EnFile.open(self, 'r')
            if self.openfile:
                code=string.join(self.openfile.readlines(),'')
                exec(code)
            else:
                seen_down = {}
            try:
                seen_down_d = seen_down
            except AttributeError:
                seen_down_d = {}
        except:
            # can't find the module
            seen_down_d = {}
        EnFile.close(self)
        return seen_down_d

    # turn the dictionary into python code to be written out to the file
    def write(self, dict1):
        # open the file for writing
        self.open()

        # write out the dictionary
        if self.openfile:
            self.do_write("seen_down = %s\n"%(dict1,))
            rtn = 1
            # close the file
            self.close()
        else:
            # could not open the file
            rtn = 0
        return rtn


class EnstoreStatusFile(EnFile):

    def __init__(self, file):
	EnFile.__init__(self, file)
        self.file_name = "%s.new"%(file,)

    def write(self, enstat, outage_d, offline_d, override_d):
	self.do_write("status=%s"%([enstat[enstore_constants.ENSTORE],
				    enstat[enstore_constants.TIME],
				    offline_d.get(enstore_constants.ENSTORE,
						  enstore_constants.ENONE),
				    outage_d.get(enstore_constants.ENSTORE,
						 enstore_constants.ENONE),
				    override_d.get(enstore_constants.ENSTORE,
						   enstore_constants.ENONE),
				    enstore_functions.get_www_host()],))


class EnstoreIngestRatesFile(EnFile):

    def __init__(self, file):
	EnFile.__init__(self, file)
        self.file_name = "%s.new"%(file,)
        self.html_dir = enstore_functions2.get_dir(file)

    # format the config entry and write it to the file
    def write(self):
        if self.openfile:
            doc = enstore_html.EnSGIngestPage(self.html_dir)
            doc.body()
	    self.do_write(str(doc))


class EnstoreBpdFile(EnDataFile):

    def __init__(self, file):
	EnFile.__init__(self, file)
	self.lines = []

    def timed_read(self, start_time, stop_time):
	if start_time is None and stop_time is None:
	    # no start time or stop time  was entered, do last 30 days
	    start_time = enstore_functions2.get_days_ago(time.time(), 30)
	    start_time = enstore_functions2.format_plot_time(start_time)
	EnDataFile.timed_read(self, start_time, stop_time, "")

def read_schedule_file(html_dir=None):
    if html_dir is None:
        html_dir = enstore_functions.get_html_dir()
    # check if the html_dir is accessible
    sfile = None
    if os.path.exists(html_dir):
        sfile = ScheduleFile(html_dir, enstore_constants.OUTAGEFILE)
        outage_d, offline_d, override_d = sfile.read()
    else:
        outage_d = {}
        offline_d = {}
	override_d = {}
    return sfile, outage_d, offline_d, override_d

def read_seen_down_file(html_dir=None):
    if html_dir is None:
        html_dir = enstore_functions.get_html_dir()
    # check if the html_dir is accessible
    sfile = None
    if os.path.exists(html_dir):
        sfile = SeenDownFile(html_dir, enstore_constants.SEENDOWNFILE)
        seen_down_d = sfile.read()
    else:
        seen_down_d = {}
    return sfile, seen_down_d
