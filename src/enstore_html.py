# $Id$

import setpath

import string
import calendar
import time
setpath.addpath("$HTMLGEN_DIR")
import HTMLgen
import types
import os
import stat

import enstore_functions
import enstore_constants
import mover_constants
import safe_dict

FUSCHIA  = "#FF00FF"
YELLOW   = "#FFFFF0"
AQUA     = "#DFF0FF"
BRICKRED = "#770000"
DARKBLUE = "#000066"
TIMED_OUT_COLOR = "#FF9966"
SERVER_ERROR_COLOR = "#FFFF00"

NAV_TABLE_COLOR = YELLOW
NBSP = "&nbsp;"
CELLP = 3
MAX_SROW_TDS = 5
AT_MOVERS = 0
PENDING = AT_MOVERS + 1
POSTSCRIPT = "(postscript)"
UNKNOWN = "???"

TAG = 'tag'

MEDIA_CHANGERS = "Media Changers"
SERVERS = "Servers"
MOVERS = "Movers"
THE_INQUISITOR = "The Inquisitor"
THE_ALARM_SERVER = "The Alarm Server"

PLOT_INFO = [[enstore_constants.MPH_FILE, "Mounts/Hour"],
	     [enstore_constants.MPD_FILE, "Mounts/Day"],
	     [enstore_constants.MLAT_FILE, "Mount Latency"],
	     [enstore_constants.BPD_FILE_R, "Bytes Read/Day"],
	     [enstore_constants.BPD_FILE_W, "Bytes Written/Day"],
	     [enstore_constants.BPD_FILE, "Bytes/Day"], 
	     [enstore_constants.XFERLOG_FILE, "Transfer Activity (log)"],
	     [enstore_constants.XFER_FILE, "Transfer Activity"]]

DEFAULT_LABEL = "UNKNOWN INQ PLOT"

def sort_keys(dict):
    keys = dict.keys()
    keys.sort()
    return keys

def empty_header(cols=0):
    th = HTMLgen.TH(NBSP, html_escape='OFF')
    if cols:
	th.colspan = cols
    return th

def empty_data(cols=0):
    td = HTMLgen.TD(NBSP, html_escape='OFF')
    if cols:
	td.colspan = cols
    return td

def empty_row(cols=0):
    # output an empty row
    return HTMLgen.TR(empty_data(cols))

def table_spacer(table):
    table.append(empty_row())
    table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR())))
    table.append(empty_row())

def check_row(num_tds_so_far, tr, table):
    if num_tds_so_far == MAX_SROW_TDS:
	# finish this row and get a new one
	num_tds_so_far = 0
	table.append(tr)
	tr = HTMLgen.TR()
    return (tr, num_tds_so_far)

def check_row_length(tr, max, table):
    if len(tr) == max:
	table.append(tr)
	tr = HTMLgen.TR()
    return(tr)

def add_to_scut_row(num_tds_so_far, tr, table, link, text):
    tr, num_tds_so_far = check_row(num_tds_so_far, tr, table)
    tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	                 HTMLgen.Href(link, text), size="+1")),
			 bgcolor=YELLOW))
    return (tr, num_tds_so_far + 1)

def fill_out_row(num_tds_so_far, tr):
    while num_tds_so_far < MAX_SROW_TDS:
	td = empty_data()
	td.bgcolor = YELLOW
	tr.append(td)
	num_tds_so_far = num_tds_so_far + 1

class EnBaseHtmlDoc(HTMLgen.SimpleDocument):

    def set_meta(self):
	self.meta = HTMLgen.Meta(equiv="Refresh", content=self.refresh)

    # this is the base class for all of the html generated enstore documents
    def __init__(self, refresh=0, background="enstore.gif", help_file="", system_tag=""):
	self.textcolor = DARKBLUE
	self.background = background
	if self.background:
	    HTMLgen.SimpleDocument.__init__(self, background=self.background, 
					    textcolor=self.textcolor)
	else:
	    HTMLgen.SimpleDocument.__init__(self, textcolor=self.textcolor)
	self.refresh = refresh
	if not self.refresh == 0:
	    self.set_meta()
        self.source_server = ""
	self.contents = []
	self.help_file = help_file
	self.system_tag = system_tag

    # generate the three button navigation table for the top of each of the
    # enstore web pages
    def nav_table(self):
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href('enstore_system.html', 'Home'), size="+2")), 
			     bgcolor=NAV_TABLE_COLOR))
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href(enstore_constants.SAAGHTMLFILE, 'System'), size="+2")), 
			     bgcolor=NAV_TABLE_COLOR))
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href('status_enstore_system.html', SERVERS),
		  size="+2")), bgcolor=NAV_TABLE_COLOR))
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href('encp_enstore_system.html', 'Encp'),
		  size="+2")), bgcolor=NAV_TABLE_COLOR))
	if self.help_file:
	    tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(HTMLgen.Href(self.help_file, 'Help'),
							   size="+2")), bgcolor=NAV_TABLE_COLOR))
	table = HTMLgen.TableLite(tr, border=1, cellspacing=5, 
				  cellpadding=CELLP, align="LEFT")
	return table

    def spacer_data(self, data):
	return HTMLgen.TD(HTMLgen.Font("%s%s"%(NBSP*6, data), color=BRICKRED,
			  html_escape='OFF'))

    def null_row(self, rows):
	# output a null row with NO data
	td = HTMLgen.TD()
	td.colspan = rows
	return HTMLgen.TR(td)

    def script_title(self, tr):
	# output the script title at the top of the page
        if self.script_title_gif :
            tr.append(HTMLgen.TD(HTMLgen.Image(self.script_title_gif), align="RIGHT"))

    def table_top_b(self, table, td):
	if self.description:
	    td.append(HTMLgen.Font(self.description, html_escape='OFF', size="+2"))
	    td.append(HTMLgen.HR(size=2, noshade=1))
	table.append(HTMLgen.TR(td))
	table.append(empty_row())
	return table

    def table_top(self, add_update=1):
	# create the outer table and its rows	
	fl_table = HTMLgen.TableLite(cellspacing=0, cellpadding=0, align="LEFT",
				     width="800")
	tr = HTMLgen.TR(HTMLgen.TD(self.nav_table()))
	self.script_title(tr)
	fl_table.append(tr)
	# only add this info if we know it
	if self.source_server:
	    tr = HTMLgen.TR(empty_data())
	    td = HTMLgen.TD(HTMLgen.Emphasis(HTMLgen.Font("Brought To You By : ", size=-1)),
			    align="RIGHT")
	    td.append(HTMLgen.Font("%s"%(self.source_server,), size=-1))
	    tr.append(td)
	    fl_table.append(tr)
	# only add update information if asked to
	if add_update:
	    if self.system_tag:
		tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(self.system_tag,
								     size="+1", 
								     color=BRICKRED)),
					   align="LEFT"))
	    else:
		tr = HTMLgen.TR(empty_data())
	    td = HTMLgen.TD(HTMLgen.Emphasis(HTMLgen.Font("Last updated : ", size=-1)),
			    align="RIGHT")
	    td.append(HTMLgen.Font("%s"%(enstore_functions.format_time(time.time()),), 
				   html_escape='OFF', size=-1))
	    tr.append(td)	    
	    fl_table.append(tr)

	table = HTMLgen.TableLite(HTMLgen.TR(HTMLgen.TD(fl_table)), 
				  cellspacing=0, cellpadding=0, align="LEFT",
				  width="800")
	table.append(empty_row())
	td = HTMLgen.TD(HTMLgen.HR(size=2, noshade=1))
	self.table_top_b(table, td)
	return table

    def set_refresh(self, refresh):
	self.refresh = refresh

    def get_refresh(self):
	return self.refresh

    # create a table header
    def make_th(self, name):
	return HTMLgen.TH(HTMLgen.Bold(HTMLgen.Font(name, size="+2", 
						    color=BRICKRED)),
			  align="CENTER")

class EnSysStatusPage(EnBaseHtmlDoc):

    def __init__(self, refresh = 60, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh, system_tag=system_tag)
	self.title = "ENSTORE System Status"
	self.script_title_gif = "ess.gif"
	self.source_server = THE_INQUISITOR
	self.description = "%s%sCurrent status of the running Enstore system as listed in the %s."%(NBSP, NBSP, 		                            HTMLgen.Href("config_enstore_system.html", "Configuration file"))

    # output the list of shortcuts on the top of the page
    def shortcut_table(self):
	# get a list of all the servers we have.  we will output a link for the
	# following - 
	#              servers in general
	#              library managers
	#              movers
	#              media changers
	self.servers = sort_keys(self.data_dict)
	got_media_changers = 0
	got_generic_servers = 0
	got_movers = 0
	shortcut_lm = []
	for server in self.servers:
	    if enstore_functions.is_library_manager(server):
		shortcut_lm.append(server)
	    elif not got_media_changers and enstore_functions.is_media_changer(server):
		got_media_changers = 1
	    elif not got_generic_servers and enstore_functions.is_generic_server(server):
		got_generic_servers = 1
	    elif not got_movers and enstore_functions.is_mover(server):
                first_mover = server
		got_movers = 1
	# now we have the list of table data elements.  now create the table.
	caption = HTMLgen.Caption(HTMLgen.Bold(HTMLgen.Font("Shortcuts", 
							    color=BRICKRED,
							    size="+2")))
	table = HTMLgen.TableLite(caption, cellspacing=5, cellpadding=CELLP,
				  align="LEFT", border=2, width = "100%")
	# now make the rows, we want them in a certain order, so look for them
	# in that order.
	num_tds_so_far = 0
	tr = HTMLgen.TR()
	if got_generic_servers:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#servers', SERVERS)
	# now do all the library managers
	for lm in shortcut_lm:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#%s'%(lm,), lm)
	# now finish up with the media changers, movers
	if got_media_changers:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#%s'%(enstore_constants.MEDIA_CHANGER,),
						  MEDIA_CHANGERS)
	if got_movers:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#%s'%(first_mover,), MOVERS)
	# fill out the row if we ended with less than a rows worth of data
	fill_out_row(num_tds_so_far, tr)
	table.append(tr)
	return table
	
    # make the row with the servers alive information
    def alive_row(self, server, data, color=None):
	srvr = HTMLgen.Bold(HTMLgen.Font(server, size="+1"))
	# change the color of the first column if the server has timed out
	if data[0] == "alive":
	    if color:
		# use the user specified color
		tr = HTMLgen.TR(HTMLgen.TD(srvr, bgcolor=color))
	    else:
		tr = HTMLgen.TR(HTMLgen.TD(srvr))
	elif  data[0] == "error":
	    tr = HTMLgen.TR(HTMLgen.TD(srvr, bgcolor=SERVER_ERROR_COLOR))
	else:
	    tr = HTMLgen.TR(HTMLgen.TD(srvr, bgcolor=TIMED_OUT_COLOR))
	for datum in data:
	    tr.append(HTMLgen.TD(datum))
	if len(data) == 4:
	    # there was no last_alive time so fill in
	    tr.append(empty_data())
	return tr

    # add in the information for the generic servers. these only have alive
    # information
    def generic_server_rows(self, table):
	for server in enstore_constants.GENERIC_SERVERS:
	    if self.data_dict[server]:
		# output its information
		table.append(self.alive_row(server, 
			       self.data_dict[server][enstore_constants.STATUS]))

    # create the suspect volume row - it is a separate table
    def suspect_volume_row(self, lm):
	tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Suspect%sVolumes"%(NBSP,), 
						color=BRICKRED, html_escape='OFF')))
	# format the suspect volumes, 3 to a row
	vols = self.data_dict[lm][enstore_constants.SUSPECT_VOLS]
	vol_str = ""
	ctr = 0
	for vol in vols:
	    if vol_str:
		# separate the new volume from the old ones
		if ctr == 3:
		    vol_str = "%s,  <BR>"%(vol_str,)
		    ctr = 0
		else:
		    vol_str = "%s,  "%(vol_str,)
	    ctr = ctr + 1
	    vol_str = "%s %s"%(vol_str, vol)
	tr.append(HTMLgen.TD(vol_str, align="LEFT", colspan=4, html_escape='OFF'))
	return tr

    # given the type of work and the type of queue, return the text to be 
    # displayed to describe this queue element
    def get_intro_text(self, work, queue):
	if queue == AT_MOVERS:
	    if work == enstore_constants.WRITE:
		text = "Writing%stape"%(NBSP,)
	    else:
		text = "Reading%stape"%(NBSP,)
	else:
	    if work == enstore_constants.WRITE:
		text = "Pending%sTape%sWrite"%(NBSP,NBSP)
	    else:
		text = "Pending%sTape%sRead"%(NBSP,NBSP)
	return text

    # put together the rows for either lm queue
    def lm_queue_rows(self, lm, queue, intro):
	table = HTMLgen.TableLite(cellpadding=0, cellspacing=0, 
				  align="LEFT", bgcolor=YELLOW, width="100%")
	qelems = self.data_dict[lm][queue]
	qelems.sort()
	for qelem in self.data_dict[lm][queue]:
	    text = self.get_intro_text(qelem[enstore_constants.WORK], intro)
	    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(text, color=BRICKRED, html_escape='OFF')))
	    if qelem.has_key(enstore_constants.MOVER):
		tr.append(HTMLgen.TD(HTMLgen.Href("#%s"%(qelem[enstore_constants.MOVER],),
						  qelem[enstore_constants.MOVER])))
	    else:
		tr.append(empty_data())
	    tr.append(HTMLgen.TD(HTMLgen.Font("Node", color=BRICKRED)))
	    tr.append(HTMLgen.TD(enstore_functions.strip_node(qelem[enstore_constants.NODE])))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Port", color=BRICKRED)))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.PORT]))
	    table.append(tr)
	    if qelem.has_key(enstore_constants.DEVICE):
		tr = HTMLgen.TR(self.spacer_data("Device%sLabel"%(NBSP,)))
		tr.append(HTMLgen.TD(qelem[enstore_constants.DEVICE]))
	    else:
		tr = HTMLgen.TR(empty_data())
		tr.append(empty_data())
	    if qelem.has_key(enstore_constants.FILE_FAMILY):
		tr.append(HTMLgen.TD(HTMLgen.Font("File%sFamily"%(NBSP,), color=BRICKRED,
						  html_escape='OFF')))
		tr.append(HTMLgen.TD(qelem[enstore_constants.FILE_FAMILY]))
		tr.append(HTMLgen.TD(HTMLgen.Font("File%sFamily%sWidth"%(NBSP, NBSP),
						  color=BRICKRED, html_escape='OFF')))
		tr.append(HTMLgen.TD(qelem[enstore_constants.FILE_FAMILY_WIDTH]))
	    elif qelem.has_key(enstore_constants.VOLUME_FAMILY):
		tr.append(HTMLgen.TD(HTMLgen.Font("Volume%sFamily"%(NBSP,), color=BRICKRED,
						  html_escape='OFF')))
		tr.append(HTMLgen.TD(qelem[enstore_constants.VOLUME_FAMILY]))
		tr.append(empty_data())
		tr.append(empty_data())
	    else:
		tr.append(empty_data())
		tr.append(empty_data())
		tr.append(empty_data())
		tr.append(empty_data())
	    table.append(tr)
	    tr = HTMLgen.TR(self.spacer_data("Job%sSubmitted"%(NBSP,)))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.SUBMITTED]))
	    if qelem.has_key(enstore_constants.DEQUEUED):
		tr.append(HTMLgen.TD(HTMLgen.Font("Dequeued", color=BRICKRED)))
		tr.append(HTMLgen.TD(qelem[enstore_constants.DEQUEUED]))
	    else:
		tr.append(empty_data())
		tr.append(empty_data())
	    if qelem.has_key(enstore_constants.MODIFICATION):
		tr.append(HTMLgen.TD(HTMLgen.Font("File%sModified"%(NBSP,),
						  color=BRICKRED, html_escape='OFF')))
		tr.append(HTMLgen.TD(qelem[enstore_constants.MODIFICATION]))
	    else:
		tr.append(empty_data())
		tr.append(empty_data())
	    table.append(tr)
	    tr = HTMLgen.TR(self.spacer_data("Priorities"))
	    tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Current",
							color=BRICKRED), 
					   NBSP*3, qelem[enstore_constants.CURRENT]),
				 html_escape='OFF'))
	    tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Base",
							color=BRICKRED),
					   NBSP*3, qelem[enstore_constants.BASE]),
				 html_escape='OFF'))
	    tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Delta",
							color=BRICKRED),
					   NBSP*3, qelem[enstore_constants.DELTA]),
				 html_escape='OFF'))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Agetime", color=BRICKRED)))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.AGETIME]))
	    table.append(tr)
	    tr = HTMLgen.TR(self.spacer_data("Local%sfile"%(NBSP,)))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.FILE], colspan=5))
	    table.append(tr)
	    tr = HTMLgen.TR(self.spacer_data("Bytes"))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.BYTES]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("ID", color=BRICKRED)))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.ID], colspan=3))
	    table.append(tr)
	    if qelem.has_key(enstore_constants.REJECT_REASON):
		tr = HTMLgen.TR(self.spacer_data("Reason%sfor%sPending"%(NBSP,NBSP)))
		tr.append(HTMLgen.TD(qelem[enstore_constants.REJECT_REASON], colspan=5))
		table.append(tr)
	    table.append(empty_row(6))
	return HTMLgen.TR(HTMLgen.TD(table, colspan=5))

    # add the work at movers info to the table
    def work_at_movers_row(self, lm, cols):
	# These are the keys used in work at movers queue
	# WORK, NODE, PORT, FILE, FILE_FAMILY, FILE_FAMILY_WIDTH, SUBMITTED,
	# DEQUEUED, MODIFICATION, CURRENT, BASE, DELTA, AGETIME, FILE, BYTES, 
	# ID
	the_work = self.data_dict[lm].get(enstore_constants.WORK,
					  enstore_constants.NO_WORK)
	if not the_work == enstore_constants.NO_WORK:
	    row = self.lm_queue_rows(lm, enstore_constants.WORK, AT_MOVERS)
	else:
	    row = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(enstore_constants.NO_WORK,
						     color=BRICKRED), 
					colspan=cols))
	return row

    # add the pending work row to the table
    def pending_work_row(self, lm, cols):
	# These are the keys used in pending work
	# NODE, PORT, FILE, FILE_FAMILY, FILE_FAMILY_WIDTH, SUBMITTED,
	# CURRENT, BASE, DELTA, AGETIME, FILE, BYTES, ID
	the_work = self.data_dict[lm].get(enstore_constants.PENDING,
					  enstore_constants.NO_PENDING)
	if not the_work == enstore_constants.NO_PENDING:
	    row = self.lm_queue_rows(lm, enstore_constants.PENDING, PENDING)
	else:
	    row = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(enstore_constants.NO_PENDING,
						     color=BRICKRED),
					colspan=cols))
	return row

    # output the state of the lirary manager
    def lm_state_row(self, lm):
	row = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("State", color=BRICKRED)))
	row.append(HTMLgen.TD(self.data_dict[lm][enstore_constants.LMSTATE], 
			      colspan=4, align="LEFT"))
	return row

    def make_xfer_row(self, tr, lm, key_label):
	for (hdg, key) in key_label:
	    td = HTMLgen.TD(HTMLgen.Font("%s%s"%(hdg, NBSP*2), color=BRICKRED,
					  html_escape='OFF'))
	    td.append(self.data_dict[lm][key])
	    tr.append(td)
	return tr

    # output the row that lists the current total/read/write transfers row
    def ongoing_xfer_row(self, lm):
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(HTMLgen.Font("Ongoing%sTransfers"%(NBSP,), color=BRICKRED,
					  html_escape='OFF'), COLSPAN=2))
	return(self.make_xfer_row(tr, lm, [("Total", enstore_constants.TOTALONXFERS),
					   ("Read", enstore_constants.READONXFERS),
					   ("Write", enstore_constants.WRITEONXFERS)]))

    # output the row that lists the pending total/read/write transfers row
    def pending_xfer_row(self, lm):
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(HTMLgen.Font("Pending%sTransfers"%(NBSP,), color=BRICKRED,
					  html_escape='OFF'), COLSPAN=2))
	return(self.make_xfer_row(tr, lm, [("Total", enstore_constants.TOTALPXFERS),
					   ("Read", enstore_constants.READPXFERS),
					   ("Write", enstore_constants.WRITEPXFERS)]))

    # output the information for a library manager
    def lm_rows(self, lm, table):
	cols = 5
	# first the alive information
	lm_status = self.data_dict[lm][enstore_constants.STATUS]
	table.append(self.alive_row(HTMLgen.Name(lm, lm), lm_status))
	# we may have gotten an error while trying to get the info, 
	# so check for a piece of it first
	if self.data_dict[lm].has_key(enstore_constants.LMSTATE):
	    # the rest of the lm information is in a separate table, it starts
	    # with the suspect volume info
	    lm_table = HTMLgen.TableLite(self.lm_state_row(lm), cellpadding=0, 
					 cellspacing=0, align="LEFT", 
					 bgcolor=YELLOW, width="100%")
	    lm_table.append(self.suspect_volume_row(lm))
	    lm_table.append(self.ongoing_xfer_row(lm))
	    lm_table.append(self.pending_xfer_row(lm))
	    lm_table.append(self.null_row(cols))
	    lm_table.append(empty_row(cols))
	    lm_table.append(self.work_at_movers_row(lm, cols))
	    lm_table.append(self.pending_work_row(lm, cols))
	    tr = HTMLgen.TR(empty_data())
	    tr.append(HTMLgen.TD(lm_table, colspan=5))
	    table.append(tr)

    # add the volume information if it exists
    def add_bytes_volume_info(self, moverd, tr, mvkey):
	if moverd.has_key(enstore_constants.VOLUME):
	    tr.append(HTMLgen.TD(moverd[mvkey]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Volume", color=BRICKRED),
				 align="CENTER"))
	    tr.append(HTMLgen.TD(moverd[enstore_constants.VOLUME]))
	else:
	    tr.append(HTMLgen.TD(moverd[mvkey], colspan=3))

    # add the eod/location cookie information if it exists
    def add_bytes_eod_info(self, moverd, tr, mvkey):
	if moverd.has_key(enstore_constants.EOD_COOKIE):
	    tr.append(HTMLgen.TD(moverd[mvkey]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("EOD%sCookie"%(NBSP,), color=BRICKRED,
					      html_escape='OFF'),
				 align="CENTER"))
	    tr.append(HTMLgen.TD(moverd[enstore_constants.EOD_COOKIE]))
	elif moverd.has_key(enstore_constants.LOCATION_COOKIE):
	    tr.append(HTMLgen.TD(moverd[mvkey]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Location%sCookie"%(NBSP,), 
					      color=BRICKRED, html_escape='OFF'),
				 align="CENTER"))
	    tr.append(HTMLgen.TD(moverd[enstore_constants.LOCATION_COOKIE]))
	else:
	    tr.append(HTMLgen.TD(moverd[mvkey], colspan=3))

    # add input and output files 
    def add_files(self, moverd, table):
	if moverd.has_key(enstore_constants.FILES):
	    # we need to make the table able to hold a long file name
	    table.width = "100%"
	    table.append(empty_row(4))
	    for i in [0, 1]:
		tr = HTMLgen.TR(HTMLgen.TD(moverd[enstore_constants.FILES][i], 
					   colspan=4))
		table.append(tr)
	    table.append(empty_row(4))
	else:
	    table.width = "40%"

    # add the mover information
    def mv_row(self, mover, table):
	# we may not have any other info on this mover as the inq may not be
	# watching it.
	moverd = self.data_dict.get(mover, {})
	mover_name = HTMLgen.Name(mover, mover)
	if moverd:
	    # we may have gotten an error when trying to get it, 
	    # so look for a piece of it.  
	    if moverd.has_key(enstore_constants.STATE):
		if moverd[enstore_constants.STATE] == mover_constants.OFFLINE:
		    table.append(self.alive_row(mover_name, moverd[enstore_constants.STATUS],
						FUSCHIA))
		else:
		    table.append(self.alive_row(mover_name, moverd[enstore_constants.STATUS]))

		tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Completed%sTransfers"%(NBSP,),
							color=BRICKRED, html_escape='OFF')))
		tr.append(HTMLgen.TD(moverd[enstore_constants.COMPLETED], align="LEFT"))
		tr.append(HTMLgen.TD(HTMLgen.Font("Failed%sTransfers"%(NBSP,),
						  color=BRICKRED, html_escape='OFF')))
		tr.append(HTMLgen.TD(moverd[enstore_constants.FAILED], align="LEFT"))
		mv_table = HTMLgen.TableLite(tr, cellspacing=0, cellpadding=0,
					     align="LEFT", bgcolor=YELLOW, width="100%")
		tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Current%sState"%(NBSP,),
							color=BRICKRED, html_escape='OFF')))
		tr.append(HTMLgen.TD(moverd[enstore_constants.STATE], colspan=3, 
				     align="LEFT"))
		mv_table.append(tr)
		mv_table.append(empty_row(4))
		if moverd.has_key(enstore_constants.LAST_READ):
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Last%sRead%s(bytes)"%(NBSP, NBSP),
							    color=BRICKRED, html_escape='OFF'),
					       align="CENTER"))
		    self.add_bytes_volume_info(moverd, tr, 
					       enstore_constants.LAST_READ)
		    mv_table.append(tr)
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Last%sWrite%s(bytes)"%(NBSP, NBSP),
						      color=BRICKRED, html_escape='OFF'),
					       align="CENTER"))
		    self.add_bytes_eod_info(moverd, tr, enstore_constants.LAST_WRITE)
		    mv_table.append(tr)
		    self.add_files(moverd, mv_table)
		elif moverd.has_key(enstore_constants.CUR_READ):
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Current%sRead%s(bytes)"%(NBSP, NBSP),
							    color=BRICKRED, html_escape='OFF'),
					       align="CENTER"))
		    self.add_bytes_volume_info(moverd, tr, enstore_constants.CUR_READ)
		    mv_table.append(tr)
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Current%sWrite%s(bytes)"%(NBSP, NBSP),
						      color=BRICKRED, html_escape='OFF'),
					       align="CENTER"))
		    self.add_bytes_eod_info(moverd, tr, enstore_constants.CUR_WRITE)
		    mv_table.append(tr)
		    self.add_files(moverd, mv_table)
		tr = HTMLgen.TR(empty_data())
		tr.append(HTMLgen.TD(mv_table, colspan=5, width="100%"))
		table.append(tr)
	    else:
		# all we have is the alive information
		table.append(self.alive_row(mover_name, moverd[enstore_constants.STATUS]))

    # output all of the library manager rows and their associated movers
    def library_manager_rows(self, table, skeys):
	for server in skeys:
	    if enstore_functions.is_library_manager(server):
		# this is a library manager. output all of its info and then
		# info for each of its movers
		self.lm_rows(server, table)

    # output all of the media changer rows 
    def media_changer_rows(self, table, skeys):
	first_time = 1
	for server in skeys:
	    if enstore_functions.is_media_changer(server):
		# this is a media changer. output its alive info
		if first_time:
		    table.append(self.alive_row(HTMLgen.Name(enstore_constants.MEDIA_CHANGER,
                                                             server), 
				self.data_dict[server][enstore_constants.STATUS]))
                    first_time = 0
		else:
		    table.append(self.alive_row(server, 
				self.data_dict[server][enstore_constants.STATUS]))

    # output all of the mover rows 
    def mover_rows(self, table, skeys):
        first_time = 1
	for server in skeys:
	    # look for movers
	    if enstore_functions.is_mover(server):
		# this is a mover. output its info
		self.mv_row(server, table)

    # generate the main table with all of the information
    def main_table(self):
	# first create the table headings for each column
	tr = HTMLgen.TR()
	for hdr in ["Name", "Status", "Host", "Port", "Date/Time", "Last Time Alive"]:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, align="CENTER", cellpadding=0,
				  cellspacing=0, bgcolor=AQUA, width="100%")
	skeys = sort_keys(self.data_dict)
	self.generic_server_rows(table)
	self.library_manager_rows(table, skeys)
	self.mover_rows(table, skeys)
	self.media_changer_rows(table, skeys)
	return table

    # generate the body of the file
    def body(self, data_dict):
	# this is the data we will output
	self.data_dict = safe_dict.SafeDict(data_dict)
	# create the outer table and its rows
	table = self.table_top()
	table.append(HTMLgen.TR(HTMLgen.TD(self.shortcut_table())))
	table.append(empty_row())
	table.append(empty_row())
	td = HTMLgen.TD(HTMLgen.Name("servers"))
	td.append(self.main_table())
	table.append(HTMLgen.TR(td))
	self.append(table)

class EnEncpStatusPage(EnBaseHtmlDoc):


    error_text = {"USER ERROR, No Such File"                  : "No such file",
                  "USER ERROR, No Read Access"                : "EACCES",
                  "USER ERROR, No access"                     : "NOACCESS",
                  "USER ERROR, Duplicate request, ignored"    : "INPROGRESS",
                  "USER ERROR, File already exists"           : "EEXIST",
                  "TAPE ERROR, Tape not in robot or admin marked as bad" : "NOTALLOWED",
		  "COMMUNICATION ERROR, retrying"             : "impostor",
                  "TAPE TROUBLE, At least 2 IO errors on vol, pending request cancelled": "STATUS=N O",
                  "HARDWARE FAILURE, Drive didn't go online, retrying"  : "BADSWMOUNT",
                  "HARDWARE FAILURE, AML/2 robot failed"      : "BADMOUNT",
                  "HARDWARE FAILURE, Read Error"              : "READ_ERROR",
                  "HARDWARE FAILURE, Write Error"             : "WRITE_ERROR",
                  "TAPE TROUBLE, IO error, retrying"          : "BROKENPIPE",
                  "USER ERROR, No Local Disk Space"           : "ENOSPC"
                  }
# too general     "USER ERROR"                                : "USERERROR",

    def __init__(self, refresh=120, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh, help_file="encpHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE Encp History"
	self.script_title_gif = "encph.gif"
	self.source_server = THE_INQUISITOR
	self.description = ""
	self.error_keys = self.error_text.keys()

    # create the body of the page. the data is a list of lists.  each outer list element
    # is a list of the encp data i.e. - 
    #        [["10:43:49", "d0ensrv1.fnal.gov", "bakken", "55,255", "samnull-1", "3.5", "0.035"]]
    def body(self, data_list):
	table = self.table_top()
	# now create the table with the data in it, first do the row with
	# the headings
	tr = HTMLgen.TR(valign="CENTER")
	headings = ["Time", "Node", "User", "Bytes", "Volume",
		    "Data Transfer Rate (MB/S)", "User Rate (MB/S)"]
	num_headings = len(headings)
	for hding in headings:
	    tr.append(self.make_th(hding))
	en_table = HTMLgen.TableLite(tr, border=1, bgcolor=AQUA, width="100%",
				     cols=7, cellspacing=5, cellpadding=CELLP,
				     align="CENTER")
	num_errors = 0
	errors = []
	for row in data_list:
            ##  XXX HACK: skip error messages that should not have gotten logged,
            ## due to a mistake in encp
            if len(row)==4 and (row[3][:5] in ('Fatal', 'Error')):
                    continue
                ##XXX end hack cgw

	    tr = HTMLgen.TR(HTMLgen.TD(row[0]))
	    # remove .fnal.gov from the node
	    row[1] = enstore_functions.strip_node(row[1])
	    if  len(row) != 4:
		# this is a normal encp data transfer row
		for item in row[1:]:
		    tr.append(HTMLgen.TD(item))
	    else:
		# this row is an error row
		tr.append(HTMLgen.TD(row[1]))
		tr.append(HTMLgen.TD(row[2]))
		num_errors = num_errors + 1
		errors.append(row[3])
		# we need to check the error text.  if it contains certain strings (specified
		# in error_text), then we will output a different string pointing to the
		# actual error message at the bottom of the page.
		for ekey in self.error_keys:
		    if string.find(row[3], self.error_text[ekey]) != -1:
			# found a match
			etxt = ekey
			break
		else:
		    # this is the default
		    etxt = "ERROR"
		tr.append(HTMLgen.TD(HTMLgen.Href("#%s"%(num_errors),
						  HTMLgen.Bold("%s (%s)"%(etxt, num_errors,))),
				     colspan=(num_headings-3)))
	    en_table.append(tr)
	table.append(HTMLgen.TR(HTMLgen.TD(en_table)))
	# now make the table with the error information
	en_table = HTMLgen.TableLite()
	for i in range(num_errors):
	    si = "%s"%(i+1,)
	    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(si), size="+2")))
	    tr.append(HTMLgen.TD(HTMLgen.Name(si, errors[i])))
	    en_table.append(tr)
	    en_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR(), colspan=2)))
	table.append(HTMLgen.TR(HTMLgen.TD(en_table)))
	self.append(table)							 

class EnConfigurationPage(EnBaseHtmlDoc):

    def __init__(self, refresh=600, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh, help_file="configHelp.html", 
			       system_tag=system_tag)
	self.title = "ENSTORE Configuration"
	self.script_title_gif = "en_cfg.gif"
	self.source_server = THE_INQUISITOR
	self.description = ""

    # create the body of the page. the incoming data is a python dictionary
    def body(self, data_dict):
	table = self.table_top()
	# now add a top table with links to the individual servers (as a shortcut)
	dkeys = sort_keys(data_dict)
	caption = HTMLgen.Caption(HTMLgen.Bold(HTMLgen.Font("Shortcuts", 
							    color=BRICKRED,
							    size="+2")))
	shortcut_table = HTMLgen.TableLite(caption, border=1, bgcolor=AQUA, 
					   cellspacing=5, cellpadding=CELLP)
	tr = HTMLgen.TR()
	num_tds_so_far = 0
	for server in dkeys:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, 
						 shortcut_table,
						 "#%s"%(server,), server)
	else:
	    fill_out_row(num_tds_so_far, tr)
	    shortcut_table.append(tr)
	table.append(HTMLgen.TR(HTMLgen.TD(shortcut_table)))
	table.append(empty_row())
	# now add the table with the information
	cfg_table = HTMLgen.TableLite(border=1, bgcolor=AQUA, 
				      cellspacing=5, cellpadding=CELLP)
	tr = HTMLgen.TR()
	for hding in ["Server", "Element", "Value"]:
	    tr.append(self.make_th(hding))
	cfg_table.append(tr)
	for server in dkeys:
	    server_dict = data_dict[server]
	    server_keys = sort_keys(server_dict)
	    first_line = 1
	    for server_key in server_keys:
		if first_line:
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font( \
			HTMLgen.Name(server, HTMLgen.Bold(server)), size="+1")))
		    first_line = 0
		else:
		    tr = HTMLgen.TR(empty_data())
		tr.append(HTMLgen.TD(server_key))
		tr.append(HTMLgen.TD(server_dict[server_key]))
		cfg_table.append(tr)
		
	# add this table to the main one
	table.append(HTMLgen.TR(HTMLgen.TD(cfg_table)))
	self.append(table)							 

class EnMiscPage(EnBaseHtmlDoc):

    def __init__(self, refresh=600, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh, help_file="miscHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE Miscellany"
	self.script_title_gif = "en_misc.gif"
	self.source_server = THE_INQUISITOR
	self.description = ""

    # create the body of the page, the incoming data is a list of strings
    def body(self, (data_list, html_dir)):
	table = self.table_top()
	tr = HTMLgen.TR(self.make_th("Miscellaneous Command Output File"))
	tr.append(self.make_th("Creation Date"))
	file_table = HTMLgen.TableLite(tr, border=1, cellspacing=5, cellpadding=CELLP,
				       align="LEFT", bgcolor=AQUA)
	if data_list:
	    # now the data
	    data_list.sort()
	    for item in data_list:
		tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Href(item, item), 
							size="+2")))
		# get the modification date of the file too
		tr.append(HTMLgen.TD(enstore_functions.format_time(os.stat("%s/%s"%(html_dir,
								     item))[stat.ST_MTIME])))
		file_table.append(tr)
	table.append(HTMLgen.TR(HTMLgen.TD(file_table)))
	# output on the same page a list of the files indicating misc jobs are active
	# this may aid in diagnosing problems with hung misc jobs.
	table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR())))
	table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold("Directories Created For Currently Running Miscellaneous Processes"), size="+4"))))
	table.append(empty_row())
	home = "%s/MISC"%(os.environ['HOME'],)
        try:
            dirs = os.listdir(home)
        except OSError:
            dirs = []
	tr = HTMLgen.TR(self.make_th("Directory"))
	tr.append(self.make_th("Creation Date"))
	dirs_table = HTMLgen.TableLite(tr, border=1, cellspacing=5, cellpadding=CELLP,
				       align="LEFT", bgcolor=AQUA)
	for dir in dirs:
	    file = "%s/%s"%(home, dir)
	    tr = HTMLgen.TR(HTMLgen.TD(file))
	    # get the modification date of the directory too
	    tr.append(HTMLgen.TD(enstore_functions.format_time(os.stat(file)[stat.ST_MTIME])))
	    dirs_table.append(tr)
	table.append(HTMLgen.TR(HTMLgen.TD(dirs_table)))
	self.append(table)

class EnLogPage(EnBaseHtmlDoc):

    def __init__(self, refresh=600, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh, help_file="logHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE Log Files"
	self.script_title_gif = "en_log.gif"
	self.source_server = THE_INQUISITOR
	self.description = "Enstore log files may also be %s"%(str(HTMLgen.Bold(HTMLgen.Href('enstore_log_file_search.html', 'searched'))),)

    def logfile_date(self, logfile):
	(prefix, year, month, day) = string.split(logfile, '-')
	month = string.atoi(month)
	year = string.atoi(year)
	day = string.atoi(day)
	return (prefix, year, month, day)

    # given a dict of log files, create a list of lists which divides the log files up by month
    # and year. most recent goes first
    def find_months(self, logs):
	lkeys = sort_keys(logs)
	lkeys.reverse()
	log_months = {}
	dates = []
	for log in lkeys:
	    (prefix, year, month, day) = self.logfile_date(log)
            if year == 0:
                # there was an error in logfile_date
                continue
	    date = "%s %s"%(calendar.month_name[month], year)
	    if not dates or not (year, month, date) in dates:
		dates.append((year, month, date))
	    if not log_months.has_key(date):
		log_months[date] = {}
	    log_months[date][day] = (logs[log], log)
	dates.sort()
	dates.reverse()
	return (dates, log_months)

    # generate the calendar looking months with url's for each day for which there exists a log
    # file. the data in logs, should be a dictionary where the log file names are the keys and
    # the value the size of the file
    def generate_months(self, table, logs, web_host, caption_title="Enstore Log Files"):
	(dates, sizes) = self.find_months(logs)
	did_title = 0
	for (year, month, date) in dates:
	    caption = HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold(date), 
						   size="+2", color=BRICKRED))
	    if not did_title:
		did_title = 1
		caption.prepend(HTMLgen.BR())
		caption.prepend(HTMLgen.BR())
		caption.prepend(HTMLgen.Font(HTMLgen.Bold(caption_title), 
					     size="+2", color=BRICKRED))
	    log_table =  HTMLgen.TableLite(caption, bgcolor=AQUA, cellspacing=5, 
					   cellpadding=CELLP, align="LEFT", border=2)
	    tr = HTMLgen.TR()
	    for day in calendar.day_abbr:
		tr.append(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(day),size="+1", color=BRICKRED)))
	    log_table.append(tr)
	    # the following generates a  list of lists, which specifies how to draw a calendar
	    # with the first of the month occuring in the correct day of the week slot.
	    mweeks = calendar.monthcalendar(year, month)
	    for mweek in mweeks:
		tr = HTMLgen.TR()
		for day in [0,1,2,3,4,5,6]:
		    if mweek[day] == 0:
			# this is null entry represented by a blank entry on the calendar
			tr.append(empty_data())
		    else:
			(size, log) = sizes[date].get(mweek[day], (-1, ""))
			if size == -1:
			    # there was no log file for this day
			    tr.append(HTMLgen.TD(HTMLgen.Bold(mweek[day]), bgcolor=YELLOW))
			else:
			    td = HTMLgen.TD(HTMLgen.Href("%s/%s"%(web_host, log), 
							 HTMLgen.Font(HTMLgen.Bold(mweek[day]),
								      size="+2")))
			    td.append(" : %s"%(size,))
			    tr.append(td)
		log_table.append(tr)
	    table.append(HTMLgen.TR(HTMLgen.TD(log_table)))
	    table.append(empty_row())

    # create the body of the page, where http_path is the web server path to the files, www_host
    # is the host where the web server is running, user_logs is a dictionary that contains
    # user logs and logs is a dictionary where the log file names are the keys and the sizes are
    # the values.
    def body(self, http_path, logs, user_logs, www_host):
	table = self.table_top()
	# now add the data, first the table with the user specified log files in it
	log_table = HTMLgen.TableLite(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold(\
	             "User Specified Log Files"), size="+2", color=BRICKRED)),
				      width="50%", cellspacing=5, cellpadding=CELLP, 
				      align="LEFT")
	ul_keys = sort_keys(user_logs)
	for ul_key in ul_keys:
	    log_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(user_logs[ul_key], 
							       str(HTMLgen.Bold(ul_key))))))
	table.append(HTMLgen.TR(HTMLgen.TD(log_table)))
	log_table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR())))
	# now create the tables for the different months.
	self.generate_months(table, logs, "%s%s"%(www_host, http_path))
	self.append(table)							 

class EnAlarmPage(EnBaseHtmlDoc):

    def __init__(self, refresh=600, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh, help_file="alarmHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE Alarms"
	self.script_title_gif = "en_act_alarms.gif"
	self.source_server = THE_ALARM_SERVER
	self.description = "%s may also be displayed."%(str(HTMLgen.Bold(HTMLgen.Href('enstore_alarm_search.html', 'Previous alarms'))),)

    def alarm_table(self, alarms):
	tr = HTMLgen.TR()
	for hdr in ["Key", "Time", "Node", "PID", "User", "Severity", 
		    "Process", "Error", "Additional Information"]:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, width="100%", border=1, cellspacing=5, 
				  cellpadding=CELLP, align="LEFT", bgcolor=AQUA)
	i = 0
	akeys = sort_keys(alarms)
	for akey in akeys:
	    alarm = alarms[akey].list_alarm()
	    td = HTMLgen.TD(HTMLgen.Input(type="radio", name="alarm%s"%(i,),
						     value=alarm[0]),
			    html_escape='OFF')
	    td.append("%s%s"%(NBSP*3, alarm[0]))
	    tr = HTMLgen.TR(td)
	    # remove .fnal.gov
	    alarm[1] = enstore_functions.strip_node(alarm[1])
	    tr.append(HTMLgen.TD(enstore_functions.format_time(float(alarm[0]))))
	    for item in alarm[1:]:
		tr.append(HTMLgen.TD(item))
	    table.append(tr)
	    i = i + 1
	return table

    def body(self, alarms, web_host):
	table = self.table_top()
	# now the data
	exe = HTMLgen.TR(HTMLgen.TD(HTMLgen.Input(value="Execute", type="submit")))
	rst = HTMLgen.TR(HTMLgen.TD(HTMLgen.Input(value="Reset", type="reset")))
	form = HTMLgen.Form("%s/cgi-bin/enstore/enstore_alarm_cgi.py"%(web_host,))
	# get rid of the default submit button, we will add our own below
	form.submit = ''
	form.append(HTMLgen.TR(HTMLgen.TD(self.alarm_table(alarms))))
	form.append(empty_row())
	form.append(empty_row())
	tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Input(value="Execute", 
						 type="submit")))
	tr.append(HTMLgen.TD(HTMLgen.Input(value="Reset", type="reset")))
	tr.append(HTMLgen.TD("Alarms may be cancelled by selecting the alarm(s), pressing the %s button and then reloading the page."%(str(HTMLgen.Bold("Execute")),), html_escape='OFF'))
	form.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.TableLite(tr, 
							    width="100%"))))
	table.append(form)
	self.append(table)

class EnPatrolPage(EnBaseHtmlDoc):

    def __init__(self, refresh=600, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh, help_file="patrolHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE Patrol"
	self.script_title_gif = "en_patrol.gif"
	self.source_server = THE_INQUISITOR
	self.description = ""

    def body(self, data):
	table = self.table_top()
	# the data will be of 1 of 2 forms.  either text to output if there
	# is no url available, or the url to link to.
	if type(data) == types.StringType:
	    # no url, just output the string
	    table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(data, size="+1"))))
	else:
	    # make the url
	    table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(data[0], data[1]))))
	self.append(table)

class EnAlarmSearchPage(EnBaseHtmlDoc):

    def __init__(self, background, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=600, background=background, system_tag=system_tag)
	self.title = "ENSTORE Alarm Search"
	self.script_title_gif = "en_alarm_hist.gif"
	self.source_server = THE_ALARM_SERVER
	self.description = "Active and resolved alarms."

    def alarm_table(self, alarms):
	tr = HTMLgen.TR()
	for hdr in ["Time", "Node", "PID", "User", "Severity", 
		    "Process", "Error", "Additional Information"]:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, width="100%", border=1, cellspacing=5, 
				  cellpadding=CELLP, align="LEFT", bgcolor=AQUA)
	akeys = sort_keys(alarms)
	for akey in akeys:
	    alarm = alarms[akey].list_alarm()
	    tr = HTMLgen.TR((HTMLgen.TD(enstore_functions.format_time(time.mktime(eval(alarm[0]))))))
	    # remove .fnal.gov
	    alarm[1] = enstore_functions.strip_node(alarm[1])
	    for item in alarm[1:]:
		tr.append(HTMLgen.TD(item))
	    table.append(tr)
	return table

    def body(self, alarms):
	table = HTMLgen.TableLite(cellspacing=0, cellpadding=0, align="LEFT",
				  width="800")
	table.append(HTMLgen.TR(HTMLgen.TD(self.alarm_table(alarms))))
	self.append(table)
	
class EnPlotPage(EnBaseHtmlDoc):

    bpd = "%s%s"

    def __init__(self, title="ENSTORE System Plots", gif="en_plots.gif", system_tag="",
		 description=""):
	EnBaseHtmlDoc.__init__(self, refresh=0, help_file="plotHelp.html",
			       system_tag=system_tag)
	self.title = title
	self.script_title_gif = gif
	self.source_server = THE_INQUISITOR
	self.description = description

    def find_label(self, text):
        # compare the passed text with the files listed in PLOT_INFO. if there
        # is a match, return the associated text. else return a default string.
        for file_label in PLOT_INFO:
            if string.find(text, file_label[0]) == 0:
                # this is a match
                return file_label[1]
        else:
            return DEFAULT_LABEL

    def find_ps_file(self, jpg_file, pss):
	# see if there is a corresponding ps file
	ps = 0
	ps_file = ""
	if pss:
	    ps_file = string.replace(jpg_file, enstore_constants.JPG,
				     enstore_constants.PS)
	    for ps in pss:
		if ps_file == ps[0]:
		    # found it
		    pss.remove(ps)
		    ps = 1
		    break
	return (ps, ps_file)

    def add_stamp(self, jpgs, stamps, pss, trs, trps, trps2):
	# for each stamp add it to the row.  if there is a corresponding large
	# jpg file, add it as a link from the stamp.  also see if there is a
	# postscript file associated with it and add it in the following row.
	# if there is no postscript file, then put a message saying this.
	if stamps:
	    stamp = stamps.pop(0)
	    jpg_file = string.replace(stamp[0], enstore_constants.STAMP, "")
	    # see if there is a corresponding jpg file
	    url = 0
	    for jpg in jpgs:
		if jpg_file == jpg[0]:
		    # found it
		    jpgs.remove(jpg)
		    url = 1
		    break
	    # see if there is a corresponding ps file
	    (ps, ps_file) = self.find_ps_file(jpg_file, pss)
	    if url:
		# we have a jpg file associated with this stamp file
		td = HTMLgen.TD(HTMLgen.Href(jpg_file, HTMLgen.Image(stamp[0])))
	    else:
		td = HTMLgen.TD(HTMLgen.Image(stamp[0]))
	    trs.append(td)
	    trps.append(HTMLgen.TD("%s (%s)%s"%(self.find_label(stamp[0]),
						enstore_functions.format_time(stamp[1]), 
						NBSP*2) , html_escape='OFF'))
	    if ps:
		# we have a corresponding ps file
		trps2.append(HTMLgen.TD(HTMLgen.Href(ps_file, POSTSCRIPT)))
	    else:
		trps2.append(self.empty_data())

    def add_leftover_jpgs(self, table, jpgs, pss):
	while jpgs:
	    tr = HTMLgen.TR()
	    for i in [1, 2, 3]:
		if jpgs:
		    jpg_file, mtime = jpgs.pop(0)
		    # see if there is a corresponding ps file
		    (ps, ps_file) = self.find_ps_file(jpg_file, pss)
		    td = HTMLgen.TD(HTMLgen.Href(jpg_file, jpg_file))
		    if ps:
			# yes there was one
			td.append(HTMLgen.Href(ps_file, POSTSCRIPT))
		    tr.append(td)
	    else:
		table.append(tr)
	    
    def add_leftover_pss(self, table, pss):
	while pss:
	    tr = HTMLgen.TR()
	    for i in [1, 2, 3]:
		if pss:
		    ps_file, mtime = pss.pop(0)
		    td = HTMLgen.TD(HTMLgen.Href(ps_file, ps_file))
		    tr.append(td)
	    else:
		table.append(tr)
	    
    def body(self, jpgs, stamps, pss):
	table = self.table_top()
	# create a grid of jpg stamp files 3 stamps wide.  attach a link to
	# the associated postscript file too
	plot_table = HTMLgen.TableLite(width="100%", cols="3", align="CENTER",
                                       cellspacing=0, cellpadding=0)
	while stamps:
	    trs = HTMLgen.TR()
	    trps = HTMLgen.TR()
	    trps2 = HTMLgen.TR()
	    for i in [1, 2, 3]:
	        self.add_stamp(jpgs, stamps, pss, trs, trps, trps2)
	    else:
                plot_table.append(trs)
                plot_table.append(trps)
		plot_table.append(trps2)
	        plot_table.append(empty_row(3))
	# look for anything leftover to add at the bottom
	if jpgs or pss:
	    # add some space between the extra files and the stamps
	    plot_table.append(empty_row(3))
	    plot_table.append(HTMLgen.TR(HTMLgen.TD(\
		HTMLgen.Font("Additional Plots", size="+2", color=BRICKRED)),
					 colspan=3))
	    plot_table.append(empty_row(3))
	    self.add_leftover_jpgs(plot_table, jpgs, pss)
	    self.add_leftover_pss(plot_table, pss)
	table.append(HTMLgen.TR(HTMLgen.TD(plot_table)))
	self.append(table)

class EnActiveMonitorPage(EnBaseHtmlDoc):

    def __init__(self, headings, refresh, system_tag=""):

        """
        Make HTMLgen objects which describe the whole page. Let
        self.perf_table be the object pointing to the table
        of performance data we must fill in
        """
	self.title = "ENSTORE Active Network Monitoring"
	self.script_title_gif = "en_net_mon.gif"
	self.description = "%s%sRecent network monitoring results."%(NBSP, NBSP)
        EnBaseHtmlDoc.__init__(self, refresh=refresh, system_tag=system_tag)
	self.source_server = "The Monitor Server"
        
        #add standard header to  html page, do not need update information
        table_top = self.table_top(0)
        self.append(table_top)

        # The standard look and feel provides for our output table to be a row
        # in a master table called "table top".   To participate in the standard look
        # and feel we make our table ("perf_table"), put it into a data field, and
        # put the data field into a row, and  then add that row to "table_top"
        
        table_top_row_for_perf_table = HTMLgen.TR(valign="CENTER")
        table_top.append(table_top_row_for_perf_table)
        data_for_perf_table = HTMLgen.TD()
        table_top_row_for_perf_table.append(data_for_perf_table)
        self.perf_table =HTMLgen.TableLite(border=1, bgcolor=AQUA,
                                           width="100%", cols=len(headings),
                                           cellspacing=5, cellpadding=CELLP,
                                           align="CENTER")
        data_for_perf_table.append(self.perf_table)

	# Populate the first row in perf_table with headings 
	head_row = HTMLgen.TR(valign="CENTER")
        self.perf_table.append(head_row)
        for h in headings:
            head_row.append(self.make_th(h))

    def add_measurement(self, measurement):
        """
        add a measurement to the top of the measurment table
        lop off the tail of the table when it becomes too long.
        """
        measurement_row = HTMLgen.TR()
        
        #self.perf_table.append(measurement_row)
        
        self.perf_table.contents.insert(1,measurement_row)  
        for m in measurement :
            measurement_row.append(HTMLgen.TD(m))

        if len(self.perf_table.contents) > 400 :
            self.perf_table.contents = self.perf_table.contents [:350]

class EnSaagPage(EnBaseHtmlDoc):

    greenball = HTMLgen.Image("greenball.gif", width=17, height=17, border=0)
    redball = HTMLgen.Image("redball.gif", width=17, height=17, border=0)
    star = HTMLgen.Image("star.gif", width=17, height=17, border=0)
    yellowball = HTMLgen.Image("yelball.gif", width=17, height=17, border=0)
    checkmark = HTMLgen.Image("checkmark.gif", width=17, height=17, border=0)

    def __init__(self, title="ENSTORE Status-At-A-Glance", gif="ess-aag.gif", system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=360, help_file="saagHelp.html", system_tag=system_tag)
	self.title = title
	self.script_title_gif = gif
	self.source_server = "SPAM"
	self.description = ""

    def get_color_ball(self, dict, key, direction=""):
	val = dict.get(key, enstore_constants.DOWN)
	if val == enstore_constants.WARNING:
	    td = HTMLgen.TD(self.yellowball, align=direction)
	elif val == enstore_constants.UP:
	    td = HTMLgen.TD(self.greenball, align=direction)
	elif val == enstore_constants.SEEN_DOWN:
	    td = HTMLgen.TD(self.star, align=direction)
	else:
	    td = HTMLgen.TD(self.redball, align=direction)
	return td

    # the  alt_key is used to specify a more explicit data item than just the key
    def get_element(self, dict, key, out_dict, offline_dict, alt_key=""):
	val = dict.get(key, enstore_constants.DOWN)
	sched = out_dict.get(key, enstore_constants.NOSCHEDOUT)
	if not alt_key:
	    alt_key = key
	if offline_dict.has_key(key):
	    # this element is known to be offline
	    td = HTMLgen.TD(HTMLgen.Strike(alt_key))
	else:
	    td = HTMLgen.TD(alt_key)
	if sched != enstore_constants.NOSCHEDOUT:
	    td.append(" ")
	    td.append(self.checkmark)
	    td.append(HTMLgen.BR())
	    td.append(HTMLgen.Emphasis(HTMLgen.Font("(%s)"%(sched,), size="-1")))
	return td

    def make_overall_table(self, enstat_d, netstat_d, medstat_d, alarms_d, outage_d, 
			   offline_d):
	entable = HTMLgen.TableLite(cellspacing=1, cellpadding=1, border=0, align="CENTER",
				    width="90%")
	entable.append(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold("Enstore Overall Status"), 
						    size="+3", color=BRICKRED)))
	entable.append(empty_row(6))
	tr = HTMLgen.TR()
	for (val, dict) in [(enstore_constants.ENSTORE, enstat_d),
			    (medstat_d[TAG], medstat_d),
			    (enstore_constants.NETWORK, netstat_d),
			    (enstore_constants.ANYALARMS, alarms_d)]:
	    tr.append(self.get_color_ball(dict, val, "RIGHT"))
	    txt = HTMLgen.Font(val, size="+2")
	    if dict.has_key(enstore_constants.URL):
		txt = HTMLgen.Href(dict[enstore_constants.URL], txt)
	    tr.append(self.get_element(dict, val, outage_d, offline_d, txt))
	entable.append(tr)
	return entable

    def get_time_row(self, dict):
	# if the dictionary has a time key, then use the value in a row.  else use "???"
	theTime = dict.get(enstore_constants.TIME, "???")
	return (HTMLgen.TR(HTMLgen.TD("(as of %s)"%(theTime,), colspan=8, align="CENTER")))

    def add_data(self, dict, keys, tr, out_dict, offline_dict):
	if len(keys) > 0:
	    key = keys.pop(0)
	    tr.append(self.get_color_ball(dict, key, "RIGHT"))
	    tr.append(self.get_element(dict, key, out_dict, offline_dict))
	else:
	    tr.append(empty_data(2))
	return keys

    def make_server_table(self, dict, out_dict, offline_dict):
	ignore = [enstore_constants.ENSTORE, enstore_constants.TIME, 
		  enstore_constants.URL]
	entable = HTMLgen.TableLite(cellspacing=1, cellpadding=1, border=0, align="CENTER",
				    width="90%")
	entable.append(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold("Enstore Individual Server Status"), 
						    size="+3", color=BRICKRED)))
	entable.append(empty_row(8))
	# add the individual column headings
	tr = HTMLgen.TR(empty_header())
	tr.append(HTMLgen.TH(HTMLgen.Font(HTMLgen.U("Servers"), size="+1", color=BRICKRED),
				 align="RIGHT", colspan=2))
	tr.append(empty_header())
	for hdr in ["Library Managers", "Media Changers"]:
	    tr.append(empty_header())
	    tr.append(HTMLgen.TH(HTMLgen.Font(HTMLgen.U(hdr), size="+1", color=BRICKRED),
				 align="LEFT"))
	entable.append(tr)
	# now split up the data into each column
	keys = sort_keys(dict)
	lm = {}
	mv = {}
	mc = {}
	gs = {}
	for key in keys:
	    if not key in ignore:
		if enstore_functions.is_library_manager(key):
		    lm[key] = dict[key]
		elif enstore_functions.is_mover(key):
		    mv[key] = dict[key]
		elif enstore_functions.is_media_changer(key):
		    mc[key] = dict[key]
		else:
		    gs[key] = dict[key]
	else:
	    lm_keys = sort_keys(lm)
	    mv_keys = sort_keys(mv)
	    mc_keys = sort_keys(mc)
	    gs_keys = sort_keys(gs)
	while (len(lm_keys) + len(mc_keys) + len(gs_keys)) > 0:
	    tr = HTMLgen.TR()
	    gs_keys = self.add_data(gs, gs_keys, tr, out_dict, offline_dict)
	    gs_keys = self.add_data(gs, gs_keys, tr, out_dict, offline_dict)
	    lm_keys = self.add_data(lm, lm_keys, tr, out_dict, offline_dict)
	    mc_keys = self.add_data(mc, mc_keys, tr, out_dict, offline_dict)
	    entable.append(tr)

	# now add the mover information
	entable.append(empty_row(8))
	entable.append(empty_row(8))
	tr = HTMLgen.TR(empty_header())
	tr.append(HTMLgen.TH(HTMLgen.Font(HTMLgen.U("Movers"), size="+1", color=BRICKRED),
				 align="RIGHT", colspan=2))
	entable.append(tr)
	while len(mv_keys) > 0:
	    tr = HTMLgen.TR()
	    mv_keys = self.add_data(mv, mv_keys, tr, out_dict, offline_dict)
	    mv_keys = self.add_data(mv, mv_keys, tr, out_dict, offline_dict)
	    mv_keys = self.add_data(mv, mv_keys, tr, out_dict, offline_dict)
	    mv_keys = self.add_data(mv, mv_keys, tr, out_dict, offline_dict)
	    entable.append(tr)
	return entable

    def make_network_table(self, dict, out_dict, offline_dict):
	ignore = [enstore_constants.NETWORK, enstore_constants.BASENODE,
		  enstore_constants.TIME, enstore_constants.URL]
	entable = HTMLgen.TableLite(cellspacing=1, cellpadding=1, border=0, align="CENTER",
				    width="90%")
	entable.append(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold("Enstore Network Interface Status"), 
						    size="+3", color=BRICKRED)))
	entable.append(empty_row())
	# now add the base node row
	base_node = dict.get(enstore_constants.BASENODE, UNKNOWN)
	entable.append(HTMLgen.TR(HTMLgen.TH(HTMLgen.Font("Base Node : %s"%(base_node,),
							  size="+1", color=BRICKRED),
					     colspan=6, align="LEFT")))
	entable.append(empty_row())
	keys = sort_keys(dict)
	tr = HTMLgen.TR()
	counter = 0
	for key in keys:
	    if not key in ignore:
		tr.append(self.get_color_ball(dict, key, "RIGHT"))
		tr.append(self.get_element(dict, key, out_dict, offline_dict))
		counter = counter + 1
		# only allow 4 nodes per row
		if counter == 4:
		    entable.append(tr)
		    counter = 0
		    tr = HTMLgen.TR()
	else:
	    # if we did partly fill in the last row append it to the table
	    if counter > 0:
		entable.append(tr)
	return entable

    def make_legend_table(self):
	entable = HTMLgen.TableLite(border=0, align="CENTER")
	tr = HTMLgen.TR()
	for (ball, txt) in [(self.redball, "Major Problem"),
			    (self.yellowball, "Minor problem")]:
	    tr.append(HTMLgen.TD(ball))
	    tr.append(HTMLgen.TD(HTMLgen.Font(txt, size="-1")))
	entable.append(tr)
	tr = HTMLgen.TR()
	for (ball, txt) in [(self.greenball, "All systems are operational"),
			    (self.star, "Situation under investigation")]:
	    tr.append(HTMLgen.TD(ball))
	    tr.append(HTMLgen.TD(HTMLgen.Font(txt, size="-1")))
	entable.append(tr)
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(self.checkmark))
	tr.append(HTMLgen.TD(HTMLgen.Font("Scheduled outage", size="-1")))
	tr.append(HTMLgen.TD(HTMLgen.Strike("Known Down"), colspan=2, align="CENTER"))
	entable.append(tr)
	return entable

    def make_node_server_table(self, dict):
	entable = HTMLgen.TableLite(cellspacing=3, cellpadding=3, align="CENTER", border=2, 
				    width="90%", bgcolor=AQUA)
	entable.append(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold("Enstore Node/Server Relationship"), 
						    size="+3", color=BRICKRED)))
	cols = 4
	# arrange each node in a table on the page
	node_keys = dict.keys()
	node_keys.sort()
	tr = HTMLgen.TR()
	for node in node_keys:
	    tr = check_row_length(tr, cols, entable)
	    tr.append(HTMLgen.TD(HTMLgen.Bold(node)))
	    servers = dict[node]
	    servers.sort()
	    list = HTMLgen.List()
	    for server in servers:
		list.append(server)
	    tr.append(HTMLgen.TD(list))
	else:
	    while len(tr) < cols:
		tr.append(empty_data())
	    entable.append(tr)
	return entable

    def body(self, enstat_d, netstat_d, medstat_d, alarms, nodes, outage_d, offline_d, 
	     media_tag):
	# create the outer table and its rows
	table = self.table_top()
	# add the table with the general status
	# fake this for now, remove when get this working
	if offline_d.has_key(media_tag):
	    # mark the robot as down
	    stat = enstore_constants.DOWN
	else:
	    stat = enstore_constants.UP
	medstat_d = {media_tag : stat, TAG : media_tag}
	table.append(HTMLgen.TR(HTMLgen.TD(self.make_overall_table(enstat_d, netstat_d,
								   medstat_d, alarms,
								   outage_d, offline_d))))
	table_spacer(table)
	# add the table with the individual server status
	if enstat_d:
	    table.append(HTMLgen.TR(HTMLgen.TD(self.make_server_table(enstat_d, outage_d,
								      offline_d))))
	    table_spacer(table)
	# add the table with the media info
	#if medstat_d:
	    #table.append(HTMLgen.TR(HTMLgen.TD(self.make_media_table(medstat_d, outage_d))))
	    #table_spacer(table)

	# add the table with the network info
	if netstat_d:
	    table.append(HTMLgen.TR(HTMLgen.TD(self.make_network_table(netstat_d, outage_d,
								       offline_d))))
	    table_spacer(table)

	# add the section where nodes are reported and which servers run on them
	table.append(HTMLgen.TR(HTMLgen.TD(self.make_node_server_table(nodes))))
	table_spacer(table)

	# add the legend table
	table.append(HTMLgen.TR(HTMLgen.TD(self.make_legend_table())))
	self.append(table)
