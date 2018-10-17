#!/usr/bin/env python

# system imports
import string
import calendar
import time
#setpath.addpath("$HTMLGEN_DIR")
import HTMLgen
import types
import os
import stat
import sys

# enstore imports
import enstore_functions2
import e_errors
import enstore_constants
import mover_constants
import volume_family
from en_eval import en_eval

YES = 1
NO = 0

FUSCHIA  = "#FF00FF"
YELLOW   = "#FFFFF0"
AQUA     = "#DFF0FF"
BRICKRED = "#770000"
DARKBLUE = "#000066"
LIGHTBLUE = "#0000FF"
TIMED_OUT_COLOR = "#FF9966"
SERVER_ERROR_COLOR = "#FFFF00"
FILE_ERROR_COLOR = "#FF0000"
NOT_MONITORING_COLOR = "#B9B9B9"
INFOTXT = "files"

NAV_TABLE_COLOR = YELLOW
NBSP = "&nbsp;"
CELLP = 3
MAX_SROW_TDS = 5
AT_MOVERS = 0
PENDING = AT_MOVERS + 1
POSTSCRIPT = "(postscript)"
UNKNOWN = "???"

DEFAULT_FULL_LM_ROWS = 60
DEFAULT_LM_ROWS = 60
DEFAULT_FILE_LIST_ROWS = 300
DEFAULT_THRESHOLDS = [DEFAULT_LM_ROWS, DEFAULT_FULL_LM_ROWS, DEFAULT_LM_ROWS]
DEFAULT_ALL_ROWS = 0

NO_INFO_STATES = [enstore_constants.TIMED_OUT, enstore_constants.DEAD, enstore_constants.NO_SUSPECT_VOLS,
                  enstore_constants.NO_WORK_QUEUE, enstore_constants.NO_ACTIVE_VOLS,
                  enstore_constants.NO_STATE]

LM_COLS = 5

TAG = 'tag'

BAD_MOVER_STATES = [mover_constants.OFFLINE, mover_constants.DRAINING,
		    mover_constants.ERROR]
BAD_MIGRATOR_STATES = [mover_constants.OFFLINE, mover_constants.ERROR]
BAD_LM_STATES = [e_errors.BROKEN]

HEADINGS = ["Name", "Status", "Host", "Date/Time", "Last Time Alive"]
MEDIA_CHANGERS = "Media Changers"
SERVERS = "Servers"
MIGRATORS = "Migrators"
MOVERS = "Movers"
UNMONITORED_SERVERS = "Unmonitored Servers"
THE_INQUISITOR = "The Inquisitor"
THE_ALARM_SERVER = "The Alarm Server"
THE_ENSTORE = "Enstore"

RESOLVEALL = "Resolve All"
RESOLVESELECTED = "Resolve Selected"

PLOT_INFO = [[enstore_constants.MPH_FILE, "Mounts/Hour (no null mvs)"],
	     [enstore_constants.D_MPD_FILE, "Mounts/Day (no null mvs as of 8/1/01)"],
	     [enstore_constants.MPD_FILE, "Mounts/Day (no null mvs as of 8/1/01)"],
	     [enstore_constants.MPD_MONTH_FILE, "Mounts/Day (30 days) (no null mvs)"],
	     [enstore_constants.MLAT_FILE, "Mount Latency (no null mvs)"],
	     [enstore_constants.BPD_MONTH_FILE_W, "Bytes Written/Day (30 days) (no null mvs)"],
	     [enstore_constants.BPD_FILE_R, "Bytes Read/Day (no null mvs)"],
	     [enstore_constants.BPD_FILE_W, "Bytes Written/Day (no null mvs)"],
	     [enstore_constants.TOTAL_BPD_FILE_W, "CDF/D0/STK Total Bytes Written/Day (30 days)"],
	     [enstore_constants.TOTAL_BPD_FILE, "CDF/D0/STK Total Bytes/Day (30 days)"],
	     [enstore_constants.BPD_MONTH_FILE, "Bytes/Day (30 days) (no null mvs)"],
	     [enstore_constants.BPD_FILE_D, "Bytes/Day"],
	     [enstore_constants.BPD_FILE, "Bytes/Day (no null mvs)"],
	     [enstore_constants.XFERLOG_FILE, "Transfer Activity (log - no null mvs))"],
	     [enstore_constants.XFER_FILE, "Transfer Activity (no null mvs)"],
             [enstore_constants.NULL_RATES, "Null Terabytes/Day"],
	     [enstore_constants.REAL_RATES, "Real Terabytes/Day (no null mvs)"],
	     [enstore_constants.UTIL_FILE, "Drive Utilization"],
	     [enstore_constants.SG_FILE, "Storage Group Activity"]
	     ]

DEFAULT_LABEL = "UNKNOWN INQ PLOT"

def noNone(aStr):
    if aStr == None:
        aStr = NBSP
    return aStr

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

def table_spacer(table, cols=1):
    table.append(empty_row())
    table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR(), colspan=cols)))
    table.append(empty_row())

def get_full_queue_name(lm_name):
    return "%s-full.html"%(lm_name,)

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
	if not self.refresh == -1:
	    self.meta = HTMLgen.Meta(equiv="Refresh", content=self.refresh)

    # this is the base class for all of the html generated enstore documents
    def __init__(self, refresh=0, background="enstore.gif", help_file="",
		 system_tag="", url_gif_dir = ""):
	self.align = YES
	self.textcolor = DARKBLUE
        ## Allow url_gif_dir to specifiy where the gifs (like background)
        ## can be found.  (script_title_gif is another example.)
        self.url_gif_dir = url_gif_dir
	self.background = os.path.join(url_gif_dir, background)
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
	self.script_title_gif = None
	self.description = None
        self.nav_link = ""
        self.do_nav_table = 1
        sys.stdout.flush()


    # generate the three button navigation table for the top of each of the
    # enstore web pages
    def nav_table(self):
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href("%senstore_system.html"%(self.nav_link,), 'Home'),
		  size="+2")),
			     bgcolor=NAV_TABLE_COLOR))
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href("%s%s"%(self.nav_link, enstore_constants.SAAGHTMLFILE),
			       'System'),
		  size="+2")), bgcolor=NAV_TABLE_COLOR))
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href("%sstatus_enstore_system.html"%(self.nav_link,), SERVERS),
		  size="+2")), bgcolor=NAV_TABLE_COLOR))
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href("%sencp_enstore_system.html"%(self.nav_link,), 'Encp'),
		  size="+2")), bgcolor=NAV_TABLE_COLOR))
	if self.help_file:
	    tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(HTMLgen.Href("%s%s"%(self.nav_link, self.help_file),
									'Help'), size="+2")),
				 bgcolor=NAV_TABLE_COLOR))
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
            ## Allow url_gif_dir to specifiy where the gifs (like background)
            ## can be found.
            use_script_title_gif = os.path.join(self.url_gif_dir,
                                                self.script_title_gif)
            tr.append(HTMLgen.TD(HTMLgen.Image(use_script_title_gif),
				 align="RIGHT"))

    def table_top_b(self, table, td, cols=1):
	if self.description:
	    td.append(HTMLgen.Font(self.description, html_escape='OFF',
				   size="+2"))
	    td.append(HTMLgen.HR(size=2, noshade=1))
	table.append(HTMLgen.TR(td))
	table.append(empty_row(cols))
	return table

    def add_source_server(self, cols=1):
	tr = HTMLgen.TR(empty_data(cols))
	td = HTMLgen.TD(HTMLgen.Emphasis(HTMLgen.Font("Brought To You By : ",
						      size=-1)),
			align="RIGHT")
	td.append(HTMLgen.Font("%s"%(self.source_server,), size=-1))
	tr.append(td)
	return tr

    def add_last_updated(self):
	td = HTMLgen.TD(HTMLgen.Emphasis(HTMLgen.Font("Last updated : ",
						      size=-1)),
			align="RIGHT")
	td.append(HTMLgen.Font("%s"%(enstore_functions2.format_time(time.time()),),
			       html_escape='OFF', size=-1))
	return td

    def table_top(self, cols=1, add_update=1):
	# create the outer table and its rows
	fl_table = HTMLgen.TableLite(cellspacing=0, cellpadding=0,
				     align="LEFT", width="800")
        if self.do_nav_table:
            tr = HTMLgen.TR(HTMLgen.TD(self.nav_table()))
        else:
            tr = HTMLgen.TR(empty_data())
	self.script_title(tr)
	fl_table.append(tr)
	if self.source_server:
	    fl_table.append(self.add_source_server())
	# only add update information if asked to
	if add_update:
	    if self.system_tag:
		tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(self.system_tag,
								     size="+1",
								     color=BRICKRED)),
					   align="LEFT"))
	    else:
		tr = HTMLgen.TR(empty_data())
	    tr.append(self.add_last_updated())
	    fl_table.append(tr)

	if self.align == YES:
	    table = HTMLgen.TableLite(HTMLgen.TR(HTMLgen.TD(fl_table, colspan=cols)),
				      cellspacing=0, cellpadding=0,
				      align="LEFT", width="800")
	else:
	    table = HTMLgen.TableLite(HTMLgen.TR(HTMLgen.TD(fl_table, colspan=cols)),
				      cellspacing=0, cellpadding=0)
	table.append(empty_row(cols))
	td = HTMLgen.TD(HTMLgen.HR(size=2, noshade=1), colspan=cols)
	self.table_top_b(table, td, cols)
	return table

    def big_title(self, txt, cols=1):
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(txt,
						       size="+4",
						       html_escape='OFF')),
			     colspan=cols))
	return tr

    def server_font(self, server):
	return HTMLgen.Bold(HTMLgen.Font(server, size="+1"))

    def server_url(self, server, text, suburl=""):
	if suburl:
	    txt = "%s#%s"%(text, suburl)
	else:
	    txt = "%s"%(text,)
	return HTMLgen.Href(txt, self.server_font(server))

    def server_heading(self, server):
	return HTMLgen.Name(server, self.server_font(server))

    def set_refresh(self, refresh):
	self.refresh = refresh

    def get_refresh(self):
	return self.refresh

    # create a table header
    def make_th(self, name):
	return HTMLgen.TH(HTMLgen.Bold(HTMLgen.Font(name, size="+2",
						    color=BRICKRED,
                                                    html_escape='OFF')),
			  align="CENTER")

    def trailer(self, table, cols=1):
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR(size=2, noshade=1), colspan=cols)))
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href("http://www.fnal.gov/pub/disclaim.html", "Legal Notices"), colspan=cols)))

    # make the row with the servers alive information
    def alive_row(self, server, data, color=None, link=None):
	if link and not data[0] == enstore_constants.NOT_MONITORING:
	    srvr = link
	else:
	    srvr = self.server_heading(server)
	# change the color of the first column if the server has timed out
	if data[0][0:5] == "alive":
	    if color:
		# use the user specified color
		tr = HTMLgen.TR(HTMLgen.TD(srvr, bgcolor=color,
					   html_escape='OFF'))
	    else:
		tr = HTMLgen.TR(HTMLgen.TD(srvr, html_escape='OFF'))
	elif  data[0][0:5] == "error":
	    tr = HTMLgen.TR(HTMLgen.TD(srvr, bgcolor=SERVER_ERROR_COLOR,
				       html_escape='OFF'))
	elif data[0] == enstore_constants.NOT_MONITORING:
	    tr = HTMLgen.TR(HTMLgen.TD(srvr, bgcolor=NOT_MONITORING_COLOR,
				       html_escape='OFF'))
	else:
	    tr = HTMLgen.TR(HTMLgen.TD(srvr, bgcolor=TIMED_OUT_COLOR,
				       html_escape='OFF'))
	for datum in data:
	    tr.append(HTMLgen.TD(datum, html_escape='OFF'))
	if len(data) == 3:
	    # there was no last_alive time so fill in
	    tr.append(empty_data())
	return tr

    # generate the body of the file
    def body(self, data_dict):
	# this is the data we will output
	self.data_dict = data_dict
	# create the outer table and its rows
	table = self.table_top()
	table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(self.main_table())))
	self.trailer(table)
	self.append(table)


class EnExtraFileListPages(EnBaseHtmlDoc):

    def __init__(self, status_page):
	extra_text = "(More Active Files)"
	EnBaseHtmlDoc.__init__(self, refresh=status_page.refresh,
			       help_file="fileHelp.html",
			       system_tag="%s %s"%(status_page.system_tag,
						   extra_text))
	self.title = "%s %s"%(status_page.title, extra_text)
	self.source_server = THE_INQUISITOR
	self.script_title_gif = status_page.script_title_gif
	self.description = ""

    # generate the body of the file
    def body(self, row):
	# create the outer table and its rows
	table = self.table_top()
	table.append(empty_row())
	table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(row, colspan=5)))
	self.trailer(table)
	self.append(table)

class EnExtraLmFullQueuePages(EnBaseHtmlDoc):

    def __init__(self, status_page, lm):
	extra_text = "(Extra %s full queue rows)"%(lm,)
	EnBaseHtmlDoc.__init__(self, refresh=status_page.refresh,
			       help_file="serverStatusFullLMQHelp.html",
			       system_tag="%s %s"%(status_page.system_tag,
						   extra_text))
	self.title = "%s %s"%(status_page.title, extra_text)
	self.source_server = THE_INQUISITOR
	self.script_title_gif = status_page.script_title_gif
	self.description = ""
	self.lm = lm

    # generate the body of the file
    def body(self, row):
	# create the outer table and its rows
	table = self.table_top()
	table.append(empty_row())
	table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(self.server_heading(self.lm),
					   colspan=5)))
	table.append(empty_row())
	table.append(row)
	self.trailer(table)
	self.append(table)

class EnExtraLmQueuePages(EnBaseHtmlDoc):

    def __init__(self, status_page, lm):
	extra_text = "(Extra %s queue rows)"%(lm,)
	EnBaseHtmlDoc.__init__(self, refresh=status_page.refresh,
			       help_file="serverStatusLMQHelp.html",
			       system_tag="%s %s"%(status_page.system_tag,
						   extra_text))
	self.title = "%s %s"%(status_page.title, extra_text)
	self.source_server = THE_INQUISITOR
	self.script_title_gif = status_page.script_title_gif
	self.description = ""
	self.lm = lm

    # generate the body of the file
    def body(self, rows):
	# create the outer table and its rows
	table = self.table_top(cols=LM_COLS)
	table.cellpadding=3
	table.append(empty_row(LM_COLS))
	table.append(empty_row(LM_COLS))
	table.append(HTMLgen.TR(HTMLgen.TD(self.server_heading(self.lm),
					   colspan=LM_COLS)))
	table.append(empty_row(LM_COLS))
	for row in rows:
	    table.append(row)
	self.trailer(table, LM_COLS)
	self.append(table)

class EnMoverStatusPage(EnBaseHtmlDoc):

    def __init__(self, refresh=60, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       help_file="moverStatusHelp.html",
			       system_tag=system_tag)
	self.title = "Movers Page"
	self.source_server = THE_INQUISITOR
	self.script_title_gif = "mv.gif"
	self.description = ""

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
	    tr.append(HTMLgen.TD(HTMLgen.Font("EOD%sCookie"%(NBSP,),
					      color=BRICKRED,
					      html_escape='OFF'),
				 align="CENTER"))
	    tr.append(HTMLgen.TD(moverd[enstore_constants.EOD_COOKIE]))
	elif moverd.has_key(enstore_constants.LOCATION_COOKIE):
	    tr.append(HTMLgen.TD(moverd[mvkey]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Location%sCookie"%(NBSP,),
					      color=BRICKRED,
					      html_escape='OFF'),
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
	if moverd:
	    # we may have gotten an error when trying to get it,
	    # so look for a piece of it.  (efb used to be 'or')
	    if moverd.has_key(enstore_constants.STATE) and \
	       moverd[enstore_constants.STATE]:
		# get the first word of the mover state, we will use this to
		# tell if this is a bad state or not
		words = string.split(moverd[enstore_constants.STATE])
		if words[0] in BAD_MOVER_STATES:
		    table.append(self.alive_row(mover,
					     moverd[enstore_constants.STATUS],
						FUSCHIA))
		else:
		    table.append(self.alive_row(mover,
					     moverd[enstore_constants.STATUS]))

		tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                                "Completed%sTransfers"%(NBSP,),
						color=BRICKRED,
						html_escape='OFF')))
		tr.append(HTMLgen.TD(moverd[enstore_constants.COMPLETED],
				     align="LEFT"))
		tr.append(HTMLgen.TD(HTMLgen.Font("Failed%sTransfers"%(NBSP,),
						  color=BRICKRED,
						  html_escape='OFF')))
		tr.append(HTMLgen.TD(moverd[enstore_constants.FAILED],
				     align="LEFT"))
		mv_table = HTMLgen.TableLite(tr, cellspacing=0, cellpadding=0,
					     align="LEFT", bgcolor=YELLOW,
					     width="100%")
		mv_table.append(empty_row(4))
		if moverd.has_key(enstore_constants.LAST_READ):
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                                  "Last%sRead%s(bytes)"%(NBSP,
									 NBSP),
						  color=BRICKRED,
						  html_escape='OFF'),
					       align="CENTER"))
		    self.add_bytes_volume_info(moverd, tr,
					       enstore_constants.LAST_READ)
		    mv_table.append(tr)
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(
                                                 "Last%sWrite%s(bytes)"%(NBSP,
									 NBSP),
						 color=BRICKRED,
						 html_escape='OFF'),
					       align="CENTER"))
		    self.add_bytes_eod_info(moverd, tr,
					    enstore_constants.LAST_WRITE)
		    mv_table.append(tr)
		    self.add_files(moverd, mv_table)
		elif moverd.has_key(enstore_constants.CUR_READ):
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                               "Current%sRead%s(bytes)"%(NBSP,
									 NBSP),
					       color=BRICKRED,
					       html_escape='OFF'),
					       align="CENTER"))
		    self.add_bytes_volume_info(moverd, tr,
					       enstore_constants.CUR_READ)
		    mv_table.append(tr)
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
			                     "Current%sWrite%s(bytes)"%(NBSP,
									NBSP),
					     color=BRICKRED,
					     html_escape='OFF'),
					       align="CENTER"))
		    self.add_bytes_eod_info(moverd, tr,
					    enstore_constants.CUR_WRITE)
		    mv_table.append(tr)
		    self.add_files(moverd, mv_table)
		tr = HTMLgen.TR(empty_data())
		tr.append(HTMLgen.TD(mv_table, colspan=5, width="100%"))
		table.append(tr)
	    else:
		# all we have is the alive information
		table.append(self.alive_row(mover,
					    moverd[enstore_constants.STATUS]))

    # generate the main table with all of the information
    def main_table(self):
	# first create the table headings for each column
	tr = HTMLgen.TR()
	for hdr in HEADINGS:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, align="CENTER", cellpadding=0,
				  cellspacing=0, bgcolor=AQUA, width="100%")
	skeys = sort_keys(self.data_dict)
        for server in skeys:
            # look for movers
            if enstore_functions2.is_mover(server):
                # this is a mover. output its info
                self.mv_row(server, table)
	return table

    # generate the body of the file
    def body(self, data_dict):
	# this is the data we will output
	self.data_dict = data_dict
	# create the outer table and its rows
	table = self.table_top()
	table.append(empty_row())
	table.append(empty_row())
	table.append(self.big_title(self.title))
	table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(self.main_table())))
	self.trailer(table)
	self.append(table)

class EnMigratorStatusPage(EnBaseHtmlDoc):

    def __init__(self, refresh=60, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       help_file="migratorStatusHelp.html",
			       system_tag=system_tag)
	self.title = "Migrators Page"
	self.source_server = THE_INQUISITOR
	self.script_title_gif = "migrator.gif"
	self.description = ""

    # add the volume information if it exists
    def add_bytes_volume_info(self, migratord, tr, mgkey):
        pass

    # add the eod/location cookie information if it exists
    def add_bytes_eod_info(self, migratord, tr, mgkey):
        pass
    # add input and output files
    def add_files(self, migratord, table):
        pass

    # add the migrator information
    def migrator_row(self, migrator, table):
	# we may not have any other info on this migrator as the inq may not be
	# watching it.
	md = self.data_dict.get(migrator, {})
	if md:
            for k in md:
                # we may have gotten an error when trying to get it,
                # so look for a piece of it.  (efb used to be 'or')
                if isinstance(md[k], dict) and md[k].has_key(enstore_constants.STATE) and \
                   md[k][enstore_constants.STATE]:
                    # get the first word of the mover state, we will use this to
                    # tell if this is a bad state or not
                    words = string.split(md[k][enstore_constants.STATE])
                    if words[0] in BAD_MIGRATOR_STATES:
                        table.append(self.alive_row(migrator,
                                                 md[enstore_constants.STATUS],
                                                    FUSCHIA))
                    else:
                        table.append(self.alive_row(migrator,
                                                 md[enstore_constants.STATUS]))

                    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                                    "ID",
                                                    color=BRICKRED,
                                                    html_escape='OFF')))
                    tr.append(HTMLgen.TD(md[k][enstore_constants.ID],
                                         align="LEFT"))
                    m_table = HTMLgen.TableLite(tr, cellspacing=0, cellpadding=0,
                                                 align="LEFT", bgcolor=YELLOW,
                                                 width="100%")
                    m_table.append(empty_row(2))
                    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                                    "state",
                                                    color=BRICKRED,
                                                    html_escape='OFF')))
                    tr.append(HTMLgen.TD(md[k][enstore_constants.STATE],
                                         align="LEFT"))

                    m_table.append(tr)
                    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                                    "file",
                                                    color=BRICKRED,
                                                    html_escape='OFF')))
                    tr.append(HTMLgen.TD(md[k][enstore_constants.FILES],
                                         align="LEFT"))

                    m_table.append(tr)
                    if md[k].has_key(enstore_constants.LAST_READ):
                        tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                                      "Last%sRead%s(bytes)"%(NBSP,
                                                                             NBSP),
                                                      color=BRICKRED,
                                                      html_escape='OFF'),
                                                   align="CENTER"))
                        self.add_bytes_volume_info(md, tr,
                                                   enstore_constants.LAST_READ)
                        m_table.append(tr)
                        tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(
                                                     "Last%sWrite%s(bytes)"%(NBSP,
                                                                             NBSP),
                                                     color=BRICKRED,
                                                     html_escape='OFF'),
                                                   align="CENTER"))
                        self.add_bytes_eod_info(md, tr,
                                                enstore_constants.LAST_WRITE)
                        m_table.append(tr)
                        self.add_files(md, m_table)
                    elif md[k].has_key(enstore_constants.CUR_READ):
                        tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                                   "Current%sRead%s(bytes)"%(NBSP,
                                                                             NBSP),
                                                   color=BRICKRED,
                                                   html_escape='OFF'),
                                                   align="CENTER"))
                        self.add_bytes_volume_info(md, tr,
                                                   enstore_constants.CUR_READ)
                        m_table.append(tr)
                        tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(\
                                                 "Current%sWrite%s(bytes)"%(NBSP,
                                                                            NBSP),
                                                 color=BRICKRED,
                                                 html_escape='OFF'),
                                                   align="CENTER"))
                        self.add_bytes_eod_info(md, tr,
                                                enstore_constants.CUR_WRITE)
                        m_table.append(tr)
                        self.add_files(md, m_table)
                    tr = HTMLgen.TR(empty_data())
                    tr.append(HTMLgen.TD(m_table, colspan=5, width="100%"))
                    table.append(tr)
                else:
                    # all we have is the alive information
                    table.append(self.alive_row(migrator,
                                                md[enstore_constants.STATUS]))

    # generate the main table with all of the information
    def main_table(self):
	# first create the table headings for each column
	tr = HTMLgen.TR()
	for hdr in HEADINGS:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, align="CENTER", cellpadding=0,
				  cellspacing=0, bgcolor=AQUA, width="100%")
	skeys = sort_keys(self.data_dict)
        for server in skeys:
            # look for migrators
            if enstore_functions2.is_migrator(server):
                # this is a migrator. Output its info
                self.migrator_row(server, table)
	return table

    # generate the body of the file
    def body(self, data_dict):
	# this is the data we will output
	self.data_dict = data_dict
	# create the outer table and its rows
	table = self.table_top()
	table.append(empty_row())
	table.append(empty_row())
	table.append(self.big_title(self.title))
	table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(self.main_table())))
	self.trailer(table)
	self.append(table)


class EnLmStatusPage(EnBaseHtmlDoc):

    def __init__(self, lm, refresh=60, system_tag="", max_lm_rows=DEFAULT_LM_ROWS):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       help_file="lmStatusHelp.html",
			       system_tag=system_tag)
	self.title = "%s Page"%(lm,)
	self.source_server = THE_INQUISITOR
	self.script_title_gif = "lmq.gif"
	self.description = ""
	self.lm = lm
	self.max_lm_rows = max_lm_rows
	self.extra_queue_pages = {}
	self.align = NO

    def read_q_list(self, mover):
	aMover = mover.get(enstore_constants.MOVER, enstore_constants.NOMOVER)
	return [mover[enstore_constants.DEVICE], mover[enstore_constants.FILE],
		mover[enstore_constants.NODE],
		aMover, mover[enstore_constants.CURRENT],
		mover[enstore_constants.LOCATION_COOKIE],
                mover[enstore_constants.STORAGE_GROUP]]

    def write_q_list(self, mover):
	device = mover.get(enstore_constants.DEVICE, None)
	aMover = mover.get(enstore_constants.MOVER, enstore_constants.NOMOVER)
	return [device, mover[enstore_constants.FILE],
		mover[enstore_constants.NODE],
		aMover, mover[enstore_constants.CURRENT],
		mover[enstore_constants.FILE_FAMILY],
		mover[enstore_constants.FILE_FAMILY_WIDTH],
                mover[enstore_constants.STORAGE_GROUP]]

    def parse_queues(self):
	# list of all vols currently being read or to be read
	self.r_vols = []
	# list of file familes currently being written to or to be written to
	self.w_ff = []
	# additional writes for file familes in w_ff
	self.ff = {}
	# additional pending elements for a vol in r_vols
	self.vols = {}
	if not self.data_dict.has_key(enstore_constants.WORK) or \
	   not self.data_dict[enstore_constants.WORK]:
            # nothing to do
            return
	# parse work at movers queue
	if not self.data_dict[enstore_constants.WORK] == \
	          enstore_constants.NO_WORK:
	    for mover in self.data_dict[enstore_constants.WORK]:
		if mover[enstore_constants.WORK] == enstore_constants.READ:
		    self.r_vols.append(self.read_q_list(mover))
		    self.vols[mover[enstore_constants.DEVICE]] = []
		else:
		    # this is a write
		    if self.ff.has_key(mover[enstore_constants.FILE_FAMILY]):
			self.ff[mover[enstore_constants.FILE_FAMILY]].append(self.write_q_list(mover))
		    else:
			self.w_ff.append(self.write_q_list(mover))
			self.ff[mover[enstore_constants.FILE_FAMILY]] = []
	# parse the pending read queue
	if self.data_dict.has_key(enstore_constants.PENDING) and \
           not self.data_dict[enstore_constants.PENDING] == enstore_constants.NO_PENDING:
	    for mover in self.data_dict[enstore_constants.PENDING][enstore_constants.READ]:
		if self.vols.has_key(mover[enstore_constants.DEVICE]):
		    self.vols[mover[enstore_constants.DEVICE]].append(self.read_q_list(mover))
		else:
		    self.r_vols.append(self.read_q_list(mover))
		    self.vols[mover[enstore_constants.DEVICE]] = []
	    # parse the write pending queue
	    for mover in self.data_dict[enstore_constants.PENDING][enstore_constants.WRITE]:
		if self.ff.has_key(mover[enstore_constants.FILE_FAMILY]):
		    self.ff[mover[enstore_constants.FILE_FAMILY]].append(self.write_q_list(mover))
		else:
		    self.w_ff.append(self.write_q_list(mover))
		    self.ff[mover[enstore_constants.FILE_FAMILY]] = []

    def read_row(self, elem, print_device=0):
	tr = HTMLgen.TR()
	if print_device:
	    td = HTMLgen.TD("%s"%(NBSP,), html_escape='OFF')
	    td.append(HTMLgen.Bold(HTMLgen.Name(elem[0],
						HTMLgen.Href("tape_inventory/%s"%(elem[0],),
								       elem[0]))))
	    tr.append(td)
	else:
	    # we do not need to print the volume label, again, must have done it before
	    tr.append(empty_data())
	if elem[3] is not enstore_constants.NOMOVER:
	    tr.append(HTMLgen.TD("[at%s%s]"%(NBSP,
					     HTMLgen.Href("%s#%s"%(enstore_functions2.get_mover_status_filename(),
								   elem[3],), elem[3])),
				 html_escape='OFF'))
	    # keep track of the busy movers so when we create the addtl movers table we do
	    # not include these.
	    self.busy_movers.append(elem[3])
	else:
	    tr.append(empty_data())
	tr.append(HTMLgen.TD(elem[2]))
	# only display the last n characters of the file name
	tr.append(HTMLgen.TD(HTMLgen.Font(elem[1][-70:], color=LIGHTBLUE)))
	# use the location cookie to find out the file number for a file on tape.  if this
	# is a disk move, then do not use it
	file_num = string.split(elem[5], '_')
	try:
	    file_num = string.atoi(file_num[-1])
	    str = "(CurPri%s:%s%s%sFile%s:%s%s)"%(NBSP, NBSP, elem[4],
						  NBSP, NBSP, NBSP, file_num)
	except ValueError:
	    # this is a disk move, or the location cookie is wrong.
	    str = "(CurPri%s:%s%s)"%(NBSP, NBSP, elem[4])

	tr.append(HTMLgen.TD(str, html_escape='OFF'))
        return tr

    def write_row(self, elem, print_ff=0):
        #
        # elem structure:
        # [
        #  Label,
        #  Source file,
        #  Source node,
        #  Mover,
        #  Priority,
        #  File family,
        #  File family width,
        #  Storage group
        # ]

	tr = HTMLgen.TR()
	if print_ff:
	    td = HTMLgen.TD("%s"%(NBSP,), html_escape='OFF')
	    td.append(HTMLgen.Bold(HTMLgen.Name(elem[5], elem[5]))) # File Family
	    tr.append(td)
	else:
	    # we do not need to print the file family, again, must have done it before
	    tr.append(empty_data())
	if elem[3] is not enstore_constants.NOMOVER:
	    tr.append(HTMLgen.TD("[at%s%s]"%(NBSP,
					     HTMLgen.Href("%s#%s"%(enstore_functions2.get_mover_status_filename(),
								   elem[3],), elem[3])),
				 html_escape='OFF'))
	    # keep track of the busy movers so when we create the addtl movers table we do
	    # not include these.
	    self.busy_movers.append(elem[3]) # mover
	else:
	    tr.append(empty_data())
	tr.append(HTMLgen.TD(elem[2]))
	# only display the last n characters of the file name
	tr.append(HTMLgen.TD(HTMLgen.Font(elem[1][-70:], color=LIGHTBLUE)))
	tr.append(HTMLgen.TD("(SG:%s%s%sCurPri:%s%s%sFFWidth:%s)"%(elem[7], NBSP, NBSP, elem[4],
								NBSP, NBSP, elem[6]),
			     html_escape='OFF'))
        return tr

    def append_vols(self, elem):
	rows = []
	rows.append(self.read_row(elem, 1))
	if self.vols.has_key(elem[0]):
	    # there are more queue elements for this volume
	    r_list = self.vols[elem[0]]
	    for n_elem in r_list:
		rows.append(self.read_row(n_elem))
	return rows

    def read_header_row(self):
	td = HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(HTMLgen.Name("reads",
							       "Reads%s"%(NBSP,)),
						  html_escape='OFF'),
				   SIZE="+3"), colspan=LM_COLS)
	td.append(HTMLgen.Font(HTMLgen.Href(get_full_queue_name(self.lm),
					    "Full%sQueue%sElements"%(NBSP, NBSP)),
			       SIZE="-1", html_escape='OFF'))
	return HTMLgen.TR(td)

    def get_in_vol_order(self, table):
	started_extra_page = 0
	num_done = 0
	num_extra = 0
	for elem in self.r_vols:
	    if num_done > self.max_lm_rows and not self.max_lm_rows == DEFAULT_ALL_ROWS:
		# we have put the max number on the main page, now make an additional page
		if not started_extra_page:
		    started_extra_page = 1
		    filename = "%s-read.html"%(self.lm,)
		    new_key = "%s-read"%(self.lm,)
		    self.extra_queue_pages[new_key] = (EnExtraLmQueuePages(self,
									   self.lm),
						       filename)
		    rhr = self.read_header_row()

		rows = self.append_vols(elem)
		if rhr:
		    # add in the header row
		    rows.insert(0, rhr)
		    rhr = None
		# subtract out the header row
		num_extra = num_extra + len(rows) - 1
		self.extra_queue_pages[new_key][0].body(rows)
	    else:
		rows = self.append_vols(elem)
		num_done = num_done + len(rows)
		for row in rows:
		    table.append(row)
	else:
	    if started_extra_page:
		table.append(empty_row(LM_COLS))
		table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(filename,
								'Extra Read Queue Rows (%s)'%(num_extra)),
						   colspan=LM_COLS)))

    def append_ff(self, elem):
	rows = []
	rows.append(self.write_row(elem, 1))
	if self.ff.has_key(elem[5]):
	    # there are more queue elements for this file family
	    w_list = self.ff[elem[5]]
	    for n_elem in w_list:
		rows.append(self.write_row(n_elem))
	return rows

    def write_header_row(self):
	txt = HTMLgen.Font(HTMLgen.Bold(HTMLgen.Name("writes",
						     "Writes%s"%(NBSP,))),
			   SIZE="+3", html_escape='OFF')
	return HTMLgen.TR(HTMLgen.TD(txt, colspan=LM_COLS))

    def get_in_ff_order(self, table):
	started_extra_page = 0
	num_done = 0
	num_extra = 0
	for elem in self.w_ff:
	    if num_done > self.max_lm_rows and not self.max_lm_rows == DEFAULT_ALL_ROWS:
		# we have put the max number on the main page, now make an additional page
		if not started_extra_page:
		    started_extra_page = 1
		    filename = "%s-write.html"%(self.lm,)
		    new_key = "%s-write"%(self.lm,)
		    self.extra_queue_pages[new_key] = (EnExtraLmQueuePages(self,
									   self.lm),
						       filename)
		    rhr = self.write_header_row()

		rows = self.append_ff(elem)
		if rhr:
		    # add in the header row
		    rows.insert(0, rhr)
		    rhr = None
		# subtract out the header row
		num_extra = num_extra + len(rows) - 1
		self.extra_queue_pages[new_key][0].body(rows)
	    else:
		rows = self.append_ff(elem)
		num_done = num_done + len(rows)
		for row in rows:
		    table.append(row)
	else:
	    if started_extra_page:
		table.append(empty_row(LM_COLS))
		table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(filename,
								'Extra Write Queue Rows (%s)'%(num_extra)),
						   colspan=LM_COLS)))

    def other_vol_info(self, table):
	if not self.data_dict.has_key(enstore_constants.ACTIVE_VOLUMES) or \
	   not self.data_dict[enstore_constants.ACTIVE_VOLUMES]:
	    # nothing to do
	    return
	other_mv = {}
	avs = self.data_dict[enstore_constants.ACTIVE_VOLUMES]
	for av in avs:
	    mover = av[enstore_constants.MOVER]
	    if av[enstore_constants.STATE] not in ['ACTIVE', 'SEEK']:
		other_mv[mover] = [av[enstore_constants.STATE], av['external_label'],
				   av['volume_family'], av['time_in_state']]
	else:
	    if other_mv:
		header_done = 0
		movers = sort_keys(other_mv)
		for mv in movers:
		    if mv not in self.busy_movers:
			if not header_done:
			    tr = HTMLgen.TR()
			    for hdr in ["Additional Mover", "State", "Seconds in State",
					"Volume", "File Family"]:
				tr.append(self.make_th(hdr))
			    mv_table = HTMLgen.TableLite(tr, border=1, cellpadding=CELLP,
					     align="LEFT", bgcolor=AQUA)
			    header_done = 1
			tr = HTMLgen.TR(HTMLgen.TD(mv))
			tr.append(HTMLgen.TD(other_mv[mv][0]))
			tr.append(HTMLgen.TD(other_mv[mv][3]))
			tr.append(HTMLgen.TD(other_mv[mv][1]))
			ff = volume_family.extract_file_family(other_mv[mv][2])
			tr.append(HTMLgen.TD(ff))
			mv_table.append(tr)
		else:
		    if header_done:
			table.append(empty_row(5))
			table.append(empty_row(5))
			table.append(HTMLgen.TR(HTMLgen.TD(mv_table, colspan=LM_COLS)))
			table.append(empty_row(5))

    def main_table(self, table):
	self.busy_movers = []
	# add the status line
	str1 = HTMLgen.Font("Status%s:"%(NBSP,), size="+1", html_escape='OFF')
	stat = HTMLgen.Font(HTMLgen.Bold("%s%s%s"%(str1, NBSP,
						   self.data_dict[enstore_constants.STATUS][0]),
					 html_escape='OFF'), color=BRICKRED, html_escape='OFF')
	table.append(HTMLgen.TR(HTMLgen.TD(stat, colspan=LM_COLS, align="LEFT",
					   html_escape='OFF')))
        table.append(empty_row(LM_COLS))
	# add the suspect volumes
	tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold("Suspect%sVolumes%s:%s"%(NBSP,
							     NBSP, NBSP), html_escape='OFF'),
						color=BRICKRED, size="+1")))
	vols = self.data_dict.get(enstore_constants.SUSPECT_VOLS, ['None'])
	if vols == ['None'] or vols == {}:
	    pass
	else:
	    for vol in vols:
		txt = "%s%s-%s"%(str(HTMLgen.Href("tape_inventory/%s"%(vol[0],), vol[0])),
				 NBSP, NBSP)
		td = HTMLgen.TD(txt, colspan=4, html_escape='OFF')
		for mv in vol[1]:
		    td.append(HTMLgen.Href("%s#%s"%(enstore_functions2.get_mover_status_filename(),
						    mv), mv))
		    td.append(" ")

		tr.append(td)
		table.append(tr)
		tr = HTMLgen.TR(empty_data())
	table.append(empty_row(LM_COLS))
	self.parse_queues()
	# add the read queue elements
	table.append(self.read_header_row())
        table.append(empty_row(LM_COLS))
	self.get_in_vol_order(table)
	# add the write queue elements
        table.append(empty_row(LM_COLS))
        table.append(empty_row(LM_COLS))
	table.append(self.write_header_row())
        table.append(empty_row(LM_COLS))
	self.get_in_ff_order(table)
	self.other_vol_info(table)
	return table

    # generate the body of the file
    def body(self, data_dict):
	# this is the data we will output
	self.data_dict = data_dict
	# create the outer table and its rows
	table = self.table_top(cols=LM_COLS)
	table.cellpadding=3
	table.append(empty_row(LM_COLS))
	table.append(self.big_title(self.title, LM_COLS))
	table.append(empty_row(LM_COLS))
	self.main_table(table)
	self.trailer(table, LM_COLS)
	self.append(table)


class EnLmFullStatusPage(EnBaseHtmlDoc):

    def __init__(self, lm, refresh=60, system_tag="", max_lm_rows=DEFAULT_FULL_LM_ROWS):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       help_file="lmFullStatusHelp.html",
			       system_tag=system_tag)
	self.title = "%s Page"%(lm,)
	self.source_server = THE_INQUISITOR
	self.script_title_gif = "lmi.gif"
	self.description = ""
	self.lm = lm
	self.max_lm_rows = max_lm_rows
	self.extra_queue_pages = {}

    # create the suspect volume row - it is a separate table
    def suspect_volume_row(self):
	# format the suspect volumes, 3 to a row
	vols = self.data_dict.get(enstore_constants.SUSPECT_VOLS, ['None'])
	vol_str = ""
	ctr = 0
	if vols == ['None'] or vols == {}:
	    tr = None
	else:
	    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Suspect%sVolumes"%(NBSP,),
						    color=BRICKRED,
						    html_escape='OFF')))
	    for vol in vols:
		if vol_str:
		    # separate the new volume from the old ones
		    if ctr == 3:
			vol_str = "%s,  <BR>"%(vol_str,)
			ctr = 0
		    else:
			vol_str = "%s,  "%(vol_str,)
		ctr = ctr + 1
		vol_str = "%s %s - %s"%(vol_str, vol[0],
					enstore_functions2.print_list(vol[1]))
	    else:
		tr.append(HTMLgen.TD(vol_str, align="LEFT", colspan=4,
				     html_escape='OFF'))
	return tr

    def priorities_row(self, qelem):
	tr = HTMLgen.TR(self.spacer_data("Priorities"))
	tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Current", color=BRICKRED),
				     NBSP*3, qelem[enstore_constants.CURRENT]),
			     html_escape='OFF'))
	tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Base", color=BRICKRED),
				       NBSP*3, qelem[enstore_constants.BASE]),
			     html_escape='OFF'))
	tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Delta", color=BRICKRED),
				       NBSP*3, qelem[enstore_constants.DELTA]),
			     html_escape='OFF'))
	tr.append(HTMLgen.TD(HTMLgen.Font("Agetime", color=BRICKRED)))
	tr.append(HTMLgen.TD(qelem[enstore_constants.AGETIME]))
	return tr

    def queue_times_row(self, qelem):
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
					      color=BRICKRED,
					      html_escape='OFF')))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.MODIFICATION]))
	else:
	    tr.append(empty_data())
	    tr.append(empty_data())
	return tr

    def file_families_row(self, qelem):
	if qelem.has_key(enstore_constants.DEVICE):
	    tr = HTMLgen.TR(self.spacer_data("Label"))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.DEVICE]))
	else:
	    tr = HTMLgen.TR(empty_data())
	    tr.append(empty_data())
	if qelem.has_key(enstore_constants.STORAGE_GROUP):
	    tr.append(HTMLgen.TD(HTMLgen.Font("SG",
					      color=BRICKRED,
					      html_escape='OFF')))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.STORAGE_GROUP]))
	if qelem.has_key(enstore_constants.FILE_FAMILY):
	    tr.append(HTMLgen.TD(HTMLgen.Font("File%sFam"%(NBSP,),
					      color=BRICKRED,
					      html_escape='OFF')))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.FILE_FAMILY]))
            work = qelem.get(enstore_constants.WORK)
            if work and work == enstore_constants.WRITE:
                tr.append(HTMLgen.TD(HTMLgen.Font("File%sFam%sWidth"%(NBSP,
                                                                      NBSP),
                                                  color=BRICKRED,
                                                  html_escape='OFF')))
                tr.append(HTMLgen.TD(qelem[enstore_constants.FILE_FAMILY_WIDTH]))
	elif qelem.has_key(enstore_constants.VOLUME_FAMILY):
	    tr.append(HTMLgen.TD(HTMLgen.Font("Volume%sFamily"%(NBSP,),
					      color=BRICKRED,
					      html_escape='OFF')))
	    tr.append(HTMLgen.TD(qelem[enstore_constants.VOLUME_FAMILY]))
	    tr.append(empty_data())
	    tr.append(empty_data())
	else:
	    tr.append(empty_data())
	    tr.append(empty_data())
	    tr.append(empty_data())
	    tr.append(empty_data())
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

    def first_queue_row(self, qelem, intro):
	text = self.get_intro_text(qelem[enstore_constants.WORK], intro)
	tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(text, color=BRICKRED,
						html_escape='OFF')))
	if qelem.has_key(enstore_constants.MOVER) and \
	   not qelem[enstore_constants.MOVER] == enstore_constants.NOMOVER:
	    tr.append(HTMLgen.TD(HTMLgen.Href("%s#%s"%(enstore_functions2.get_mover_status_filename(),
						       qelem[enstore_constants.MOVER],),
					      qelem[enstore_constants.MOVER])))
	else:
	    tr.append(empty_data())
	tr.append(HTMLgen.TD(HTMLgen.Font("Node", color=BRICKRED)))
	tr.append(HTMLgen.TD(enstore_functions2.strip_node(qelem[enstore_constants.NODE])))
	tr.append(HTMLgen.TD(HTMLgen.Font("Port", color=BRICKRED)))
	tr.append(HTMLgen.TD(qelem[enstore_constants.PORT]))
	return tr

    def local_file_row(self, qelem):
	tr = HTMLgen.TR(self.spacer_data("Local%sfile"%(NBSP,)))
	tr.append(HTMLgen.TD(qelem[enstore_constants.FILE], colspan=5))
	return tr

    def bytes_id_row(self, qelem):
	tr = HTMLgen.TR(self.spacer_data("Bytes"))
	tr.append(HTMLgen.TD(qelem[enstore_constants.BYTES]))
	tr.append(HTMLgen.TD(HTMLgen.Font("ID", color=BRICKRED)))
	tr.append(HTMLgen.TD(qelem[enstore_constants.ID], colspan=3))
	return tr

    def reject_reason_row(self, qelem):
	tr = HTMLgen.TR(self.spacer_data("Reason%sfor%sPending"%(NBSP,NBSP)))
	tr.append(HTMLgen.TD(qelem[enstore_constants.REJECT_REASON],
			     colspan=5))
	return tr

    def make_lm_queue_rows(self, qelems, intro):
	table = HTMLgen.TableLite(cellpadding=0, cellspacing=0,
				  align="LEFT", bgcolor=YELLOW, width="100%")
	for qelem in qelems:
	    table.append(self.first_queue_row(qelem, intro))
	    table.append(self.file_families_row(qelem))
	    table.append(self.queue_times_row(qelem))
	    table.append(self.priorities_row(qelem))
	    table.append(self.local_file_row(qelem))
	    table.append(self.bytes_id_row(qelem))
	    if qelem.has_key(enstore_constants.REJECT_REASON):
		table.append(self.reject_reason_row(qelem))
	    table.append(empty_row(6))
	return HTMLgen.TR(HTMLgen.TD(table, colspan=5))

    # put together the rows for either lm queue
    def lm_queue_rows(self, qelems, queue, intro):
	qlen = len(qelems)
	tr0 = None
	if (not self.max_lm_rows == DEFAULT_ALL_ROWS) and (qlen > self.max_lm_rows):
	    # we will need to cut short the number of queue elements that we
	    # output on the main status page, and add a link to point to the
	    # rest that will be on another page. however, there is only one
	    # other page at this time
	    filename = "%s-full_%s.html"%(self.lm, queue)
	    tr0 = HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(filename,
					      'Extra Queue Rows (%s)'%(qlen -
							     self.max_lm_rows,)),
					colspan=4))
	    qlen = self.max_lm_rows
	    new_key = "%s-%s"%(self.lm, queue)
	    self.extra_queue_pages[new_key] = (EnExtraLmFullQueuePages(self,
								       self.lm),
					       filename)
	    self.extra_queue_pages[new_key][0].body(self.make_lm_queue_rows(qelems[qlen:],
									    intro))

	tr1 = self.make_lm_queue_rows(qelems[:qlen], intro)
	return (tr1, tr0)

    # add the work at movers info to the table
    def work_at_movers_row(self, cols):
	# These are the keys used in work at movers queue
	# WORK, NODE, PORT, FILE, FILE_FAMILY, FILE_FAMILY_WIDTH, SUBMITTED,
	# DEQUEUED, MODIFICATION, CURRENT, BASE, DELTA, AGETIME, FILE, BYTES,
	# ID
	the_work = self.data_dict.get(enstore_constants.WORK,
				      enstore_constants.NO_WORK)
	if not the_work == enstore_constants.NO_WORK:
	    rows = self.lm_queue_rows(the_work, enstore_constants.WORK, AT_MOVERS)
	else:
	    rows = (HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(enstore_constants.NO_WORK,
						      color=BRICKRED),
					 colspan=cols)),)
	return rows

    # add the pending work row to the table
    def pending_work_row(self, cols):
	# These are the keys used in pending work
	# NODE, PORT, FILE, FILE_FAMILY, FILE_FAMILY_WIDTH, SUBMITTED,
	# CURRENT, BASE, DELTA, AGETIME, FILE, BYTES, ID
	the_work = self.data_dict.get(enstore_constants.PENDING,
				      enstore_constants.NO_PENDING)
        if the_work and not the_work == enstore_constants.NO_PENDING and \
           (the_work[enstore_constants.READ] or the_work[enstore_constants.WRITE]):
	    qelems = self.data_dict[enstore_constants.PENDING]['read'] + \
		     self.data_dict[enstore_constants.PENDING]['write']
	    rows = self.lm_queue_rows(qelems, enstore_constants.PENDING, PENDING)
	else:
	    rows = (HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(enstore_constants.NO_PENDING,
						      color=BRICKRED),
					 colspan=cols)),)
	return rows

    # output the information for a library manager
    def lm_rows(self, table):
	cols = 4
	# first the alive information
	table.append(self.alive_row(self.lm,
				    self.data_dict[enstore_constants.STATUS]))
	# we may have gotten an error while trying to get the info,
	# so check for a piece of it first
	if self.data_dict.has_key(enstore_constants.LMSTATE):
	    # the rest of the lm information is in a separate table, it starts
	    # with the suspect volume info
	    lm_table = HTMLgen.TableLite(cellpadding=0, cellspacing=0,
					 align="LEFT", bgcolor=YELLOW,
					 width="100%")
	    tr = self.suspect_volume_row()
	    if tr:
		lm_table.append(tr)
	    lm_table.append(empty_row(cols))
	    rows = self.work_at_movers_row(cols)
	    for row in rows:
		lm_table.append(row)
	    rows = self.pending_work_row(cols)
	    for row in rows:
		lm_table.append(row)
	    tr = HTMLgen.TR(empty_data())
	    tr.append(HTMLgen.TD(lm_table, colspan=cols))
	    table.append(tr)

    def main_table(self):
	# first create the table headings for each column
	tr = HTMLgen.TR()
	for hdr in HEADINGS:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, align="CENTER", cellpadding=0,
				  cellspacing=0, bgcolor=AQUA, width="100%")
	self.lm_rows(table)
	return table

class EnFileListPage(EnBaseHtmlDoc):

    def __init__(self, refresh = 60, system_tag="", max_rows=DEFAULT_FILE_LIST_ROWS):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       help_file="fileHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE File Transfers"
	self.source_server = THE_INQUISITOR
	self.script_title_gif = "afl.gif"
	self.description = ""
	self.extra_queue_pages = {}
	self.max_rows = max_rows

    def file_list_table(self, filelist, extra_row=None):
	tr = HTMLgen.TR(HTMLgen.TH(HTMLgen.Font(HTMLgen.Bold("Node"), size="+3"),
				   align="LEFT"))
	tr.append(HTMLgen.TH(HTMLgen.Font(HTMLgen.Bold("Currently Active User Files"),
					  size="+3"), align="LEFT"))
	file_table = HTMLgen.TableLite(tr, cellpadding=0, cellspacing=0, width="100%")
	file_table.append(empty_row())
	if extra_row:
	    file_table.append(extra_row)
	    file_table.append(empty_row())
	for item in filelist:
	    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(item[0], size="+1", color=BRICKRED)))
	    if item[3] is None:
		# there is no associated volume to link to
		tr.append(HTMLgen.TD(item[1]))
	    else:
		tr.append(HTMLgen.TD(HTMLgen.Href("%s.html#%s"%(item[2], item[3]),
						  item[1])))
	    file_table.append(tr)
	return file_table

    def main_list(self, filelist, table):
	filelist.sort()
	flen = len(filelist)
	tr = None
	if (not self.max_rows == DEFAULT_ALL_ROWS) and flen > self.max_rows:
	    # we will need to cut short the number of files that we
	    # output on the main status page, and add a link to point to the
	    # rest that will be on another page. however, there is only one
	    # other page at this time
	    filename = "enstore_files-1.html"
	    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(filename,
						    'More Files Here (%s)'%(flen -
									    self.max_rows,)),
					       colspan=2))
	    flen = self.max_rows
	    new_key = "file_list"
	    self.extra_queue_pages[new_key] = (EnExtraFileListPages(self), filename)
	    self.extra_queue_pages[new_key][0].body(self.file_list_table(filelist[flen:]))
	table.append(HTMLgen.TR(HTMLgen.TD(self.file_list_table(filelist[0:flen], tr))))

    # generate the body of the file
    def body(self, filelist):
	# create the outer table and its rows
	table = self.table_top()
	table.append(empty_row())
	self.main_list(filelist, table)
	self.trailer(table)
	self.append(table)


class EnSysStatusPage(EnBaseHtmlDoc):

    def __init__(self, refresh = 60, system_tag="", max_lm_rows={}):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       help_file="serverStatusHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE System Status"
	self.script_title_gif = "en_srvr.gif"
	self.source_server = THE_INQUISITOR
	self.description = ""
	self.extra_queue_pages = {}
	self.max_lm_rows = max_lm_rows
	self.unmonitored_servers = []

    def not_being_monitored(self, server):
	if self.data_dict[server][enstore_constants.STATUS][0] == \
	   enstore_constants.NOT_MONITORING:
	    return 1
	else:
	    return None

    # output the list of shortcuts on the top of the page
    def shortcut_table(self):
	# get a list of all the servers we have.  we will output a link for the
	# following -
	#              library managers
	#              movers
	self.servers = sort_keys(self.data_dict)
	got_movers = 0
	got_migrators = False
	got_unmonitored_server = 0
	shortcut_lm = []
	for server in self.servers:
	    if not got_unmonitored_server and self.not_being_monitored(server):
		got_unmonitored_server = 1
		first_unmonitored_server = server
	    if enstore_functions2.is_library_manager(server):
		shortcut_lm.append(server)
	    elif not got_movers and enstore_functions2.is_mover(server):
                first_mover = server
		got_movers = 1
	    elif not got_migrators and enstore_functions2.is_migrator(server):
                first_migrator = server
		got_migrators = True
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
	# now do all the library managers
	for lm in shortcut_lm:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#%s'%(lm,), lm)
	# now the movers
	if got_movers:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#%s'%(first_mover,), MOVERS)
	# now the migrators
	if got_migrators:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#%s'%(first_migrator,), MIGRATORS)

	# now the unmonitored servers
	if got_unmonitored_server:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#%s'%(first_unmonitored_server,),
						 UNMONITORED_SERVERS)
	# add a link to the full file list page
	tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
				       '%s'%(enstore_constants.FILE_LIST_NAME),
					     "Full File List")

	# fill out the row if we ended with less than a rows worth of data
	fill_out_row(num_tds_so_far, tr)
	table.append(tr)
	return table

    def server_row(self, server):
	return self.alive_row(server,
			      self.data_dict[server][enstore_constants.STATUS])
    # add in the information for the generic servers. these only have alive
    # information
    def generic_server_rows(self, table):
	for server in enstore_constants.GENERIC_SERVERS:
	    if self.data_dict.has_key(server) and self.data_dict[server]:
		if self.not_being_monitored(server):
		    self.unmonitored_servers.append(self.server_row(server))
		else:
		    # output its information
		    table.append(self.server_row(server))

    # output all of the media changer rows
    def media_changer_rows(self, table, skeys):
	# now output the media changer information
	for server in skeys:
	    if enstore_functions2.is_media_changer(server):
		if self.not_being_monitored(server):
		    self.unmonitored_servers.append(self.server_row(server))
		else:
		    # this is a media changer. output its alive info
		    table.append(self.server_row(server))

    # output all of the udp proxy server rows
    def udp_proxy_server_rows(self, table, skeys):
	# now output the udp proxy serverr information
	for server in skeys:
	    if enstore_functions2.is_udp_proxy_server(server):
		if self.not_being_monitored(server):
		    self.unmonitored_servers.append(self.server_row(server))
		else:
		    # this is a udp proxy server. Output its alive info
		    table.append(self.server_row(server))

    # output the row that lists the total transfers (current and pending) row
    def xfer_row(self, lm):
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(HTMLgen.Font("Ongoing%sTransfers"%(NBSP,),
					  color=BRICKRED, html_escape='OFF')))
	tr.append(HTMLgen.TD(self.data_dict[lm][enstore_constants.TOTALONXFERS]))
	tr.append(HTMLgen.TD(HTMLgen.Font("Pending%sTransfers"%(NBSP,),
					  color=BRICKRED, html_escape='OFF')))
	tr.append(HTMLgen.TD(self.data_dict[lm][enstore_constants.TOTALPXFERS]))
	# now add a link to the page with the full queue elements
	tr.append(HTMLgen.TD(HTMLgen.Href(get_full_queue_name(lm),
					  "Full%sQueue%sElements"%(NBSP,NBSP)),
			     html_escape='OFF'))
	return tr

    def make_lm_wam_queue_rows(self, qelem, cols):
	if qelem[enstore_constants.WORK] == enstore_constants.WRITE:
	    r_type = "Writing%s"%(NBSP,)
	else:
	    r_type = "Reading%s"%(NBSP,)
	vol = HTMLgen.Href("tape_inventory/%s"%(qelem[enstore_constants.DEVICE]),
			   qelem[enstore_constants.DEVICE])
	mover = HTMLgen.Href("%s#%s"%(enstore_functions2.get_mover_status_filename(),
				      qelem[enstore_constants.MOVER]),
			     qelem[enstore_constants.MOVER])
        ff = qelem[enstore_constants.FILE_FAMILY]
        sg = qelem[enstore_constants.STORAGE_GROUP]
	txt = "%s%s(%s%s%s)%susing%s%s%sfrom%s%s%sby%s%s"%(r_type, str(vol), sg, NBSP,ff, NBSP, NBSP,
						   str(mover), NBSP, NBSP,
		       enstore_functions2.strip_node(qelem[enstore_constants.NODE]),
						   NBSP, NBSP,
						   qelem[enstore_constants.USERNAME])
	return HTMLgen.TR(HTMLgen.TD(txt, colspan=cols, html_escape='OFF'))

    def work_at_movers_row(self, lm, cols):
	the_work = self.data_dict[lm].get(enstore_constants.WORK,
					  enstore_constants.NO_WORK)
	rows = []
	if not the_work == enstore_constants.NO_WORK:
	    qlen = len(the_work)
	    max_lm_rows = self.max_lm_rows.get(lm, DEFAULT_THRESHOLDS)[0]
	    if max_lm_rows == DEFAULT_ALL_ROWS or not qlen > max_lm_rows:
		rows_on_page = qlen
		extra_rows = 0
	    else:
		rows_on_page = max_lm_rows
		extra_rows = qlen - max_lm_rows

	    for qelem in the_work[0:rows_on_page]:
		rows.append(self.make_lm_wam_queue_rows(qelem, cols))

	    if extra_rows > 0:
		# we will need to cut short the number of queue elements that
		# we output on the main status page, and add a link to point
		# to the rest that will be on another page. however, there is
		# only one other page at this time
		filename = "%s_%s.html"%(lm, enstore_constants.WORK)
		rows.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(filename,
					'Extra Queue Rows (%s)'%(extra_rows,)),
						  colspan=cols)))
		qlen = max_lm_rows
		new_key = "%s-%s"%(lm, enstore_constants.WORK)
		self.extra_queue_pages[new_key] = (EnExtraLmQueuePages(self, lm),
						   filename)
                rows = []
                for elem in the_work[qlen:]:
                    rows.append(self.make_lm_wam_queue_rows(elem, cols))
		self.extra_queue_pages[new_key][0].body(rows)
	return rows

    def make_lm_pend_read_row(self, qelem, cols):
	r_type = "Pending%sread%sof%s"%(NBSP, NBSP, NBSP)
	vol = HTMLgen.Href("tape_inventory/%s"%(qelem[enstore_constants.DEVICE]),
			   qelem[enstore_constants.DEVICE])
	txt = "%s%s%sfrom%s%s%sby%s%s%s[%s]"%(r_type, str(vol), NBSP, NBSP,
				 enstore_functions2.strip_node(qelem[enstore_constants.NODE]),
				 NBSP, NBSP, qelem[enstore_constants.USERNAME], NBSP,
				 qelem.get(enstore_constants.REJECT_REASON, ""))
	return HTMLgen.TR(HTMLgen.TD(txt, colspan=cols, html_escape='OFF'))

    def make_lm_pend_write_row(self, qelem, cols):
	r_type = "Pending%swrite%sfor%s%s%s%s"%(NBSP, NBSP, NBSP,
                                               qelem[enstore_constants.STORAGE_GROUP], NBSP,
					  qelem[enstore_constants.FILE_FAMILY])
	##r_type = "Pending%swrite%sfor%s%s"%(NBSP, NBSP, NBSP,
        ##                                    qelem[enstore_constants.FILE_FAMILY])
	txt = "%s%sfrom%s%s%sby%s%s%s[%s]"%(r_type, NBSP, NBSP,
				 enstore_functions2.strip_node(qelem[enstore_constants.NODE]),
				 NBSP, NBSP, qelem[enstore_constants.USERNAME], NBSP,
				 qelem.get(enstore_constants.REJECT_REASON, ""))
	return HTMLgen.TR(HTMLgen.TD(txt, colspan=cols, html_escape='OFF'))

    def pending_work_row(self, lm, cols):
	the_work = self.data_dict[lm].get(enstore_constants.PENDING, {})
	rows = []
	extra_read_rows = []
	extra_write_rows = []
	filename = "%s_%s.html"%(lm, enstore_constants.PENDING)
	max_lm_rows = self.max_lm_rows.get(lm, DEFAULT_THRESHOLDS)[0]
	qlen = 0
	# do the read queue first
	if the_work and type(the_work) == types.DictionaryType:
	    if not the_work[enstore_constants.READ] == []:
	        qlen = len(the_work[enstore_constants.READ])
		if max_lm_rows == DEFAULT_ALL_ROWS or qlen <= max_lm_rows:
		    rows_on_page = qlen
		    extra_rows = 0
		else:
		    rows_on_page = max_lm_rows
		    extra_rows = qlen - max_lm_rows

		for qelem in the_work[enstore_constants.READ][0:rows_on_page]:
		    rows.append(self.make_lm_pend_read_row(qelem, cols))

		if extra_rows > 0:
		    for qelem in  the_work[enstore_constants.READ][rows_on_page:]:
			extra_read_rows.append(self.make_lm_pend_read_row(qelem,
									  cols))
	    if not the_work[enstore_constants.WRITE] == []:
		qlen = len(the_work[enstore_constants.WRITE])
		if max_lm_rows == DEFAULT_ALL_ROWS or not qlen > max_lm_rows:
		    rows_on_page = qlen
		    extra_rows = 0
		else:
		    rows_on_page = max_lm_rows
		    extra_rows = qlen - max_lm_rows

		for qelem in the_work[enstore_constants.WRITE][0:rows_on_page]:
                    #print "THW", qelem
		    rows.append(self.make_lm_pend_write_row(qelem, cols))

		if extra_rows > 0:
		    for qelem in  the_work[enstore_constants.WRITE][rows_on_page:]:
			extra_write_rows.append(self.make_lm_pend_write_row(qelem,
									    cols))
	extra_rows = extra_read_rows + extra_write_rows
	if extra_rows:
	    # we will need to cut short the number of queue elements that we
	    # output on the main status page, and add a link to point to the
	    # rest that will be on another page. however, there is only one
	    # other page at this time
	    rows.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(filename,
				   'Extra Queue Rows (%s)'%(len(extra_rows),)),
					      colspan=cols)))
	    new_key = "%s-%s"%(lm, enstore_constants.PENDING)
	    self.extra_queue_pages[new_key] = (EnExtraLmQueuePages(self, lm),
						  filename)
	    self.extra_queue_pages[new_key][0].body(extra_rows)

	return rows

    # output the information for a library manager
    def lm_rows(self, lm):
	table_rows = []
	cols = 5
	# first the alive information
	lm_d = self.data_dict.get(lm, {})
	# if we are updating the web page faster that receiving the new
	# info, then we already have a correct status
	if lm_d and string.find(lm_d[enstore_constants.STATUS][0], NBSP) == -1:
	    if lm_d.has_key(enstore_constants.LMSTATE) and \
	       lm_d[enstore_constants.STATUS][0] not in NO_INFO_STATES:
		# append the lm state to the status information
		lm_d[enstore_constants.STATUS][0] = \
			 "%s%s:%s%s"%(lm_d[enstore_constants.STATUS][0], NBSP, NBSP,
				      lm_d[enstore_constants.LMSTATE])
	# get the first word of the lm state, we will use this to
	# tell if this is a bad state or not
	if lm_d.has_key(enstore_constants.LMSTATE) and \
	   type(lm_d[enstore_constants.LMSTATE]) == types.StringType:
	    words = string.split(lm_d[enstore_constants.LMSTATE])
	else:
	    words = ["",]
	name = self.server_url(lm, "%s.html"%(lm,))
        table_rows.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.NAME(lm))))
	if words[0] in BAD_LM_STATES:
	    table_rows.append(self.alive_row(lm, lm_d[enstore_constants.STATUS],
					     FUSCHIA, link=lm))
	else:
	    table_rows.append(self.alive_row(lm, lm_d[enstore_constants.STATUS],
					     link = name))
	# we may have gotten an error while trying to get the info,
	# so check for a piece of it first
	if lm_d.has_key(enstore_constants.LMSTATE):
	    # the rest of the lm information is in a separate table, it starts
	    # with the suspect volume info
	    lm_table = HTMLgen.TableLite(cellpadding=0,
					 cellspacing=0, align="LEFT",
					 bgcolor=YELLOW, width="100%")
	    lm_table.append(self.xfer_row(lm))
	    lm_table.append(self.null_row(cols))
	    lm_table.append(empty_row(cols))
	    rows = self.work_at_movers_row(lm, cols)
	    for row in rows:
		lm_table.append(row)
	    rows = self.pending_work_row(lm, cols)
	    for row in rows:
		lm_table.append(row)
	    tr = HTMLgen.TR(empty_data())
	    tr.append(HTMLgen.TD(lm_table, colspan=cols))
	    table_rows.append(tr)
	return table_rows

    # output all of the library manager rows and their associated movers
    def library_manager_rows(self, table, skeys):
	for server in skeys:
	    if enstore_functions2.is_library_manager(server):
		# this is a library manager. output all of its info
		rows = self.lm_rows(server)
		if self.not_being_monitored(server):
		    aList = self.unmonitored_servers
		else:
		    aList = table
		for row in rows:
		    aList.append(row)

    def mover_row(self, server):
	# this is a mover. output its info
	mover_d = self.data_dict.get(server, {})
	name = self.server_url(server, enstore_functions2.get_mover_status_filename(),
			       server)
	if mover_d.has_key(enstore_constants.STATE) and \
	   mover_d[enstore_constants.STATE]:
	    # append the movers state to its status information
	    # if we are updating the web page faster that receiving the new
	    # info, then we already have a correct status
	    if string.find(mover_d[enstore_constants.STATUS][0], NBSP) == -1 and \
	       mover_d[enstore_constants.STATUS][0] not in NO_INFO_STATES:
		mover_d[enstore_constants.STATUS][0] = \
			      "%s%s:%s%s"%(mover_d[enstore_constants.STATUS][0],
					   NBSP, NBSP,
					   mover_d[enstore_constants.STATE])
	    # get the first word of the mover state, we will use this
	    # to tell if this is a bad state or not
	    words = string.split(mover_d[enstore_constants.STATE])
	    if words[0] in BAD_MOVER_STATES:
		return self.alive_row(server,
				      mover_d[enstore_constants.STATUS],
				      FUSCHIA, link=name)
	    else:
		return self.alive_row(server,
				      mover_d[enstore_constants.STATUS],
				      link=name)
	else:
	    return self.alive_row(server,
				  mover_d[enstore_constants.STATUS],
				  link=name)

    # output all of the mover rows
    def mover_rows(self, table, skeys):
	for server in skeys:
	    if enstore_functions2.is_mover(server):
		if self.not_being_monitored(server):
		    self.unmonitored_servers.append(self.mover_row(server))
		else:
                    table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.NAME(server))))
		    table.append(self.mover_row(server))

    def migrator_row(self, server):
	# this is a migrator. output its info
	m_d = self.data_dict.get(server, {})
	name = self.server_url(server, enstore_functions2.get_migrator_status_filename(),
			       server)
        rows = []

        for k in m_d:
            if isinstance(m_d[k], dict) and m_d[k].has_key(enstore_constants.STATE) and \
               m_d[k][enstore_constants.STATE]:
                # append the migrators state to its status information
                # if we are updating the web page faster that receiving the new
                # info, then we already have a correct status
                if string.find(m_d[enstore_constants.STATUS][0], NBSP) == -1 and \
                   m_d[enstore_constants.STATUS][0] not in NO_INFO_STATES:
                    tr = HTMLgen.TR()
                    tr.append(HTMLgen.TD(m_d[k][enstore_constants.STATE],
                                         align="LEFT", colspan=3, size="-1"))
                    rows.append(tr)

                # get the first word of the migrator state, we will use this
                # to tell if this is a bad state or not
                words = string.split(m_d[k][enstore_constants.STATE])
                if words[0] in BAD_MIGRATOR_STATES:
                    return self.alive_row(server,
                                          m_d[enstore_constants.STATUS],
                                          FUSCHIA, link=name)
        r = self.alive_row(server,
                              m_d[enstore_constants.STATUS],
                              link=name)
        r.append(empty_data())
        for row in rows:
            r.append(row)
        return r

    # output all of the migrator rows
    def migrator_rows(self, table, skeys):
	for server in skeys:
	    if enstore_functions2.is_migrator(server):
                m_row = self.migrator_row(server)
		if self.not_being_monitored(server):
		    self.unmonitored_servers.append(m_row)
		else:
                    table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.NAME(server))))
		    table.append(m_row)

    def unmonitored_server_rows(self, table):
	for row in self.unmonitored_servers:
	    table.append(row)

    # generate the main table with all of the information
    def main_table(self):
	# first create the table headings for each column
	tr = HTMLgen.TR()
	for hdr in HEADINGS:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, align="CENTER", cellpadding=0,
				  cellspacing=0, bgcolor=AQUA, width="100%")
	skeys = sort_keys(self.data_dict)
	self.unmonitored_servers = []
	self.generic_server_rows(table)
	self.media_changer_rows(table, skeys)
	self.udp_proxy_server_rows(table, skeys)
	self.library_manager_rows(table, skeys)
	self.mover_rows(table, skeys)
	self.migrator_rows(table, skeys)
	self.unmonitored_server_rows(table)
	return table

    # generate the body of the file
    def body(self, data_dict):
	# this is the data we will output
	self.data_dict = data_dict
	# create the outer table and its rows
	table = self.table_top()
	table.append(HTMLgen.TR(HTMLgen.TD(self.shortcut_table())))
	table.append(empty_row())
	table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(self.main_table())))
	self.trailer(table)
	self.append(table)

class EnEncpStatusPage(EnBaseHtmlDoc):


    error_text = {"USER ERROR, No Such File"                  : "No such file",
                  "USER ERROR, No Such Directory"             : "No such directory",
                  "USER ERROR, Last field not a directory"    : "Not a directory",
                  "USER ERROR, No Read Access"                : "EACCES",
                  "USER ERROR, No Read Access"                : "Cannot read file",
                  "USER ERROR, No Write Access"               : "No write access",
                  "TAPE ERROR, No access"                     : "NOACCESS",
                  "USER ERROR, need at least 1 /pnfs/... path": "copying unixfile to unixfile",
                  "USER ERROR, Duplicated file in list"       : "Duplicated entry",
                  "USER ERROR, Duplicated file in list"       : "Duplicate entry",
                  "USER ERROR, Duplicate request, ignored"    : "INPROGRESS",
                  "USER ERROR, File already exists"           : "EEXIST",
                  "USER ERROR, Control-C'd connection"        : "ENCP_GONE",
                  "TAPE ERROR, Admin marked tape as unavailable" : "NOTALLOWED",
                  "TAPE ERROR, At least 2 IO errors on vol, pending request cancelled": "STATUS=N O",
                  "HARDWARE FAILURE, Drive didn't go online, retrying"  : "BADSWMOUNT",
                  "HARDWARE FAILURE, AML/2 robot failed"      : "BADMOUNT",
                  "HARDWARE FAILURE, Read Error"              : "READ_ERROR",
                  "HARDWARE FAILURE, Write Error"             : "WRITE_ERROR",
                  "USER ERROR, No Local Disk Space"           : "ENOSPC",
		  "LIBRARY MANAGER LOCKED"                    : "locked for external access"
                  }
# too general     "USER ERROR"                                : "USERERROR",

    def __init__(self, refresh=120, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       help_file="encpHelp.html",
			       system_tag=system_tag)
	self.align = NO
	self.title = "ENSTORE Encp History"
	self.script_title_gif = "encph.gif"
	self.source_server = THE_INQUISITOR
	self.description = ""
	self.error_keys = self.error_text.keys()

    # create the body of the page. the data is a list of lists.  each outer
    # list element is a list of the encp data i.e. -
    #   [["10:43:49", "d0ensrv1.fnal.gov", "bakken", "55,255", "samnull-1", "3.5", "0.035"]]
    def body(self, data_list):
	table = self.table_top()
	self.append(table)
	# now create the table with the data in it, first do the row with
	# the headings
	tr = HTMLgen.TR(valign="CENTER")
	headings = ["Time", "Node", "User/Storage Group", "Mover Interface", "Bytes",
		    "Volume", "Network Rate (MB/S)", "Transfer Rate (MB/S)",
                    "Drive Rate (MB/S)", "Disk Rate (MB/S)", "Overall Rate (MB/S)"]
	num_headings = len(headings)
	for hding in headings:
	    tr.append(self.make_th(hding))
	en_table = HTMLgen.TableLite(tr, border=1, bgcolor=AQUA, width="100%",
				     cols=num_headings, cellspacing=5,
				     cellpadding=CELLP)
	num_errors = 0
	errors = []
	num_successes = 0
	self.encp_files = []
#        for row in data_list[::-1]:
# above statement does not work with lint, yetr perfectly legal. Oh well (litvinse@fnal.gov)
#
        j=len(data_list)
        while j>0:
            j = j-1
            row=data_list[j]
	    tr = HTMLgen.TR(HTMLgen.TD(row[0]))
	    # remove .fnal.gov from the node
	    row[1] = enstore_functions2.strip_node(row[1])
	    if  len(row) != 4:
		num_successes = num_successes + 1
		# this is a normal encp data transfer row
		tr.append(HTMLgen.TD(row[1]))
		tr.append(HTMLgen.TD(row[2]))
		tr.append(HTMLgen.TD(row[9]))
		tr.append(HTMLgen.TD(HTMLgen.Href("#%s%s"%(INFOTXT,
							   num_successes),
						  "%s (%s)"%(row[3],
						HTMLgen.Bold(num_successes)))))
		self.encp_files.append([row[7], row[8]])
		tr.append(HTMLgen.TD(row[4]))
		tr.append(HTMLgen.TD(row[5]))
		tr.append(HTMLgen.TD(row[6]))
                if not row[11]:
                    tr.append(empty_data())
                else:
                    tr.append(HTMLgen.TD(row[11]))
                if not row[12]:
                    tr.append(empty_data())
                else:
                    tr.append(HTMLgen.TD(row[12]))
                if not row[10]:
                    tr.append(empty_data())
                else:
                    tr.append(HTMLgen.TD(row[10]))
	    else:
		# this row is an error row
		tr.append(HTMLgen.TD(row[1]))
		tr.append(HTMLgen.TD(row[2]))
		num_errors = num_errors + 1
		errors.append(row[3])
		# we need to check the error text.  if it contains certain
		# strings  (specified in error_text), then we will output a
		# different string pointing to the actual error message at
		# the bottom of the page.
		for ekey in self.error_keys:
		    if string.find(row[3], self.error_text[ekey]) != -1:
			# found a match
			etxt = ekey
			break
		else:
		    # this is the default
		    etxt = "ERROR"
		tr.append(HTMLgen.TD(HTMLgen.Href("#%s"%(num_errors),
						  HTMLgen.Bold("%s (%s)"%(etxt,
								num_errors,))),
				     colspan=(num_headings-3)))
	    en_table.append(tr)
	self.append(en_table)
	# now make the table with the file information
	en_table = HTMLgen.TableLite()
	en_table.append(empty_row(2))
	if num_successes > 0:
	    tr = HTMLgen.TR(empty_data())
	    tr.append(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold("Files Transferred"),
					      size="+2")))
	    en_table.append(tr)
	for i in range(num_successes):
	    si = "%s"%(i+1,)
	    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(si),
						    size="+2")))
	    tr.append(HTMLgen.TD(HTMLgen.Name("%s%s"%(INFOTXT, si),
					     "%s -> %s"%(self.encp_files[i][0],
						      self.encp_files[i][1]))))
	    en_table.append(tr)
	    en_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR(), colspan=2)))
	self.append(en_table)

	# now make the table with the error information
	en_table = HTMLgen.TableLite()
	if num_errors > 0:
	    tr = HTMLgen.TR(empty_data())
	    tr.append(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold("ERRORS"),
					      size="+2", color=FILE_ERROR_COLOR)))
	    en_table.append(tr)
	for i in range(num_errors):
	    si = "%s"%(i+1,)
	    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(si),
						    size="+2",
						    color=FILE_ERROR_COLOR)))
	    tr.append(HTMLgen.TD(HTMLgen.Name(si, HTMLgen.Font(errors[i],
							color=FILE_ERROR_COLOR))))
	    en_table.append(tr)
	    en_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR(), colspan=2)))
	self.trailer(en_table, 2)
	self.append(en_table)

class EnConfigurationPage(EnBaseHtmlDoc):

    def __init__(self, refresh=600, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       help_file="configHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE Configuration"
	self.script_title_gif = "en_cfg.gif"
	self.source_server = THE_INQUISITOR
	self.description = ""

    # create the body of the page. the incoming data is a python dictionary
    def body(self, data_dict):
	table = self.table_top()
	# now add a top table with links to the individual servers (as a
	# shortcut)
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
            server_keys.remove('status')
	    first_line = 1
	    for server_key in server_keys:
		if first_line:
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font( \
			HTMLgen.Name(server, HTMLgen.Bold(server)),
			size="+1")))
		    first_line = 0
		else:
		    tr = HTMLgen.TR(empty_data())
                link = "config_params.html#%s"%(server_key,)
                href=HTMLgen.Href(link,"%s"%(server_key,))
                href.target="new_page"
                #href = '<A HREF="http:/rip7.fnaal.gov/enstore/config_params.html#%s" target="new_page">%s</A>'%(server_key, server_key)
                #href=HTMLgen.Href(link)
                #href.append('tagret=new_page')
		tr.append(HTMLgen.TD(href))
		#tr.append(HTMLgen.TD(href))
		tr.append(HTMLgen.TD(server_dict[server_key]))
		cfg_table.append(tr)

	# add this table to the main one
	table.append(HTMLgen.TR(HTMLgen.TD(cfg_table)))
	self.trailer(table)
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

    # given a dict of log files, create a list of lists which divides the log
    # files up by month and year. most recent goes first
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

    # generate the calendar looking months with url's for each day for which
    # there  exists a log file. the data in logs, should be a dictionary where
    # the log file names are the keys and the value the size of the file
    def generate_months(self, table, logs, web_host,
			caption_title="Enstore Log Files"):
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
	    log_table =  HTMLgen.TableLite(caption, bgcolor=AQUA,
					   cellspacing=5,
					   cellpadding=CELLP, align="LEFT",
					   border=2)
	    tr = HTMLgen.TR()
	    for day in calendar.day_abbr:
		tr.append(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(day),size="+1",
						  color=BRICKRED)))
	    log_table.append(tr)
	    # the following generates a  list of lists, which specifies how to draw
	    # a calendar with the first of the month occuring in the correct day of
	    # the week slot.
	    mweeks = calendar.monthcalendar(year, month)
	    for mweek in mweeks:
		tr = HTMLgen.TR()
		for day in [0,1,2,3,4,5,6]:
		    if mweek[day] == 0:
			# this is null entry represented by a blank entry on the
			# calendar
			tr.append(empty_data())
		    else:
			(size, log) = sizes[date].get(mweek[day], (-1, ""))
			if size == -1:
			    # there was no log file for this day
			    tr.append(HTMLgen.TD(HTMLgen.Bold(mweek[day]),
						 bgcolor=YELLOW))
			else:
                            # do not change unless you know what you are doing!
                            # you may fix one thing and break another!
			    td = HTMLgen.TD(HTMLgen.Href("%s/%s"%(web_host, log),
					       HTMLgen.Font(HTMLgen.Bold(mweek[day]),
							    size="+2")))
			    td.append(" : %s"%(size,))
			    tr.append(td)
		log_table.append(tr)
	    table.append(HTMLgen.TR(HTMLgen.TD(log_table)))
	    table.append(empty_row())

    # create the body of the page, where http_path is the web server path to the
    # files, www_host# is the host where the web server is running, user_logs is a
    # dictionary that contains user logs and logs is a dictionary where the log file
    # names are the keys and the sizes are the values.
    def body(self, http_path, logs, user_logs, www_host):
	table = self.table_top()
	# now add the data, first the table with the user specified log files in it
	log_table = HTMLgen.TableLite(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold(\
	             "User Specified Log Files"), size="+2", color=BRICKRED)),
				      width="50%", cellspacing=5, cellpadding=CELLP,
				      align="LEFT")
	if user_logs:
	    ul_keys = sort_keys(user_logs)
	    for ul_key in ul_keys:
		log_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(user_logs[ul_key],
							str(HTMLgen.Bold(ul_key))))))
	table.append(HTMLgen.TR(HTMLgen.TD(log_table)))
	log_table.append(empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR())))
	# now create the tables for the different months.
	self.generate_months(table, logs, "%s%s"%(www_host, http_path))
	self.trailer(table)
	self.append(table)

def latest_time_sort(one, two):
    if one[0] < two[0]:
        return 1
    elif one[0] > two[0]:
        return -1
    else:
        return 0

class EnAlarmPage(EnBaseHtmlDoc):

    def __init__(self, refresh=600, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh, help_file="alarmHelp.html",
			       system_tag=system_tag)
	self.title = "ENSTORE Alarms"
	self.script_title_gif = "en_act_alarms.gif"
	self.source_server = THE_ALARM_SERVER
	self.description = "%s may also be displayed.%sAnd a %s."%(str(HTMLgen.Bold(HTMLgen.Href('enstore_alarm_search.html', 'Previous alarms'))),
                                                                   str(HTMLgen.BR()),
                  str(HTMLgen.Bold(HTMLgen.Href("volume_audit.html", "volume audit"))))

    def sort_by_latest_time(self, alarms):
        # return a list of keys to the alarms hash, sorted from most recent alarmed to earliest
        keys = alarms.keys()
        # build array
        latest_time_keys = []
        for key in keys:
            latest_time_keys.append([alarms[key].timedate_last, key])
        else:
            latest_time_keys.sort(latest_time_sort)
        return latest_time_keys

    def alarm_table(self, alarms):
	tr = HTMLgen.TR()
        for hdr in ["%sKey%s"%(NBSP*8,NBSP*8), "Time\n(last)", "Node", "PID", "User", "Severity",
                    "Process", "Error", "Ticket Generated\n(Condition/Type)",
                    "Additional Information"]:
            tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, width="100%", border=1, cellspacing=5,
				  cellpadding=CELLP, align="LEFT", bgcolor=AQUA)
	i = 0
	akeys = self.sort_by_latest_time(alarms)
	for akey in akeys:
            akey = akey[1]    # pick out the actual key to the alarms hash. 0 is the latest time
	    alarm = alarms[akey].list_alarm()
	    td = HTMLgen.TD(HTMLgen.Input(type="checkbox", name="alarm%s"%(i,),
					  value=alarm[0]),
			    html_escape='OFF')
	    td.append("%s%s"%(NBSP*3, alarm[0]))
	    tr = HTMLgen.TR(td)
	    # remove .fnal.gov
	    alarm[2] = enstore_functions2.strip_node(alarm[2])
            td = HTMLgen.TD("%s"%(enstore_functions2.format_time(float(alarm[0]))))
            td.append(HTMLgen.BR())
	    td.append(HTMLgen.Emphasis(HTMLgen.Font("(%s)"%(enstore_functions2.format_time(alarm[1],)), size="-1")))
	    tr.append(td)
	    for item in alarm[2:8]:
		tr.append(HTMLgen.TD(item))
            tmp = noNone(alarm[10])
            td = HTMLgen.TD(noNone(tmp), html_escape='OFF')
            if not tmp == NBSP:
                td.append(HTMLgen.BR())
                td.append("(%s/%s)"%(alarm[8], alarm[9]))
            tr.append(td)
            # if the additional info is a dictionary and has the r_a key in it, get rid of it,
            # it is a leftover from udp_server
            if type(alarm[11]) == types.DictionaryType:
                if alarm[11].has_key(enstore_constants.RA):
                    del alarm[11][enstore_constants.RA]
            tr.append(HTMLgen.TD(alarm[11]))
	    table.append(tr)
	    i = i + 1
	return table

    def addButtons(self):
	tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Input(value="Resolve Selected",
						 type="submit",
						 name=RESOLVESELECTED)))
	tr.append(HTMLgen.TD(HTMLgen.Input(value="Resolve All",
						 type="submit",
						 name=RESOLVEALL)))
	tr.append(HTMLgen.TD(HTMLgen.Input(value="Reset", type="reset",
					   name="Reset")))
	tr.append(HTMLgen.TD("Alarms may be cancelled by selecting the alarm(s), pressing the %s button and then reloading the page. All alarms may be cancelled by pressing the %s button."%(str(HTMLgen.Bold(RESOLVESELECTED)),
		    str(HTMLgen.Bold(RESOLVEALL))), html_escape='OFF'))
        return tr

    def body(self, alarms, web_host):
	table = self.table_top()
	# now the data
	# form = HTMLgen.Form("%s/cgi-bin/enstore/enstore_alarm_cgi.py"%(web_host,))
	form = HTMLgen.Form("/cgi-bin/enstore_alarm_cgi.py")
	# get rid of the default submit button, we will add our own below
	form.submit = ''
        tr = self.addButtons()
	form.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.TableLite(tr,
							    width="100%"))))
	form.append(empty_row())
	form.append(empty_row())
	form.append(HTMLgen.TR(HTMLgen.TD(self.alarm_table(alarms))))
	form.append(empty_row())
	form.append(empty_row())
        tr = self.addButtons()
	form.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.TableLite(tr,
							    width="100%"))))
	table.append(form)
	self.trailer(table)
	self.append(table)

class EnAlarmSearchPage(EnBaseHtmlDoc):

    def __init__(self, background, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=600, background=background,
			       system_tag=system_tag)
	self.title = "ENSTORE Alarm Search"
	self.script_title_gif = "en_alarm_hist.gif"
	self.source_server = THE_ALARM_SERVER
	self.description = "Active and resolved alarms."

    def alarm_table(self, alarms):
	tr = HTMLgen.TR()
	for hdr in ["Time", "Node", "PID", "User", "Severity",
		    "Process", "Error", "Ticket Generated\n(Condition/Type)",
		    "Additional Information"]:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, width="100%", border=1, cellspacing=5,
				  cellpadding=CELLP, align="LEFT", bgcolor=AQUA)
	akeys = sort_keys(alarms)
	for akey in akeys:
	    alarm = alarms[akey].list_alarm()
	    tr = HTMLgen.TR((HTMLgen.TD(enstore_functions2.format_time(time.mktime(en_eval(alarm[0]))))))
	    # remove .fnal.gov
	    alarm[2] = enstore_functions2.strip_node(alarm[2])
	    for item in alarm[2:8]:
		tr.append(HTMLgen.TD(item))
            tmp = noNone(alarm[10])
            td = HTMLgen.TD(noNone(tmp), html_escape='OFF')
            if not tmp == NBSP:
                td.append(HTMLgen.BR())
                td.append("(%s/%s)"%(alarm[8], alarm[9]))
            tr.append(td)
            tr.append(HTMLgen.TD(alarm[11]))
	    table.append(tr)
	return table

    def body(self, alarms):
	table = HTMLgen.TableLite(cellspacing=0, cellpadding=0, align="LEFT",
				  width="800")
	table.append(HTMLgen.TR(HTMLgen.TD(self.alarm_table(alarms))))
	self.trailer(table)
	self.append(table)

class EnPlotPage(EnBaseHtmlDoc):

    bpd = "%s%s"

    def __init__(self, title="ENSTORE System Plots", gif="en_plots.gif",
		 system_tag="", description="", mount_label=None,
		 links_l=None, nav_link="", url_gif_dir = ""):
	EnBaseHtmlDoc.__init__(self, refresh=0, help_file="plotHelp.html",
			       system_tag=system_tag,
                               url_gif_dir = url_gif_dir)
	self.title = title
	self.script_title_gif = gif
	self.source_server = THE_INQUISITOR
	self.description = description
	self.mount_label = mount_label
	self.links_l = links_l
	self.nav_link = nav_link
        # this will be set in the child class
        self.outofdate = 0

    def find_label(self, text):
        # compare the passed text with the files listed in PLOT_INFO. if there
        # is a match, return the associated text. else return a default string.
	stamp_len = len(enstore_constants.STAMP)
        for file_label in PLOT_INFO:
	    index = string.find(text, file_label[0])
            if not index == -1:
                # this is a match
		if file_label[0] == enstore_constants.UTIL_FILE:
		    # fix up the label
		    p_type = string.split(text, "_", 1)
		    return "%s %s"%(p_type[0], file_label[1])
		elif file_label[0] == enstore_constants.BPD_FILE_D:
		    # this is a mover specific label, pull out the mover name
		    just_mover = string.replace(text[index:], file_label[0], "")
		    # get rid of _stamp.jpg too
		    index = string.find(just_mover, enstore_constants.STAMP)
		    return "%s %s"%(just_mover[0:index], file_label[1])
                elif file_label[0] == enstore_constants.XFERLOG_FILE or file_label[0] == enstore_constants.XFER_FILE:
		    just_sg = string.replace(text[index:], file_label[0], "")
		    index = string.find(just_sg, enstore_constants.STAMP)

                    if just_sg[1:index] == "":
                        return  file_label[1]
                    tmp = string.split(just_sg,"_",1)[1]
                    tmp1 = string.replace(tmp,"."," ")
                    tmp2 = string.replace(tmp1,"log","Log Scale")
                    index = string.find(tmp2, enstore_constants.STAMP)
                    return "%s "%(tmp2[0:index])
		elif file_label[0] == enstore_constants.MPD_FILE or \
		     file_label[0] == enstore_constants.MPD_MONTH_FILE:
		    # add the mount_label to the front
		    if self.mount_label:
			return "%s %s"%(self.mount_label, file_label[1])
		    else:
			return file_label[1]
                elif file_label[0] == enstore_constants.D_MPD_FILE:
                    # we must add in the drive type
                    return "%s %s"%(text[0:index], file_label[1])
                elif file_label[0] == enstore_constants.MLAT_FILE:
                    return "%s %s"%(text[index:-1].split('_')[0], file_label[1])
		else:
		    return file_label[1]
        else:
	    # we could not find a label.  use the text without the _stamp.jpg
	    t = "%s%s"%(enstore_constants.STAMP, enstore_constants.JPG)
	    return string.replace(text, t, "")

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
            text = "%s (%s)%s"%(self.find_label(stamp[0]),
                                enstore_functions2.format_time(stamp[1]),
                                NBSP*2)
            #if self.outofdate and stamp[1] < self.yesterday: (commented by Dmitry)
            today = time.time()
            day_secs = 26.*60.*60.
            self.yesterday = today - day_secs
            if stamp[1] < self.yesterday:
                #trps.append(HTMLgen.TD(HTMLgen.Font(text, color=FUSCHIA, html_escape='OFF')))
                trps.append(HTMLgen.TD(HTMLgen.Font(text, html_escape='OFF'),bgcolor=FUSCHIA))
            else:
                trps.append(HTMLgen.TD(text, html_escape='OFF'))
	    if ps:
		# we have a corresponding ps file
		trps2.append(HTMLgen.TD(HTMLgen.Href(ps_file, POSTSCRIPT)))
	    else:
		trps2.append(empty_data())

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
	# add any links to other plot pages
	if self.links_l:
	    for link,txt in self.links_l:
		tr = HTMLgen.TR()
		tr.append(HTMLgen.TD(HTMLgen.Href(link,
						  HTMLgen.Font(txt, size="+2")),
				     colspan=3, align="LEFT"))
		plot_table.append(tr)
	    else:
		plot_table.append(empty_row(3))
		plot_table.append(empty_row(3))

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
	self.trailer(table)
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
        table_top = self.table_top(add_update=0)
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
	self.trailer(table_top)

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

    redball_gif = "redball.gif"

    greenball = HTMLgen.Image("greenball.gif", width=17, height=17, border=0)
    redball = HTMLgen.Image(redball_gif, width=17, height=17, border=0)
    question = HTMLgen.Image("star.gif", width=17, height=17, border=0)
    yellowball = HTMLgen.Image("yelball.gif", width=17, height=17, border=0)
    checkmark = HTMLgen.Image("checkmark.gif", width=17, height=17, border=0)

    def __init__(self, title="Mass Storage Status-At-A-Glance", gif="ess-aag.gif",
		 system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=360, help_file="saagHelp.html",
			       system_tag=system_tag)
	self.title = title
	self.script_title_gif = gif
	self.source_server = THE_ENSTORE
	self.description = ""

    def get_gif(self, dict, key):
	val = dict.get(key, enstore_constants.DOWN)
	if val == enstore_constants.WARNING:
	    return self.yellowball
	elif val == enstore_constants.UP:
	    return self.greenball
	elif val == enstore_constants.SEEN_DOWN:
	    return self.question
	else:
	    return self.redball

    def get_color_ball(self, dict, key, direction=""):
	return HTMLgen.TD(self.get_gif(dict, key), align=direction)

    # the  alt_key is used to specify a more explicit data item than just the key
    def get_element(self, dict, key, out_dict, offline_dict, alt_key="",
		    make_link=0):
	val = dict.get(key, enstore_constants.DOWN)
	sched = out_dict.get(key, enstore_constants.NOSCHEDOUT)
	if not alt_key:
	    alt_key = key
	if make_link != 0:
	    # make the name be a link to the server status page element
	    if enstore_constants.SERVER_NAMES.has_key(alt_key):
		name = enstore_constants.SERVER_NAMES[alt_key]
	    else:
		name = alt_key
	    h_alt_key = HTMLgen.Href("%s#%s"%(self.status_file_name, name), alt_key)
	else:
	    h_alt_key = alt_key
	if offline_dict.has_key(key):
	    # this element is known to be offline
	    td = HTMLgen.TD(HTMLgen.Strike(h_alt_key))
	    # record the fact that this one is offline so we can add the reason
	    # later. we do it later to allow a scheduled outage checkmark to appear
	    # next to the server name and not after the offline reason
	    is_offline = 1
	else:
	    is_offline = 0
	    td = HTMLgen.TD(h_alt_key)
	if sched != enstore_constants.NOSCHEDOUT:
	    if sched != "":
		td.append(" ")
		td.append(self.checkmark)
		td.append(HTMLgen.BR())
		td.append(HTMLgen.Emphasis(HTMLgen.Font("(%s)"%(sched,), size="-1")))
	if is_offline == 1 and offline_dict[key] != "":
	    td.append(HTMLgen.BR())
	    td.append(HTMLgen.Emphasis(HTMLgen.Font("(%s)"%(offline_dict[key],),
						    size="-1")))
	return td

    def check_for_red(self, enstat_d, table, empty_rows=4):
	# check if the enstore ball is red, if so we need to add something
	ball_gif = self.get_gif(enstat_d, enstore_constants.ENSTORE)
	if ball_gif.filename == self.redball_gif:
	    self.background = None
	    self.bgcolor = "YELLOW"
	    rb_tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Image("bigredball.gif",
						     title="Enstore ball has turned red"),
				       align="CENTER"))
	    rb_tr.append(HTMLgen.TD(HTMLgen.Image("enstore_is_red.gif"), align="LEFT"))
	    table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.TableLite(rb_tr, cellspacing=1,
								 cellpadding=1, border=0,
								 align="CENTER",
								 width="90%"))))
	    while empty_rows > 0:
		table.append(empty_row())
		empty_rows = empty_rows - 1
	    return 1
	else:
	    return None

    def get_med_list(self, dict):
	keys = dict.keys()
	keys.sort()
	new_list = []
	# make a list of dicts out of it
	for key in keys:
	    elem = dict[key]
	    new_list.append({elem[1]:elem[0]})
	return new_list

    def add_to_row(self, tr, val, dict, outage_d, offline_d):
	tr.append(self.get_color_ball(dict, val, "RIGHT"))
	txt = HTMLgen.Font(val, size="+2")
	if dict.has_key(enstore_constants.URL) and dict[enstore_constants.URL]:
	    txt = HTMLgen.Href(dict[enstore_constants.URL], txt)
	tr.append(self.get_element(dict, val, outage_d, offline_d, txt))

    def make_overall_table(self, enstat_d, other_d, medstat_d, alarms_d, outage_d,
			   offline_d):
	entable = HTMLgen.TableLite(cellspacing=1, cellpadding=1, border=0,
				    align="CENTER", width="90%")
	entable.append(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold("Overall Status"),
						    size="+3", color=BRICKRED)))
	entable.append(empty_row(6))
	# first get a list of the media tags we will have (1 per row)
	med_l = self.get_med_list(medstat_d)
	tr = HTMLgen.TR()
	med_keys = med_l[0].keys()
	self.add_to_row(tr, enstore_constants.ENSTORE, enstat_d, outage_d, offline_d)
	self.add_to_row(tr, med_keys[0], med_l[0], outage_d, offline_d)
	del med_l[0]
	self.add_to_row(tr, enstore_constants.ANYALARMS, alarms_d, outage_d, offline_d)
	entable.append(tr)
	# get any more media rows if needed
	for item in med_l:
	    tr = HTMLgen.TR()
	    tr.append(empty_data(2)) # cover for no enstore element
	    self.add_to_row(tr, item.keys()[0], item, outage_d, offline_d)
	    tr.append(empty_data(4)) # cover for no network & alarm elements
	    entable.append(tr)

        # add any other information we need
        if other_d:
            cols = 8
            num = 0
            entable.append(empty_row(cols))
            elems = other_d.keys()
            elems.sort()
            tr = HTMLgen.TR()
            for elem in elems:
                #fix up dict in order to use already existing function
                d = {other_d[elem][0] : other_d[elem][2],
                     enstore_constants.URL : other_d[elem][1]}
                self.add_to_row(tr, other_d[elem][0], d, outage_d, offline_d)
                # added ball and link
                num = num + 2
                if num == cols:
                    entable.append(tr)
                    tr = HTMLgen.TR()
                    num = 0
            else:
                # fix up the end of the row
                if num > 0:
                    cols_left = cols - num
                    tr.append(empty_data(cols_left))
                    entable.append(tr)

	return entable

    def get_time_row(self, dict):
	# if the dictionary has a time key, then use the value in a row.  else
	# use "???"
	theTime = dict.get(enstore_constants.TIME, "???")
	return (HTMLgen.TR(HTMLgen.TD("(as of %s)"%(theTime,), colspan=8,
				      align="CENTER")))

    def add_data(self, dict, keys, tr, out_dict, offline_dict):
	if len(keys) > 0:
	    key = keys.pop(0)
	    tr.append(self.get_color_ball(dict, key, "RIGHT"))
	    # by putting something in the last parameter, we are telling the
	    # function to make the element it creates a link to the server status
	    # page
	    tr.append(self.get_element(dict, key, out_dict, offline_dict, "", 2))
	else:
	    tr.append(empty_data(2))
	return keys

    def make_server_table(self, dict, out_dict, offline_dict):
	ignore = [enstore_constants.ENSTORE, enstore_constants.TIME,
		  enstore_constants.URL]
	entable = HTMLgen.TableLite(cellspacing=1, cellpadding=1, border=0,
				    align="CENTER", width="90%")
	entable.append(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold("Enstore Individual Server Status"),
						    size="+3", color=BRICKRED)))
	entable.append(empty_row(8))
	# add the individual column headings
	tr = HTMLgen.TR(empty_header())
	tr.append(HTMLgen.TH(HTMLgen.Font(HTMLgen.U("Servers"), size="+1",
					  color=BRICKRED), align="RIGHT", colspan=2))
	tr.append(empty_header())
	for hdr in ["Library Managers", "Media Changers"]:
	    tr.append(empty_header())
	    tr.append(HTMLgen.TH(HTMLgen.Font(HTMLgen.U(hdr), size="+1",
					      color=BRICKRED), align="LEFT"))
	entable.append(tr)
	# now split up the data into each column
	keys = sort_keys(dict)
	lm = {}
	mv = {}
        migrators = {}
	mc = {}
	gs = {}
	for key in keys:
	    if not key in ignore:
		if enstore_functions2.is_library_manager(key):
		    lm[key] = dict[key]
		elif enstore_functions2.is_mover(key):
		    mv[key] = dict[key]
		elif enstore_functions2.is_media_changer(key):
		    mc[key] = dict[key]
		elif enstore_functions2.is_migrator(key):
		    migrators[key] = dict[key]
		else:
		    gs[key] = dict[key]
	else:
	    lm_keys = sort_keys(lm)
	    mv_keys = sort_keys(mv)
	    mc_keys = sort_keys(mc)
            migrator_keys = sort_keys(migrators)
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
	tr.append(HTMLgen.TH(HTMLgen.Font(HTMLgen.U("Movers"), size="+1",
					  color=BRICKRED), align="RIGHT", colspan=2))
	entable.append(tr)
	while len(mv_keys) > 0:
	    tr = HTMLgen.TR()
	    mv_keys = self.add_data(mv, mv_keys, tr, out_dict, offline_dict)
	    mv_keys = self.add_data(mv, mv_keys, tr, out_dict, offline_dict)
	    mv_keys = self.add_data(mv, mv_keys, tr, out_dict, offline_dict)
	    mv_keys = self.add_data(mv, mv_keys, tr, out_dict, offline_dict)
	    entable.append(tr)

       # now add the migrator information
	tr = HTMLgen.TR(empty_header())
	tr.append(HTMLgen.TH(HTMLgen.Font(HTMLgen.U("Migrators"), size="+1",
					  color=BRICKRED), align="RIGHT", colspan=2))
	entable.append(tr)
	while len( migrator_keys) > 0:
	    tr = HTMLgen.TR()
	    migrator_keys = self.add_data(migrators, migrator_keys, tr, out_dict, offline_dict)
	    migrator_keys = self.add_data(migrators, migrator_keys, tr, out_dict, offline_dict)
	    migrator_keys = self.add_data(migrators, migrator_keys, tr, out_dict, offline_dict)
	    migrator_keys = self.add_data(migrators, migrator_keys, tr, out_dict, offline_dict)
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
			    (self.question, "Situation under investigation")]:
	    tr.append(HTMLgen.TD(ball))
	    tr.append(HTMLgen.TD(HTMLgen.Font(txt, size="-1")))
	entable.append(tr)
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(self.checkmark))
	tr.append(HTMLgen.TD(HTMLgen.Font("Scheduled outage", size="-1")))
	tr.append(HTMLgen.TD(empty_data()))
	tr.append(HTMLgen.TD(HTMLgen.Font(HTMLgen.Strike("Known Down"), size="-1"), colspan=2))
	entable.append(tr)
	return entable

    def make_node_server_table(self, dict):
	entable = HTMLgen.TableLite(cellspacing=3, cellpadding=3, align="CENTER",
				    border=2, width="90%", bgcolor=AQUA)
	entable.append(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold("Enstore Node/Server Mapping"),
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
	    l_list = HTMLgen.List()
	    for server in servers:
		l_list.append(server)
	    tr.append(HTMLgen.TD(l_list))
	else:
	    while len(tr) < cols:
		tr.append(empty_data())
	    entable.append(tr)
	return entable

    def body(self, enstat_d, other_d, medstat_d, alarms, nodes, outage_d,
	     offline_d, media_tag, status_file_name):
	# name of file that we will create links to
	self.status_file_name = status_file_name
	# create the outer table and its rows
	table = self.table_top()
	# add the table with the general status
	# fake this for now, remove when get this working
	medstat_d = {}
	mtags = media_tag.keys()
	for mtag in mtags:
	    if offline_d.has_key(mtag):
		# mark the robot as down
		medstat_d[mtag] = [enstore_constants.DOWN, media_tag[mtag]]
	    else:
		medstat_d[mtag] = [enstore_constants.UP, media_tag[mtag]]
	self.check_for_red(enstat_d, table)
	table.append(HTMLgen.TR(HTMLgen.TD(self.make_overall_table(enstat_d, other_d,
								   medstat_d, alarms,
								   outage_d,
								   offline_d))))
	table_spacer(table)
	# add the table with the individual server status
	if enstat_d:
	    table.append(HTMLgen.TR(HTMLgen.TD(self.make_server_table(enstat_d,
								      outage_d,
								      offline_d))))
	    table_spacer(table)
	# add the table with the media info
	#if medstat_d:
	    #table.append(HTMLgen.TR(HTMLgen.TD(self.make_media_table(medstat_d, outage_d))))
	    #table_spacer(table)

	# add the section where nodes are reported and which servers run on them
	table.append(HTMLgen.TR(HTMLgen.TD(self.make_node_server_table(nodes))))
	table_spacer(table)

	# add the legend table
	table.append(HTMLgen.TR(HTMLgen.TD(self.make_legend_table())))
	self.trailer(table)
	self.append(table)


class EnSaagNetworkPage(EnSaagPage):

    def __init__(self, title="ENSTORE Network Status-At-A-Glance", gif="ess-aag.gif",
		 system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=360, help_file="saagNetworkHelp.html",
			       system_tag=system_tag)
	self.title = title
	self.script_title_gif = gif
	self.source_server = THE_ENSTORE
	self.description = ""

    def make_network_table(self, dict, out_dict, offline_dict):
        ignore = [enstore_constants.NETWORK, enstore_constants.BASENODE,
                  enstore_constants.TIME, enstore_constants.URL]
        entable = HTMLgen.TableLite(cellspacing=1, cellpadding=1, border=0,
                                    align="CENTER", width="90%")
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

    def body(self, netstat_d, outage_d, offline_d):
	# create the outer table and its rows
	table = self.table_top()
	# add the table with the network info
	if netstat_d:
	    table.append(HTMLgen.TR(HTMLgen.TD(self.make_network_table(netstat_d,
								       outage_d,
								       offline_d))))
	    table_spacer(table)

	# add the legend table
	table.append(HTMLgen.TR(HTMLgen.TD(self.make_legend_table())))
	self.trailer(table)
	self.append(table)


class EnStatusOnlyPage(EnSaagPage):

    def __init__(self, title="Mass Storage Production System's Status",
                 gif="en_all.gif"):
        print "in EnStatusOnlyPage"
        sys.stdout.flush()
        EnBaseHtmlDoc.__init__(self, refresh=370, background="enstore_background.gif")
        print "EnBaseHtmlDoc__init__done"
        sys.stdout.flush()

        self.title = title
        self.script_title_gif = gif
        self.source_server = THE_ENSTORE
        self.description = ""

    def table_top(self, cols=1):
        # create the outer table and its rows
        table = HTMLgen.TableLite(cellspacing=0, cellpadding=0,
                                  align="LEFT", width="800")
        tr = empty_row(3)
        self.script_title(tr)
        table.append(tr)
        # only add this info if we know it
        if self.source_server:
            table.append(self.add_source_server(3))
        tr = HTMLgen.TR(empty_data(3))
        tr.append(self.add_last_updated())
        table.append(tr)
        table.append(empty_row(4))
        table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR(size=2, noshade=1), colspan=4)))
        table.append(empty_row(4))
        return table

    def add_to_row(self, tr, val, dict, outage_d, offline_d, alt_txt=None):
        tr.append(self.get_color_ball(dict, val, "RIGHT"))
        if not alt_txt:
            txt = HTMLgen.Font(val, size="+2")
        else:
            txt = HTMLgen.Font(alt_txt, size="+2")
        if dict.has_key(enstore_constants.URL):
            txt = HTMLgen.Href(dict[enstore_constants.URL], txt)
        td = self.get_element(dict, val, outage_d, offline_d, txt)
        td.colspan = 2
        tr.append(td)

    def add_status_row(self, estatus, txt, table):
        # this list has the following elements -
        #     enstore status
        #     time status obtained
        #     txt if known down, otherwise a -1
        #     txt if scheduled outage, otherwise a -1
        #     value overridden to, otherwise a -1
        #     web server address
        status, eftime, offline, outage, override, web_address = estatus
        # first check the time.  if it is 10 minutes different than ours, enstore is
        # assumed to be down
        etime = enstore_functions2.unformat_time(eftime)
        if time.time() - etime > 600 and override == enstore_constants.ENONE:
            # the time is too far off, make the enstore ball red
            status = enstore_constants.DOWN
        # use the functions already provided so, need to format things a little
        if offline == enstore_constants.ENONE:
            offline_d = {}
        else:
            offline_d = {enstore_constants.ENSTORE : offline}
        if outage == enstore_constants.ENONE:
            outage_d = {}
        else:
            outage_d = {enstore_constants.ENSTORE : outage}
        enstat_d = {enstore_constants.ENSTORE : status}
        if not web_address == enstore_constants.ENONE:
            enstat_d[enstore_constants.URL] = "%s/enstore/%s"%(web_address,
                                                               enstore_constants.SAAGHTMLFILE)
        if not self.check_for_red(enstat_d, table, 0):
            tr = HTMLgen.TR(empty_data())
            self.add_to_row(tr, enstore_constants.ENSTORE, enstat_d, outage_d, offline_d,
                            txt)
            table.append(tr)
        else:
            # enstore ball is red need to identify it better on the page as to which one
            txt = HTMLgen.Font(txt, size="+2", color=BRICKRED)
            if enstat_d.has_key(enstore_constants.URL):
                txt = HTMLgen.Href(enstat_d[enstore_constants.URL], txt)
            table.append(HTMLgen.TR(HTMLgen.TD(txt, colspan=4, align="center")))

    def body(self, status_d, txt_d):
        # create the outer table and its rows
        table = self.table_top()
        keys = status_d.keys()
        keys.sort()
        for key in keys:
            self.add_status_row(status_d[key], " %s (%s)"%(txt_d[key],
                                                           status_d[key][1]), table)
            table.append(empty_row(4))
        else:
            # add the legend table
            table_spacer(table, 4)
            table.append(HTMLgen.TR(HTMLgen.TD(self.make_legend_table(), colspan=4)))
            self.trailer(table, 4)
            self.append(table)

class EnSGIngestPage(EnBaseHtmlDoc):

    def __init__(self, dir, refresh=0, system_tag=""):
	EnBaseHtmlDoc.__init__(self, refresh=refresh,
			       system_tag=system_tag)
	self.align = NO
	self.title = "Tape Ingest Rates by Storage Group"
        self.web_dir = dir

    def body(self):
        plots='burn-rate'
        plots_dir = os.path.join(self.web_dir,plots)
        if not os.path.exists(plots_dir):
            os.makedirs(plots_dir)
            os.system("cp ${ENSTORE_DIR}/etc/*.gif %s"%(plots_dir))
        stamps=[]
        images=[]
        files=os.listdir(plots_dir)
        for filename in files:
            if filename.find("_stamp.jpg") > 0:
                stamps.append(filename)
            if filename.find(".jpg") > 0 and filename.find("_stamp") == -1:
                images.append(filename)

        libraries = {}
        for stamp in stamps:
            sep ="_"
            tape_sg = stamp.split("_")[0]
            a = stamp.split("_")
            b= []
            for i in a:
              if i.find("stamp") == -1:
                  b.append(i)
            if len(b) > 1:
                a=sep.join(b)
            else:
                a=b[0]
            s1 ="."

            image = s1.join((a, "jpg"))
            if len(image.split(".")) == 3:
                lib = image.split(".")[0]
                sg = image.split(".")[1]
            else:
               lib = image.split(".")[0]
               sg = "All storage groups"

            if not libraries.has_key(lib):
                libraries[lib]=[]
            if image in images:
                libraries[lib].append((sg,os.path.join(plots,stamp), os.path.join(plots,image)))


        libs=libraries.keys()
        libs.sort()
        for lib in libs:
            tr = HTMLgen.TR()
            en_table = HTMLgen.TableLite(tr, border=1, bgcolor="#ffff00", width="100%",
                                         cellspacing=5,
                                         cellpadding=CELLP)
            en_table.append(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold(lib),
                                                         size="+3", color=BRICKRED)))

            rows = len(libraries[lib])/6 # 6 entries per raw
            if len(libraries[lib])%6:
                rows = rows+1


            sg_count = 0
            stop = 0
            for i in range(rows):

                tr = HTMLgen.TR(valign="CENTER")
                for j in range(6):
                    link=libraries[lib][sg_count][2]
                    href=HTMLgen.Href(link,"<img src=%s><br>%s"%(libraries[lib][sg_count][1],
                                                             libraries[lib][sg_count][0]))
                    tr.append(HTMLgen.TD(href))
                    sg_count = sg_count + 1
                    if sg_count >= len(libraries[lib]):
                        stop = 1
                        break
                en_table.append(tr)

            self.append(en_table)


class EnGeneratedWebPage(EnBaseHtmlDoc):

    def __init__(self, title="Generated Enstore Web Pages", gif="",
		 system_tag="", description="", mount_label=None,
		 links_l=None, nav_link="", url_gif_dir = ""):
	EnBaseHtmlDoc.__init__(self, refresh=0,
                               #help_file = "",
			       system_tag=system_tag,
                               url_gif_dir = url_gif_dir)
	self.title = title
	self.script_title_gif = gif
	self.source_server = THE_ENSTORE
	self.description = description
	self.mount_label = mount_label
	self.links_l = links_l
	self.nav_link = nav_link
        # this will be set in the child class
        self.outofdate = 0

        self.TEXTSIZE="+2"
        self.TEXTCOLOR="#000066"

    def add_row_to_table(self,table,link,name,explanation):
        tr=HTMLgen.TR()
        tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(HTMLgen.Href(link,name), size=self.TEXTSIZE,color=self.TEXTCOLOR),align="LEFT")))
        tr.append(HTMLgen.TD(HTMLgen.Font(explanation,size=self.TEXTSIZE,color=self.TEXTCOLOR),valign="CENTER"))
        table.append(tr)

    def body(self, directory_list):
	table = self.table_top()
	# create a grid of jpg stamp files 3 stamps wide.  attach a link to
	# the associated postscript file too
	plot_table = HTMLgen.TableLite(width="100%", cols="3", align="CENTER",
                                       cellspacing=0, cellpadding=0)
	# add any links to other directory's pages
	if self.links_l:
	    for link,txt in self.links_l:
		tr = HTMLgen.TR()
		tr.append(HTMLgen.TD(HTMLgen.Href(link,
						  HTMLgen.Font(txt, size="+2")),
				     colspan=3, align="LEFT"))
		plot_table.append(tr)
	    else:
		plot_table.append(empty_row(3))
		plot_table.append(empty_row(3))

        for filename in directory_list:
            basename = os.path.basename(filename)
            explanation = ""  #What should this be?
            self.add_row_to_table(plot_table, basename, basename, explanation)

        table.append(HTMLgen.TR(HTMLgen.TD(plot_table)))
	self.trailer(table)
	self.append(table)

