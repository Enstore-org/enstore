#
# enstore import
import string
import calendar
import time
import HTMLgen

import enstore_status

YELLOW   = "#FFFFF0"
AQUA     = "#DFF0FF"
BRICKRED = "#770000"
DARKBLUE = "#000066"
TIMED_OUT_COLOR = "#FF9966"

NAV_TABLE_COLOR = YELLOW
NBSP = "&nbsp;"
CELLP = 3
MAX_SROW_TDS = 5

MEDIA_CHANGERS = "Media Changers"
MEDIA_CHANGER = "media_changer"
SERVERS = "Servers"
ORPHAN_MOVERS = "Orphan Movers"

GENERIC_SERVERS = ["alarm_server", "config_server", "file_clerk",
		   "inquisitor", "log_server", "volume_clerk"]

def is_this(server, suffix):
    stype = string.split(server, ".")
    if stype[len(stype)-1] == suffix:
	return 1
    return 0

# return true if the passed server name ends in ".library_manager"
def is_library_manager(server):
    return is_this(server, "library_manager")

# return true if the passed server name ends in ".mover"
def is_mover(server):
    return is_this(server, "mover")

# return true if the passed server name ends in ".media_changer"
def is_media_changer(server):
    return is_this(server, MEDIA_CHANGER)

# return true if the passed server name is one of the following -
#   file_clerk, volume_clerk, alarm_server, inquisitor, log_server, config
#   server
def is_generic_server(server):
    if server in GENERIC_SERVERS:
	return 1
    return 0

# return try if this server is the blocksizes
def is_blocksizes(server):
    if server == enstore_status.BLOCKSIZES:
	return 1
    return 0

# check if this mover is listed as belonging to any library manager.
def is_orphan_mover(mover, dict_keys, server_dict):
    if is_mover(mover):
	for dict_key in dict_keys:
	    if is_library_manager(dict_key):
		if server_dict[dict_key].has_key(enstore_status.MOVERS) and \
		   mover in server_dict[dict_key][enstore_status.MOVERS]:
		    break
	else:
	    # we did not find any library managers that know this mover.
	    return 1
    return 0

def check_row(num_tds_so_far, tr, table):
    if num_tds_so_far == MAX_SROW_TDS:
	# finish this row and get a new one
	num_tds_so_far = 0
	table.append(tr)
	tr = HTMLgen.TR()
    return (tr, num_tds_so_far)

def add_to_scut_row(num_tds_so_far, tr, table, link, text):
    tr, num_tds_so_far = check_row(num_tds_so_far, tr, table)
    tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	                 HTMLgen.Href(link, text), size="+1")),
			 bgcolor=YELLOW))
    return (tr, num_tds_so_far + 1)

class EnBaseHtmlDoc(HTMLgen.SimpleDocument):

    def set_meta(self):
	self.meta = HTMLgen.Meta(equiv="Refresh", content=self.refresh)

    # this is the base class for all of the html generated enstore documents
    def __init__(self, refresh=0):
	self.textcolor = DARKBLUE
	self.background = "enstore.gif"
	HTMLgen.SimpleDocument.__init__(self, background=self.background, textcolor=self.textcolor)
	self.refresh = refresh
	self.set_meta()
	self.contents = []

    # generate the three button navigation table for the top of each of the
    # enstore web pages
    def nav_table(self):
	tr = HTMLgen.TR()
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href('enstore_system.html', 'Home'), size="+2")), 
			     bgcolor=NAV_TABLE_COLOR))
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href('status_enstore_system.html', SERVERS),
		  size="+2")), bgcolor=NAV_TABLE_COLOR))
	tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
	          HTMLgen.Href('encp_enstore_system.html', 'Encp'),
		  size="+2")), bgcolor=NAV_TABLE_COLOR))
	table = HTMLgen.TableLite(tr, border=1, cellspacing=5, 
				  cellpadding=CELLP, align="LEFT")
	return table

    def spacer_data(self, data):
	return HTMLgen.TD(HTMLgen.Font("%s%s"%(NBSP*6, data), color=BRICKRED,
			  html_escape='OFF'))

    def empty_data(self, cols=0):
	td = HTMLgen.TD(NBSP, html_escape='OFF')
	if cols:
	    td.colspan = cols
	return td

    def empty_row(self, cols=0):
	# output an empty row
	return HTMLgen.TR(self.empty_data(cols))

    def null_row(self, rows):
	# output a null row with NO data
	td = HTMLgen.TD()
	td.colspan = rows
	return HTMLgen.TR(td)

    def script_title(self, table):
	# output the script title at the top of the page surrounded by empty
	# rows
	table.append(self.empty_row())
	table.append(self.empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Center(HTMLgen.Image(self.script_title_gif)))))
	table.append(self.empty_row())
	table.append(self.empty_row())

    def table_top_b(self, table, td):
	td.append(HTMLgen.Font(self.description, html_escape='OFF', size="+2"))
	td.append(HTMLgen.HR())
	table.append(HTMLgen.TR(td))
	table.append(self.empty_row())
	return table

    def table_top(self):
	# create the outer table and its rows
	table = HTMLgen.TableLite(HTMLgen.TR(HTMLgen.TD(self.nav_table())), cellspacing=0,
				  cellpadding=0)
	self.script_title(table)
	td = HTMLgen.TD(HTMLgen.HR())
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

    def __init__(self, refresh = 60):
	EnBaseHtmlDoc.__init__(self, refresh)
	self.title = "ENSTORE System Status"
	self.script_title_gif = "ess.gif"
	self.description = "%s%sCurrent status of the running Enstore system as listed in the %s. This page is created by the Inquisitor and periodically updated."%(NBSP, NBSP, 										     HTMLgen.Href("config_enstore_system.html",
							               "Configuration file"))

    # output the list of shortcuts on the top of the page
    def shortcut_table(self):
	# get a list of all the servers we have.  we will output a link for the
	# following - 
	#              servers in general
	#              each library manager
	#              media changers
	#              orphan movers (not listed with any library manager
	#              blocksizes
	self.servers = self.data_dict.keys()
	self.servers.sort()
	got_media_changers = got_generic_servers = got_blocksizes = 0
	got_orphan_movers = 0
	shortcut_lm = []
	for server in self.servers:
	    if is_library_manager(server):
		shortcut_lm.append(server)
	    elif not got_media_changers and is_media_changer(server):
		got_media_changers = 1
	    elif not got_generic_servers and is_generic_server(server):
		got_generic_servers = 1
	    elif not got_orphan_movers and \
		 is_orphan_mover(server, self.servers, self.data_dict):
		got_orphan_movers = 1
	    elif is_blocksizes(server):
		got_blocksizes = 1
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
	# now finish up with the media changers, orphan movers and blocksizes
	if got_media_changers:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#%s'%(MEDIA_CHANGER,),
						  MEDIA_CHANGERS)
	if got_orphan_movers:
	    tr, num_tds_so_far = add_to_scut_row(num_tds_so_far, tr, table,
						  '#mover', ORPHAN_MOVERS)
	if got_blocksizes:
	    tr, num_tds_so_far = check_row(num_tds_so_far, tr, table)
	    tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(\
		                 HTMLgen.Href("#blocksizes", "Blocksizes"), 
				 size="+1")), bgcolor=YELLOW, align="LEFT"))
	    num_tds_so_far = num_tds_so_far +1
	# fill out the row if we ended with less than a rows worth of data
	while num_tds_so_far < MAX_SROW_TDS:
	    td = self.empty_data()
	    td.bgcolor = YELLOW
	    tr.append(td)
	    num_tds_so_far = num_tds_so_far + 1
	table.append(tr)
	return table
	
    # make the row with the servers alive information
    def alive_row(self, server, data):
	srvr = HTMLgen.Bold(HTMLgen.Font(server, size="+1"))
	# change the color of the first column if the server has timed out
	if data[0] == "alive":
	    tr = HTMLgen.TR(HTMLgen.TD(srvr))
	else:
	    tr = HTMLgen.TR(HTMLgen.TD(srvr, bgcolor=TIMED_OUT_COLOR))
	for datum in data:
	    tr.append(HTMLgen.TD(datum))
	if len(data) == 4:
	    # there was no last_alive time so fill in
	    tr.append(self.empty_data())
	return tr

    # add in the information for the generic servers. these only have alive
    # information
    def generic_server_rows(self, table):
	for server in GENERIC_SERVERS:
	    if self.data_dict.has_key(server):
		# output its information
		table.append(self.alive_row(server, 
			       self.data_dict[server][enstore_status.STATUS]))

    # create the suspect volume row - it is a separate table
    def suspect_volume_row(self, lm):
	tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Suspect Volumes", 
						color=BRICKRED)))
	tr.append(HTMLgen.TD(string.join(self.data_dict[lm][enstore_status.SUSPECT_VOLS],
					 ", "), align="LEFT", colspan=4))
	return tr

    # add the known movers to the table
    def known_mover_rows(self, table, lm):
	tr = HTMLgen.TR()
	for header in ["Known Movers", "Port", "State", "Last Summoned",
		       "Try Count"]:
	    tr.append(HTMLgen.TD(HTMLgen.Font(header, color=BRICKRED)))
	table.append(tr)
	kmovers = self.data_dict[lm][enstore_status.KNOWN_MOVERS]
	for kmover in kmovers:
	    tr = HTMLgen.TR(HTMLgen.TD(kmover[0]))
	    tr.append(HTMLgen.TD(kmover[1]))
	    tr.append(HTMLgen.TD(kmover[2]))
	    tr.append(HTMLgen.TD(kmover[3]))
	    tr.append(HTMLgen.TD(kmover[4]))
	    table.append(tr)

    # put together the rows for either lm queue
    def lm_queue_rows(self, lm, queue, intro):
	for qelem in self.data_dict[lm][queue]:
	    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(intro, color=BRICKRED)))
	    if qelem.has_key(enstore_status.MOVER):
		tr.append(HTMLgen.TD(qelem[enstore_status.MOVER]))
	    else:
		tr.append(self.empty_data())
	    tr.append(HTMLgen.TD(HTMLgen.Font("Node", color=BRICKRED)))
	    tr.append(HTMLgen.TD(qelem[enstore_status.NODE]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Port", color=BRICKRED)))
	    tr.append(HTMLgen.TD(qelem[enstore_status.PORT]))
	    table = HTMLgen.TableLite(tr, cellpadding=0, cellspacing=0, 
				      align="LEFT", bgcolor=YELLOW, width="100%")
	    if qelem.has_key(enstore_status.DEVICE):
		tr = HTMLgen.TR(self.spacer_data("Device Label"))
		tr.append(HTMLgen.TD(qelem[enstore_status.DEVICE]))
	    else:
		tr = HTMLgen.TR(self.empty_data())
		tr.append(self.empty_data())
	    tr.append(HTMLgen.TD(HTMLgen.Font("File Family", color=BRICKRED)))
	    tr.append(HTMLgen.TD(qelem[enstore_status.FILE_FAMILY]))
	    if qelem.has_key(enstore_status.FILE_FAMILY_WIDTH):
		tr.append(HTMLgen.TD(HTMLgen.Font("File Family Width",
						  color=BRICKRED)))
		tr.append(HTMLgen.TD(qelem[enstore_status.FILE_FAMILY_WIDTH]))
	    else:
		tr.append(self.empty_data())
		tr.append(self.empty_data())
	    table.append(tr)
	    tr = HTMLgen.TR(self.spacer_data("Job Submitted"))
	    tr.append(HTMLgen.TD(qelem[enstore_status.SUBMITTED]))
	    if qelem.has_key(enstore_status.DEQUEUED):
		tr.append(HTMLgen.TD(HTMLgen.Font("Dequeued", color=BRICKRED)))
		tr.append(HTMLgen.TD(qelem[enstore_status.DEQUEUED]))
	    else:
		tr.append(self.empty_data())
		tr.append(self.empty_data())
	    if qelem.has_key(enstore_status.MODIFICATION):
		tr.append(HTMLgen.TD(HTMLgen.Font("File Modified",
						  color=BRICKRED)))
		tr.append(HTMLgen.TD(qelem[enstore_status.MODIFICATION]))
	    else:
		tr.append(self.empty_data())
		tr.append(self.empty_data())
	    table.append(tr)
	    tr = HTMLgen.TR(self.spacer_data("Priorities"))
	    tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Current",
							color=BRICKRED), 
					   NBSP*3, qelem[enstore_status.CURRENT]),
				 html_escape='OFF'))
	    tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Base",
							color=BRICKRED),
					   NBSP*3, qelem[enstore_status.BASE]),
				 html_escape='OFF'))
	    tr.append(HTMLgen.TD("%s%s%s"%(HTMLgen.Font("Delta",
							color=BRICKRED),
					   NBSP*3, qelem[enstore_status.DELTA]),
				 html_escape='OFF'))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Agetime", color=BRICKRED)))
	    tr.append(HTMLgen.TD(qelem[enstore_status.AGETIME]))
	    table.append(tr)
	    if qelem[enstore_status.WORK] == enstore_status.READ:
		tr = HTMLgen.TR(self.spacer_data("Read from"))
	    else:
		tr = HTMLgen.TR(self.spacer_data("Write to"))
	    tr.append(HTMLgen.TD(qelem[enstore_status.FILE], colspan=3))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Bytes", color=BRICKRED)))
	    tr.append(HTMLgen.TD(qelem[enstore_status.BYTES]))
	    table.append(tr)
	    tr = HTMLgen.TR(self.spacer_data("ID"))
	    tr.append(HTMLgen.TD(qelem[enstore_status.ID], colspan=5))
	    table.append(tr)
	    if qelem.has_key(enstore_status.REJECT_REASON):
		tr = HTMLgen.TR(self.spacer_data("Reason for Pending"))
		tr.append(HTMLgen.TD(qelem[enstore_status.REJECT_REASON], colspan=5))
		table.append(tr)
	    table.append(self.empty_row(6))
	return HTMLgen.TR(HTMLgen.TD(table, colspan=5))

    # add the work at movers info to the table
    def work_at_movers_row(self, lm, cols):
	# These are the keys used in work at movers queue
	# WORK, NODE, PORT, FILE, FILE_FAMILY, FILE_FAMILY_WIDTH, SUBMITTED,
	# DEQUEUED, MODIFICATION, CURRENT, BASE, DELTA, AGETIME, FILE, BYTES, 
	# ID
	if self.data_dict[lm].has_key(enstore_status.WORK) and \
	   not self.data_dict[lm][enstore_status.WORK] == \
	       enstore_status.NO_WORK:
	    row = self.lm_queue_rows(lm, enstore_status.WORK, "Work for")
	else:
	    row = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(enstore_status.NO_WORK,
						     color=BRICKRED), 
					colspan=cols))
	return row

    # add the pending work row to the table
    def pending_work_row(self, lm, cols):
	# These are the keys used in pending work
	# NODE, PORT, FILE, FILE_FAMILY, FILE_FAMILY_WIDTH, SUBMITTED,
	# CURRENT, BASE, DELTA, AGETIME, FILE, BYTES, ID
	if self.data_dict[lm].has_key(enstore_status.PENDING) and \
	   not self.data_dict[lm][enstore_status.PENDING] == \
	       enstore_status.NO_PENDING:
	    row = self.lm_queue_rows(lm, enstore_status.PENDING,
				     "Pending Work")
	else:
	    row = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(enstore_status.NO_PENDING,
						     color=BRICKRED),
					colspan=cols))
	return row

    # output the state of the lirary manager
    def lm_state_row(self, lm):
	row = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("State", color=BRICKRED)))
	row.append(HTMLgen.TD(self.data_dict[lm][enstore_status.LMSTATE], 
			      colspan=4, align="LEFT"))
	return row

    # output the information for a library manager
    def lm_rows(self, lm, table):
	cols = 5
	# first the alive information
	table.append(self.alive_row(HTMLgen.Name(lm, lm), 
				    self.data_dict[lm][enstore_status.STATUS]))
	# the rest of the lm information is in a separate table, it starts
	# with the suspect volume info
	lm_table = HTMLgen.TableLite(self.lm_state_row(lm), cellpadding=0, 
				     cellspacing=0, align="LEFT", 
				     bgcolor=YELLOW, width="100%")
	lm_table.append(self.suspect_volume_row(lm))
	lm_table.append(self.null_row(cols))
	self.known_mover_rows(lm_table, lm)
	lm_table.append(self.empty_row(cols))
	lm_table.append(self.work_at_movers_row(lm, cols))
	lm_table.append(self.pending_work_row(lm, cols))
	tr = HTMLgen.TR(self.empty_data())
	tr.append(HTMLgen.TD(lm_table, colspan=5))
	table.append(tr)

    # add the volume information if it exists
    def add_bytes_volume_info(self, moverd, tr, mvkey):
	if moverd.has_key(enstore_status.VOLUME):
	    tr.append(HTMLgen.TD(moverd[mvkey]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Volume", color=BRICKRED),
				 align="CENTER"))
	    tr.append(HTMLgen.TD(moverd[enstore_status.VOLUME]))
	else:
	    tr.append(HTMLgen.TD(moverd[mvkey], colspan=3))

    # add the eod/location cookie information if it exists
    def add_bytes_eod_info(self, moverd, tr, mvkey):
	if moverd.has_key(enstore_status.EOD_COOKIE):
	    tr.append(HTMLgen.TD(moverd[mvkey]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("EOD Cookie", color=BRICKRED),
				 align="CENTER"))
	    tr.append(HTMLgen.TD(moverd[enstore_status.EOD_COOKIE]))
	elif moverd.has_key(enstore_status.LOCATION_COOKIE):
	    tr.append(HTMLgen.TD(moverd[mvkey]))
	    tr.append(HTMLgen.TD(HTMLgen.Font("Location Cookie", 
					      color=BRICKRED),
				 align="CENTER"))
	    tr.append(HTMLgen.TD(moverd[enstore_status.LOCATION_COOKIE]))
	else:
	    tr.append(HTMLgen.TD(moverd[mvkey], colspan=3))

    # add input and output files 
    def add_files(self, moverd, table):
	if moverd.has_key(enstore_status.FILES):
	    tr = HTMLgen.TR(HTMLgen.TD(moverd[enstore_status.FILES], colspan=4, 
				       align="CENTER"))
	    # we need to make the table able to hold a long file name
	    table.width = "100%"
	    table.append(self.empty_row(4))
	    table.append(tr)
	    table.append(self.empty_row(4))
	else:
	    table.width = "40%"

    # add the mover information for this library manager
    def mv_rows(self, lm, table):
	movers = self.data_dict[lm][enstore_status.MOVERS]
	for mover in movers:
	    moverd = self.data_dict[mover]
	    # mark this mover as not being an orphan, so we can easily find the
	    # orphans later
	    moverd[enstore_status.FOUND_LM] = 1
	    # check if this mover has been ipdated yet, we may not have gotten to it
	    # yet, as we are really processing the library manager info.  if there is
	    # no STATUs key, then we have no info on this mover yet.
	    table.append(self.alive_row(mover, moverd[enstore_status.STATUS]))

	    # we do not have any of this info if the mover has timed out and was
	    # never alive during this incarnation of the inquisitor.  if the last
	    # time alive is ------ , then it was never alive.
	    if len(moverd[enstore_status.STATUS]) == 4 or \
	       len(moverd[enstore_status.STATUS]) == 5 and \
	       not moverd[enstore_status.STATUS][4] == enstore_status.NO_INFO:
		tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Completed Transfers",
							color=BRICKRED)))
		if not moverd.has_key(enstore_status.COMPLETED):
		    print moverd
		tr.append(HTMLgen.TD(moverd[enstore_status.COMPLETED], colspan=3, 
				     align="LEFT"))
		mv_table = HTMLgen.TableLite(tr, cellspacing=0, cellpadding=0,
					     align="LEFT", bgcolor=YELLOW)
		tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Current State",
							color=BRICKRED)))
		tr.append(HTMLgen.TD(moverd[enstore_status.STATE], colspan=3, 
				     align="LEFT"))
		mv_table.append(tr)
		mv_table.append(self.empty_row(4))
		if moverd.has_key(enstore_status.LAST_READ):
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Last Read (bytes)",
							    color=BRICKRED),
					       align="CENTER"))
		    self.add_bytes_volume_info(moverd, tr, 
					       enstore_status.LAST_READ)
		    mv_table.append(tr)
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Last Write (bytes)",
						      color=BRICKRED),
					       align="CENTER"))
		    self.add_bytes_eod_info(moverd, tr, enstore_status.LAST_WRITE)
		    mv_table.append(tr)
		    self.add_files(moverd, mv_table)
		elif moverd.has_key(enstore_status.CUR_READ):
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Current Read (bytes)",
							    color=BRICKRED),
					       align="CENTER"))
		    self.add_bytes_volume_info(moverd, tr, enstore_status.CUR_READ)
		    mv_table.append(tr)
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font("Current Write (bytes)",
						      color=BRICKRED),
					 align="CENTER"))
		    self.add_bytes_eod_info(moverd, tr, enstore_status.CUR_WRITE)
		    mv_table.append(tr)
		    self.add_files(moverd, mv_table)
		tr = HTMLgen.TR(self.empty_data())
		tr.append(HTMLgen.TD(mv_table, colspan=5))
		table.append(tr)

    # output all of the library manager rows and their associated movers
    def lm_mv_rows(self, table):
	skeys = self.data_dict.keys()
	skeys.sort()
	for server in skeys:
	    if is_library_manager(server):
		# this is a library manager. output all of its info and then
		# info for each of its movers
		# only output other things if the library manager ia alive
		if self.data_dict[server][enstore_status.STATUS][0] == "alive":
		    self.lm_rows(server, table)
		    self.mv_rows(server, table)
		else:
		    # only need timedout status
		    table.append(self.alive_row(server, 
				 self.data_dict[server][enstore_status.STATUS]))

    # output all of the media changer rows 
    def media_changer_rows(self, table):
	skeys = self.data_dict.keys()
	skeys.sort()
	first_time = 1
	for server in skeys:
	    if is_media_changer(server):
		# this is a media changer. output its alive info
		if first_time:
		    table.append(self.alive_row(HTMLgen.Name(MEDIA_CHANGER, server), 
				self.data_dict[server][enstore_status.STATUS]))
		else:
		    table.append(self.alive_row(server, 
				self.data_dict[server][enstore_status.STATUS]))

    # output all of the orphan mover rows 
    def orphan_mover_rows(self, table):
	skeys = self.data_dict.keys()
	skeys.sort()
	first_time = 1
	for server in skeys:
	    # look for movers that are not listed with a library manager
	    if is_mover(server) and not \
	       self.data_dict[server].has_key(enstore_status.FOUND_LM):
		# this is an orphan mover. output its alive info
		if first_time:
		    table.append(self.alive_row(HTMLgen.Name(server, server), 
				self.data_dict[server][enstore_status.STATUS]))
		else:
		    table.append(self.alive_row(server, 
				self.data_dict[server][enstore_status.STATUS]))

    # output the table containing the blocksizes
    def blocksize_row(self, table):
	tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(\
	     HTMLgen.Name("blocksizes", "Blocksizes")), size="+1")))
	blsizes = self.data_dict[enstore_status.BLOCKSIZES]
	blkeys = blsizes.keys()
	blkeys.sort()
	bl_table = HTMLgen.TableLite(cellpadding=0, cellspacing=0, align="LEFT", 
				     bgcolor=YELLOW, width="40%")
	for bl in blkeys:
	    trb = HTMLgen.TR(HTMLgen.TD(bl))
	    trb.append(HTMLgen.TD(blsizes[bl]))
	    bl_table.append(trb)
	tr.append(HTMLgen.TD(bl_table, colspan=5))
	table.append(tr)

    # generate the main table with all of the information
    def main_table(self):
	# first create the table headings for each column
	tr = HTMLgen.TR()
	for hdr in ["Name", "Status", "Host", "Port", "Date/Time", "Last Time Alive"]:
	    tr.append(self.make_th(hdr))
	table = HTMLgen.TableLite(tr, align="CENTER", cellpadding=0,
				  cellspacing=0, bgcolor=AQUA, width="100%")
	self.generic_server_rows(table)
	# now output all of the library manager rows and mover rows
	self.lm_mv_rows(table)
	self.media_changer_rows(table)
	self.orphan_mover_rows(table)
	self.blocksize_row(table)
	return table

    # generate the body of the file
    def body(self, data_dict):
	# this is the data we will output
	self.data_dict = data_dict
	# create the outer table and its rows
	table = self.table_top()
	table.append(HTMLgen.TR(HTMLgen.TD(self.shortcut_table())))
	table.append(self.empty_row())
	table.append(self.empty_row())
	td = HTMLgen.TD(HTMLgen.Name("servers"))
	td.append(self.main_table())
	table.append(HTMLgen.TR(td))
	self.append(table)

class EnEncpStatusPage(EnBaseHtmlDoc):

    def __init__(self, refresh = 120):
	EnBaseHtmlDoc.__init__(self, refresh)
	self.title = "ENSTORE Encp History"
	self.script_title_gif = "encph.gif"
	self.description = "%s%sHistory of the most recent Encp commands. This page is created by the Inquisitor and periodically updated."%(NBSP, NBSP)

    # create the body of the page. the data is a list of lists.  each outer list element
    # is a list of the encp data i.e. - 
    #        [["10:43:49", "d0ensrv1.fnal.gov", "bakken", "55,255", "samnull-1", "3.5", "0.035"]]
    def body(self, data_list):
	table = self.table_top()
	# now create the table with the data in it, first do the row with
	# the headings
	tr = HTMLgen.TR(valign="CENTER")
	for hding in ["Time", "Node", "User", "Bytes", "Volume",
		      "Data Transfer Rate (MB/S)", "User Rate (MB/S)"]:
	    tr.append(self.make_th(hding))
	en_table = HTMLgen.TableLite(tr, border=1, bgcolor=AQUA, width="100%",
				     cols=7, cellspacing=5, cellpadding=CELLP,
				     align="CENTER")
	for row in data_list:
	    tr = HTMLgen.TR(HTMLgen.TD(row[0]))
	    for item in row[1:]:
		tr.append(HTMLgen.TD(item))
	    en_table.append(tr)
	table.append(HTMLgen.TR(HTMLgen.TD(en_table)))
	self.append(table)							 

class EnConfigurationPage(EnBaseHtmlDoc):

    def __init__(self, refresh = 600):
	EnBaseHtmlDoc.__init__(self, refresh)
	self.title = "ENSTORE Configuration"
	self.script_title_gif = "en_cfg.gif"
	self.description = "%s%sCurrent Enstore Configuration. This page is created by the Inquisitor and periodically updated."%(NBSP, NBSP)

    # create the body of the page. the incoming data is a python dictionary
    def body(self, data_dict):
	table = self.table_top()
	# now add the table with the information
	cfg_table = HTMLgen.TableLite(border=1, bgcolor=AQUA, 
				      cellspacing=5, cellpadding=CELLP)
	tr = HTMLgen.TR()
	for hding in ["Server", "Element", "Value"]:
	    tr.append(self.make_th(hding))
	cfg_table.append(tr)
	dkeys = data_dict.keys()
	dkeys.sort()
	for server in dkeys:
	    server_dict = data_dict[server]
	    server_keys = server_dict.keys()
	    server_keys.sort()
	    first_line = 1
	    for server_key in server_keys:
		if first_line:
		    tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(server), size="+1")))
		    first_line = 0
		else:
		    tr = HTMLgen.TR(self.empty_data())
		tr.append(HTMLgen.TD(server_key))
		tr.append(HTMLgen.TD(server_dict[server_key]))
		cfg_table.append(tr)
		
	# add this table to the main one
	table.append(HTMLgen.TR(HTMLgen.TD(cfg_table)))
	self.append(table)							 

class EnMiscPage(EnBaseHtmlDoc):

    def __init__(self, refresh = 600):
	EnBaseHtmlDoc.__init__(self, refresh)
	self.title = "ENSTORE Miscellany"
	self.script_title_gif = "en_misc.gif"
	self.description = "%s%sMiscellaneous Enstore information specified by the user (in the configuration file) for inclusion here. This page is created by the Inquisitor and periodically updated."%(NBSP, NBSP)

    # create the body of the page, the incoming data is a list of strings
    def body(self, data_list):
	table = self.table_top()
	# now the data
	data_list.sort()
	for item in data_list:
	    table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Font(HTMLgen.Href(item, item), size="+2"))))
	self.append(table)

class EnLogPage(EnBaseHtmlDoc):

    def __init__(self, refresh = 600):
	EnBaseHtmlDoc.__init__(self, refresh)
	self.title = "ENSTORE Log Files"
	self.script_title_gif = "en_log.gif"
	self.description = "%s%sThis is a list of the existing Enstore log files. Additionally, user specified log files are included at the top. This page is created by the Inquisitor and periodically updated. Enstore log files may be %s"%(NBSP, NBSP, str(HTMLgen.Href('enstore_log_file_search.html', 'searched')))

    # given a dict of log files, create a list of lists which divides the log files up by month and
    # year. most recent goes first
    def find_months(self, logs):
	lkeys = logs.keys()
	lkeys.sort()
	lkeys.reverse()
	log_months = {}
	dates = []
	for log in lkeys:
	    (prefix, year, month, day) = string.split(log, "-")
	    month = string.atoi(month)
	    date = "%s %s"%(calendar.month_name[month], year)
	    year = string.atoi(year)
	    day = string.atoi(day)
	    if not dates or not date == dates[len(dates)-1][0]:
		dates.append((date, year, month))
	    if not log_months.has_key(date):
		log_months[date] = {}
	    log_months[date][day] = (logs[log], log)
	return (dates, log_months)

    # create the body of the page, where http_path is the web server path to the files, www_host is
    # the host where the web server is running, user_logs is a dictionary that contains
    # user logs and logs is a dictionary where the log file names are the keys and the sizes are the
    # values.
    def body(self, http_path, logs, user_logs, www_host):
	table = self.table_top()
	# now add the data, first the table with the user specified log files in it
	log_table = HTMLgen.TableLite(HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold(\
	             "User Specified Log Files"), size="+2", color=BRICKRED)),
				      width="50%", cellspacing=5, cellpadding=CELLP, 
				      align="LEFT")
	ul_keys = user_logs.keys()
	ul_keys.sort()
	for ul_key in ul_keys:
	    log_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href(user_logs[ul_key], 
							       str(HTMLgen.Bold(ul_key))))))
	table.append(HTMLgen.TR(HTMLgen.TD(log_table)))
	log_table.append(self.empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.HR())))
	table.append(self.empty_row())
	table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Href("enstore_log_file_search.html",
							"Search the Enstore Log Files"))))
	table.append(self.empty_row())
	# now create the tables for the different months.
	(dates, sizes) = self.find_months(logs)
	did_title = 0
	for (date, year, month) in dates:
	    caption = HTMLgen.Caption(HTMLgen.Font(HTMLgen.Bold(date), 
						   size="+2", color=BRICKRED))
	    if not did_title:
		did_title = 1
		caption.prepend(HTMLgen.BR())
		caption.prepend(HTMLgen.BR())
		caption.prepend(HTMLgen.Font(HTMLgen.Bold("Enstore Log Files"), 
					     size="+2", color=BRICKRED))
	    log_table =  HTMLgen.TableLite(caption, bgcolor=AQUA, cellspacing=5, 
					   cellpadding=CELLP, align="LEFT", border=2)
	    tr = HTMLgen.TR()
	    for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
		tr.append(HTMLgen.TD(HTMLgen.Font(HTMLgen.Bold(day),size="+1", color=BRICKRED)))
	    log_table.append(tr)
	    # the following generates a  list of lists, which specifies how to draw a calendar with
	    # the first of the month occuring in the correct day of the week slot.
	    mweeks = calendar.monthcalendar(year, month)
	    for mweek in mweeks:
		tr = HTMLgen.TR()
		for day in [0,1,2,3,4,5,6]:
		    if mweek[day] == 0:
			# this is null entry represented by a blank entry on the calendar
			tr.append(HTMLgen.TD(self.empty_data()))
		    else:
			(size, log) = sizes[date].get(mweek[day], (-1, ""))
			if size == -1:
			    # there was no log file for this day
			    tr.append(HTMLgen.TD(HTMLgen.Bold(mweek[day]), bgcolor=YELLOW))
			else:
			    td = HTMLgen.TD(HTMLgen.Href("%s%s/%s"%(www_host, http_path,
									 log), 
							 HTMLgen.Font(HTMLgen.Bold(mweek[day]),
								      size="+2")))
			    td.append(" : %s"%(size,))
			    tr.append(td)
		log_table.append(tr)
	    table.append(HTMLgen.TR(HTMLgen.TD(log_table)))
	    table.append(self.empty_row())
	self.append(table)							 

class EnAlarmPage(EnBaseHtmlDoc):

    def __init__(self, refresh=600):
	EnBaseHtmlDoc.__init__(self, refresh)
	self.title = "ENSTORE Alarms"
	self.script_title_gif = "en_alarms.gif"
	self.description = "List of the currently raised alarms.  This page is created by the Alarm Server."

    def alarm_table(self, alarms):
	tr = HTMLgen.TR()
	for hdr in ["Key", "Time", "Node", "PID", "User", "Severity", 
		    "Process", "Error", "Additional Information"]:
	    tr.append(HTMLgen.TH(HTMLgen.Font(hdr, size="+2", color=BRICKRED)))
	table = HTMLgen.TableLite(tr, width="100%", border=1, cellspacing=5, 
				  cellpadding=CELLP, align="LEFT", bgcolor=AQUA)
	i = 0
	akeys = alarms.keys()
	akeys.sort()
	for akey in akeys:
	    alarm = alarms[akey].list_alarm()
	    td = HTMLgen.TD(HTMLgen.Input(type="radio", name="alarm%s"%(i,),
						     value=alarm[0]),
			    html_escape='OFF')
	    td.append("%s%s"%(NBSP*3, alarm[0]))
	    tr = HTMLgen.TR(td)
	    tr.append(HTMLgen.TD(enstore_status.format_time(float(alarm[0]))))
	    for item in alarm[1:]:
		tr.append(HTMLgen.TD(item))
	    table.append(tr)
	    i = i + 1
	return table

    def body(self, alarms):
	table = self.table_top()
	# now the data
	exe = HTMLgen.TR(HTMLgen.TD(HTMLgen.Input(value="Execute", type="submit")))
	rst = HTMLgen.TR(HTMLgen.TD(HTMLgen.Input(value="Reset", type="reset")))
	form = HTMLgen.Form(onSubmit="http://rip8:/cgi-bin/enstore/enstore_alarm_cgi.py")
	# get rid of the default submit button, we will add our own below
	form.submit = ''
	form.append(HTMLgen.TR(HTMLgen.TD(self.alarm_table(alarms))))
	form.append(self.empty_row())
	form.append(self.empty_row())
	tr = HTMLgen.TR(HTMLgen.TD(HTMLgen.Input(value="Execute", 
						 type="submit")))
	tr.append(HTMLgen.TD(HTMLgen.Input(value="Reset", type="reset")))
	tr.append(HTMLgen.TD("Alarms may be cancelled by selecting the alarm(s), pressing the %s button and then reloading the page."%(str(HTMLgen.Bold("Execute")),), html_escape='OFF'))
	form.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.TableLite(tr, 
							    width="100%"))))
	table.append(form)
	self.append(table)
