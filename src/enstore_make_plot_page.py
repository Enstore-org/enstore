import enstore_plots.py
import generic_client

class PlotHtmlPage(EnBaseHtmlDoc):

    def __init__(self, title, gif, description):
	EnBaseHtmlDoc.__init__(self, refresh=0)
	self.title = title
	self.script_title_gif = gif
	self.description = description

    def find_ps_file(self, jpg_file, pss):
	# see if there is a corresponding ps file
	ps = 0
	ps_file = ""
	if pss:
	    ps_file = string.replace(jpg_file, enstore_constants.JPG,
				     enstore_constants.PS)
	    if ps_file in pss:
		# found it
		pss.remove(ps_file)
		ps = 1
	return (ps, ps_file)

    def add_stamp(self, jpgs, stamps, pss, trs, trps):
	# for each stamp add it to the row.  if there is a corresponding large
	# jpg file, add it as a link from the stamp.  also see if there is a
	# postscript file associated with it and add it in the following row.
	# if there is no postscript file, then put a message saying this.
	if stamps:
	    stamp = stamps.pop(0)
	    jpg_file = string.replace(stamp, enstore_constants.STAMP, "")
	    # see if there is a corresponding jpg file
	    url = 0
	    if jpgs:
		if jpg_file in jpgs:
		    # found it
		    jpgs.remove(jpg_file)
		    url = 1
	    # see if there is a corresponding ps file
	    (ps, ps_file) = self.find_ps_file(jpg_file, pss)
	    if url:
		# we have a jpg file associated with this stamp file
		td = HTMLgen.TD(HTMLgen.Href(jpg_file, HTMLgen.Image(stamp)))
	    else:
		td = HTMLgen.TD(HTMLgen.Image(stamp))
	    trs.append(td)
	    label = find_label(stamp)
	    td = HTMLgen.TD("%s%s"%(label, NBSP*2) , html_escape='OFF')
	    if ps:
		# we have a corresponding ps file
		td.append(HTMLgen.Href(ps_file, POSTSCRIPT))
	    trps.append(td)

    def add_leftover_jpgs(self, table, jpgs, pss):
	while jpgs:
	    tr = HTMLgen.TR()
	    for i in [1, 2, 3]:
		if jpgs:
		    jpg_file = jpgs.pop(0)
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
		    ps_file = pss.pop(0)
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
	    for i in [1, 2, 3]:
	        self.add_stamp(jpgs, stamps, pss, trs, trps)
	    else:
                plot_table.append(trs)
                plot_table.append(trps)
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


class PlotPageInterface(generic_client.GenericClientInterface):

    def __init__(self, flags=1, opts=[]):
	# fill in the defaults for the possible options
	self.description = "Graphical representation of the exit status of Enstore cron jobs."
	self.title = "Enstore Cron Processes Output"
	self.title_gif = "en_cron_pics.gif"
	self.dir = "/fnal/ups/prd/www_pages/enstore"
	self.input_dir = "%s/CRON"%(self.dir,)
	self.output_dir = self.input_dir
	self.html_file = "%s/cron_pics.html"%(self.dir,)
	generic_client.GenericClientInterface.__init__(self)

    def options(self):
	return self.help_options() +\
	       ["input_dir=", "description=", "title=", "output_dir=",
		"html_file=", "title_gif="]


def do_work(intf):
    html_file = PlotHtmlPage(intf.title, intf.title_gif, intf.description)
    jpg_files = JPGFiles(intf.
    intf.html_file.open()
    # get the list of stamps and jpg files
    (jpgs, stamps, pss) = enstore_plots.find_jpg_files(self.html_dir)
    self.htmlfile.write(jpgs, stamps, pss)
    self.htmlfile.close()
    self.move_file(1, self.plothtmlfile_orig)


if __name__ == "__main__" :

    intf = PlotPageInterface()

    do_work(intf)
