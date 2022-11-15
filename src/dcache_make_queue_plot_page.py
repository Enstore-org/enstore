import os
import sys
import string
import stat

import enstore_plots
import enstore_html
import enstore_files
import generic_client
import enstore_make_plot_page

TMP = ".tmp"

class QueuePlotPage(enstore_html.EnPlotPage):

    def __init__(self, title, gif, description, url):
        self.url = url
        enstore_html.EnPlotPage.__init__(self, title, gif, description)
	self.source_server = "DCache"
        self.do_nav_table = 0
	self.help_file = ""

    def find_label(self, text):
        l = len(self.url)
        # get rid of the .jpg ending and the url at the beginning and _stamp
	text_lcl = text[l:-10]
        text_lcl = string.replace(text_lcl, "/", "")
	return "%s<BR>"%(text_lcl,)

class PlotPageInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, args=sys.argv, user_mode=1):
	# fill in the defaults for the possible options
        self.do_parse = flag
	self.description = "Graphical representation of the queues in the disk cache."
	self.title = "DCache Queue Plots"
	self.title_gif = "dc_queue_plots.jpg"
	self.dir = "/fnal/ups/prd/www_pages/dcache"
	self.input_dir = "%s/queue"%(self.dir,)
	self.html_file = "%s/dc_queue_plots.html"%(self.dir,)
        self.url = "queue"
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def options(self):
	return self.help_options() +\
	       ["input_dir=", "description=", "title=",
		"html_file=", "title_gif=", "url="]

def do_work(intf):
    # this is where the work is really done
    # get the list of stamps and jpg files
    (jpgs, stamps, pss) = enstore_make_plot_page.do_the_walk(intf.input_dir, intf.url)
    html_page = QueuePlotPage(intf.title, intf.title_gif, intf.description,
                              intf.url)
    html_page.body(jpgs, stamps, pss)
    # open the temporary html file and output the html text to it
    tmp_html_file = "%s%s"%(intf.html_file, TMP)
    html_file = enstore_files.EnFile(tmp_html_file)
    html_file.open()
    html_file.write(html_page)
    html_file.close()
    os.rename(tmp_html_file, intf.html_file)

if __name__ == "__main__":   # pragma: no cover

    intf = PlotPageInterface(user_mode=0)

    do_work(intf)
