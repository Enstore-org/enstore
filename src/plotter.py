import sys

import inquisitor_plots
import enstore_files
import enstore_constants
import enstore_functions
import udp_client
import generic_client
import www_server
import e_errors
import Trace

DEFAULT_REFRESH = 1000
MY_NAME = "Plotter"

class Plotter(inquisitor_plots.InquisitorPlots, generic_client.GenericClient):

    def __init__(self, csc, rcv_timeout, rcv_retry, refresh, logfile_dir, 
		 start_time, stop_time, media_changer, keep,
		 keep_dir, output_dir, html_file):
	# we need to get information from the configuration server
        generic_client.GenericClient.__init__(self, csc, MY_NAME)
        # we always need to be talking to our configuration server
        self.u = udp_client.UDPClient()

	self.logfile_dir = logfile_dir
	self.start_time = start_time
	self.stop_time = stop_time
	self.media_changer = media_changer
	self.keep = keep
	self.keep_dir = keep_dir
	self.output_dir = output_dir
        self.startup_state = e_errors.OK

        config_d = self.csc.dump(rcv_timeout, rcv_retry)
        if enstore_functions.is_timedout(config_d):
            Trace.trace(1, "plotter init - ERROR, getting config dict timed out")
            self.startup_state = e_errors.TIMEDOUT
            self.startup_text = enstore_constants.CONFIG_SERVER
            return
        self.config_d = config_d['dump']

	self.inq_d = self.config_d.get(enstore_constants.INQUISITOR, {})

        self.www_server = self.config_d.get(enstore_constants.WWW_SERVER, {})
        self.system_tag = self.www_server.get(www_server.SYSTEM_TAG, 
                                              www_server.SYSTEM_TAG_DEFAULT)

        # get the directory where the files we create will go.  this should
        # be in the configuration file.
        if html_file is None:
	    if self.inq_d.has_key("html_file"):
                self.html_dir = self.inq_d["html_file"]
                plot_file = "%s/%s"%(self.html_dir,
                                     enstore_files.plot_html_file_name())
            else:
                self.html_dir = enstore_files.default_dir
                plot_file = enstore_files.default_plot_html_file()
	else:
	    self.html_dir = html_file
	    #NOTE: this needs some work here

        # if no html refresh was entered on the command line, get it from
        # the configuration file.
        if refresh == -1:
            refresh = self.inquisitor.get("refresh", DEFAULT_REFRESH)

        self.system_tag = self.www_server.get(www_server.SYSTEM_TAG, 
                                              www_server.SYSTEM_TAG_DEFAULT)

        # these are the files to which we will write, they are html files
        self.plotfile = enstore_files.HTMLPlotFile(plot_file, 
                                                   self.system_tag)


class PlotterInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
        # fill in the defaults for the possible options
        self.do_parse = flag
        self.restricted_opts = opts
        self.alive_rcv_timeout = 5
        self.alive_retries = 1
	self.refresh = DEFAULT_REFRESH
	self.plot = 0
	self.logfile_dir = None
	self.start_time = None
	self.stop_time = None
        self.media_changer = []
        self.keep = 0
        self.keep_dir = ""
        self.output_dir = None
	self.html_file = None
        generic_client.GenericClientInterface.__init__(self)
        
    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options() +[
                "refresh=", "plot", "logfile-dir=",
                "start-time=", "stop-time=", "media-changer=", "keep",
                "keep-dir=", "output-dir="]

if __name__ == "__main__":
    Trace.trace(1, "plotter called with args %s"%(sys.argv,))

    # get interface
    intf = PlotterInterface()

    # get the plotter
    plotter = Plotter((intf.config_host, intf.config_port), 
		      intf.alive_rcv_timeout, intf.alive_retries,
		      intf.refresh, intf.logfile_dir, intf.start_time, 
		      intf.stop_time, intf.media_changer, intf.keep, 
		      intf.keep_dir, intf.output_dir, intf.html_file)

    if plotter.startup_state == e_errors.TIMEDOUT:
        Trace.trace(1, 
                    "Plotter TIMED OUT when contacting %s"%(plotter.startup_text,))
    else:
	plotter.plot()

    del plotter.csc.u
    del plotter.u     # del now, otherwise get name exception (just for python v1.5???)
