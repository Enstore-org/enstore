import sys

import inquisitor_plots
import enstore_files
import enstore_constants
import enstore_functions
import udp_client
import generic_client
import option
import www_server
import e_errors
import Trace

MY_NAME = "Plotter"

class Plotter(inquisitor_plots.InquisitorPlots, generic_client.GenericClient):

    def __init__(self, csc, rcv_timeout, rcv_retry, logfile_dir, 
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
	    plot_file = "%s/%s"%(self.html_dir,
				 enstore_files.plot_html_file_name())

        self.system_tag = self.www_server.get(www_server.SYSTEM_TAG, 
                                              www_server.SYSTEM_TAG_DEFAULT)

        # these are the files to which we will write, they are html files
        self.plotfile = enstore_files.HTMLPlotFile(plot_file, 
                                                   self.system_tag)

class PlotterInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.alive_rcv_timeout = 5
        self.alive_retries = 1
	self.logfile_dir = None
	self.start_time = None
	self.stop_time = None
        self.media_changer = []
        self.keep = 0
        self.keep_dir = ""
        self.output_dir = None
	self.html_file = None
	self.encp = None
	self.mount = None
	self.sg = None
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)
        
    plotter_options = {
        option.ENCP:{option.HELP_STRING:"create the bytes transfered and " \
                     "transfer activity plots",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.USER,
                   },
        option.KEEP:{option.HELP_STRING:"keep all intermediate files " \
                     "generated in order to make the plots",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.USER,
                   },
        option.KEEP_DIR:{option.HELP_STRING:"location of log files is not " \
                        "in directory in config file",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"directory",
                        option.USER_LEVEL:option.USER,
                   },
        option.LOGFILE_DIR:{option.HELP_STRING:"location of log files is not" \
                            " in directory in config file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"directory",
                            option.USER_LEVEL:option.USER,
                   },
        option.MOUNT:{option.HELP_STRING:"create the bytes mounts/day and " \
                      "mount latency plots",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_USAGE:option.IGNORED,
                      option.USER_LEVEL:option.USER,
                      },
        option.OUTPUT_DIR:{option.HELP_STRING:"directory in which to store " \
                           "the output plot files",
                           option.VALUE_TYPE:option.STRING,
                           option.VALUE_USAGE:option.REQUIRED,
                           option.VALUE_LABEL:"directory",
                           option.USER_LEVEL:option.USER,
                           },
        option.SG:{option.HELP_STRING:"create the storage group plot",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_USAGE:option.IGNORED,
                   option.USER_LEVEL:option.USER,
                   },
        option.START_TIME:{option.HELP_STRING:"date/time at which to " \
                           "start each specified plot",
                           option.VALUE_TYPE:option.STRING,
                           option.VALUE_USAGE:option.REQUIRED,
                           option.VALUE_LABEL:"YYYY-MM-DD-HH:MM:SS",
                           option.USER_LEVEL:option.USER,
                           },
        option.STOP_TIME:{option.HELP_STRING:"date/time at which to " \
                           "stop each specified plot",
                           option.VALUE_TYPE:option.STRING,
                           option.VALUE_USAGE:option.REQUIRED,
                           option.VALUE_LABEL:"YYYY-MM-DD-HH:MM:SS",
                           option.USER_LEVEL:option.USER,
                           },
        }
    
    def valid_dictionaries(self):
        return (self.plotter_options, self.help_options, self.trace_options)


if __name__ == "__main__":
    Trace.trace(1, "plotter called with args %s"%(sys.argv,))

    # get interface
    intf = PlotterInterface(user_mode=0)

    # get the plotter
    plotter = Plotter((intf.config_host, intf.config_port), 
		      intf.alive_rcv_timeout, intf.alive_retries,
		      intf.logfile_dir, intf.start_time, 
		      intf.stop_time, intf.media_changer, intf.keep, 
		      intf.keep_dir, intf.output_dir, intf.html_file)

    if plotter.startup_state == e_errors.TIMEDOUT:
        Trace.trace(1, 
                    "Plotter TIMED OUT when contacting %s"%(plotter.startup_text,))
    else:
	plotter.plot(intf.encp, intf.mount, intf.sg)

    del plotter.csc.u
    del plotter.u     # del now, otherwise get name exception (just for python v1.5???)
