#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
import os

# enstore modules
import inquisitor_plots
import enstore_files
import enstore_constants
import enstore_functions
import enstore_functions2
import udp_client
import generic_client
import option
import www_server
import e_errors
import Trace
import configuration_client
import socket
import enstore_functions2

MY_NAME = "Plotter"
BURN_RATE = "burn-rate"
ENCP_RATE = "encp-rates"
QUOTAS = "quotas"
FILE_FAMILY_USAGE = "file_family_usage"

class Plotter(inquisitor_plots.InquisitorPlots, generic_client.GenericClient):

    def __init__(self, csc, rcv_timeout, rcv_retry, logfile_dir,
		 start_time, stop_time, media_changer, keep,
		 keep_dir, output_dir, html_file, mount_label=None,
		 pts_dir=None, pts_nodes=None, no_plot_html=None):
	# we need to get information from the configuration server
        generic_client.GenericClient.__init__(self, csc, MY_NAME)

	self.logfile_dir = logfile_dir
	self.start_time = start_time
	self.stop_time = stop_time
	self.media_changer = media_changer
	self.keep = keep
	self.keep_dir = keep_dir
	self.output_dir = output_dir
	self.mount_label = mount_label
	self.pts_dir = pts_dir
	self.pts_nodes = pts_nodes
	self.no_plot_html = no_plot_html
        self.startup_state = e_errors.OK
        self.acc_db = None

        config_d = self.csc.dump(rcv_timeout, rcv_retry)
        if e_errors.is_timedout(config_d):
            Trace.trace(enstore_constants.PLOTTING,
                        "plotter init - ERROR, getting config dict timed out")
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
	    pfile = enstore_files.plot_html_file_name()
	    plot_file = "%s/%s"%(self.html_dir, pfile)

	bpd_dir = enstore_functions2.get_bpd_subdir(self.html_dir)

        # these are the files to which we will write, they are html files
	self.plotfile_l = []
	links_to_add = []
        plotfile1 = enstore_files.HTMLPlotFile(plot_file, self.system_tag)
        # if the bpd_dir is the same as self.html_dir, then the page above
        # already contains these plots, so skip this in that case
	if not bpd_dir == self.html_dir:
	    plot_file = "%s/%s"%(bpd_dir, enstore_files.plot_html_file_name())
	    plotfile2 = enstore_files.HTMLPlotFile(plot_file,
						   self.system_tag, "../")
	    self.plotfile_l.append([plotfile2, bpd_dir])
	    links_to_add.append(("%s/%s"%(enstore_constants.BPD_SUBDIR,
					  enstore_files.plot_html_file_name()),
				 "Bytes/Day per Mover Plots"))
	# --------------------------------------------------
        # added by Dmitry, subdirectory displays encp rates per storage group
        # --------------------------------------------------
	dir = "%s/%s"%(self.html_dir, ENCP_RATE)
        if not os.access(dir, os.F_OK):
            os.makedirs(dir)
        os.system("cp ${ENSTORE_DIR}/etc/*.gif %s"%(dir))
	if os.path.isdir(dir):
            Trace.trace(enstore_constants.PLOTTING,
                    "adding links to encp rate plots")
            # there are plots here
            plot_file = "%s/%s"%(dir, enstore_files.plot_html_file_name())
            plotfile2 = enstore_files.HTMLPlotFile(plot_file,
						   self.system_tag, "../")
            self.plotfile_l.append([plotfile2, dir])
            links_to_add.append(("%s/%s"%(ENCP_RATE,
                                          enstore_files.plot_html_file_name()),
                                 "Encp rates per Storage Group Plots"))
	# --------------------------------------------------
        # added by Dmitry, subdirectory displays mover data
        # --------------------------------------------------
	dir = "%s/%s"%(self.html_dir, enstore_constants.MOVER_SUMMARY_PLOTS_SUBDIR)
        if not os.access(dir, os.F_OK):
            os.makedirs(dir)
        os.system("cp ${ENSTORE_DIR}/etc/*.gif %s"%(dir))
	if os.path.isdir(dir):
            Trace.trace(enstore_constants.PLOTTING,
                    "adding links to mover plots")
            # there are plots here
            plot_file = "%s/%s"%(dir, enstore_files.plot_html_file_name())
            plotfile2 = enstore_files.HTMLPlotFile(plot_file,
						   self.system_tag, "../")
            self.plotfile_l.append([plotfile2, dir])
            links_to_add.append(("%s/%s"%(enstore_constants.MOVER_SUMMARY_PLOTS_SUBDIR,
                                          enstore_files.plot_html_file_name()),
                                 "Mover Plots"))
	# --------------------------------------------------
        # added by Dmitry, subdirectory displays latency data
        # --------------------------------------------------
	dir = "%s/%s"%(self.html_dir, enstore_constants.MOUNT_LATENCY_SUMMARY_PLOTS_SUBDIR)
        if not os.access(dir, os.F_OK):
            os.makedirs(dir)
        os.system("cp ${ENSTORE_DIR}/etc/*.gif %s"%(dir))
	if os.path.isdir(dir):
            Trace.trace(enstore_constants.PLOTTING,
                    "adding links to mount latency plots")
            # there are plots here
            plot_file = "%s/%s"%(dir, enstore_files.plot_html_file_name())
            plotfile2 = enstore_files.HTMLPlotFile(plot_file,
						   self.system_tag, "../")
            self.plotfile_l.append([plotfile2, dir])
            links_to_add.append(("%s/%s"%(enstore_constants.MOUNT_LATENCY_SUMMARY_PLOTS_SUBDIR,
                                          enstore_files.plot_html_file_name()),
                                 "Mount Latency"))
        # --------------------------------------------------
	dir = "%s/%s"%(self.html_dir, FILE_FAMILY_USAGE)
        if not os.access(dir, os.F_OK):
            os.makedirs(dir)
        os.system("cp ${ENSTORE_DIR}/etc/*.gif %s"%(dir))
	if os.path.isdir(dir):
            Trace.trace(enstore_constants.PLOTTING,
                    "adding links to encp rate plots")
            # there are plots here
            plot_file = "%s/%s"%(dir, enstore_files.plot_html_file_name())
            plotfile2 = enstore_files.HTMLPlotFile(plot_file,
						   self.system_tag, "../")
            self.plotfile_l.append([plotfile2, dir])
            links_to_add.append(("%s/%s"%(FILE_FAMILY_USAGE,
                                          enstore_files.plot_html_file_name()),
                                 "Tape occupancies per Storage Group Plots"))
        # --------------------------------------------------
        dir = "%s/%s"%(self.html_dir, inquisitor_plots.XFER_SIZE)
        if not os.access(dir, os.F_OK):
            os.makedirs(dir)
        os.system("cp ${ENSTORE_DIR}/etc/*.gif %s"%(dir))
	if os.path.isdir(dir):
            Trace.trace(enstore_constants.PLOTTING,
                    "adding links to encp size plots")
            # there are plots here
            plot_file = "%s/%s"%(dir, enstore_files.plot_html_file_name())
            plotfile2 = enstore_files.HTMLPlotFile(plot_file,
						   self.system_tag, "../")
            self.plotfile_l.append([plotfile2, dir])
            links_to_add.append(("%s/%s"%(inquisitor_plots.XFER_SIZE,
                                          enstore_files.plot_html_file_name()),
                                 "Xfer size per Storage Group Plots"))
        # --------------------------------------------------
	dir = "%s/%s"%(self.html_dir, BURN_RATE)
        if not os.access(dir, os.F_OK):
            os.makedirs(dir)
        os.system("cp ${ENSTORE_DIR}/etc/*.gif %s"%(dir))
	if os.path.isdir(dir):
	    Trace.trace(enstore_constants.PLOTTING,
                        "adding links to burn rate plots")
            # there are plots here
	    plot_file = "%s/%s"%(dir, enstore_files.plot_html_file_name())
	    plotfile2 = enstore_files.HTMLPlotFile(plot_file,
						   self.system_tag, "../")
	    self.plotfile_l.append([plotfile2, dir])
	    links_to_add.append(("%s/%s"%(BURN_RATE,
					  enstore_files.plot_html_file_name()),
				 "Bytes Written per Storage Group Plots"))
        # --------------------------------------------------
        dir = "%s/%s"%(self.html_dir, QUOTAS)
        if not os.access(dir, os.F_OK):
            os.makedirs(dir)
        os.system("cp ${ENSTORE_DIR}/etc/*.gif %s"%(dir))
	if os.path.isdir(dir):
	    Trace.trace(enstore_constants.PLOTTING,
                        "adding links to quotas plots")
            # there are plots here
	    plot_file = "%s/%s"%(dir, enstore_files.plot_html_file_name())
	    plotfile2 = enstore_files.HTMLPlotFile(plot_file,
						   self.system_tag, "../")
	    self.plotfile_l.append([plotfile2, dir])
	    links_to_add.append(("%s/%s"%(QUOTAS,
					  enstore_files.plot_html_file_name()),
				 "Quota per Storage Group Plots"))
        # --------------------------------------------------
	# add any links that are specified in the config file
	extra_links = self.csc.get(enstore_constants.EXTRA_LINKS)
	if extra_links.has_key(enstore_constants.ENSTORE_PLOTS):
	    plot_links_d = extra_links[enstore_constants.ENSTORE_PLOTS]
	    links = plot_links_d.keys()
	    for link in links:
                Trace.trace(enstore_constants.PLOTTING,
                            "adding extra link from config file")
		links_to_add.append((link, plot_links_d[link]))
	# the first plotfile needs to have a link to the second one on it, if
	# the second one exists
	if not links_to_add:
	    # no link is required
	    self.plotfile_l.append((plotfile1, self.html_dir))
	else:
	    # we made the plotfile2 page(s), add a link to it on the 1st page
	    tmp_l = [plotfile1, self.html_dir] + links_to_add
	    self.plotfile_l.append(tmp_l)


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
	self.label = None
	self.sg = None
	self.total_bytes = None
	self.pts_dir = None
	self.pts_nodes = None
	self.no_plot_html = None
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
        option.PTS_DIR:{option.HELP_STRING:"location of file with history of bpd data points",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"directory",
                        option.USER_LEVEL:option.USER,
                   },
        option.PTS_NODES:{option.HELP_STRING:"nodes to get pts files from ",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_LABEL:"node1[,node2]...",
                        option.USER_LEVEL:option.USER,
                   },
        option.NO_PLOT_HTML:{option.HELP_STRING:"do not create the plot web page",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.USER,
                   },
        option.LOGFILE_DIR:{option.HELP_STRING:"location of log files is not" \
                            " in directory in config file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"directory",
                            option.USER_LEVEL:option.USER,
                   },
        option.MOUNT:{option.HELP_STRING:"create the mounts/day and " \
                      "mount latency plots",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_USAGE:option.IGNORED,
                      option.USER_LEVEL:option.USER,
                      },
        option.TOTAL_BYTES:{option.HELP_STRING:"create the total bytes/day for all systems ",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_USAGE:option.IGNORED,
                      option.USER_LEVEL:option.USER,
                      },
        option.LABEL:{option.HELP_STRING:"append this to mount plot titles ",
		      option.VALUE_TYPE:option.STRING,
		      option.VALUE_USAGE:option.REQUIRED,
		      option.VALUE_LABEL:"label",
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



def do_work(intf):

    if intf.do_print:
        Trace.do_print(intf.do_print)

    Trace.trace(enstore_constants.PLOTTING,
                "plotter called with args %s"%(sys.argv,))
    csc   =  configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                       enstore_functions2.default_port()))
    system_name = csc.get_enstore_system(timeout=1,retry=0)
    config_dict={}

    if system_name:
        config_dict = csc.dump(timeout=1, retry=3)
        config_dict = config_dict['dump']
    else:
        try:
            # I am not sure fixing this code is wise
            configfile = os.environ.get('ENSTORE_CONFIG_FILE')
            print "Failed to connect to config server, using configuration file %s"%(configfile,)
            f = open(configfile,'r')
            code = ''.join(f.readlines())
            configdict={}
            exec(code)
            config_dict=configdict
        except:
            pass
    inq_d = config_dict.get(enstore_constants.INQUISITOR, {})
    intf.output_dir = inq_d["html_file"]
    if not intf.pts_dir:
        intf.pts_dir = intf.output_dir

    # get the plotter
    plotter = Plotter((intf.config_host, intf.config_port),
		      intf.alive_rcv_timeout, intf.alive_retries,
		      intf.logfile_dir, intf.start_time,
		      intf.stop_time, intf.media_changer, intf.keep,
		      intf.keep_dir, intf.output_dir, intf.html_file,
		      intf.label, intf.pts_dir, intf.pts_nodes,
		      intf.no_plot_html)

    if plotter.startup_state == e_errors.TIMEDOUT:
        Trace.trace(enstore_constants.PLOTTING,
                    "Plotter TIMED OUT when contacting %s"%(plotter.startup_text,))
    else:
	plotter.plot(intf.encp, intf.mount, intf.sg, intf.total_bytes)

    del plotter.csc.u
    del plotter.u     # del now, otherwise get name exception (just for python v1.5???)

if __name__ == "__main__":   # pragma: no cover

    # get interface
    intf_of_plotter = PlotterInterface(user_mode=0)

    do_work(intf_of_plotter)
