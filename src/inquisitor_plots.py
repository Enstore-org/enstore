#
# Routines to create the inquisitor plots.
#
##############################################################################
# system import
import threading
import string

# enstore imports
import Trace
import e_errors
import enstore_files
import enstore_plots
import enstore_functions
import enstore_constants

LOGFILE_DIR = "logfile_dir"

class InquisitorPlots:

    # create the html file with the inquisitor plot information
    def	make_plot_html_page(self):
	self.plotfile.open()
	# get the list of stamps and jpg files
	(jpgs, stamps, pss) = enstore_plots.find_jpg_files(self.html_dir)
	self.plotfile.write(jpgs, stamps, pss)
	self.plotfile.close()
	self.plotfile.install()

    # make the mount plots (mounts per hour and mount latency
    def mount_plot(self, prefix):
	# get the config file so we can not plot null mover data
	config = enstore_functions.get_config_dict()
	if config:
	    config = config.configdict
	else:
	    config = {}

	ofn = self.output_dir+"/mount_lines.txt"

        # parse the log files to get the media changer mount/dismount
        # information, put this info in a separate file
        # always add /dev/null to the end of the list of files to search thru 
        # so that grep always has > 1 file and will always print the name of 
        # the file at the beginning of the line.
        mountfile = enstore_files.EnMountDataFile("%s* /dev/null"%(prefix,), ofn, 
                                        "-e %s -e %s"%(Trace.MSG_MC_LOAD_REQ,
                                                       Trace.MSG_MC_LOAD_DONE),
                                                   self.logfile_dir, "", config)

	# only extract the information from the newly created file that is
	# within the requested timeframe.
	mountfile.open('r')
	mountfile.timed_read(self.start_time, self.stop_time)
	# now pull out the info we are going to plot from the lines
	mountfile.parse_data(self.media_changer)
        mountfile.close()
        mountfile.cleanup(self.keep, self.keep_dir)

        # only do the plotting if we have some data
        if mountfile.data:
            # create the data files
            mphfile = enstore_plots.MphDataFile(self.output_dir)
            mphfile.open()
            mphfile.plot(mountfile.data)
            mphfile.close()
            mphfile.install(self.html_dir)
            mphfile.cleanup(self.keep, self.keep_dir)

            mlatfile = enstore_plots.MlatDataFile(self.output_dir)
            mlatfile.open()
            mlatfile.plot(mphfile.latency)
            mlatfile.close()
            mlatfile.install(self.html_dir)
            mlatfile.cleanup(self.keep, self.keep_dir)

	    # now save any new mount count data for the continuing total count. and create the
	    # overall total mount plot
	    mpdfile = enstore_plots.MpdDataFile(self.output_dir)
            mpdfile.open()
            mpdfile.plot(mphfile.total_mounts)
            mpdfile.close()
            mpdfile.install(self.html_dir)

	    mmpdfile = enstore_plots.MpdMonthDataFile(self.output_dir)
            mmpdfile.open()
            mmpdfile.plot()
            mmpdfile.close()
            mmpdfile.install(self.html_dir)

    # make the total transfers per unit of time and the bytes moved per day
    # plot
    def encp_plot(self, prefix):
	ofn = self.output_dir+"/bytes_moved.txt"

	# always add /dev/null to the end of the list of files to search thru 
	# so that grep always has > 1 file and will always print the name of 
	# the file at the beginning of the line. do not count any null moves.
	encpfile = enstore_files.EnEncpDataFile("%s* /dev/null"%(prefix,), ofn,
						"-e %s"%(Trace.MSG_ENCP_XFER,),
						self.logfile_dir,
				  "| grep -v %s"%(enstore_constants.NULL_DRIVER,))
	# only extract the information from the newly created file that is
	# within the requested timeframe.
	encpfile.open('r')
	encpfile.timed_read(self.start_time, self.stop_time)
	# now pull out the info we are going to plot from the lines
	encpfile.parse_data(self.media_changer)
        encpfile.close()
        encpfile.cleanup(self.keep, self.keep_dir)

        # only do the plotting if we have some data
        if encpfile.data:
	    bpdfile = enstore_plots.BpdDataFile(self.output_dir)
	    bpdfile.open()
	    bpdfile.plot(encpfile.data)
	    bpdfile.close()
	    bpdfile.install(self.html_dir)

            xferfile = enstore_plots.XferDataFile(self.output_dir, bpdfile.ptsfile)
            xferfile.open()
            xferfile.plot(encpfile.data)
            xferfile.close()
            xferfile.install(self.html_dir)

            # delete any extraneous files. do it here because the xfer file
            # plotting needs the bpd data file
            bpdfile.cleanup(self.keep, self.keep_dir)
            xferfile.cleanup(self.keep, self.keep_dir)

    # make the plot showing queue movement for different storage groups plot
    def sg_plot(self, prefix):
	ofn = self.output_dir+"/sg_lmq.txt"

	# always add /dev/null to the end of the list of files to search thru 
	# so that grep always has > 1 file and will always print the name of 
	# the file at the beginning of the line.
	sgfile = enstore_files.EnSgDataFile("%s* /dev/null"%(prefix,), ofn,
					    "-e %s"%(Trace.MSG_ADD_TO_LMQ,),
					    self.logfile_dir)
	# only extract the information from the newly created file that is
	# within the requested timeframe.
	sgfile.open('r')
	sgfile.timed_read(self.start_time, self.stop_time)
	# now pull out the info we are going to plot from the lines
	sgfile.parse_data(prefix)
        sgfile.close()
        sgfile.cleanup(self.keep, self.keep_dir)

        # only do the plotting if we have some data
        if sgfile.data:
	    sgplot = enstore_plots.SgDataFile(self.output_dir)
	    sgplot.open()
	    sgplot.plot(sgfile.data)
	    sgplot.close()
	    sgplot.install(self.html_dir)

            # delete any extraneous files. do it here because the xfer file
            # plotting needs the bpd data file
            sgplot.cleanup(self.keep, self.keep_dir)

    #  make all the plots
    def plot(self):
	ld = {}
	# find out where the log files are located
	if self.logfile_dir is None:
	    ld = self.config_d.get("log_server")
	    self.logfile_dir = ld["log_file_path"]

	if self.output_dir is None:
	    self.output_dir = self.logfile_dir

	# get the list of alternate log files.  we may need to grep these instead of the
	# normal ones
	if not ld:
	    ld = self.config_d.get("log_server")
	alt_logs = ld.get("msg_type_logs", {})

	Trace.trace(1, "Creating inq transfer plots")
	alt_key = string.strip(Trace.MSG_ENCP_XFER)
	self.encp_plot(alt_logs.get(alt_key, enstore_constants.LOG_PREFIX))
	Trace.trace(1, "Creating inq mount plots")
	alt_key = string.strip(Trace.MSG_MC_LOAD_REQ)
	self.mount_plot(alt_logs.get(alt_key, enstore_constants.LOG_PREFIX))
	Trace.trace(1, "Creating inq storage group plots")
	alt_key = string.strip(Trace.MSG_ADD_TO_LMQ)
	self.sg_plot(alt_logs.get(alt_key, enstore_constants.LOG_PREFIX))
	# update the inquisitor plots web page
	Trace.trace(1, "Creating the inq plot web page")
	self.make_plot_html_page()
