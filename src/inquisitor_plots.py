#
# Routines to create the inquisitor plots.
#
##############################################################################
# system import
import threading
import string
import os

# enstore imports
import Trace
import e_errors
import enstore_files
import enstore_plots
import enstore_functions
import enstore_functions2
import enstore_constants

LOGFILE_DIR = "logfile_dir"

class InquisitorPlots:

    # create the html file with the inquisitor plot information
    def	make_plot_html_page(self):
	for plotfile_t  in self.plotfile_l:
	    if len(plotfile_t) > 2:
		plotfile = plotfile_t[0]
		html_dir = plotfile_t[1]
		links_l = plotfile_t[2:]
	    else:
		(plotfile, html_dir) = plotfile_t
		links_l = None
		nav_link = ""
	    plotfile.open()
	    # get the list of stamps and jpg files
	    (jpgs, stamps, pss) = enstore_plots.find_jpg_files(html_dir)
	    plotfile.write(jpgs, stamps, pss, self.mount_label, links_l)
	    plotfile.close()
	    plotfile.install()

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
	mountfile.timed_read(self.start_time, self.stop_time, prefix)
	# now pull out the info we are going to plot from the lines
	mountfile.parse_data(self.media_changer, prefix)
        mountfile.close()
        mountfile.cleanup(self.keep, self.keep_dir)

        # only do the plotting if we have some data
        if mountfile.data:
            # create the data files
            mphfile = enstore_plots.MphDataFile(dir=self.output_dir, 
						mount_label=self.mount_label)
            mphfile.open()
            # we need to do this because this is where the latency is
            # calculated
            mphfile.plot(mountfile.data)
            mphfile.close()
	    # we aren't using these plots, so do not create them.
            #mphfile.install(self.html_dir)
            mphfile.cleanup(self.keep, self.keep_dir)

            mlatfile = enstore_plots.MlatDataFile(self.output_dir, self.mount_label)
            mlatfile.open()
            mlatfile.plot(mphfile.latency)
            mlatfile.close()
            mlatfile.install(self.html_dir)
            mlatfile.cleanup(self.keep, self.keep_dir)

	    # now save any new mount count data for the continuing total count. and create the
	    # overall total mount plot
	    mpdfile = enstore_plots.MpdDataFile(self.output_dir, self.mount_label)
            mpdfile.open()
            mpdfile.plot(mphfile.total_mounts, mphfile.drive_type_d)
            mpdfile.close()
            mpdfile.install(self.html_dir)

	    mmpdfile = enstore_plots.MpdMonthDataFile(self.output_dir, self.mount_label)
            mmpdfile.open()
            mmpdfile.plot()
            mmpdfile.close()
            mmpdfile.install(self.html_dir)
	    mmpdfile.cleanup(self.keep, self.keep_dir)

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
        encpfile.read_and_parse(self.start_time, self.stop_time, prefix, self.media_changer)
        encpfile.close()
        encpfile.cleanup(self.keep, self.keep_dir)

        # only do the plotting if we have some data
        if encpfile.data:
	    # overall bytes/per/day count
	    bpdfile = enstore_plots.BpdDataFile(self.output_dir)
	    bpdfile.open()
	    bpdfile.plot(encpfile.data)
	    bpdfile.close()
	    bpdfile.install(self.html_dir)

	    mbpdfile = enstore_plots.BpdMonthDataFile(self.output_dir)
	    mbpdfile.open()
	    mbpdfile.plot(bpdfile.write_ctr)
	    mbpdfile.close()
	    mbpdfile.install(self.html_dir)

            xferfile = enstore_plots.XferDataFile(self.output_dir,
                                                  mbpdfile.ptsfile)
            xferfile.open()
            xferfile.plot(encpfile.data)
            xferfile.close()
            xferfile.install(self.html_dir)

            # delete any extraneous files. do it here because the xfer file
            # plotting needs the bpd data file
	    bpdfile.cleanup(self.keep, self.keep_dir)
	    mbpdfile.cleanup(self.keep, self.keep_dir)
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
	sgfile.timed_read(self.start_time, self.stop_time, prefix)
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

    def get_bpd_files(self):
	nodes_l = string.split(self.pts_nodes, ",")
	files_l = []
	this_node = enstore_functions2.strip_node(os.uname()[1])
	pts_file_only = "%s%s"%(enstore_constants.BPD_FILE, enstore_plots.PTS)
	pts_file = "%s/%s"%(self.pts_dir, pts_file_only)
	for node in nodes_l:
	    node = enstore_functions2.strip_node(node)
	    # make sure node is up before rcping
	    if enstore_functions2.ping(node) == enstore_constants.ALIVE:
		new_file = "/tmp/%s.%s"%(pts_file_only, node)
		if node == this_node:
		    rtn = os.system("cp %s %s"%(pts_file, new_file))
		else:
		    rtn = enstore_functions2.get_remote_file(node, pts_file, new_file)
		if rtn == 0:
		    # the copy was a success
		    files_l.append((new_file, node))
		else:
		    # record the error
		    Trace.log(e_errors.WARNING,
			      "could not copy %s from %s for total bytes plot"%(pts_file,
										node))
	    else:
		Trace.log(e_errors.WARNING,
			  "could not copy %s from %s for total bytes plot"%(pts_file,
									    node))
	return files_l

    def fill_in_bpd_d(self, bpdfile, data_d, node):
	if bpdfile.lines:
	    for line in bpdfile.lines:
		fields = string.split(string.strip(line))
		# this is the date
		if not data_d.has_key(fields[0]):
		    data_d[fields[0]] = {}
		if len(fields) > 2:
		    # we have data , need date, total, ctr, writes
		    data_d[fields[0]][node] = {enstore_plots.TOTAL : float(fields[1]),
					       enstore_plots.CTR : enstore_plots.get_ctr(fields),
					       enstore_plots.WRITES : float(fields[3])}
		else:
		    data_d[fields[0]][node] = {enstore_plots.TOTAL : 0.0, 
					       enstore_plots.CTR : 0L,
					       enstore_plots.WRITES : 0.0}

    def read_bpd_data(self, files_l):
	data_d = {}
	for filename, node in files_l:
	    bpdfile = enstore_files.EnstoreBpdFile(filename)
	    bpdfile.open('r')
	    # only extract the info that is within the requested time frame
	    bpdfile.timed_read(self.start_time, self.stop_time)
	    bpdfile.close()
	    # now fill in the data dictionary with the data
	    self.fill_in_bpd_d(bpdfile, data_d, node)
	return data_d

    def total_bytes_plot(self):
	# we need to copy the enplot_bpd.pts files from all the nodes that are not
	# our own.
	files_l = self.get_bpd_files()
	# for each file we have, open it and read the requested data
	data_d = self.read_bpd_data(files_l)
	# now write out the data and plot it
	if data_d:
	    tbpdfile = enstore_plots.TotalBpdDataFile(self.output_dir)
	    tbpdfile.open()
	    tbpdfile.plot(data_d)
	    tbpdfile.close()
	    tbpdfile.install(self.html_dir)
	    tbpdfile.cleanup(self.keep, self.keep_dir)

    #  make all the plots
    def plot(self, encp, mount, sg, total_bytes):
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

	if encp:
	    enstore_functions.inqTrace(enstore_constants.PLOTTING,
                                       "Creating inq transfer plots")
	    alt_key = string.strip(Trace.MSG_ENCP_XFER)
	    self.encp_plot(alt_logs.get(alt_key, enstore_constants.LOG_PREFIX))
	if mount:
	    enstore_functions.inqTrace(enstore_constants.PLOTTING,
                                       "Creating inq mount plots")
	    alt_key = string.strip(Trace.MSG_MC_LOAD_REQ)
	    self.mount_plot(alt_logs.get(alt_key, enstore_constants.LOG_PREFIX))
	if sg:
	    enstore_functions.inqTrace(enstore_constants.PLOTTING,
                                       "Creating inq storage group plots")
	    alt_key = string.strip(Trace.MSG_ADD_TO_LMQ)
	    self.sg_plot(alt_logs.get(alt_key, enstore_constants.LOG_PREFIX))
	if total_bytes:
	    enstore_functions.inqTrace(enstore_constants.PLOTTING,
                                       "Creating inq total bytes summary plot")
	    self.total_bytes_plot()

	# update the inquisitor plots web page
	if not self.no_plot_html:
	    Trace.trace(11, "Creating the inq plot web page")
	    self.make_plot_html_page()
