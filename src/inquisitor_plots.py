#
# Routines to create the inquisitor plots.
#
##############################################################################
# system import
import threading

# enstore imports
import Trace
import e_errors
import enstore_files
import enstore_plots
import enstore_functions
import enstore_constants

PLOTTHRNAME = "PLOT_THREAD"

LOGFILE_DIR = "logfile_dir"

class InquisitorPlots:

    # create the html file with the inquisitor plot information
    def	make_plot_html_page(self):
	self.plothtmlfile.open()
	# get the list of stamps and jpg files
	(jpgs, stamps, pss) = enstore_plots.find_jpg_files(self.html_dir)
	self.plothtmlfile.write(jpgs, stamps, pss)
	self.plothtmlfile.close()
	self.plothtmlfile.install()

    #  make all the plots
    def plot(self, ticket):
	# make sure we do not have a thread doing this already
	if self.plot_thread and self.plot_thread.isAlive():
	    self.send_reply({ 'status'   : (e_errors.INPROGRESS, None) })
	else:
	    # find out where the log files are located
	    if ticket.has_key(LOGFILE_DIR):
		lfd = ticket[LOGFILE_DIR]
	    else:
		t = self.csc.get("log_server")
		if enstore_functions.is_timedout(t):
		    return
		lfd = t["log_file_path"]

	    out_dir = ticket.get("out_dir", lfd)

	    keep = ticket.get("keep", 0)
	    pts_dir = ticket.get("keep_dir", "")

	    # create a thread to deal with this.
	    plot_args = (ticket, lfd, keep, pts_dir, out_dir)
	    Trace.log(e_errors.INFO, "Creating plots in thread")
	    self.plot_thread = threading.Thread(group=None, 
						target=self.plot_function,
						name=PLOTTHRNAME, args=plot_args)
	    self.plot_thread.setDaemon(1)	    
	    self.plot_thread.start()

    # plot thread
    def plot_function(self, ticket, lfd, keep, pts_dir, out_dir):
	self.encp_plot(ticket, lfd, keep, pts_dir, out_dir)
	self.mount_plot(ticket, lfd, keep, pts_dir, out_dir)
	ret_ticket = { 'status'   : (e_errors.OK, None) }
	self.send_reply(ret_ticket)

    # make the mount plots (mounts per hour and mount latency
    def mount_plot(self, ticket, lfd, keep, pts_dir, out_dir):
	ofn = out_dir+"/mount_lines.txt"

	# parse the log files to get the media changer mount/dismount
	# information, put this info in a separate file
	# always add /dev/null to the end of the list of files to search thru 
	# so that grep always has > 1 file and will always print the name of 
	# the file at the beginning of the line.
	mountfile = enstore_files.EnMountDataFile(enstore_constants.LOG_PREFIX+\
						  "* /dev/null", ofn, 
	                                "-e %s -e %s"%(Trace.MSG_MC_LOAD_REQ,
                                                       Trace.MSG_MC_LOAD_DONE),
                                                   lfd)

	# only extract the information from the newly created file that is
	# within the requested timeframe.
	mountfile.open('r')
	mountfile.timed_read(ticket)
	# now pull out the info we are going to plot from the lines
	mountfile.parse_data(ticket.get("mcs", []))
        mountfile.close()
        mountfile.cleanup(keep, pts_dir)

        # only do the plotting if we have some data
        if mountfile.data:
            # create the data files
            mphfile = enstore_plots.MphDataFile(out_dir)
            mphfile.open()
            mphfile.plot(mountfile.data)
            mphfile.close()
            mphfile.install(self.html_dir)
            mphfile.cleanup(keep, pts_dir)

            mlatfile = enstore_plots.MlatDataFile(out_dir)
            mlatfile.open()
            mlatfile.plot(mountfile.data)
            mlatfile.close()
            mlatfile.install(self.html_dir)
            mlatfile.cleanup(keep, pts_dir)

	    # now save any new mount count data for the continuing total count. and create the
	    # overall total mount plot
	    mpdfile = enstore_plots.MpdDataFile(out_dir)
            mpdfile.open()
            mpdfile.plot(mphfile.total_mounts)
            mpdfile.close()
	    mpdfile.install(self.html_dir)

    # make the total transfers per unit of time and the bytes moved per day
    # plot
    def encp_plot(self, ticket, lfd, keep, pts_dir, out_dir):
	ofn = out_dir+"/bytes_moved.txt"

	# always add /dev/null to the end of the list of files to search thru 
	# so that grep always has > 1 file and will always print the name of 
	# the file at the beginning of the line.
	encpfile = enstore_files.EnEncpDataFile(enstore_constants.LOG_PREFIX+\
						"* /dev/null",
						ofn,
						"-e %s"%(Trace.MSG_ENCP_XFER,),
						lfd)
	# only extract the information from the newly created file that is
	# within the requested timeframe.
	encpfile.open('r')
	encpfile.timed_read(ticket)
	# now pull out the info we are going to plot from the lines
	encpfile.parse_data(ticket.get("mcs", []))
        encpfile.close()
        encpfile.cleanup(keep, pts_dir)

        # only do the plotting if we have some data
        if encpfile.data:
	    bpdfile = enstore_plots.BpdDataFile(out_dir)
	    bpdfile.open()
	    bpdfile.plot(encpfile.data)
	    bpdfile.close()
	    bpdfile.install(self.html_dir)

            xferfile = enstore_plots.XferDataFile(out_dir, bpdfile.ptsfile)
            xferfile.open()
            xferfile.plot(encpfile.data)
            xferfile.close()
            xferfile.install(self.html_dir)

            # delete any extraneous files. do it here because the xfer file
            # plotting needs the bpd data file
            bpdfile.cleanup(keep, pts_dir)
            xferfile.cleanup(keep, pts_dir)

