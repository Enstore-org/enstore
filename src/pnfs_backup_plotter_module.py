#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import pg
import os
import time
import sys

# enstore imports
import enstore_plotter_module
import enstore_constants

WEB_SUB_DIRECTORY = enstore_constants.PNFS_BACKUP_TIME_PLOTS_SUBDIR

class PnfsBackupPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)

    #Write out the file that gnuplot will use to plot the data.
    # plot_filename = The file that will be read in by gnuplot containing
    #                 the gnuplot commands.
    # data_filename = The data file that will be read in by gnuplot
    #                 containing the data to be plotted.
    # ps_filename = The postscript file that will be created by gnuplot.
    def write_plot_file(self, plot_filename, data_filename, ps_filename):

        # Generate plot for a month period of time.  The lower and upper
        # variable format need to match the "set timefmt" line below.
        t = time.time()
        t0 = t - 30*24*60*60 #One month ago.
        t1 = t + 24*60*60    #Make sure to include all of today!
        lower = "%s 00:00:00" % (time.strftime("%Y-%m-%d", time.localtime(t0)))
        upper = "%s 00:00:00" % (time.strftime("%Y-%m-%d", time.localtime(t1)))

        #Write the gnuplot commands out.

        plot_fp = open(plot_filename, "w+")

        plot_fp.write("set terminal postscript color solid\n")
        plot_fp.write("set title 'pnfs backup time generated on %s'\n" \
                      % (time.ctime(),))
        plot_fp.write("set xlabel 'Date'\n")
        plot_fp.write("set ylabel \"Seconds\"\n")
        plot_fp.write("set timefmt \"%s\"\n" % ("%Y-%m-%d %H:%M:%S"))
        plot_fp.write("set xdata time\n")
        plot_fp.write("set format x \"%s\"\n" % ("%Y-%m-%d\\n%H:%M:%S"))
        plot_fp.write("set xrange ['%s':'%s']\n" % (lower, upper))
        plot_fp.write("set yrange [ 0 : ]\n")
        plot_fp.write("set size 1.4,1.2\n")
        plot_fp.write("set grid\n")
        plot_fp.write("set output \"%s\"\n" % ps_filename)
        plot_fp.write("plot \"%s\" using 1:3 title \"backup time\" with impulses lw 10\n" \
                        % (data_filename,))

        plot_fp.close()

    #######################################################################
    # The following functions must be defined by all plotting modueles.
    #######################################################################

    def book(self, frame):
        #Get cron directory information.
        cron_dict = frame.get_configuration_client().get("crons", {})

        #Pull out just the information we want.
        temp_dir = cron_dict.get("tmp_dir", "/tmp")
        html_dir = cron_dict.get("html_dir", "")

        #Handle the case were we don't know where to put the output.
        if not html_dir:
            sys.stderr.write("Unable to determine html_dir.\n")
            sys.exit(1)
        self.web_dir = os.path.join(html_dir, WEB_SUB_DIRECTORY)
        if not os.path.exists(self.web_dir):
            os.makedirs(self.web_dir)

        #Define variables that hold the path needed in fill() and plot().
        self.data_filename = "%s/pnfs_backup.pts" % (temp_dir,)

        self.output_filename = "%s/pnfs_backup_time.ps" % (self.web_dir,)
        self.output_filename_jpeg = "%s/pnfs_backup_time.jpg" % (self.web_dir,)
        self.output_filename_stamp_jpeg = "%s/pnfs_backup_time_stamp.jpg" % \
                                          (self.web_dir,)

        self.plot_filename = "%s/pnfs_format.plot" % (temp_dir,)

    def fill(self, frame):

        #  here we create data points

        acc = frame.get_configuration_client().get(enstore_constants.ACCOUNTING_SERVER, {})
        db = pg.DB(host   = acc.get('dbhost', "localhost"),
                   dbname = acc.get('dbname', "accounting"),
                   port   = acc.get('dbport', 5432),
                   user   = acc.get('dbuser', "enstore")
                   )

        sql_cmd = "select start,finish - start as duration from event " \
                   "where name = 'pnfsFastBackup' and " \
                   "start > CURRENT_TIMESTAMP - interval '30 days';"

        res = db.query(sql_cmd).getresult() #Get the values from the DB.

        #Open the file that will contain the data gnuplot will plot.
        self.data_file = open(self.data_filename, "w")

        # Loop over the SQL query results.
        for row in res:
            if not row[0] or not row[1]:
                #row[1] could be None if a backup is still going on.
                continue

            start = row[0]     #YYYY:MM:DD HH:MM:SS
            duration = row[1]  #hh:mm:ss

            #Convert the duration into a single numerical value in seconds.

            #First, handle any possible "day" values.
            try:
                days, hms = duration.split(" days ")
            except ValueError:
                try:
                    days, hms = duration.split(" day ")
                except ValueError:
                    days = 0
                    hms = duration
            #Second, process the hour, minutes and seconds.
            try:
                split_time_hms = hms.split(":")
            except AttributeError:
                #When we get here, it just so happens that a pnfs backup is
                # in progress.  start is set to a time, but duration is
                # set to None because it is still empty in the DB.
                continue
            seconds = int(days) * 86400 + \
                      int(split_time_hms[0]) * 3600 + \
                      int(split_time_hms[1]) * 60 + \
                      int(split_time_hms[2])

            #Write the data to be plotted into a file that will be read
            # by gnuplot.
            self.data_file.write("%s %s\n" % (start, seconds))

        #If there are no valid data points, we need to insert one zero
        # valued point for gnuplot to plot.  If this file is empty, gnuplot
        # throws and error.  This will happen, most likely, when the pnfs
        # backup cronjob has not run at all in the last month.
        if not res:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.data_file.write("%s %s\n" % (current_time, 0))

        #Close these to avoid resource leaks.
        db.close()
        self.data_file.close()

    def plot(self):

        #Make the file that tells gnuplot what to do.
        self.write_plot_file(self.plot_filename, self.data_filename,
                             self.output_filename)

        # make the plot
	rtn = os.system("gnuplot < %s" % (self.plot_filename,))
	if rtn:
		sys.stderr.write("gnuplot failed\n")
		sys.exit(1)

        # convert to jpeg
        rtn = os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s" %
                  (self.output_filename, self.output_filename_jpeg))
	if rtn:
		sys.stderr.write("convert failed\n")
                sys.exit(1)
        rtn = os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s" %
                  (self.output_filename, self.output_filename_stamp_jpeg))
	if rtn:
		sys.stderr.write("convert failed\n")
                sys.exit(1)

        #clean up the temporary files.
        try:
            os.remove(self.plot_filename)
            pass
        except:
            pass
        try:
            os.remove(self.data_file.name)
            pass
        except:
            pass
