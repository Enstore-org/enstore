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
import string
import sys

# enstore imports
import enstore_plotter_module
import enstore_constants

BP15S_TO_TBPD = 5.7e-09

WEB_SUB_DIRECTORY = enstore_constants.RATEKEEPER_PLOTS_SUBDIR


class RateKeeperPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(
            self, name, isActive)
        self.time_in_days = 2
        self.smooth_num = 40

    # Write out the file that gnuplot will use to plot the data.
    # smooth_filename = the file that contains the data to be ploted.
    # plot_filename = the file that will be read in by gnuplot.
    # graphic_filename = the ps file that gnuplot will create.
    # group = Either "real" or "null".  Describes the type of rate info.
    # group_n = Is 0 or 1.  Used to determine the correct column to plot.
    def write_plot_file(self, smooth_filename, plot_filename,
                        graphic_filename, group, group_n,
                        supplemental_title_text=""):

        # Convert the first character to uppercase.
        group_title = string.upper(group[0]) + group[1:]

        try:
            plot_file = open(plot_filename, "w+")

            plot_file.write("set title \"%s Data Rates (Plotted: %s) %s\"\n" %
                            (group_title, time.ctime(time.time()),
                             supplemental_title_text))
            plot_file.write("set ylabel \"Terabytes/day\"\n")
            plot_file.write("set xdata time\n")
            plot_file.write("set timefmt \"%s\"\n" % ("%m-%d-%Y %H:%M:%S"))
            plot_file.write("set format x \"%s\"\n" % ("%m-%d-%Y\\n%H:%M:%S"))
            plot_file.write("set grid ytics\n")
            plot_file.write("set terminal postscript color solid\n")
            plot_file.write("set size 1.4,1.2\n")
            plot_file.write("set output \"%s\"\n" % graphic_filename)
            plot_file.write("plot \"%s\" using 1:%d title \"read\" with lines,"
                            "\"%s\" using 1:%d title \"write\" with lines,"
                            "\"%s\" using 1:%d title \"both\" with lines"
                            % ((smooth_filename, group_n * 3 + 3,
                                smooth_filename, group_n * 3 + 4,
                                smooth_filename, group_n * 3 + 5,)))

            plot_file.close()
        except (OSError, IOError):
            exc, msg = sys.exc_info()[:2]
            try:
                sys.stderr.write("Unable to write plot file: %s: %s\n" %
                                 (str(exc), str(msg)))
                sys.stderr.flush()
            except IOError:
                pass
            sys.exit(1)

    #######################################################################
    # The following functions must be defined by all plotting modueles.
    #######################################################################

    def book(self, frame):

        # Get cron directory information.
        cron_dict = frame.get_configuration_client().get("crons", {})

        # Pull out just the information we want.
        self.temp_dir = cron_dict.get("tmp_dir", "/tmp")
        html_dir = cron_dict.get("html_dir", "")

        # Handle the case were we don't know where to put the output.
        if not html_dir:
            sys.stderr.write("Unable to determine html_dir.\n")
            sys.exit(1)

        self.web_dir = os.path.join(html_dir, WEB_SUB_DIRECTORY)
        if not os.path.exists(self.web_dir):
            os.makedirs(self.web_dir)

        # Where would these values be set???
        if self.get_parameter("smooth_num"):
            self.smooth_num = self.get_parameter("smooth_num")
        if self.get_parameter("time_in_days"):
            self.time_in_days = self.get_parameter("time_in_days")

    def fill(self, frame):

       #  here we create data points

        acc = frame.get_configuration_client().get(
            enstore_constants.ACCOUNTING_SERVER, {})
        db = pg.DB(host=acc.get('dbhost', "localhost"),
                   dbname=acc.get('dbname', "accounting"),
                   port=acc.get('dbport', 5432),
                   user=acc.get('dbuser', "enstore"))
        now_time = time.time()
        then_time = now_time - self.time_in_days * 24 * 3600
        db.query("begin")
        db.query("declare rate_cursor cursor for select to_char(time,'MM-DD-YYYY HH24:MI:SS'), read, write, read_null, \
        write_null from rate where time between '%s' and '%s'" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(then_time)),
                                                                  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_time))))

        pts_filename = os.path.join(self.temp_dir, "ratekeeper.pts")
        self.data_file = open(pts_filename, "w")
        c = 0
        tr, tw = 0.0, 0.0
        tnr, tnw = 0.0, 0.0

        while True:
            res = db.query("fetch 10000 from rate_cursor").getresult()
            for row in res:
                date = row[0]
                tr = tr + float(row[1])
                tw = tw + float(row[2])
                tnr = tnr + float(row[3])
                tnw = tnw + float(row[4])
                c = (c + 1) % self.smooth_num
                n = self.smooth_num
                if not c:
                    self.data_file.write("%s %s %s %s %s %s %s\n" %
                                         (date,
                                          tr / n * BP15S_TO_TBPD,
                                          tw / n * BP15S_TO_TBPD,
                                          (tr + tw) / n * BP15S_TO_TBPD,
                                          tnr / n * BP15S_TO_TBPD,
                                          tnw / n * BP15S_TO_TBPD,
                                          (tnw + tnr) / n * BP15S_TO_TBPD))
                    tr, tw = 0.0, 0.0
                    tnr, tnw = 0.0, 0.0

            l = len(res)
            if (l < 10000):
                break

        db.close()
        self.data_file.close()

    def plot(self):

        # The order of groups is important, because the indexes of this list
        # are used to map the values to the correct columns in the data file.
        groups = ["real", "null"]  # Order matters here!
        sst = {'null': "(only null movers)", 'real': "(no null movers)"}

        for group in groups:
            # Get some filenames for the various files that get created.
            ps_filename = os.path.join(self.web_dir,
                                       "%s_rates.ps" % (group,))
            jpg_filename = os.path.join(self.web_dir,
                                        "%s_rates.jpg" % (group,))
            jpg_stamp_filename = os.path.join(self.web_dir,
                                              "%s_rates_stamp.jpg" % (group,))

            plot_filename = "%s/%s_ratekeeper_plot.plot" \
                            % (self.temp_dir, group)

            # Write the gnuplot command file(s).
            self.write_plot_file(self.data_file.name, plot_filename,
                                 ps_filename, group, groups.index(group),
                                 sst[group])

            # Make the plot and convert it to jpg.
            os.system("gnuplot < %s" % plot_filename)
            os.system("convert -flatten -background lightgray -rotate 90  %s %s\n"
                      % (ps_filename, jpg_filename))
            os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                      % (ps_filename, jpg_stamp_filename))

            # Cleanup the temporary files for this loop.
            try:
                os.remove(plot_filename)
            except BaseException:
                pass

        # Cleanup the temporary files shared by all loops.
        try:
            os.remove(self.data_file.name)
        except BaseException:
            pass
