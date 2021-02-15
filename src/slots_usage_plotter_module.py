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

WEB_SUB_DIRECTORY = enstore_constants.SLOT_USAGE_PLOTS_SUBDIR


class SlotUsagePlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(
            self, name, isActive)
        self.time_in_days = 91
        self.data_files = {}
        self.free = {}

    # staticmethod
    def clean_string(self, var):
        return string.replace(string.replace(var, '/', ''), ' ', '-')

    # Write out the file that gnuplot will use to plot the data.
    # plot_filename = The file that will be read in by gnuplot containing
    #                 the gnuplot commands.
    # data_filename = The data file that will be read in by gnuplot
    #                 containing the data to be plotted.
    # ps_filename = The postscript file that will be created by gnuplot.
    def write_plot_file(self, plot_filename, pts_filename, ps_filename, name):
        try:
            plot_fp = open(plot_filename, "w")
            plot_fp.write("set output \"%s\"\n" % (ps_filename,))
            plot_fp.write("set terminal postscript color solid\n")
            plot_fp.write("set title \"Used Slots in %s\"\n" % (name,))
            plot_fp.write("set xlabel \"Date\"\n")
            plot_fp.write("set timefmt \"%m-%d-%Y %H:%M:%S\"\n")
            plot_fp.write("set xdata time\n")
            plot_fp.write("set ylabel \"Number of Slots\"\n")
            plot_fp.write("set grid \n")
            plot_fp.write("set yrange [0: ]\n")
            plot_fp.write("set format x \"%m-%d\"\n")
            plot_fp.write("set nokey \n")
            plot_fp.write(
                "set label \"Plotted `date` \" at graph .99,0 rotate font \"Helvetica,10\"\n")
            plot_fp.write(
                "set label \"%d Free\" at graph .2,.9 front font \"Helvetica,80\"\n" %
                (self.free[name],))
            plot_fp.write("plot \"%s\" using 1:3 w impulses linetype 2 lw 2, "
                          "\"%s\"  using 1:6 t \"Disabled Slots\" w impulses linetype 3 lw 2 , "
                          "\"%s\" using 1:5 t \"Used Slots\" w impulses linetype 1 lw 2 \n"
                          % (pts_filename, pts_filename, pts_filename))
            plot_fp.close()
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
        if self.get_parameter("time_in_days"):
            self.time_in_days = self.get_parameter("time_in_days")

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

        self.dest_dir = self.web_dir

    def fill(self, frame):
        self.data = []
        acc = {}
        acc = frame.get_configuration_client().get(
            enstore_constants.ACCOUNTING_SERVER, 5, 2)
        db = pg.DB(host=acc.get('dbhost', "localhost"),
                   dbname=acc.get('dbname', "accounting"),
                   port=acc.get('dbport', 5432),
                   user=acc.get('dbuser', "enstore")
                   )

        for row in db.query(
                "select distinct tape_library,location,media_type from tape_library_slots_usage").getresult():
            if not row:
                continue
            self.data.append(
                self.clean_string(
                    row[0]) +
                "_" +
                self.clean_string(
                    row[1]) +
                "_" +
                self.clean_string(
                    row[2]))

        for d in self.data:
            pts_filename = os.path.join(self.temp_dir, d + ".pts")
            self.data_files[d] = open(pts_filename, 'w')

        now_time = time.time()
        then_time = now_time - self.time_in_days * 24 * 3600
        db.query("begin")
        db.query("declare rate_cursor cursor for select to_char(time,'MM-DD-YYYY HH24:MI:SS'),tape_library,location,media_type,\
                  total,free,used,disabled    from  tape_library_slots_usage\
                  where time between '%s' and '%s'" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(then_time)),
                                                       time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_time))))
        while True:
            res = db.query("fetch 10000 from rate_cursor").getresult()
            for row in res:
                if not row:
                    continue
                date = row[0]
                key = self.clean_string(
                    row[1]) + "_" + self.clean_string(row[2]) + "_" + self.clean_string(row[3])
                fd = self.data_files[key]
                fd.write(
                    "%s %s %s %s %s\n" %
                    (date, row[4], row[5], row[6], row[6] + row[7]))
                self.free[key] = row[5]
            l = len(res)
            if (l < 10000):
                break
        db.close()

        for fd in self.data_files.values():
            fd.close()

    def plot(self):
        for d in self.data:
            if d not in self.free.keys():
                # If a key is in the data list, but not the free list, then
                # there is not any data from the current time period.
                continue

            # Get some filenames for the various files that get created.
            plot_filename = os.path.join(self.temp_dir, d + ".plot")
            pts_filename = os.path.join(self.temp_dir, d + ".pts")
            ps_filename = os.path.join(self.web_dir, d + ".ps")
            jpg_filename = os.path.join(self.web_dir, d + ".jpg")
            stamp_jpg_filename = os.path.join(self.web_dir, d + "_stamp.jpg")

            # Create the file that contains the commands for gnuplot to run.
            self.write_plot_file(plot_filename, pts_filename,
                                 ps_filename, d)

            # Make the plot and convert it to jpg.
            os.system("gnuplot < %s" % (plot_filename,))
            os.system("convert -flatten -background lightgray -rotate 90 %s %s\n" %
                      (ps_filename, jpg_filename))
            os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                      % (ps_filename, stamp_jpg_filename))

            # Cleanup the temporary files.
            try:
                os.remove(plot_filename)
            except BaseException:
                pass
            try:
                os.remove(pts_filename)
            except BaseException:
                pass
