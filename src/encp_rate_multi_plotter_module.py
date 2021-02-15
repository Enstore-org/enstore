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
import stat

# enstore imports
import enstore_plotter_module
import enstore_constants

DELTA_TIME = 2592000  # one month 30*24*60*60

WEB_SUB_DIRECTORY = enstore_constants.ENCP_RATE_MULTI_PLOTS_SUBDIR


class EncpRateMultiPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(
            self, name, isActive)

    # Write out the file that gnuplot will use to plot the data.
    # plot_filename = The file that will be read in by gnuplot containing
    #                 the gnuplot commands.
    # data_filename = The data file that will be read in by gnuplot
    #                 containing the data to be plotted.
    # ps_filename = The postscript file that will be created by gnuplot.
    # storage_group = The current storage group.
    # rw = Either "r" or "w" to indicate if the data is currently read or
    #      write data.
    def write_plot_file(self, plot_filename, data_filename, ps_filename,
                        storage_group, rw):

        plot_fp = open(plot_filename, 'w')

        now_time = time.time()

        lower = "%s" % (time.strftime("%Y-%m-%d",
                                      time.localtime(now_time - DELTA_TIME)))
        upper = "%s" % (time.strftime("%Y-%m-%d",
                                      time.localtime(now_time)))

        # Get full words for r and w.
        if rw == "r":
            direction = "read"
        elif rw == "w":
            direction = "write"
        else:
            direction = "unknown"

        plot_fp.write("set terminal postscript color solid\n")
        plot_fp.write("set output '%s'\n" % (ps_filename,))
        plot_fp.write("set xlabel 'Date'\n")
        tf = '"%Y-%m-%d %H:%M"'
        plot_fp.write("set timefmt %s\n" % (tf,))
        plot_fp.write("set yrange [0 : ]\n")
        plot_fp.write("set xdata time\n")
        plot_fp.write("set xrange [ '%s':'%s' ]\n" % (lower, upper))
        tf = '"%m-%d %H"'
        plot_fp.write("set format x %s\n" % (tf,))
        plot_fp.write("set ylabel 'Rate MB/s'\n")
        plot_fp.write("set grid\n")
        plot_fp.write("set size 1.0, 2.0\n")
        plot_fp.write("set origin 0.0, 0.0\n")
        plot_fp.write("set multiplot\n")
        plot_fp.write("set size 0.5, 0.6\n")

        # Overall
        plot_fp.write("set origin 0.25,1.2\n")
        title = "%s: overall encp %s rate generated at %s" \
                % (storage_group, direction, time.ctime(time.time()))
        plot_fp.write("set title '%s'\n" % (title,))
        plot_fp.write("plot '%s' using 1:3 t '' with impulses lw 5\n"
                      % (data_filename,))

        # Network
        plot_fp.write("set origin 0.0,0.6\n")
        plot_fp.write("set title '%s: network encp %s rate'\n"
                      % (storage_group, direction))
        plot_fp.write("plot '%s' using 1:5 t '' with impulses lw 5\n"
                      % (data_filename,))

        # disk
        plot_fp.write("set origin 0.5,0.6\n")
        plot_fp.write("set title '%s: disk encp %s rate'\n"
                      % (storage_group, direction))
        plot_fp.write("plot '%s' using 1:7 t '' with impulses lw 5\n"
                      % (data_filename,))

        # transfer
        plot_fp.write("set origin 0.,0.0\n")
        plot_fp.write("set title '%s: transfer encp %s rate'\n"
                      % (storage_group, direction))
        plot_fp.write("plot '%s' using 1:9 t '' with impulses lw 5\n"
                      % (data_filename,))

        # tape
        plot_fp.write("set origin 0.5,0.0\n")
        plot_fp.write("set title '%s: driver encp %s rate'\n"
                      % (storage_group, direction))
        plot_fp.write("plot '%s' using 1:11 t '' with impulses lw 5\n"
                      % (data_filename,))

        plot_fp.close()

    def get_max_time(self, db):
        sql_cmd = "select distinct(storage_group) " \
                  "from encp_xfer_average_by_storage_group;"

        res = db.query(sql_cmd).getresult()  # Get the values from the DB.

        # Loop over the SQL query results.
        for row in res:
            return row[0]

        return int(time.time())

    def get_storage_groups(self, db):
        sql_cmd = "select distinct(storage_group) " \
                  "from encp_xfer_average_by_storage_group;"

        res = db.query(sql_cmd).getresult()  # Get the values from the DB.

        # Loop over the SQL query results.
        storage_groups = []
        for row in res:
            storage_groups.append(row[0])

        return storage_groups

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

    def fill(self, frame):

        #  here we create data points

        acc = frame.get_configuration_client().get(
            enstore_constants.ACCOUNTING_SERVER, {})
        db = pg.DB(host=acc.get('dbhost', "localhost"),
                   dbname=acc.get('dbname', "accounting"),
                   port=acc.get('dbport', 5432),
                   user=acc.get('dbuser', "enstore")
                   )

        #max_time = self.get_max_time(db)
        now_time = int(time.time())
        # delta_time =  30*24*60*60 #one month

        self.storage_groups = self.get_storage_groups(db)

        sql_cmd = "select date,unix_time,storage_group,rw," \
                  "avg_overall_rate,stddev_overall_rate," \
                  "avg_network_rate,stddev_network_rate," \
                  "avg_disk_rate,stddev_disk_rate," \
                  "avg_transfer_rate,stddev_transfer_rate," \
                  "avg_drive_rate,stddev_drive_rate," \
                  "avg_size,stddev_size,counter " \
                  "from encp_xfer_average_by_storage_group " \
                  "where unix_time between %s and %s;" \
                  % (str(now_time - DELTA_TIME), str(now_time))

        res = db.query(sql_cmd).getresult()  # Get the values from the DB.

        # Open the datafiles.
        open_files = {}
        for storage_group in self.storage_groups:
            r_fname = os.path.join(self.temp_dir,
                                   "encp_rates_%s_r.pts" % (storage_group,))
            w_fname = os.path.join(self.temp_dir,
                                   "encp_rates_%s_w.pts" % (storage_group,))
            open_files['%s_r' % storage_group] = open(r_fname, "w")
            open_files['%s_w' % storage_group] = open(w_fname, "w")

        # Output to temporary files the data the gnuplot needs to plot.
        for row in res:
            try:
                (d, ut, sg, rw, a_or, s_o_r, a_nr, s_n_r, a_dsk_r, s_dsk_r,
                 a_t_r, s_t_r, a_drv_r, s_drv_r, a_s, s_s, counter) = row
            except ValueError:
                sys.stderr.write("Unable to parse line.\n")
                continue

            out_line = "%s %s %s %s %s %s %s %s %s %s %s %s %s %s\n" % \
                       (d, a_or, s_o_r, a_nr, s_n_r, a_dsk_r, s_dsk_r, a_t_r, s_t_r,
                        a_drv_r, s_drv_r, a_s, s_s, counter)

            try:
                open_files['%s_%s' % (sg, rw)].write(out_line)
            except KeyError:
                continue

        for each_file in open_files.values():
            # If the size of the file is zero, then there were no valid
            # reads or writes.  This also means there are no valid points
            # to plot.  We need to give gnuplot something to plot to force
            # the plots to be drawn, even if they contain an empty plot.
            try:
                status = os.stat(each_file.name)
                if status[stat.ST_SIZE] == 0:
                    # output the first zero value data points.
                    current_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                                 time.localtime(now_time))
                    out_line = "%s %s %s %s %s %s %s %s %s %s %s %s %s %s\n" % \
                               ((current_time,) + (0.0,) * 13)
                    each_file.write(out_line)
                    # output the second zero value data points.
                    old_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                             time.localtime(now_time - DELTA_TIME))
                    out_line = "%s %s %s %s %s %s %s %s %s %s %s %s %s %s\n" % \
                               ((old_time,) + (0.0,) * 13)
                    each_file.write(out_line)
            except OSError as msg:
                message = "Error adding zero value data points to %s: %s\n" \
                          % (each_file.name, str(msg),)
                sys.stderr.write(message)
                pass

            # Avoid resource leaks.
            each_file.close()

    def plot(self):

        for sg in self.storage_groups:

            self.__plot(sg, "r")
            self.__plot(sg, "w")

    def __plot(self, storage_group, rw):

        ps_filename = os.path.join(self.web_dir,
                                   "encp_rates_%s_%s.ps" %
                                   (storage_group, rw))
        jpeg_filename = os.path.join(self.web_dir,
                                     "encp_rates_%s_%s.jpg" %
                                     (storage_group, rw))
        jpeg_filename_stamp = os.path.join(self.web_dir,
                                           "encp_rates_%s_%s_stamp.jpg" %
                                           (storage_group, rw))

        plot_filename = os.path.join(self.temp_dir,
                                     "encp_rates_%s_%s.plot"
                                     % (storage_group, rw))

        # Filename must match that in fill().
        data_filename = os.path.join(self.temp_dir,
                                     "encp_rates_%s_%s.pts"
                                     % (storage_group, rw))

        if (os.path.exists(data_filename)):
            # Create the file that contains the commands for gnuplot to run.
            self.write_plot_file(plot_filename, data_filename, ps_filename,
                                 storage_group, rw)

            # Make the plot and convert it to jpg.
            os.system("gnuplot < %s" % (plot_filename))
            os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                      % (ps_filename, jpeg_filename))
            os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
                      % (ps_filename, jpeg_filename_stamp))

            # clean up the temporary files.
            try:
                os.remove(plot_filename)
                pass
            except BaseException:
                pass
            try:
                os.remove(data_filename)
                pass
            except BaseException:
                pass
