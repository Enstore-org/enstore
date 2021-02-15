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
import types

# enstore imports
import enstore_plotter_module
import enstore_constants

WEB_SUB_DIRECTORY = enstore_constants.BYTES_PER_DAY_PLOTS_SUBDIR

DAYS_IN_MONTH = 30
DAYS_IN_WEEK = 7
SECONDS_IN_DAY = 86400  # Seconds in one day. (24*60*60)

MONTH_AGO = time.strftime("%m-%d-%Y",
                          time.localtime(time.time() - DAYS_IN_MONTH * SECONDS_IN_DAY))
WEEK_AGO = time.strftime("%m-%d-%Y",
                         time.localtime(time.time() - DAYS_IN_MONTH * SECONDS_IN_DAY))

DAYS_AGO_START = DAYS_IN_MONTH * 4  # 4 months ago to start drawing the plot.
DAYS_AHEAD_END = DAYS_IN_MONTH  # One month to plot ahead.

# Set some sane limits to these values.
if DAYS_AGO_START < DAYS_IN_MONTH:
    DAYS_AGO_START = DAYS_IN_MONTH
if DAYS_AHEAD_END < 0:
    DAYS_AHEAD_END = 0


class BytesPerDayPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(
            self, name, isActive)

        # The keys to this dictionary are either a mover, media type
        # or the literal "enstore".  The values are file handles to the
        # pts files written out in the fill() function.
        self.pts_files_dict = {}

        # The keys to this dictionary are either a mover, media_type
        # or the literal "enstore".  The values are a dictionary with keys
        # 'r' and/or 'w' for reads and writes.  Those dictionaries contain
        # a dictionary with the keys 'sum', 'average' and 'count' for the
        # sum of the file sizes, the average of the files sizes and the
        # number of files.
        self.total_values = {}

        # This one is used by summary_bytes_per_day_plotter_module.py.
        # Appears here only for compatibility.
        self.output_fname_prefix = ""

        # This is also used by summary_bytes_per_day_plotter_module.py.
        self.extra_title_info = []

        # Dictionary to hold the values while the read and writes
        # are being collated together.  Make it a class member in case
        # for the summary plots that want to reorder stuff.
        self.store_dict = {}
        self.day_dict = {}
        self.day_dict_r = {}
        self.day_dict_w = {}

    # Write out the file that gnuplot will use to plot the data.
    # plot_filename = The file that will be read in by gnuplot containing
    #                 the gnuplot commands.
    # data_filenames = The data file that will be read in by gnuplot
    #                  containing the data to be plotted.  This can be a list
    #                  of files if the keys argument is also a list.
    # ps_filename = The postscript file that will be created by gnuplot.
    # key = The key to access information in the various instance member
    #       variables.  It is either a string containing the library
    #       or a tuple consiting of the library and storage group.
    # writes_only = Set this true when only the bytes written are to
    #               be plotted.
    # title_target = Normally, the key is used in the title, but some of
    #                the system wide plots should have have more descriptive
    #                names than what the key contains.
    def write_plot_file(self, plot_filename, data_filenames,
                        ps_filename, keys, writes_only=False,
                        total_only=False,
                        title_target=None):

        EMPTY_AGGREGATES = {'sum': 0, 'average': 0, 'count': 0}

        # Make these lists for processing.
        if not isinstance(keys, list):
            keys = [keys]
        if not isinstance(data_filenames, list):
            data_filenames = [data_filenames]

        # Sum up the numbers.
        read_aggregates = 0
        write_aggregates = 0
        total_bytes = 0
        meansize = 0
        transfers = 0
        if writes_only:
            for key in keys:
                write_aggregates = self.total_values.get(
                    key, {}).get('w', EMPTY_AGGREGATES)

                total_bytes = total_bytes + write_aggregates['sum']
                #meansize = write_aggregates['average']
                transfers = transfers + write_aggregates['count']

            try:
                meansize = total_bytes / transfers
            except ZeroDivisionError:
                meansize = 0

            title_label = "Written"
        else:
            for key in keys:
                write_aggregates = self.total_values.get(
                    key, {}).get('w', EMPTY_AGGREGATES)
                read_aggregates = self.total_values.get(
                    key, {}).get('r', EMPTY_AGGREGATES)

                total_bytes = total_bytes + \
                    read_aggregates['sum'] + write_aggregates['sum']
                #meansize = read_aggregates['average'] + write_aggregates['average']
                transfers = transfers + \
                    read_aggregates['count'] + write_aggregates['count']

            try:
                meansize = total_bytes / transfers
            except ZeroDivisionError:
                meansize = 0

            title_label = "Transfered"

        # Need the current time to add to the plot.
        now = time.strftime("%m-%d-%Y %H:%M:%S")

        # In case there is something else we want to add.
        if title_target is None:
            title_target = str(key)  # Should be a string already.
        # if hasattr(self, "extra_title_info"):
        #    title_target = "%s %s" % (title_target, self.extra_title_info)

        plot_fp = open(plot_filename, "w+")

        plot_fp.write('set terminal postscript color solid\n')
        plot_fp.write('set output "%s"\n' % (ps_filename,))
        plot_fp.write('set title "Total Bytes %s Per Day for %s (no null movers)"\n' %
                      (title_label, title_target,))
        plot_fp.write('set xlabel "Date (year-month-day)"\n')
        plot_fp.write('set ylabel "Bytes"\n')
        plot_fp.write('set timefmt "%Y-%m-%d"\n')
        plot_fp.write('set xdata time\n')
        plot_fp.write('set size 1.5,1\n')
        plot_fp.write('set grid\n')
        plot_fp.write('set format x "%Y-%m-%d"\n')
        plot_fp.write(
            'set label "Plotted %s " at graph .99,0 rotate font "Helvetica,10"\n' %
            (now,))
        plot_fp.write('set xrange[ : ]\n')
        plot_fp.write('set yrange[ 0 : ]\n')

        plot_fp.write('set key right top Right samplen 1 '
                      'title "Total Bytes : %.2e\\n'
                      'Mean Xfer Size : %.2e\\n'
                      'Number of Transfers : %d\n'
                      % (total_bytes, meansize, transfers))

        plot_line = ""
        for i in range(len(data_filenames)):
            data_filename = data_filenames[i]

            if plot_line:
                # After the first loop, append a comma to seperate the
                # different plots.
                plot_line = "%s," % (plot_line,)

            if writes_only and total_only:
                # Used only for the summary plots of multiple Enstore systems.
                name = keys[i]
                rl = ""
                wl = '"%s" using 1:7 t "%s" with impulses linewidth 20 ' % (
                    data_filename, name)
            elif writes_only:
                rl = ""
                wl = '"%s" using 1:4 t "writes" with impulses linewidth 20 ' % (
                    data_filename,)
            elif total_only:
                # Used only for the summary plots of multiple Enstore systems.
                name = keys[i]
                # Column 6 contains 'corrected' values.  This allows us to
                # show the summary where the impluses in "back" get bumped
                # up over the impluses in front.
                rl = '"%s" using 1:6 t "%s" with impulses linewidth 20 ' % (
                    data_filename, name)
                wl = ""
            else:  # reads and writes seperate.
                rl = '"%s" using 1:3 t "reads" with impulses linewidth 18, ' % (
                    data_filename,)
                wl = '"%s" using 1:4 t "writes" with impulses linewidth 20 ' % (
                    data_filename,)

            plot_line = "%s %s %s" % (plot_line, rl, wl)

        # The first plot is the read, the second is the write.
        plot_fp.write('plot %s\n' % (plot_line,))

        plot_fp.close()

    # Get the daily information from the DB and write it to a data file.

    def _fill(self, sql_cmd, adb):
        adb_res = adb.query(sql_cmd).getresult()  # Get the values from the DB.

        self.store_dict = {}

        for row in adb_res:

            # row[0] is the day
            # row[1] is the drive type, mover or the literal "enstore"
            # row[2] indicates read (r) or write (w)
            # row[3] is the sum for that day

            if row[1] not in self.pts_files_dict:
                # Create the temporary data points file.
                fname = os.path.join(self.temp_dir,
                                     "en_bpd_%s.pts" % (row[1],))
                self.pts_files_dict[row[1]] = open(fname, "w")

            # Add this data to the dictionary.
            if row[1] not in self.store_dict:
                self.store_dict[row[1]] = {}
            if row[0] not in self.store_dict[row[1]]:
                self.store_dict[row[1]][row[0]] = {}
            self.store_dict[row[1]][row[0]][row[2]] = int(row[3])

        # Key is either a mover, drive type or the literal "enstore".
        for key in self.store_dict.keys():
            #day is YYYY-mm-dd
            for day in self.store_dict[key].keys():
                read_sum = self.store_dict[key][day].get('r', 0)
                write_sum = self.store_dict[key][day].get('w', 0)

                # c stands for corrected.  Some plots want to print the
                # diffent plots summed together but have each segment a
                # differnt color representing how much of the segment came
                # from each source.  We fake this by adding the previous
                # values to the the data to bump up the next successive
                # set of values to be plotted.
                self.day_dict[day] = self.day_dict.get(day, 0) + \
                    read_sum + write_sum
                self.day_dict_r[day] = self.day_dict_r.get(day, 0) + \
                    read_sum
                self.day_dict_w[day] = self.day_dict_w.get(day, 0) + \
                    write_sum

                self.store_dict[key][day]['c'] = self.day_dict[day]
                self.store_dict[key][day]['cr'] = self.day_dict_r[day]
                self.store_dict[key][day]['cw'] = self.day_dict_w[day]

                # day
                # key is mover, drive type or "enstore"
                # read sum for the day
                # write sum for the day
                # read+write sum for the day
                # corrected read+write sum for the day
                # corrected write sum for the day
                # corrected read sum for the day
                line = "%s %s %s %s %s %s %s %s\n" % (
                    day, key, read_sum, write_sum,
                    read_sum + write_sum,
                    self.store_dict[key][day]['c'],
                    self.store_dict[key][day]['cw'],
                    self.store_dict[key][day]['cr'])

                # Write the information to the correct file.
                self.pts_files_dict[key].write(line)

        # Key is either a mover, drive type or the literal "enstore".
        for key in self.store_dict.keys():
            self.pts_files_dict[key].close()

    # Get the summary information for the entire time period.
    def _fill2(self, sql_cmd, adb):
        adb_res = adb.query(sql_cmd).getresult()  # Get the values from the DB.

        for row in adb_res:
            # row[0] is either the mover, drive type or the literal "enstore"
            # row[1] is r for read or w for write
            # row[2] is the summation of the file size
            # row[3] is the average of the file size
            # row[4] is the number of files

            if row[0] not in self.total_values:
                self.total_values[row[0]] = {}

            self.total_values[row[0]][row[1]] = {'sum': row[2],
                                                 'average': row[3],
                                                 'count': row[4]}

    #######################################################################
    # The following functions must be defined by all plotting modules.
    #######################################################################

    def book(self, frame):
        # Get cron directory information.
        cron_dict = frame.get_configuration_client().get("crons", {})

        # Pull out just the information we want.
        self.temp_dir = cron_dict.get("tmp_dir", "/tmp")
        self.html_dir = cron_dict.get("html_dir", "")

        # Handle the case were we don't know where to put the output.
        if not self.html_dir:
            sys.stderr.write("Unable to determine html_dir.\n")
            sys.exit(1)
        # Should the plot_dir ever be defined in enstore_constants to
        # not be the same as html_dir; this situation should be handled
        # by using plot_subdir instead of html_dir when wanting the top
        # plot page instead of the sub-directory containing most of the
        # bytes per day plots.  Use web_dir for the mover and drive type
        # specific plots.
        self.plot_dir = os.path.join(self.html_dir,
                                     enstore_constants.PLOTS_SUBDIR)
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)
        self.web_dir = os.path.join(self.html_dir, WEB_SUB_DIRECTORY)
        if not os.path.exists(self.web_dir):
            os.makedirs(self.web_dir)

    def fill(self, frame):

        #  here we create data points

        adb_info = frame.get_configuration_client().get("accounting_server", {})
        adb = pg.DB(host=adb_info.get('dbhost', "localhost"),
                    dbname=adb_info.get('dbname', "enstore"),
                    port=adb_info.get('dbport', 5432),
                    user=adb_info.get('dbuser_reader', "enstore_reader"))

        ###
        # Get information from the Enstore Database.
        ###

        #
        #Get the bytes per day for each mover. ######################
        #

        sql_cmd = "select date(encp_xfer.date) as day,mover,rw,sum(size) " \
                  "from encp_xfer " \
                  "where date(CURRENT_TIMESTAMP - interval '30 days') < date "\
                  "      and driver != 'NullDriver' " \
                  "group by day,mover,rw "\
                  "order by day,mover,rw;"
        self._fill(sql_cmd, adb)

        sql_cmd2 = "select mover,rw,sum(size),avg(size),count(size) " \
                   "from encp_xfer " \
                   "where date(CURRENT_TIMESTAMP - interval '30 days') < date " \
                   "      and driver != 'NullDriver' " \
                   "group by mover,rw " \
                   "order by mover,rw;"
        self._fill2(sql_cmd2, adb)

        #
        #Get the bytes per day for each media_type. ######################
        #

        sql_cmd = "select date(encp_xfer.date) as day,drive_id,rw,sum(size) " \
                  "from encp_xfer " \
                  "where date(CURRENT_TIMESTAMP - interval '30 days') < date "\
                  "      and driver != 'NullDriver' " \
                  " group by day,drive_id,rw "\
                  "order by day,drive_id,rw;"
        self._fill(sql_cmd, adb)

        sql_cmd2 = "select drive_id,rw,sum(size),avg(size),count(size) " \
                   "from encp_xfer " \
                   "where date(CURRENT_TIMESTAMP - interval '30 days') < date " \
                   "      and driver != 'NullDriver' " \
                   "group by drive_id,rw " \
                   "order by drive_id,rw;"
        self._fill2(sql_cmd2, adb)

        #
        #Get the bytes per day for the system. ######################
        #

        # The case is just a filler in place of the mover or drive type.
        sql_cmd = "select date(encp_xfer.date) as day,CASE when 1 = 1 then 'enstore' else 'enstore' END as enstore,rw,sum(size) " \
                  "from encp_xfer " \
                  "where date(CURRENT_TIMESTAMP - interval '30 days') < date" \
                  "      and driver != 'NullDriver' " \
                  "group by day,enstore,rw "\
                  "order by day,enstore,rw;"
        self._fill(sql_cmd, adb)

        sql_cmd2 = "select CASE when 1 = 1 then 'enstore' else 'enstore' END as enstore,rw,sum(size),avg(size),count(size) " \
                   "from encp_xfer " \
                   "where date(CURRENT_TIMESTAMP - interval '30 days') < date"\
                   "      and driver != 'NullDriver' " \
                   "group by enstore,rw " \
                   "order by enstore,rw;"
        self._fill2(sql_cmd2, adb)

        #
        #Get the bytes per day for the system going back to the beginning. ###
        #

        # The case is just a filler in place of the mover or drive type.
        sql_cmd = "select date(encp_xfer.date) as day,CASE when 1 = 1 then 'enstore_all' else 'enstore_all' END as enstore_all,rw,sum(size) " \
                  "from encp_xfer " \
                  "where driver != 'NullDriver' " \
                  "group by day,enstore_all,rw "\
                  "order by day,enstore_all,rw;"
        self._fill(sql_cmd, adb)

        sql_cmd2 = "select CASE when 1 = 1 then 'enstore_all' else 'enstore_all' END as enstore_all,rw,sum(size),avg(size),count(size) " \
                   "from encp_xfer " \
                   "where driver != 'NullDriver' " \
                   "group by enstore_all,rw " \
                   "order by enstore_all,rw;"
        self._fill2(sql_cmd2, adb)

        # Avoid resource leaks.
        for key in self.pts_files_dict.keys():
            self.pts_files_dict[key].close()

    def plot(self):
        # for key in self.LM_dict.keys() + self.LM_SG_dict.keys():
        for key in self.pts_files_dict.keys():
            # Key at this point is either, the mover, media_type or the
            # literal "enstore".

            # Get some filenames for the various files that get created.
            plot_filename = os.path.join(self.temp_dir,
                                         "enplot_bpd-%s.plot" % (key,))

            if key in self.pts_files_dict:
                pts_filename = self.pts_files_dict[key].name
            else:
                sys.stderr.write("No pts file for %s.\n" % (key,))
                continue
            ps_filename = os.path.join(self.web_dir,
                                       "%s%s.ps" % (self.output_fname_prefix,
                                                    key,))
            jpg_filename = os.path.join(self.web_dir,
                                        "%senplot_bpd-%s.jpg" % (self.output_fname_prefix,
                                                                 key,))
            jpg_stamp_filename = os.path.join(self.web_dir,
                                              "%senplot_bpd-%s_stamp.jpg" % (self.output_fname_prefix,
                                                                             key,))

            # Write the gnuplot command file(s).
            self.write_plot_file(plot_filename, pts_filename, ps_filename, key)

            # Make the plot and convert it to jpg.
            os.system("gnuplot < %s" % plot_filename)
            os.system("convert -flatten -background lightgray -rotate 90  %s %s\n"
                      % (ps_filename, jpg_filename))
            os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                      % (ps_filename, jpg_stamp_filename))

        #
        # For the entire system, there are a few more plots to go.
        #

        # First is the number of bytes written per day for the last month.
        plot_filename = os.path.join(self.temp_dir,
                                     "enplot_bpd_w_month.plot")
        ps_filename = os.path.join(self.plot_dir,
                                   "%senplot_bpd_w_month.ps" % (self.output_fname_prefix))
        jpg_filename = os.path.join(self.plot_dir,
                                    "%senplot_bpd_w_month.jpg" % (self.output_fname_prefix))
        jpg_stamp_filename = os.path.join(self.plot_dir,
                                          "%senplot_bpd_w_month_stamp.jpg" % (self.output_fname_prefix))

        try:
            pts_filename = self.pts_files_dict["enstore"].name
        except KeyError:
            # We get here if there were no encp's in the last month.
            pts_filename = os.path.join(self.temp_dir, "bpd_dummy.pts")
            self.pts_files_dict["enstore"] = open(pts_filename, "w")
            # Write one data point to appease gnuplot.
            self.pts_files_dict["enstore"].write("%s %s %s %s %s %s\n" %
                                                 (time.strftime("%Y-%m-%d"),
                                                  "enstore",
                                                  0, 0, 0, 0))
            self.pts_files_dict["enstore"].close()

        # Write the gnuplot command file(s).
        self.write_plot_file(plot_filename, pts_filename, ps_filename,
                             "enstore", writes_only=True,
                             title_target="Enstore")

        # Make the plot and convert it to jpg.
        os.system("gnuplot < %s" % plot_filename)
        os.system("convert -flatten -background lightgray -rotate 90  %s %s\n"
                  % (ps_filename, jpg_filename))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                  % (ps_filename, jpg_stamp_filename))

        # Second is the number of bytes written per day since the beginning.
        plot_filename = os.path.join(self.temp_dir,
                                     "enplot_bpd_w.plot")
        ps_filename = os.path.join(self.plot_dir,
                                   "%senplot_bpd_w.ps" % (self.output_fname_prefix))
        jpg_filename = os.path.join(self.plot_dir,
                                    "%senplot_bpd_w.jpg" % (self.output_fname_prefix))
        jpg_stamp_filename = os.path.join(self.plot_dir,
                                          "%senplot_bpd_w_stamp.jpg" % (self.output_fname_prefix))

        try:
            pts_filename = self.pts_files_dict["enstore_all"].name
        except KeyError:
            # We get here if there were no encp's - ever.
            pts_filename = os.path.join(self.temp_dir, "bpd_dummy.pts")
            self.pts_files_dict["enstore_all"] = open(pts_filename, "w")
            # Write one data point to appease gnuplot.
            self.pts_files_dict["enstore_all"].write("%s %s %s %s %s %s\n" %
                                                     (time.strftime("%Y-%m-%d"),
                                                      "enstore",
                                                      0, 0, 0, 0))
            self.pts_files_dict["enstore_all"].close()

        # Write the gnuplot command file(s).
        self.write_plot_file(plot_filename, pts_filename, ps_filename,
                             "enstore_all", writes_only=True,
                             title_target="Enstore")

        # Make the plot and convert it to jpg.
        os.system("gnuplot < %s" % plot_filename)
        os.system("convert -flatten -background lightgray -rotate 90  %s %s\n"
                  % (ps_filename, jpg_filename))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                  % (ps_filename, jpg_stamp_filename))

        # Cleanup the temporary files.
        for key in self.pts_files_dict.keys():

            try:
                os.remove(plot_filename)
            except BaseException:
                pass
            try:
                os.remove(pts_filename)
            except BaseException:
                pass
