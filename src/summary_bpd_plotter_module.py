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
import bytes_per_day_plotter_module
import e_errors
import configuration_client

#DAYS_IN_MONTH = tapes_burn_rate_plotter_module.DAYS_IN_MONTH
#DAYS_IN_WEEK = tapes_burn_rate_plotter_module.DAYS_IN_WEEK

#SECONDS_IN_DAY = tapes_burn_rate_plotter_module.SECONDS_IN_DAY

WEB_SUB_DIRECTORY = bytes_per_day_plotter_module.WEB_SUB_DIRECTORY


class SummaryBpdPlotterModule(
        bytes_per_day_plotter_module.BytesPerDayPlotterModule):
    def __init__(self, name, isActive=True):
        bytes_per_day_plotter_module.BytesPerDayPlotterModule.__init__(
            self, name, isActive)

        # The keys to this dictionary are either a mover, media type
        # or the literal "enstore".  The values are file handles to the
        # pts files written out in the fill() function.
        #self.pts_files_dict = {}

        # The keys to this dictionary are either a mover, media_type
        # or the literal "enstore".  The values are a dictionary with keys
        # 'r' and/or 'w' for reads and writes.  Those dictionaries contain
        # a dictionary with the keys 'sum', 'average' and 'count' for the
        # sum of the file sizes, the average of the files sizes and the
        # number of files.
        #self.total_values = {}

        # Override the tapes_burn_rate default value of empty string.
        #self.output_fname_prefix = ""

    #######################################################################
    # The following functions must be defined by all plotting modules.
    #######################################################################

    def book(self, frame):

        # Get cron directory information.
        cron_dict = frame.get_configuration_client().get("crons", {})

        # Pull out just the information we want.
        self.temp_dir = cron_dict.get("tmp_dir", "/tmp")
        # If the output path was included on the command line, use that.
        # Failing that, use the path in the configuration file.
        self.html_dir = self.get_parameter("web_dir")
        if self.html_dir is None:
            self.html_dir = cron_dict.get("html_dir", "")

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

        # Get cron directory information.
        config_servers_dict = frame.get_configuration_client().get(
            "known_config_servers", {})
        if not e_errors.is_ok(config_servers_dict):
            sys.stderr.write("Unable to obtain configurtion server list: %s\n"
                             % (config_servers_dict.get("status", "Unknown")))
            sys.exit(1)
        else:
            del config_servers_dict['status']  # Remove the status element.

        # Loop over all known configuration servers and obtaining information
        # from all of them.  We need to reverse this list so that the
        # corrected sums (only applies to multiple system bytes per day plots)
        # can be plotted correctly.
        go_list = config_servers_dict.keys()
        go_list.reverse()
        for name in go_list:
            values = config_servers_dict[name]
            csc = configuration_client.ConfigurationClient(values)

            ###
            # Get information from the Accounting Database.
            ###
            adb = csc.get("accounting_server", {})
            try:
                adb = pg.DB(host=adb.get('dbhost', "localhost"),
                            dbname=adb.get('dbname', "enstore"),
                            port=adb.get('dbport', 5432),
                            user=adb.get('dbuser_reader', "enstore_reader"))
            except pg.InternalError as msg:
                message = "Unable to contact (%s, %s): %s\n" % \
                          (adb['dbhost'], adb['dbport'], msg)
                sys.stderr.write(message)
                continue

            # Make sure to include a little extra information to name all
            # systems that were successful at being included.
            self.extra_title_info.append(name)

            sql_cmd = "select date(encp_xfer.date) as day,CASE when 1 = 1 then '%s' else '%s' END as %s,rw,sum(size) " \
                "from encp_xfer " \
                "where date(CURRENT_TIMESTAMP - interval '30 days') < date" \
                "      and driver != 'NullDriver' " \
                "group by day,%s,rw "\
                "order by day,%s,rw;" % ((name,) * 5)
            self._fill(sql_cmd, adb)

            sql_cmd2 = "select CASE when 1 = 1 then '%s' else '%s' END as %s,rw,sum(size),avg(size),count(size) " \
                "from encp_xfer " \
                "where date(CURRENT_TIMESTAMP - interval '30 days') < date" \
                "      and driver != 'NullDriver' " \
                "group by %s,rw " \
                "order by %s,rw;" % ((name,) * 5)
            self._fill2(sql_cmd2, adb)

    # Plot the number of bytes transfered in the last month for all known
    # Enstore systems.
    def plot(self):

        #
        # Do the total bytes transfered per day.
        #

        plot_filename = os.path.join(self.temp_dir,
                                     "enplot_total_bpd.plot")
        ps_filename = os.path.join(self.plot_dir,
                                   "%senplot_total_bpd.ps" % (self.output_fname_prefix))
        jpg_filename = os.path.join(self.plot_dir,
                                    "%senplot_total_bpd.jpg" % (self.output_fname_prefix))
        jpg_stamp_filename = os.path.join(self.plot_dir,
                                          "%senplot_total_bpd_stamp.jpg" % (self.output_fname_prefix))

        pts_filenames = []
        for key in self.extra_title_info:
            try:
                pts_filenames.append(self.pts_files_dict[key].name)
            except KeyError:
                # We get here if there were no encp's in the last month.
                pts_filename = os.path.join(self.temp_dir, "bpd_dummy.pts")
                self.pts_files_dict[key] = open(pts_filename, "w")
                # Write one data point to appease gnuplot.
                self.pts_files_dict[key].write("%s %s %s %s %s %s\n" %
                                               (time.strftime("%Y-%m-%d"),
                                                "enstore",
                                                0, 0, 0, 0))
                self.pts_files_dict[key].close()
                pts_filenames.append(pts_filename)  # Don't forget to add this!

        # Need to reverse the order after the reverse in fill() so that they
        # are plotted correctly.
        pts_filenames.reverse()
        self.extra_title_info.reverse()

        # Write the gnuplot command file(s).
        # Use the self.extra_title_info to also server as a way to pass
        # multiple columns into one plot.
        self.write_plot_file(plot_filename, pts_filenames, ps_filename,
                             self.extra_title_info,
                             title_target=self.extra_title_info,
                             total_only=True)

        # Make the plot and convert it to jpg.
        os.system("gnuplot < %s" % plot_filename)
        os.system("convert -flatten -background lightgray -rotate 90  %s %s\n"
                  % (ps_filename, jpg_filename))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                  % (ps_filename, jpg_stamp_filename))

        #
        # Do the total bytes written per day.
        #

        plot_filename = os.path.join(self.temp_dir,
                                     "enplot_total_bpd_w.plot")
        ps_filename = os.path.join(self.plot_dir,
                                   "%senplot_total_bpd_w.ps" % (self.output_fname_prefix))
        jpg_filename = os.path.join(self.plot_dir,
                                    "%senplot_total_bpd_w.jpg" % (self.output_fname_prefix))
        jpg_stamp_filename = os.path.join(self.plot_dir,
                                          "%senplot_total_bpd_w_stamp.jpg" % (self.output_fname_prefix))

        pts_filenames = []
        for key in self.extra_title_info:
            try:
                pts_filenames.append(self.pts_files_dict[key].name)
            except KeyError:
                # We get here if there were no encp's in the last month.
                pts_filename = os.path.join(self.temp_dir, "bpd_dummy.pts")
                self.pts_files_dict[key] = open(pts_filename, "w")
                # Write one data point to appease gnuplot.
                self.pts_files_dict[key].write("%s %s %s %s %s %s\n" %
                                               (time.strftime("%Y-%m-%d"),
                                                "enstore",
                                                0, 0, 0, 0))
                self.pts_files_dict[key].close()
                pts_filenames.append(pts_filename)  # Don't forget to add this!

        # Need to reverse the order after the reverse in fill() so that they
        # are plotted correctly.  We don't need to reverse these here.
        # They were already reversed when the first plot was made.
        # pts_filenames.reverse()
        # self.extra_title_info.reverse()

        # Write the gnuplot command file(s).
        # Use the self.extra_title_info to also server as a way to pass
        # multiple columns into one plot.
        self.write_plot_file(plot_filename, pts_filenames, ps_filename,
                             self.extra_title_info,
                             writes_only=True,
                             title_target=self.extra_title_info,
                             total_only=True)

        # Make the plot and convert it to jpg.
        os.system("gnuplot < %s" % plot_filename)
        os.system("convert -flatten -background lightgray -rotate 90  %s %s\n"
                  % (ps_filename, jpg_filename))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                  % (ps_filename, jpg_stamp_filename))
