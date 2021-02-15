#!/usr/bin/env python

"""
Plot drive usage hours versus date, stacked by storage group, individually for
each unique drive type.

.. note::

   - This module is referenced by the :mod:`plotter_main` module.
   - This module has code in common with the
     :mod:`drive_hours_sep_plotter_module` module.
"""

# Python imports
from __future__ import division, print_function
import os
import time

# Enstore imports
import dbaccess
import enstore_constants
import enstore_plotter_module
import histogram

WEB_SUB_DIRECTORY = enstore_constants.DRIVE_HOURS_PLOTS_SUBDIR
"""Subdirectory in which to write plots. This constant is also referenced by
the :mod:`enstore_make_plot_page` module."""


class DriveHoursPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    """Plot drive usage hours versus date, stacked by storage group,
    individually for each unique drive type."""

    num_bins = 32

    def book(self, frame):
        """
        Create destination directory for plots as needed.

        :type frame: :class:`enstore_plotter_framework.EnstorePlotterFramework`
        :arg frame: provides configuration client.
        """

        cron_dict = frame.get_configuration_client().get('crons', {})
        self.html_dir = cron_dict.get('html_dir', '')
        self.plot_dir = os.path.join(self.html_dir,
                                     enstore_constants.PLOTS_SUBDIR)
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)
        self.web_dir = os.path.join(self.html_dir, WEB_SUB_DIRECTORY)
        if not os.path.exists(self.web_dir):
            os.makedirs(self.web_dir)
        print('Plots sub-directory: {}'.format(self.web_dir))

    def fill(self, frame):
        """
        Read and store values for plots from the database into memory.

        :type frame: :class:`enstore_plotter_framework.EnstorePlotterFramework`
        :arg frame: provides configuration client.
        """

        # Get database
        csc = frame.get_configuration_client()
        db_info = csc.get(enstore_constants.ACCOUNTING_SERVER)
        db_get = db_info.get
        db = dbaccess.DatabaseAccess(maxconnections=1,
                                     host=db_get('dbhost', 'localhost'),
                                     database=db_get('dbname', 'accounting'),
                                     port=db_get('dbport', 8800),
                                     user=db_get('dbuser', 'enstore'),
                                     )

        # Read from database
        db_query = ("select * from tape_mounts where "
                    "start notnull and finish notnull "
                    "and storage_group notnull "
                    "and start > CURRENT_TIMESTAMP - interval '{} days' "
                    "and state in ('M','D') "
                    "order by start asc").format(self.num_bins)
        res = db.query_dictresult(db_query)

        # Populate mounts
        mounts = {}
        for row in res:

            start = time.strptime(row.get('start'), '%Y-%m-%d %H:%M:%S')
            start = time.mktime(start)
            finish = time.strptime(row.get('finish'), '%Y-%m-%d %H:%M:%S')
            finish = time.mktime(finish)

            t = row.get('type')
            label = row.get('volume')
            sg = row.get('storage_group')
            if not sg:
                continue

            state = row.get('state')
            if state == 'M':
                if t not in mounts:
                    mounts[t] = {}
                if sg not in mounts[t]:
                    mounts[t][sg] = {}
                if label not in mounts[t][sg]:
                    mounts[t][sg][label] = {'M': [], 'D': []}

                # Before each mount, we should have equal mounts and dismounts.
                len_m = len(mounts[t][sg][label]['M'])
                len_d = len(mounts[t][sg][label]['D'])
                if len_m != len_d:
                    continue

                mounts[t][sg][label]['M'].append(start)

            elif state == 'D':
                if t not in mounts:
                    continue
                if sg not in mounts[t]:
                    continue
                if label not in mounts[t][sg]:
                    continue

                # Before each dismount, mounts should be ahead of dismounts by
                # 1.
                len_m = len(mounts[t][sg][label]['M'])
                len_d = len(mounts[t][sg][label]['D'])
                if len_m == len_d:
                    continue

                mounts[t][sg][label]['D'].append(finish)

        db.close()
        self.mounts = mounts

    def plot(self):
        """Write plot files."""

        str_time_format = "%Y-%m-%d %H:%M:%S"

        now_time = time.time()
        now_time = enstore_plotter_module.roundtime(now_time, 'ceil')
        now_time -= enstore_constants.SECS_PER_HALF_DAY  # For bin placement.
        now_time_str = time.strftime(str_time_format,
                                     time.localtime(now_time))

        start_time = now_time - self.num_bins * enstore_constants.SECS_PER_DAY
        # Note: "start_time = enstore_plotter_module.roundtime(start_time,
        #                     'ceil')" has no effect.
        start_time_str = time.strftime(str_time_format,
                                       time.localtime(start_time))

        plot_key_setting = 'set key outside width 2'
        # Note: "width 2" is used above to prevent a residual overlap of the
        # legend's labels and the histogram.
        ylabel = 'Drive usage hours'
        ylabel_i = 'Accumulative {}'.format(ylabel.lower())

        set_xrange_cmds = ('set xdata time',
                           'set timefmt "{}"'.format(str_time_format),
                           'set xrange ["{}":"{}"]'.format(start_time_str,
                                                           now_time_str))

        # Make plots
        for t, v1 in self.mounts.iteritems():

            plot_name = '%s' % (t,)
            plot_title = 'Drive usage by storage group for %s drives.' % (t,)
            plotter = histogram.Plotter(plot_name, plot_title)
            plotter.add_command(plot_key_setting)
            for cmd in set_xrange_cmds:
                plotter.add_command(cmd)

            iplot_name = 'Accumulative_%s' % (t,)
            iplot_title = ('Accumulative drive usage by storage group for '
                           '%s drives.') % (t,)
            iplotter = histogram.Plotter(iplot_name, iplot_title)
            iplotter.add_command(plot_key_setting)
            for cmd in set_xrange_cmds:
                iplotter.add_command(cmd)

            s_name = 'h_{}'.format(t)
            s = histogram.Histogram1D(s_name, s_name, self.num_bins,
                                      float(start_time), float(now_time))
            s_i_name = 'acc_{}'.format(s_name)
            s_i = histogram.Histogram1D(s_i_name, s_i_name, self.num_bins,
                                        float(start_time), float(now_time))

            s.set_time_axis(True)
            s_i.set_time_axis(True)

            hists = []
            hists_i = []

            plot_enabled = False
            for sg, v2 in v1.iteritems():

                h_name = '{}_{}'.format(t, sg)
                h = histogram.Histogram1D(h_name, h_name,
                                          self.num_bins,
                                          float(start_time), float(now_time))

                for _volume, data in v2.iteritems():
                    mounts = data['M']
                    dismounts = data['D']
                    durations = [y - x for x, y in zip(mounts, dismounts)]
                    for i, v in enumerate(durations):
                        finish_time = dismounts[i]
                        finish_time -= enstore_constants.SECS_PER_HALF_DAY
                        # Note: The shift above is to match the previously
                        # applied shift of now_time and start_time by half day.
                        v /= 3600.
                        if v > 0:
                            # Note: This check ensures that the number of
                            # entries in the histogram can if needed later be
                            # used as an indicator of whether the histogram
                            # contains nonzero data.
                            h.fill(finish_time, v)
                        # Note: If a mount-start and the corresponding
                        # dismount-finish times occur on separate dates, the
                        # duration is recorded only for the dismount date.

                status = 'Plotting' if (h.n_entries() > 0) else 'Skipping'
                print('{}: drive={}; storage_group={}'.format(status, t, sg))

                if h.n_entries() > 0:
                    plot_enabled = True
                else:
                    continue

                h.set_time_axis(True)
                h.set_xlabel('Date (year-month-day)')
                h.set_ylabel(ylabel)
                h.set_line_width(20)
                h.set_marker_text(sg)
                h.set_marker_type('impulses')
                hists.append(h)

                h_i = h.integral()
                h_i.set_xlabel('Date (year-month-day)')
                h_i.set_ylabel(ylabel_i)
                h_i.set_line_width(20)
                h_i.set_marker_text(sg)
                h_i.set_marker_type('impulses')
                hists_i.append(h_i)

            hists.sort()
            hists_i.sort()
            color = 0
            for h, h_i in zip(hists, hists_i):

                s += h
                s_name = 'drive_time_{}'.format(h.get_name())
                s.set_name(s_name)
                s.set_data_file_name(s_name)
                s.set_marker_text(h.get_marker_text())
                s.set_time_axis(True)
                s.set_ylabel(ylabel)
                s.set_marker_type('impulses')
                s.set_line_color(color)
                s.set_line_width(20)
                plotter.add(s)

                s_i += h_i
                s_i_name = 'accumulated_drive_time_{}'.format(h_i.get_name())
                s_i.set_name(s_i_name)
                s_i.set_data_file_name(s_i_name)
                s_i.set_marker_text(h_i.get_marker_text())
                s_i.set_time_axis(True)
                s_i.set_ylabel(ylabel_i)
                s_i.set_marker_type('impulses')
                s_i.set_line_color(color)
                s_i.set_line_width(20)
                iplotter.add(s_i)

                color += 1

            if plot_enabled:  # Check prevents error if no hist was added.
                plotter.reshuffle()
                plotter.plot(directory=self.web_dir)
                iplotter.reshuffle()
                iplotter.plot(directory=self.web_dir)
