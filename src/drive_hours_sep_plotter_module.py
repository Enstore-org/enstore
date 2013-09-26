#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
Plot drive usage hours versus date, separately for each unique drive type and
storage group combination.

.. note::

   - This module is referenced by the :mod:`plotter_main` module.
   - This module has code in common with the
     :mod:`drive_hours_plotter_module` module.
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

WEB_SUB_DIRECTORY = enstore_constants.DRIVE_HOURS_SEP_PLOTS_SUBDIR
"""Subdirectory in which to write plots. This constant is also referenced by
the :mod:`enstore_make_plot_page` module."""

TIME_CONDITION = "CURRENT_TIMESTAMP - interval '1 month'"
"""PostgreSQL condition for the period of time for which to plot."""


class DriveHoursSepPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    """Plot drive usage hours versus date, separately for each unique drive
    type and storage group combination."""

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
                    "and start > {0} "
                    "and state in ('M','D') "
                    "order by start asc").format(TIME_CONDITION)
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
        """Write all plot files."""

        str_time_format = "%Y-%m-%d %H:%M:%S"

        now_time = time.time()
        now_time = enstore_plotter_module.roundtime(now_time, 'ceil')
        now_time -= enstore_constants.SECS_PER_HALF_DAY  # For bin placement.
        self.now_time = now_time
        now_time_str = time.strftime(str_time_format,
                                     time.localtime(now_time))

        start_time = now_time - self.num_bins * enstore_constants.SECS_PER_DAY
        # Note: "start_time = enstore_plotter_module.roundtime(start_time,
        #                     'ceil')" has no effect.
        self.start_time = start_time
        start_time_str = time.strftime(str_time_format,
                                       time.localtime(start_time))

        self.set_xrange_cmds = ('set xdata time',
                                'set timefmt "{}"'.format(str_time_format),
                                'set xrange ["{}":"{}"]'.format(start_time_str,
                                                                now_time_str))

        # Make plots
        for drive, drive_dict in self.mounts.iteritems():
            for sg, sg_dict in drive_dict.iteritems():
                self._write_plot(drive, sg, sg_dict)

    def _write_plot(self, drive, sg, sg_dict):
        """
        Write plots for the indicated drive type and storage group.

        :type drive: :obj:`str`
        :arg drive: drive type.
        :type sg: :obj:`str`
        :arg sg: storage group.
        :type sg_dict: :obj:`dict`
        :arg sg_dict: This corresponds to ``self.mounts[drive][sg]``.
        """

        for plot_type in ('basic', 'integral'):

            # Note: The "basic" plot type must be first. The "integral"
            # histogram is generated from the basic histogram.

            print('Plotting: drive={}; storage_group={}; type={}'
                  .format(drive, sg, plot_type))

            # Initialize plotter
            if plot_type == 'basic':
                plot_name = '{}_{}'.format(sg, drive)
                plot_title = ('Drive usage for {} storage group for {} drives.'
                              ).format(sg, drive)
            elif plot_type == 'integral':
                plot_name = 'Accumulative_{}_{}'.format(sg, drive)
                plot_title = ('Accumulative drive usage for {} storage group '
                              'for {} drives.').format(sg, drive)
            plotter = histogram.Plotter(plot_name, plot_title)
            for cmd in self.set_xrange_cmds:
                plotter.add_command(cmd)

            # Initialize histogram
            if plot_type == 'basic':
                hists = {}
                hist = histogram.Histogram1D(plot_name, plot_title,
                                             self.num_bins,
                                             float(self.start_time),
                                             float(self.now_time))
                hists['basic'] = hist
            elif plot_type == 'integral':
                hist = hists['integral'] = hists['basic'].integral()

            # Configure histogram
            hist.set_time_axis(True)
            hist.set_xlabel('Date (year-month-day)')
            if plot_type == 'basic':
                ylabel = 'Drive usage hours'
            elif plot_type == 'integral':
                ylabel = 'Accumulative drive usage hours'
            hist.set_ylabel(ylabel)
            hist.set_line_width(20)
            hist.set_marker_type('impulses')
            hist.set_name(plot_name)
            hist.set_data_file_name(plot_name)

            # Fill histogram
            if plot_type == 'basic':
                for _volume, md_times in sg_dict.iteritems():
                    starts = md_times['M']  # starts of mounts
                    finishes = md_times['D']  # finishes of dismounts
                    durations = [f - s for s, f in zip(starts, finishes)]
                    for i, duration in enumerate(durations):
                        finish_time = finishes[i]
                        finish_time -= enstore_constants.SECS_PER_HALF_DAY
                        # Note: The shift above is to match the previously
                        # applied shift of now_time and start_time by half day.
                        duration /= 3600.  # convert seconds to hours
                        hist.fill(finish_time, duration)
                        # Note: If a mount-start and the corresponding
                        # dismount-finish times occur on separate dates, the
                        # duration is recorded only for the dismount date.

            # Plot histogram
            plotter.add(hist)
            plotter.reshuffle()
            plotter.plot(directory=self.web_dir)
