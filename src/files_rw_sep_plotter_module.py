#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
Plot number of files read and written per mount versus date, separately for
each unique drive type and storage group combination.

.. note::

   - This module is referenced by the :mod:`plotter_main` module.
   - This module has code in common with the
     :mod:`files_rw_plotter_module` module.
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

WEB_SUB_DIRECTORY = enstore_constants.FILES_RW_SEP_PLOTS_SUBDIR
"""Subdirectory in which to write plots. This constant is also referenced by
the :mod:`enstore_make_plot_page` module."""

TIME_CONDITION = "CURRENT_TIMESTAMP - interval '1 month'"
"""PostgreSQL condition for the period of time for which to plot."""

class FilesRWSepPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    """Plot number of files read and written per mount versus date, separately
    for each unique drive type and storage group combination."""

    plot_accumulative = False

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
        db_query = ("select type as drive, storage_group, "
                    "date_trunc('day', finish) as date, "
                    "sum(reads)::float/count(state) as reads_per_dismount, "
                    "sum(writes)::float/count(state) as writes_per_dismount, "
                    "(sum(reads)+sum(writes))::float/count(state) "
                    " as reads_and_writes_per_dismount "
                    "from tape_mounts where "
                    "storage_group notnull and finish notnull "
                    "and finish > {0} and state='D' "
                    "group by drive, storage_group, date "
                    "order by drive, storage_group, date"
                    ).format(TIME_CONDITION)
        res = db.query_dictresult(db_query)

        # Restructure data into nested dict
        counts = {}
        for row in res:
            row = row.get
            counts.setdefault(row('drive'), {}) \
                  .setdefault(row('storage_group'), {}) \
                   [row('date')] = {'reads': row('reads_per_dismount'),
                                    'writes': row('writes_per_dismount'),
                                    'reads+writes':
                                        row('reads_and_writes_per_dismount')
                                    }

        db.close()
        self.counts = counts

    def plot(self):
        """Write all plot files."""

        str_time_format = "%Y-%m-%d %H:%M:%S"

        now_time = time.time()
        Y, M, D, _h, _m, _s, wd, jd, dst = time.localtime(now_time)
        now_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
        self.now_time = now_time
        now_time_str = time.strftime(str_time_format,
                                     time.localtime(now_time))

        start_time = now_time - 32 * 86400  # (32 days)
        Y, M, D, _h, _m, _s, wd, jd, dst = time.localtime(start_time)
        start_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
        self.start_time = start_time
        start_time_str = time.strftime(str_time_format,
                                       time.localtime(start_time))

        self.set_xrange_cmds = ('set xdata time',
                                'set timefmt "{}"'.format(str_time_format),
                                'set xrange ["{}":"{}"]'.format(start_time_str,
                                                                now_time_str))

        # Make plots
        for action in ('reads', 'writes'): #, 'reads+writes'):
            for drive, drive_dict in self.counts.iteritems():
                for sg, sg_dict in drive_dict.iteritems():
                    self._write_plot(action, drive, sg, sg_dict)

    def _write_plot(self, action, drive, sg, sg_dict):
        """
        Write plots for the indicated action, drive type and storage group.

        :type action: :obj:`str`
        :arg action: This can be ``reads``, ``writes`` or ``reads+writes``.
        :type drive: :obj:`str`
        :arg drive: drive type.
        :type sg: :obj:`str`
        :arg sg: storage group.
        :type sg_dict: :obj:`dict`
        :arg sg_dict: This corresponds to ``self.counts[drive][sg]``.
        """

        # Establish plot types
        plot_types = ['basic']
        if self.plot_accumulative:
            plot_types.append('integral')

        # Create and write plots
        for plot_type in plot_types:

            # Note: The "basic" plot type must be first. The "integral"
            # histogram is generated from the basic histogram.

            print('Plotting: action={}; drive={}; storage_group={}; type={}'
                  .format(action, drive, sg, plot_type))

            # Initialize plotter
            if plot_type == 'basic':
                plot_name = '_'.join((sg, drive, action))
                plot_title = ('Average file {} per mount for {} storage group '
                              'for {} drives.').format(action, sg, drive)
            elif plot_type == 'integral':
                plot_name = '_'.join(('Accumulative', sg, drive, action))
                plot_title = ('Accumulative average file {} per mount for {} '
                              'storage group for {} drives.'
                              ).format(action, sg, drive)
            plotter = histogram.Plotter(plot_name, plot_title)
            for cmd in self.set_xrange_cmds:
                plotter.add_command(cmd)

            # Initialize histogram
            if plot_type == 'basic':
                hists = {}
                hist = histogram.Histogram1D(plot_name, plot_title, 32,
                                             float(self.start_time),
                                             float(self.now_time))
                hists['basic'] = hist
            elif plot_type == 'integral':
                hist = hists['integral'] = hists['basic'].integral()

            # Configure histogram
            hist.set_time_axis(True)
            hist.set_xlabel('Date (year-month-day)')
            if plot_type == 'basic':
                ylabel = 'Average file {} per mount'.format(action)
            elif plot_type == 'integral':
                ylabel = ('Accumulative average file {} per mount'
                          ).format(action)
            hist.set_ylabel(ylabel)
            hist.set_line_width(20)
            hist.set_marker_type('impulses')
            hist.set_name(plot_name)
            hist.set_data_file_name(plot_name)

            # Fill histogram
            if plot_type == 'basic':
                for date, date_dict in sg_dict.iteritems():
                    date = time.mktime(time.strptime(date,
                                                     '%Y-%m-%d %H:%M:%S'))
                    value = date_dict[action]
                    hist.fill(date, value)

            # Plot histogram
            plotter.add(hist)
            plotter.reshuffle()
            plotter.plot(directory=self.web_dir)
