#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
Plot drive usage hours versus date, stacked by storage group, individually for
each unique drive type.
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

# Note: This module is referenced by the "plotter_main" module.

WEB_SUB_DIRECTORY = enstore_constants.DRIVE_HOURS_PLOTS_SUBDIR
# Note: Above constant is referenced by "enstore_make_plot_page" module.
TIME_CONDITION = "CURRENT_TIMESTAMP - interval '1 month'"


class DriveHoursPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    """Plot drive usage hours versus date, stacked by storage group,
    individually for each unique drive type."""

    def book(self, frame):
        """Create destination directory for plots, as needed."""

        cron_dict = frame.get_configuration_client().get('crons', {})
        self.html_dir = cron_dict.get('html_dir', '')
        self.plot_dir = os.path.join(self.html_dir,
                                     enstore_constants.PLOTS_SUBDIR)
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)
        self.web_dir = os.path.join(self.html_dir, WEB_SUB_DIRECTORY)
        if not os.path.exists(self.web_dir):
            os.makedirs(self.web_dir)

    def fill(self, frame):
        """Read and store values for plots from the database into memory."""

        # Get database
        csc = frame.get_configuration_client()
        db_info = csc.get(enstore_constants.ACCOUNTING_SERVER)
        db_get = db_info.get
        db = dbaccess.DatabaseAccess(maxconnections=1,
                                     host=db_get('dbhost', 'localhost'),
                                     database=db_get('dbname', 'accounting'),
                                     port=db_get('dbport', 9900),
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
        """Write plot files."""

        str_time_format = "%Y-%m-%d %H:%M:%S"

        now_time = time.time()
        Y, M, D, _h, _m, _s, wd, jd, dst = time.localtime(now_time)
        now_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
        now_time_str = time.strftime(str_time_format,
                                     time.localtime(now_time))

        start_time = now_time - 32 * 86400  # (32 days)
        Y, M, D, _h, _m, _s, wd, jd, dst = time.localtime(start_time)
        start_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
        start_time_str = time.strftime(str_time_format,
                                       time.localtime(start_time))

        #print('xrange: min={}; max={};'.format(start_time_str, now_time_str))

        plot_key_setting = 'set key outside width 2'
        # Note: "width 2" is used above to prevent a residual overlap of the
        # legend's labels and the histogram.
        ylabel = 'Drive time (hours/day)'

        set_xrange_cmds =  ('set xdata time',
                            'set timefmt "{}"'.format(str_time_format),
                            'set xrange ["{}":"{}"]'.format(start_time_str,
                                                            now_time_str))

        for t, v1 in self.mounts.iteritems():

            plot_name = '%s' % (t,)
            plot_title = 'Drive time by storage group for %s drives.' % (t,)
            plotter = histogram.Plotter(plot_name, plot_title)
            plotter.add_command(plot_key_setting)
            for cmd in set_xrange_cmds:
                plotter.add_command(cmd)

            iplot_name = 'accumulative_%s' % (t,)
            iplot_title = ('Accumulative drive time by storage group for '
                           '%s drives.') % (t,)
            iplotter = histogram.Plotter(iplot_name, iplot_title)
            iplotter.add_command(plot_key_setting)
            for cmd in set_xrange_cmds:
                iplotter.add_command(cmd)

            s = histogram.Histogram1D('h_' + t, 'h_' + t, 32,
                                      float(start_time), float(now_time))
            s_i = histogram.Histogram1D('acc_' + t, 'acc_' + t, 32,
                                        float(start_time), float(now_time))

            s.set_time_axis(True)
            s_i.set_time_axis(True)

            color = 1
            for sg, v2 in v1.iteritems():

                h = histogram.Histogram1D(t + '__' + sg, t + '__' + sg, 32,
                                          float(start_time), float(now_time))

                duration = 0
                for _volume, data in v2.iteritems():
                    mounts = data['M']
                    dismounts = data['D']
                    durations = [y - x for x, y in zip(mounts, dismounts)]
                    duration += sum(durations)
                    for i, v in enumerate(durations):
                        h.fill(mounts[i], v / 3600.)

                h.set_time_axis(True)
                h.set_ylabel(ylabel)
                h.set_xlabel('Date (year-month-day)')
                h.set_line_color(color)
                h.set_line_width(20)
                h.set_marker_type('impulses')

                tmp = s + h
                tmp.set_name('drive_%s_%s' % (t, sg,))
                tmp.set_data_file_name(t + '_' + sg)
                tmp.set_marker_text(sg)
                tmp.set_time_axis(True)
                tmp.set_ylabel(ylabel)
                tmp.set_marker_type('impulses')
                tmp.set_line_color(color)
                tmp.set_line_width(20)
                plotter.add(tmp)
                s = tmp

                integral = h.integral()
                integral.set_marker_text(sg)
                integral.set_marker_type('impulses')
                integral.set_ylabel(ylabel)

                tmp = s_i + integral
                marker_text = integral.get_marker_text()
                name_ = 'accumulated_drive_time_%s' % (marker_text,)
                tmp.set_name(name_)
                data_file_name = 'accumulated_drive_time_%s' % (marker_text,)
                tmp.set_data_file_name(data_file_name)
                tmp.set_marker_text(marker_text)
                tmp.set_time_axis(True)
                tmp.set_ylabel(integral.get_ylabel())
                tmp.set_marker_type(integral.get_marker_type())
                tmp.set_line_color(color)
                tmp.set_line_width(20)
                iplotter.add(tmp)

                s_i = tmp
                color += 1
                print(t, sg, duration)

            plotter.reshuffle()
            plotter.plot(directory=self.web_dir)
            iplotter.reshuffle()
            iplotter.plot(directory=self.web_dir)
