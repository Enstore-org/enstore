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
import histogram

MINUTE = 3600

WEB_SUB_DIRECTORY = enstore_constants.DRIVE_UTILIZATION_PLOTS_SUBDIR

library_name_map = {
    'CD PowderHorn 9310': 'CD_STK',
    'D0 AML/2 R1': 'D0_AML',
    'GCC StreamLine 8500': 'GCC_SL8500',
    'CDF PowderHorn 9310': 'CDF_STK',
    'D0 PowderHorn 9310': 'D0_STK',
    'FCC StreamLine 8500': 'FCC_SL8500',
    'D0 AML/2': 'D0_AML'
}

type_name_map = {
    'IBM-LTO-3': 'LTO3',
    'IBM-LTO4': 'LTO4'
}


class DriveUtilizationPlotterModule(
        enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(
            self, name, isActive)
        self.bin = 15 * MINUTE
        self.histograms = []
        self.days_ago = 90

    def get_histogram(self, name):
        for h in self.histograms:
            if h.get_name() == name:
                return h
        h = histogram.Ntuple(name, name)
        h.set_time_axis(True)
        h.set_marker_type("impulses")
#        h.set_marker_type("points")
        h.set_time_axis_format("%m-%d")
        self.histograms.append(h)
        h.set_ylabel("Number in Use")
        h.set_xlabel("month-day")
        return h

    #######################################################################
    # The following functions must be defined by all plotting modueles.
    #######################################################################

    def book(self, frame):
        if self.get_parameter("days_ago"):
            self.days_ago = self.get_parameter("days_ago")
        now_time = time.time()
        Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)
        self.end_day = time.mktime((Y, M, D, 23, 52, 30, wd, jd, dst))
        self.start_day = self.end_day - self.days_ago * MINUTE * 24
        self.nbins = int((self.end_day - self.start_day) / self.bin)

        # Get cron directory information.
        cron_dict = frame.get_configuration_client().get("crons", {})

        # Pull out just the information we want.
        #self.temp_dir = cron_dict.get("tmp_dir", "/tmp")
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
            enstore_constants.ACCOUNTING_SERVER, 5, 2)
        db = pg.DB(host=acc.get('dbhost', 'localhost'),
                   dbname=acc.get('dbname', 'accounting'),
                   port=acc.get('dbport', 5432),
                   user=acc.get('dbuser', 'enstore'))

        #now_time  = time.time()
        #then_time = now_time - self.days_ago*24*3600
        db.query("begin")
        db.query("declare rate_cursor cursor for select to_char(time,'YYYY-MM-DD HH24:MI:SS'), type, total, busy, tape_library, storage_group \
        from drive_utilization  where time between '%s' and '%s'" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.start_day)),
                                                                     time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.end_day))))
        while True:
            res = db.query("fetch 10000 from rate_cursor").getresult()
            for row in res:
                #                lib=row[4].replace(" ","_").replace("/","")
                lib = library_name_map.get(
                    row[4], row[4].replace(
                        " ", "_").replace(
                        "/", ""))
                lib_type = type_name_map.get(
                    row[1], row[1].replace(
                        " ", "_").replace(
                        "/", ""))
                sg = row[5]
                if (sg is None):
                    continue
                if (sg == "cms"):
                    h = self.get_histogram(
                        "%s-%s-%s-Utilization" %
                        (lib, lib_type, sg))
                    h.get_data_file().write("%s %d\n" % (row[0], row[3]))
            l = len(res)
            if (l < 10000):
                break
        db.close()

        db = pg.DB(host=acc.get('dbhost', 'localhost'),
                   dbname=acc.get('dbname', 'accounting'),
                   port=acc.get('dbport', 5432),
                   user=acc.get('dbuser', 'enstore'))

        db.query("begin")
        db.query("declare rate_cursor cursor for select to_char(time,'YYYY-MM-DD HH24:MI:SS'), type,  sum(busy), tape_library \
        from drive_utilization  where time between '%s' and '%s' and storage_group!='cms' group by time, type,tape_library" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.start_day)),
                                                                                                                               time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.end_day))))
        while True:
            res = db.query("fetch 10000 from rate_cursor").getresult()
            for row in res:
                #                lib=row[3].replace(" ","_").replace("/","")
                lib = library_name_map.get(
                    row[3], row[3].replace(
                        " ", "_").replace(
                        "/", ""))
                lib_type = type_name_map.get(
                    row[1], row[1].replace(
                        " ", "_").replace(
                        "/", ""))
                h = self.get_histogram(
                    "%s-%s-%s-Utilization" %
                    (lib, lib_type, "OTHER"))
                h.get_data_file().write("%s %d\n" % (row[0], row[2]))
            l = len(res)
            if (l < 10000):
                break
        db.close()

        db = pg.DB(host=acc.get('dbhost', 'localhost'),
                   dbname=acc.get('dbname', 'accounting'),
                   port=acc.get('dbport', 5432),
                   user=acc.get('dbuser', 'enstore'))

        db.query("begin")
        db.query("declare rate_cursor cursor for select to_char(time,'YYYY-MM-DD HH24:MI:SS'), type,  sum(busy), tape_library \
        from drive_utilization  where time between '%s' and '%s' group by time, type,tape_library" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.start_day)),
                                                                                                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.end_day))))
        while True:
            res = db.query("fetch 10000 from rate_cursor").getresult()
            for row in res:
                #                lib=row[3].replace(" ","_").replace("/","")
                lib = library_name_map.get(
                    row[3], row[3].replace(
                        " ", "_").replace(
                        "/", ""))
                lib_type = type_name_map.get(
                    row[1], row[1].replace(
                        " ", "_").replace(
                        "/", ""))
                h = self.get_histogram(
                    "%s-%s-%s-Utilization" %
                    (lib, lib_type, "ALL"))
                h.get_data_file().write("%s %d\n" % (row[0], row[2]))
            l = len(res)
            if (l < 10000):
                break
        db.close()

        for h in self.histograms:
            h.get_data_file().close()

    def plot(self):
        for h in self.histograms:
            #
            # crazy hack to plot only non-empty histograms
            #
            total = 0.0
            fd = open(h.get_data_file_name(), "r")
            for l in fd:
                d, t, b = string.split(l, " ")
                total = total + float(b)
            if total > 0:
                h.set_marker_type("impulses")
                h.plot("1:3", directory=self.web_dir)
