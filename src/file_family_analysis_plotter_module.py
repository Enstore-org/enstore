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

# enstore imports
import enstore_plotter_module
import histogram
import enstore_constants

WEB_SUB_DIRECTORY = enstore_constants.FILE_FAMILY_ANALYSIS_PLOT_SUBDIR


class FileFamilyAnalysisPlotterModule(
        enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(
            self, name, isActive)

    #######################################################################
    # The following functions must be defined by all plotting modueles.
    #######################################################################

    def book(self, frame):
        # Get cron directory information.
        cron_dict = frame.get_configuration_client().get("crons", {})

        # Pull out just the information we want.
        #temp_dir = cron_dict.get("tmp_dir", "/tmp")
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

        edb = frame.get_configuration_client().get('database', {})
        db = pg.DB(host=edb.get('dbhost', "localhost"),
                   dbname=edb.get('dbname', "enstoredb"),
                   port=edb.get('dbport', 5432),
                   user=edb.get('dbuser', "enstore"),
                   )

        sql_cmd = "select distinct(storage_group) from volume;"

        res = db.query(sql_cmd)

        # Remember the list of storage groups to pass to plot().
        self.storage_groups = []
        for row in res.getresult():
            if not row:
                continue
            self.storage_groups.append(row[0])

        now_time = time.time()
        start_time = now_time - 365 * 3600 * 24  # One year ago.

        # Execute the fill() function for each histogram.
        self.histograms = []
        for sg in self.storage_groups:
            h1 = histogram.Histogram1D(sg, "%s tape occupancy" % (sg),
                                       1000, 0, 100)
            h1.set_logy(True)
            h1.set_ylabel("Number of Volumes / %s" % (h1.get_bin_width(0)))
            h1.set_xlabel("Fill Fraction")
            h1.set_marker_type("impulses")
            h1.set_opt_stat(True)

            h2 = histogram.Histogram1D("%s_active" % sg,
                                       "%s active volumes vs last access time" % (
                                           sg),
                                       120, float(start_time), float(now_time))
            h2.set_ylabel("Number of active volumes ")
            h2.set_xlabel("Date")
            h2.set_time_axis(True)
            h2.set_marker_type("points")

            h3 = histogram.Histogram1D("%s_time" % sg,
                                       "%s tape occupancy  vs last access time" % (
                                           sg),
                                       120, float(start_time), float(now_time))
            h3.set_ylabel("Fill Fraction")
            h3.set_xlabel("Date")
            h3.set_time_axis(True)
            h3.set_profile(True)
            h3.set_marker_type("points")

            select_stmt = "select last_access,system_inhibit_1,(1.-remaining_bytes/1024./1024./1024. / (capacity_bytes/1024./1024./1024))*100 as percentage from volume where file_family!='none' and label not like '%deleted' and  capacity_bytes>0 and (1.-remaining_bytes/1024./1024./1024. / (capacity_bytes/1024./1024./1024))>0 and last_access>'1970-12-31 17:59:59' and storage_group='" + sg + "'"
            res = db.query(select_stmt)
            for row in res.getresult():
                if not row:
                    continue
                h1.fill(row[2])
                if (row[1] == 'none'):
                    h2.fill(time.mktime(time.strptime(row[0],
                                                      '%Y-%m-%d %H:%M:%S')))
                    h3.fill(time.mktime(time.strptime(row[0],
                                                      '%Y-%m-%d %H:%M:%S')),
                            row[2])

            # Add this to the list to pass to plot().
            self.histograms.append(h1)
            self.histograms.append(h2)
            self.histograms.append(h3)

        # Close these to avoid resource leaks.
        db.close()

    def plot(self):
        for hist in self.histograms:
            # crazy hack to plot only non-empty histograms
            # if (hist.n_entries() > 0):
            hist.plot(directory=self.web_dir)
