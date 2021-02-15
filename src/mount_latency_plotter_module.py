#!/usr/bin/env python

###############################################################################
#
# purpose: create mount/dismount latency plots per robot, per media type and
#          install them in WEB_SUB_DIRECTORY
# $Id$
#
###############################################################################

import pg
import string
import sys
import os
import time

import enstore_plots
import configuration_client
import enstore_functions2
import enstore_plotter_module
import enstore_constants
import histogram
import enstore_plots

WEB_SUB_DIRECTORY = enstore_constants.MOUNT_LATENCY_SUMMARY_PLOTS_SUBDIR

TIME_CONDITION = " current_timestamp - '1 mons'::interval "

# SELECT_STATEMENT="select start, to_char(finish-start, 'HH24:MI:SS') as
# latency "+\

SELECT_STATEMENT = "select start, finish " +\
    "from tape_mounts where start > " + TIME_CONDITION +\
    " and state='%s' and type = '%s' and logname in ('%s') " +\
    "order by to_char(start, 'YY-MM-DD')"

SELECT_ALL_MOUNTS = "select start, finish " +\
    "from tape_mounts where start > " + TIME_CONDITION +\
    " and state='M' and type = '%s' " +\
    "order by to_char(start, 'YY-MM-DD')"

SELECT_ALL_DISMOUNTS = "select start, finish " +\
    "from tape_mounts where start > " + TIME_CONDITION +\
    " and state='D' and type = '%s' " +\
    "order by to_char(start, 'YY-MM-DD')"


class MountLatencyPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(
            self, name, isActive)
        self.types = []         # this array will holds dictionary
        # with media types returned from accounting db
        self.mc_mover_map = {}  # this dictionary
        # hold media changer [list of mover] map
        self.db = None
        self.ntuples = {}

    def book(self, frame):
        #
        # this handles destination directory creation if needed
        #
        cron_dict = frame.get_configuration_client().get("crons", {})
        self.html_dir = cron_dict.get("html_dir", "")
        self.plot_dir = os.path.join(self.html_dir,
                                     enstore_constants.PLOTS_SUBDIR)
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)
        self.web_dir = os.path.join(self.html_dir, WEB_SUB_DIRECTORY)
        if not os.path.exists(self.web_dir):
            os.makedirs(self.web_dir)

    def fill(self, frame):
        csc = frame.get_configuration_client()
        acc = csc.get(enstore_constants.ACCOUNTING_SERVER)
        self.db = pg.DB(host=acc.get('dbhost', 'localhost'),
                        dbname=acc.get('dbname', 'accounting'),
                        port=acc.get('dbport', 5432),
                        user=acc.get('dbuser_reader', 'enstore_reader'))
        #
        # get list of media changers
        #
        mcs = csc.get_media_changers()
        #
        # get all movers
        #
        mover_list = csc.get_movers(None)
        #
        # fill { "media changer" : ["mover1","mover2"] } map of movers
        # belonging to this media changer
        #
        for mc in mcs:
            movers = []
            mc_name = "%s.media_changer" % (mc,)
            for mover_name in mover_list:
                mover = csc.get(mover_name['mover'])
                if mc_name == mover.get('media_changer'):
                    movers.append(mover.get('logname'))
            self.mc_mover_map[mc] = movers
        #
        # determine existing media types. Time condition is added for economy
        #
        self.types = self.db.query("select distinct type from " +
                                   " tape_mounts where start > " + TIME_CONDITION).dictresult()
        for type in self.types:
            drive_type = type.get('type', None)
            if drive_type == "Unknown":
                continue
            #
            # two plots per drive type
            #
            self.ntuples["%s_%s" % (drive_type, "mount",)] = histogram.Ntuple("mounts_" +
                                                                              drive_type,
                                                                              "%s Mount Latency in Seconds" % (drive_type,))
            self.ntuples["%s_%s" % (drive_type, "dismount",)] = histogram.Ntuple("dismounts_" +
                                                                                 drive_type,
                                                                                 "%s Dismount Latency in Seconds" % (drive_type,))
            for mc in self.mc_mover_map.keys():
                #
                # two plots per drive type
                #
                self.ntuples["%s_%s_%s" % (mc, drive_type, "dismount",)] = histogram.Ntuple("dismounts_" +
                                                                                            mc + "_" +
                                                                                            drive_type,
                                                                                            "%s %s Dismount Latency in Seconds" % (mc, drive_type,))
                self.ntuples["%s_%s_%s" % (mc, drive_type, "mount",)] = histogram.Ntuple("mounts_" +
                                                                                         mc + "_" +
                                                                                         drive_type,
                                                                                         "%s %s Mount Latency in Seconds" % (mc, drive_type,))
        for type in self.types:
            drive_type = type.get('type', None)
            if drive_type == "Unknown":
                continue
            if drive_type:
                #
                # this piece is moved from inquisitor_plots. It plots total mount latencies
                # per media type
                #
                q = SELECT_ALL_MOUNTS % (drive_type,)
                self.__fill_ntuple(q, drive_type)
                q = SELECT_ALL_DISMOUNTS % (drive_type,)
                self.__fill_ntuple(q, drive_type, "dismount")
                #
                # now plot mount/dismounts per tape library per media type
                #
                for mc in self.mc_mover_map.keys():
                    movers = self.mc_mover_map.get(mc)
                    #
                    # plot Mount latencies per robot library per media type
                    #
                    q = SELECT_STATEMENT % (
                        "M", drive_type, string.join(movers, "','"),)
                    self.__fill_ntuple(q, drive_type, mc=mc)
                    #
                    # plot Dismount latencies  per robot library per media type
                    #
                    q = SELECT_STATEMENT % (
                        "D", drive_type, string.join(movers, "','"),)
                    self.__fill_ntuple(q, drive_type, mount="dismount", mc=mc)
        self.db.close()

    def plot(self):
        for ntuple in self.ntuples.values():
            ntuple.get_data_file().close()
            if not ntuple.get_entries():
                #
                # remove stale plots. Stale plot - plot w/o entries.
                #
                plot_name = os.path.join(self.web_dir, ntuple.get_name())
                cmd = "rm -f %s*" % (plot_name,)
                os.system(cmd)
                continue
            ntuple.set_logy()
            ntuple.set_time_axis()
            ntuple.set_time_axis_format("%y-%m-%d")
            if ntuple.get_name().find("dismount") != -1:
                ntuple.set_ylabel("Dismount Latency [seconds]")
            else:
                ntuple.set_ylabel("Mount Latency [seconds]")
            ntuple.set_xlabel("Date year-month-day")
            ntuple.plot("1:3", directory=self.web_dir)

    def __fill_ntuple(self, q, drive_type, mount="mount", mc=None):
        #
        # utility function to cut down on code duplication
        #
        latencies = self.db.query(q).dictresult()
        tag = "%s_%s" % (drive_type, mount,)
        if mc:
            tag = "%s_%s_%s" % (mc, drive_type, mount,)
        if len(latencies) > 0:
            for name in self.ntuples.keys():
                if name == tag:
                    ntuple = self.ntuples.get(name, None)
                    if ntuple:
                        ntuple.entries = len(latencies)
                        for datum in latencies:
                            start = time.mktime(time.strptime(
                                datum["start"], '%Y-%m-%d %H:%M:%S'),)
                            finish = time.mktime(time.strptime(
                                datum["finish"], '%Y-%m-%d %H:%M:%S'),)
                            ntuple.get_data_file().write("%s %f\n" % (datum["start"],
                                                                      finish - start,))
