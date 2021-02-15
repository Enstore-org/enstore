#!/usr/bin/env python

###############################################################################
#
# purpose: create mount/day plots per tape library and
#          install them in MOUNTS_PER_ROBOT_PLOTS_SUBDIR
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
import e_errors

WEB_SUB_DIRECTORY = enstore_constants.MOUNTS_PER_ROBOT_PLOTS_SUBDIR

TIME_CONDITION = " current_timestamp - '1 mons'::interval "

SELECT_STATEMENT = "select start, finish " +\
    "from tape_mounts where start > " + TIME_CONDITION +\
    " and state='%s' and logname in ('%s') "


class MountsPerRobotPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(
            self, name, isActive)
        self.db = None
        self.ntuples = {}
        self.now_time = time.time()
        self.nbins = 30
        Y, M, D, h, m, s, wd, jd, dst = time.localtime(self.now_time)
        self.now_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
        self.start_time = self.now_time - self.nbins * 3600 * 24
        self.histograms = {}

    def __decorate(self, h, color, ylabel, marker):
        h.set_time_axis(True)
        h.set_ylabel(ylabel)
        h.set_xlabel("Date (year-month-day)")
        h.set_line_color(color)
        h.set_line_width(20)
        h.set_marker_text(marker)
        h.set_marker_type("impulses")

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

    def __fill(self, server_tuple):
        csc = configuration_client.ConfigurationClient(server_tuple)
        acc = csc.get(enstore_constants.ACCOUNTING_SERVER)
        db = pg.DB(host=acc.get('dbhost', 'localhost'),
                   dbname=acc.get('dbname', 'accounting'),
                   port=acc.get('dbport', 5432),
                   user=acc.get('dbuser_reader', 'enstore_reader'))
        #
        # get list of media changers
        #
        mcs = csc.get_media_changers()
        #
        # filter out null and disk
        #
        for m in mcs.keys():
            if m.find('null') != -1 or m.find('disk') != -1:
                del mcs[m]
        #
        # get all movers
        #
        mover_list = csc.get_movers(None)
        #
        # fill { "media changer" : ["mover1","mover2",....] } map of movers
        # belonging to this media changer
        #
        mc_mover_map = {}
        for mc in mcs:
            movers = []
            mc_name = "%s.media_changer" % (mc,)
            for mover_name in mover_list:
                mover = csc.get(mover_name['mover'])
                if mc_name == mover.get('media_changer'):
                    movers.append(mover.get('logname'))
            #
            # MC labelled "SL8500" are actually "SL8500G1"
            #
            if mc == "SL8500":
                mc = "SL8500G1"
            mc_mover_map[mc] = movers

        for mc in mc_mover_map.keys():

            if mc not in self.histograms:
                self.histograms[mc] = histogram.Histogram1D(
                    mc, mc, self.nbins, self.start_time, self.now_time)
            h = self.histograms.get(mc)
            movers = mc_mover_map.get(mc)
            #
            # plot Mounts / day per robot library
            #
            q = SELECT_STATEMENT % ("M", string.join(movers, "','"),)
            results = db.query(q).dictresult()
            for r in results:
                h.fill(
                    time.mktime(
                        time.strptime(
                            r.get('start'),
                            "%Y-%m-%d %H:%M:%S")))
        db.close()

    def fill(self, frame):
        csc = frame.get_configuration_client()
        known_config_servers = csc.get('known_config_servers')
        if known_config_servers.get('status')[0] != e_errors.OK:
            return
        del known_config_servers['status']
        for name, server in known_config_servers.iteritems():
            self.__fill(server)
        return

    def plot(self):
        for h in self.histograms.values():
            self.__decorate(
                h, 1, "Mounts / day", "Total %d" %
                (h.get_entries(),))
            h.plot(directory=self.web_dir)
            h.plot()
