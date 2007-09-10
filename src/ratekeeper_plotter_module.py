#!/usr/bin/env python

import pg
import os
import time
import tempfile
import string
import sys

import enstore_plotter_framework
import enstore_plotter_module
import enstore_constants
import makeplot

BP15S_TO_TBPD = 5.7e-09

class RateKeeperPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)
        self.time_in_days=2
        self.smooth_num=40

    def book(self,frame):
        
        if self.get_parameter("smooth_num"):
            self.smooth_num=self.get_parameter("smooth_num")
        if self.get_parameter("time_in_days"):
            self.time_in_days=self.get_parameter("time_in_days")
    def fill(self,frame):
        
       #  here we create data points 
        
        acc = frame.get_configuration_client().get(enstore_constants.ACCOUNTING_SERVER, {})
        db = pg.DB(host  = acc.get('dbhost', "localhost"),
                   dbname= acc.get('dbname', "accounting"),
                   port  = acc.get('dbport', 5432),
                   user  = acc.get('dbuser', "enstore"))
        now_time  = time.time()
        then_time = now_time - self.time_in_days*24*3600
        db.query("begin");
        db.query("declare rate_cursor cursor for select to_char(time,'MM-DD-YYYY HH24:MI:SS'), read, write, read_null, \
        write_null from rate where time between '%s' and '%s'"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(then_time)),
                                                                time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(now_time))))

        self.data_file=open("ratekeeper.pts","w")
        c = 0
        tr, tw = 0.0, 0.0
        tnr, tnw = 0.0, 0.0

        while True:
            res =  db.query("fetch 10000 from rate_cursor").getresult()
            for row in res:
                date = row[0]
                tr = tr + float(row[1])
                tw = tw + float(row[2])
                tnr = tnr + float(row[3])
                tnw = tnw + float(row[4])
                c = (c+1)%self.smooth_num
                n = self.smooth_num
                if not c:
                    self.data_file.write("%s %s %s %s %s %s %s\n" %
                                         (date,
                                          tr / n * BP15S_TO_TBPD,
                                          tw / n * BP15S_TO_TBPD,
                                          (tr+tw) / n * BP15S_TO_TBPD,
                                          tnr / n * BP15S_TO_TBPD, 
                                          tnw / n * BP15S_TO_TBPD,
                                          (tnw+tnr) / n * BP15S_TO_TBPD))
                    tr, tw = 0.0, 0.0
                    tnr, tnw = 0.0, 0.0


            l=len(res)
            if (l < 10000):
                break

        db.close()
        self.data_file.close()
        ratekeep = frame.get_configuration_client().get('ratekeeper', timeout=15, retry=3)
        
    def plot(self):
        
        groups = ["real", "null"] #Order matters here!
        sst = {'null':"(only null movers)", 'real':"(no null movers)"}
        ps_filename = {}
        jpg_filename = {}
        jpg_stamp_filename = {}
        plot_filename = {}

        log_dir, tmp_dir, sys_name, rate_nodes, ps_filename_template, \
                 jpg_filename_template, jpg_stamp_filename_template = makeplot.get_rate_info()

        
        
        for group in groups:
            ps_filename[group] = string.replace(ps_filename_template, '*',
                                                group + "_")
            jpg_filename[group] = string.replace(jpg_filename_template, '*',
                                                 group + "_")
            jpg_stamp_filename[group] = string.replace(jpg_stamp_filename_template,
                                                   '*', group + "_")

            
        tempfile.tempdir = tmp_dir
        
        for group in groups:
            plot_filename[group] = tempfile.mktemp(".plot")

        for group in groups:
            #Write the gnuplot command file(s).
            try:
                plot_file = open(plot_filename[group], "w+")
                makeplot.write_plot_file(sys_name, self.data_file.name, plot_file,
                                ps_filename[group], group, groups.index(group),
                                sst[group])
                plot_file.close()     #Close this file so gnuplot can read it.
            except (OSError, IOError):
                exc, msg = sys.exc_info()[:2]
                try:
                    sys.stderr.write("Unable to write plot file: %s: %s\n" %
                                     (str(exc), str(msg)))
                    sys.stderr.flush()
                except IOError:
                    pass
                sys.exit()

            os.system("gnuplot < %s" % plot_filename[group])

            os.system("convert -rotate 90  %s %s\n" % (ps_filename[group],
                                                       jpg_filename[group]))
            os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                      % (ps_filename[group], jpg_stamp_filename[group]))

            #Close the files.
            for group in groups:
                try:
                    os.remove(plot_filename[group])
                except:
                    pass
