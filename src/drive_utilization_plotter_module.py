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
import histogram

MINUTE=3600L

class DriveUtilizationPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)
        self.bin=15*MINUTE
        self.histograms=[]
        self.days_ago=90

    def get_histogram(self,name) :
        for h in self.histograms:
            if h.get_name() == name:
                return h
        h=histogram.Ntuple(name,name)
        h.set_time_axis(True)
        h.set_marker_type("impulses")
#        h.set_marker_type("points")
        h.set_time_axis_format("%m-%d")
        self.histograms.append(h)
        h.set_ylabel("Number in Use")
        h.set_xlabel("month-day")
        return h
    
    def book(self,frame):
        if self.get_parameter("days_ago"):
            self.days_ago=self.get_parameter("days_ago")
        now_time    = time.time()
        Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)
        self.end_day = time.mktime((Y, M, D, 23, 52, 30, wd, jd, dst))
        self.start_day = self.end_day - self.days_ago*MINUTE*24;
        self.nbins=int((self.end_day-self.start_day)/self.bin)

    def fill(self,frame):
        
       #  here we create data points 
        
        acc = frame.get_configuration_client().get(enstore_constants.ACCOUNTING_SERVER, {})
        db = pg.DB(host  = acc.get('dbhost', 'localhost'),
                   dbname= acc.get('dbname', 'accounting'),
                   port  = acc.get('dbport', 5432),
                   user  = acc.get('dbuser', 'enstore'))
        self.install_dir = frame.get_configuration_client().get(enstore_constants.INQUISITOR, {}).get('html_file','./')

        now_time  = time.time()
        then_time = now_time - self.days_ago*24*3600
        db.query("begin");
        db.query("declare rate_cursor cursor for select to_char(time,'YYYY-MM-DD HH24:MI:SS'), type, total, busy, tape_library \
        from drive_utilization  where time between '%s' and '%s'"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(self.start_day)),
                                                                   time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(self.end_day))))
        while True:
            res =  db.query("fetch 10000 from rate_cursor").getresult()
            for row in res:
                lib=row[4].replace(" ","_").replace("/","")
                type=row[1].replace(" ","_").replace("/","")
                h=self.get_histogram("%s:%s"%(lib,type))
                h.get_data_file().write("%s %d\n"%(row[0],row[3]))

            l=len(res)
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
            total=0.0
            fd = open(h.get_data_file_name(),"r")
            for l in fd:
                d,t,b=string.split(l," ");
                total=total+float(b)
            if total>0 :
                h.plot("1:3")
                os.system("mv %s.ps %s"%(h.name,self.install_dir))
                os.system("mv %s_stamp.jpg %s"%(h.name,self.install_dir))
                os.system("mv %s.jpg %s"%(h.name,self.install_dir))
