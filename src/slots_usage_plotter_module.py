#!/usr/bin/env python
#
# $Id$ 
#

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


class SlotUsagePlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)
        self.time_in_days=91
        self.smooth_num=40
        self.data_files={}
        self.free={}

    @staticmethod
    def clean_string(var):
        return string.replace(string.replace(var,'/',''),' ','-')
    
    def book(self,frame):
        
        if self.get_parameter("smooth_num"):
            self.smooth_num=self.get_parameter("smooth_num")
        if self.get_parameter("time_in_days"):
            self.time_in_days=self.get_parameter("time_in_days")
        inq =  frame.get_configuration_client().get(enstore_constants.INQUISITOR, {})
        self.dest_dir=inq.get('html_file','')
    def fill(self,frame):
        self.data=[]
        acc = frame.get_configuration_client().get(enstore_constants.ACCOUNTING_SERVER, {})
        db = pg.DB(host  = acc.get('dbhost', "localhost"),
                   dbname= acc.get('dbname', "accounting"),
                   port  = acc.get('dbport', 5432),
                   user  = acc.get('dbuser', "enstore")
                   )

        for row in db.query("select distinct tape_library,location,media_type from tape_library_slots_usage").getresult():
            if not row :
                continue
            self.data.append(self.clean_string(row[0])+"_"+self.clean_string(row[1])+"_"+self.clean_string(row[2]))

        for d in self.data:
            self.data_files[d] = open("/tmp/"+d+".pts",'w')

        now_time  = time.time()
        then_time = now_time - self.time_in_days*24*3600
        db.query("begin");
        db.query("declare rate_cursor cursor for select to_char(time,'MM-DD-YYYY HH24:MI:SS'),tape_library,location,media_type,\
                  total,free,used    from  tape_library_slots_usage\
                  where time between '%s' and '%s'"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(then_time)),
                                                     time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(now_time))))
        while True:
            res =  db.query("fetch 10000 from rate_cursor").getresult()
            for row in res:
                if not row: continue;
                date = row[0]
                key=self.clean_string(row[1])+"_"+self.clean_string(row[2])+"_"+ self.clean_string(row[3])
                fd=self.data_files[key]
                fd.write("%s %s %s %s\n"%(date,row[4],row[5],row[6],))
                self.free[key]=row[5]
            l=len(res)
            if (l < 10000):
                break
        db.close()

        for fd in self.data_files.values():
            fd.close()

        for d in self.data:
            ddd = "/tmp/"+d+".plot"
            fd = open(ddd,"w")
            fd.write("set output \"/tmp/%s.ps\"\n"%(d,))
            fd.write("set terminal postscript color solid\n")
            fd.write("set title \"Used Slots in %s\"\n"%(d,));
            fd.write("set xlabel \"Date\"\n")
            fd.write("set timefmt \"%m-%d-%Y %H:%M:%S\"\n")
            fd.write("set xdata time\n")
            fd.write("set ylabel \"Number Used\"\n")
            fd.write("set grid \n")
            fd.write("set yrange [0: ]\n")
            fd.write("set format x \"%m-%d\"\n")
            fd.write("set nokey \n")
            fd.write("set label \"Plotted `date` \" at graph .99,0 rotate font \"Helvetica,10\"\n")
            fd.write("set label \"%d Free\" at graph .2,.9 font \"Helvetica,80\"\n"%(self.free[d],))
            fd.write("plot \"%s.pts\" using 1:3 w impulses linetype 2, \"%s.pts\" using 1:5 t \"Used Slots\" w impulses linetype 1"%(d,d,))
            fd.close()

    def plot(self):
        for d in self.data:
            pf = "/tmp/"+d+".plot"
            cmd="gnuplot < %s" % pf
            os.system(cmd)
            os.system("convert -rotate 90  /tmp/%s.ps /tmp/%s.jpg\n" % (d,d,))
            os.system("convert  -geometry 120x120 -modulate 80 /tmp/%s.jpg /tmp/%s_stamp.jpg\n"          % (d,d,))
            os.system("mv -f /tmp/%s.ps %s"%(d,self.dest_dir,))
            os.system("mv -f /tmp/%s.jpg %s"%(d,self.dest_dir,))
            os.system("mv -f /tmp/%s_stamp.jpg %s"%(d,self.dest_dir,))
            os.remove("/tmp/%s.plot"%(d,))

            #Close the files.

