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
import types
import histogram
# enstore imports
import enstore_plotter_module
import enstore_constants
import mover_client
import pg 

WEB_SUB_DIRECTORY = enstore_constants.BYTES_PER_DAY_PLOTS_SUBDIR

DAYS_IN_MONTH = 30
DAYS_IN_WEEK = 7
SECONDS_IN_DAY = 86400   #Seconds in one day. (24*60*60)

MONTH_AGO = time.strftime("%m-%d-%Y",
                 time.localtime(time.time() - DAYS_IN_MONTH * SECONDS_IN_DAY))
WEEK_AGO = time.strftime("%m-%d-%Y",
                 time.localtime(time.time() - DAYS_IN_MONTH * SECONDS_IN_DAY))

DAYS_AGO_START = DAYS_IN_MONTH * 4  #4 months ago to start drawing the plot.
DAYS_AHEAD_END = DAYS_IN_MONTH    #One month to plot ahead.

#Set some sane limits to these values.
if DAYS_AGO_START < DAYS_IN_MONTH:
    DAYS_AGO_START = DAYS_IN_MONTH
if DAYS_AHEAD_END < 0:
    DAYS_AHEAD_END = 0

MB = 1048576L

Q = "select date, drive_rate from encp_xfer where date between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP and mover='%s' and size>2147483648"
Q1 = "select time,write_errors,read_errors from status where time between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP and  host='%s' "

class MoverSummaryPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)
        self.mover_list=[]
        self.drive_rate_histograms={}
        self.drive_rate_ntuples={}
        self.drive_error_ntuples={}

    def fill(self, frame):
        acc = frame.get_configuration_client().get(enstore_constants.ACCOUNTING_SERVER)
        db = pg.DB(host  = acc.get('dbhost', 'localhost'),
                   dbname= acc.get('dbname', 'accounting'),
                   port  = acc.get('dbport', 5432),
                   user  = acc.get('dbuser_reader', 'enstore_reader'))
        for mover in self.mover_list:
            h=self.drive_rate_histograms.get(mover['name'])
            n=self.drive_rate_ntuples.get(mover['name'])
            e=self.drive_error_ntuples.get(mover['name'])
            q=Q%(mover['name'],)
            res=db.query(q)
            for row in res.getresult():
                h.fill(row[1]/float(MB))
                n.get_data_file().write("%s %f \n"%(row[0],row[1]/float(MB)))
                n.entries=1
        db.close()


        drv = frame.get_configuration_client().get(enstore_constants.DRIVESTAT_SERVER)
        db = pg.DB(host  = drv.get('dbhost', 'localhost'),
                   dbname= drv.get('dbname', 'drivestat'),
                   port  = drv.get('dbport', 5432),
                   user  = drv.get('dbuser_reader', 'enstore_reader'))
        print drv
        for mover in self.mover_list:
            e=self.drive_error_ntuples.get(mover['name'])
            q=Q1%(mover['host'])
            res=db.query(q)
            for row in res.getresult():
                e.get_data_file().write("%s %d %d \n"%(row[0],row[1],row[2]))
                e.entries=1
        db.close()




    def plot(self):
        for h in self.drive_rate_histograms.values():
            if not h.get_entries() : continue
            h.set_opt_stat()
            h.set_xlabel("drive_rate [MB/s]")
            h.set_ylabel("Entries / 1 [MB/s]")
            h.plot()

        for h in self.drive_rate_ntuples.values():
            if not h.get_entries() : continue
            h.set_time_axis()
            h.set_time_axis_format("%m-%d");
            h.get_data_file().close()
            h.set_opt_stat()
            h.set_ylabel("drive_rate [MB/s]")
            h.set_xlabel("date")
            h.plot("1:3")

        self.plot_errors(self.drive_error_ntuples.values());

    def plot_errors(self,ntuples):
        for n in ntuples:
            if not n.get_entries() : continue
            pts_file_name=n.data_file_name
            name=n.get_name()
            ps_file_name=name+".ps"
            jpg_file_name=name+".jpg"
            gnu_file_name=name+"_gnuplot.cmd"
            gnu_cmd = open(gnu_file_name,'w')
            long_string="set output '" + ps_file_name + "'\n"+ \
                         "set terminal postscript color solid\n"\
                         "set title '"+name+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                         "set xrange [ : ]\n"+ \
                         "set size 1.5,1\n"+ \
                         "set grid\n"+ \
                         "set ylabel '# Errors'\n"+ \
                         "set xlabel 'date'\n"+ \
                         "set xdata time\n"+ \
                         "set timefmt \"%Y-%m-%d %H:%M:%S\"\n"+ \
                         "#set key outside\n"+\
                         "set format x \"%m-%d\"\n"
            long_string = long_string+"plot "
            long_string=long_string+"'"+pts_file_name+"' using 1:3 t 'write' with points lt 1 , '"+pts_file_name+"'  using 1:4 t 'read' with points lt 3"
            gnu_cmd.write(long_string)
            gnu_cmd.close()
            os.system("gnuplot %s"%(gnu_file_name))
            os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(ps_file_name, jpg_file_name))
            os.unlink(gnu_file_name)


    def book(self, frame):
        mover_list=frame.get_configuration_client().get_movers(None,
                                                               timeout=5,
                                                               retry=3)
        for mover_name in mover_list:
            mover = frame.get_configuration_client().get(mover_name['mover'])
            mover['name']=mover_name['mover']
            if mover_name['mover'] != 'LTO4_99.mover' : continue
            print mover_name
            print mover
            if type(mover['library']) == types.StringType:
                if mover['library'].find("null") != -1 : continue
                if mover['library'].find("disk") != -1 : continue
            skip=False
            if type(mover['library']) == types.ListType:
                for l in mover['library']:
                    if l.find("null") != -1 :
                        skip=True
                        break
                    if l.find("disk") != -1 :
                        skip=True
                        break

            if  skip  : continue
            self.mover_list.append(mover)
            self.drive_rate_histograms[mover_name['mover']]= histogram.Histogram1D(mover_name['mover']+"_drive_rate",
                                                                                   mover_name['mover']+" drive_rate",100,0,100)
                
            self.drive_rate_ntuples[mover_name['mover']]=histogram.Ntuple(mover_name['mover']+"_drive_rate_vs_date",
                                                                          mover_name['mover'])

            self.drive_error_ntuples[mover_name['mover']]=histogram.Ntuple(mover_name['mover']+"_drive_error_vs_date",
                                                                          mover_name['mover'])
            
            
        
