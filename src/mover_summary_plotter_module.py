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
import types
import histogram
# enstore imports
import enstore_plotter_module
import enstore_constants

WEB_SUB_DIRECTORY = enstore_constants.MOVER_SUMMARY_PLOTS_SUBDIR

MB = 1048576L

Q = "select date, drive_rate from encp_xfer where date between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP and mover='%s' and size>536870912" # files greater than 500MiB
Q1 = "select time,write_errors,read_errors from status where time between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP and mover_name='%s' "
SELECT_MIN_MAX = "select min(drive_rate),max(drive_rate)  from encp_xfer where date between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP"

class MoverSummaryPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)
        self.mover_list=[]
        self.drive_rate_histograms={}
        self.drive_rate_ntuples={}
        self.drive_error_ntuples={}

    def book(self, frame):
        cron_dict = frame.get_configuration_client().get("crons", {})
        self.html_dir = cron_dict.get("html_dir", "")
        self.plot_dir = os.path.join(self.html_dir,
                                     enstore_constants.PLOTS_SUBDIR)
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)
        self.web_dir = os.path.join(self.html_dir, WEB_SUB_DIRECTORY)
        if not os.path.exists(self.web_dir):
            os.makedirs(self.web_dir)

        mover_list=frame.get_configuration_client().get_movers(None,
                                                               timeout=5,
                                                               retry=3)

        acc = frame.get_configuration_client().get(enstore_constants.ACCOUNTING_SERVER)
        #
        # histograms are booked(aka created) in advance, and then filled. We
        # need to get values for lowest and highest bins first
        #
        db = pg.DB(host  = acc.get('dbhost', 'localhost'),
                   dbname= acc.get('dbname', 'accounting'),
                   port  = acc.get('dbport', 5432),
                   user  = acc.get('dbuser_reader', 'enstore_reader'))

        res = db.query(SELECT_MIN_MAX).getresult()
        if not res[0][0] or not res[0][1]:
            return
        min = res[0][0]/MB
        max = res[0][1]/MB
        min = int(min)
        max = int(max)
        if min == max:
            min = 0
        db.close()

        for mover_name in mover_list:
            mover = frame.get_configuration_client().get(mover_name['mover'])
            mover['name']=mover_name['mover']
            if type(mover['library']) == types.StringType:
                if mover['library'].find("null") != -1 :
                    continue
                if mover['library'].find("disk") != -1 :
                    continue
            skip=False
            if type(mover['library']) == types.ListType:
                for l in mover['library']:
                    if l.find("null") != -1 :
                        skip=True
                        break
                    if l.find("disk") != -1 :
                        skip=True
                        break

            if  skip  :
                continue
            name=mover_name['mover'].split(".")[0]
            self.mover_list.append(mover)
            self.drive_rate_histograms[mover_name['mover']]= histogram.Histogram1D(name+"_drive_rate",
                                                                                   name+" drive_rate",max-min,float(min),float(max))

            self.drive_rate_ntuples[mover_name['mover']]=histogram.Ntuple(name+"_drive_rate_vs_date",
                                                                          name)

            self.drive_error_ntuples[mover_name['mover']]=histogram.Ntuple(name+"_error_vs_date",
                                                                           name)


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
        for mover in self.mover_list:
            e=self.drive_error_ntuples.get(mover['name'])
            q=Q1%(mover['name'])
            res=db.query(q)
            for row in res.getresult():
                e.get_data_file().write("%s %d %d \n"%(row[0],row[1],row[2]))
                e.entries=1
        db.close()



    def plot(self):
        for h in self.drive_rate_histograms.values():
            if not h.get_entries() :
                continue
            h.set_marker_type("impulses")
            h.set_line_width(10)
            h.set_opt_stat()
            h.set_xlabel("drive_rate [MB/s]")
            h.set_ylabel("Entries / 1 [MB/s]")
            h.plot(directory = self.web_dir)

        for h in self.drive_rate_ntuples.values():
            if not h.get_entries() :
                continue
            h.set_time_axis()
            h.set_time_axis_format("%m-%d");
            h.get_data_file().close()
            h.set_opt_stat()
            h.set_ylabel("drive_rate [MB/s]")
            h.set_xlabel("date")
            h.plot("1:3", directory=self.web_dir)

        #
        # to plot errors I need to superimpose two ntuples on the
        # same plot. histogram module does not do it. So use this
        # function:
        self.__plot_errors(self.drive_error_ntuples.values());

    def __plot_errors(self,ntuples):
        for n in ntuples:
            if not n.get_entries() :
                continue
            n.get_data_file().close()
            pts_file_name=n.data_file_name
            name=n.get_name()
            ps_file_name=os.path.join(self.web_dir,name+".ps")
            jpg_file_name=os.path.join(self.web_dir,name+".jpg")
            stamp_jpg_file_name=os.path.join(self.web_dir,name + "_stamp.jpg")
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
            os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                      % (ps_file_name, jpg_file_name))
            os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(ps_file_name, stamp_jpg_file_name))
            os.unlink(gnu_file_name)







