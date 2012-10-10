#!/usr/bin/env python

###############################################################################
#
# $Id$
# Plot Small Files Aggregation Statistics
#
###############################################################################

# system imports
import pg
import os
import sys
import time
import types

# enstore imports
import histogram
import enstore_plotter_module
import enstore_constants

WEB_SUB_DIRECTORY = enstore_constants.SFA_STATS_PLOTS_SUBDIR

MB = 1048576L

class SFAStatsPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)

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
        dbInfo = frame.get_configuration_client().get('database', None)
        if dbInfo == None:
            print "No database info"
            sys.exit(1)

        #Open conection to the Enstore DB.
        try:
            # proper default values are supplied by edb.FileDB constructor
            self.db = pg.DB(host  = dbInfo.get('dbhost', 'localhost'),
                            dbname= dbInfo.get('dbname', 'enstoredb'),
                            port  = dbInfo.get('dbport', 5432),
                            user  = dbInfo.get('dbuser_reader', 'enstore_reader'))
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = str(exc_type)+' '+str(exc_value)+' IS POSTMASTER RUNNING?'
            print message
            print "CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!"
            sys.exit(1)


    def fill(self, frame):
        # Files cached histogram
        #################################################
        files_cached_query = "select count(bfid) , sum(size), cache_mod_time::date from file where cache_status='CACHED' and cache_mod_time  between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP group by cache_mod_time::date order by cache_mod_time::date;"
        res = self.db.query(files_cached_query).getresult()
        # find maximum number of files (res[i][0])
        h = histogram.Ntuple(self.name+ "_files_cached",
                             self.name)
        h.set_time_axis(True)
        h.set_marker_type("impulses")
        h.set_time_axis_format("%m-%d")
        h.set_ylabel("Files Cached")
        h.set_xlabel("month-day")
        max = 0L
        for i in range(len(res)):
            if res[i][0] > max:
                max = res[i][0]
            h.get_data_file().write("%d %f %s\n"%(res[i][0], res[i][1], res[i][2]))
        h.get_data_file().close()
        self.files_cached_histogram = h

        ##################################################

        # Files archived histogram
        #################################################
        files_cached_query = "select count(bfid) , sum(size), archive_mod_time::date from file where archive_status='ARCHIVED' and archive_mod_time  between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP group by archive_mod_time::date order by archive_mod_time::date;"
        res = self.db.query(files_cached_query).getresult()
        # find maximum number of files (res[i][0])
        h = histogram.Ntuple(self.name+ "_files_archived",
                             self.name)
        h.set_time_axis(True)
        h.set_marker_type("impulses")
        h.set_time_axis_format("%m-%d")
        h.set_ylabel("Files Archived")
        h.set_xlabel("month-day")
        max = 0L
        for i in range(len(res)):
            if res[i][0] > max:
                max = res[i][0]
            h.get_data_file().write("%d %f %s\n"%(res[i][0], res[i][1]/MB, res[i][2]))
        h.get_data_file().close()
        self.files_archived_histogram = h

        ##################################################

        # Files purged histogram
        #################################################
        files_cached_query = "select count(bfid) , sum(size), cache_mod_time::date from file where cache_status='PURGED' and cache_mod_time  between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP group by cache_mod_time::date order by cache_mod_time::date;"
        res = self.db.query(files_cached_query).getresult()
        # find maximum number of files (res[i][0])
        h = histogram.Ntuple(self.name+ "_files_purged",
                             self.name)
        h.set_time_axis(True)
        h.set_marker_type("impulses")
        h.set_time_axis_format("%m-%d")
        h.set_ylabel("Files Purged")
        h.set_xlabel("month-day")
        max = 0L
        for i in range(len(res)):
            if res[i][0] > max:
                max = res[i][0]
            h.get_data_file().write("%d %f %s\n"%(res[i][0], res[i][1]/MB, res[i][2]))
        h.get_data_file().close()
        self.files_purged_histogram = h

        ##################################################

        self.db.close()



    def plot(self):
        # Files cached plot
        h = self.files_cached_histogram
        h.set_time_axis(True)
        h.set_marker_type("impulses")
        h.set_line_width(10)
        h.set_time_axis_format("%m-%d")
        h.set_ylabel("Files Cached")
        h.set_xlabel("month-day")
        h.plot("3:1", directory = self.web_dir)

        # Bytes cached plot
        h.set_title("Bytes Cached")
        h.set_name(self.name+ "_bytes_cached")
        h.set_ylabel("Bytes Cached (MB)")
        h.plot("3:2", directory = self.web_dir)
        
        # Files archived plot
        h = self.files_archived_histogram
        h.set_time_axis(True)
        h.set_marker_type("impulses")
        h.set_line_width(10)
        h.set_time_axis_format("%m-%d")
        h.set_ylabel("Files Archived")
        h.set_xlabel("month-day")
        h.plot("3:1", directory = self.web_dir)

        # Bytes archived plot
        h.set_title("Bytes Archived")
        h.set_name(self.name+ "_bytes_archived")
        h.set_ylabel("Bytes Archived (MB)")
        h.plot("3:2", directory = self.web_dir)
        
        # Files purged plot
        h = self.files_purged_histogram
        h.set_time_axis(True)
        h.set_marker_type("impulses")
        h.set_line_width(10)
        h.set_time_axis_format("%m-%d")
        h.set_ylabel("Files Purged")
        h.set_xlabel("month-day")
        h.plot("3:1", directory = self.web_dir)

        # Bytes archived plot
        h.set_title("Bytes Purged")
        h.set_name(self.name+ "_bytes_purged")
        h.set_ylabel("Bytes Purged (MB)")
        h.plot("3:2", directory = self.web_dir)

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
            os.system("convert -rotate 90 -modulate 80 %s %s"
                      % (ps_file_name, jpg_file_name))
            os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(ps_file_name, stamp_jpg_file_name))
            os.unlink(gnu_file_name)


if __name__ == "__main__":
    import configuration_client
    import enstore_functions2
    csc   = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                      enstore_functions2.default_port()))
    pm = SFAStatsPlotterModule("SFAStatistics")
    pm.book(csc)




