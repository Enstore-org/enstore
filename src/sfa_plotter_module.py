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

MB = 1048576

class SFAStatsPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)

    def create_histogram(self, name, title, label):
        h = histogram.Ntuple(name, title)
        h.set_time_axis(True)
        h.set_marker_type("impulses")
        h.set_line_width(10)
        h.set_time_axis_format("%m-%d")
        h.set_ylabel(label)
        h.set_xlabel("month-day")
        return h

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
        dbInfo = frame.get_configuration_client().get('database')
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
        self.files_cached_histogram = self.create_histogram(self.name+"_files_cached",
                                                            "Files Cached",
                                                            "Number of Files Cached") 
        self.files_purged_histogram = self.create_histogram(self.name+"_files_purged",
                                                            "Files Purged",
                                                            "Number of Files Purged")
        self.files_archived_histogram = self.create_histogram(self.name+"_files_archived",
                                                              "Files Archived",
                                                              "Number of Files Archived")

        
    def fill(self, frame):
        # Files cached and purged histograms
        #################################################
        hc = self.files_cached_histogram
        hp = self.files_purged_histogram
        files_cached_purged_query = "select cache_status, count(*), sum(size), cache_mod_time::date from file where cache_status in ('PURGED','CACHED') and cache_mod_time>CURRENT_TIMESTAMP - interval '1 mons' group by cache_mod_time::date, cache_status order by cache_mod_time::date;"
        
        res = self.db.query(files_cached_purged_query).getresult()
        cached_data_file = hc.get_data_file()
        purged_data_file = hp.get_data_file()
        for row in res:
            data_file = None
            if row[0] == 'CACHED':
                h = hc
                data_file = cached_data_file
            elif row[0] == 'PURGED':
                h = hp
                data_file = purged_data_file
            if data_file:
                data_file.write("%d %f %s\n"%(row[1], row[2]/MB, row[3]))
                h.entries += 1 # temporary work around for Ntuple
        cached_data_file.close()
        purged_data_file.close()
        
        ##################################################
        # Files archived histogram
        #################################################
        ha = self.files_archived_histogram

        files_archived_query = "select count(bfid) , sum(size), archive_mod_time::date from file where archive_status='ARCHIVED' and archive_mod_time  between CURRENT_TIMESTAMP - interval '1 mons' and CURRENT_TIMESTAMP group by archive_mod_time::date order by archive_mod_time::date;"
        res = self.db.query(files_archived_query).getresult()
        data_file = ha.get_data_file()
        for row in res:
            data_file.write("%d %f %s\n"%(row[0], row[1]/MB, row[2]))
            ha.entries += 1 # temporary work around for Ntuple
        data_file.close()

        self.db.close()



    def plot(self):
        h = self.files_cached_histogram
        if h.n_entries() > 0:
            # Files cached plot
            h.plot("3:1", directory = self.web_dir)
            # Bytes cached plot
            h.set_title("Bytes Cached")
            h.set_name(self.name+ "_bytes_cached")
            h.set_ylabel("Bytes Cached (MB)")
            h.plot("3:2", directory = self.web_dir)
        
        h = self.files_archived_histogram
        if h.n_entries() > 0:
            # Files archived plot
            h.plot("3:1", directory = self.web_dir)
            # Bytes archived plot
            h.set_title("Bytes Archived")
            h.set_name(self.name+ "_bytes_archived")
            h.set_ylabel("Bytes Archived (MB)")
            h.plot("3:2", directory = self.web_dir)
        
        h = self.files_purged_histogram
        if h.n_entries() > 0:
            # Files purged plot
            h.plot("3:1", directory = self.web_dir)
            # Bytes archived plot
            h.set_title("Bytes Purged")
            h.set_name(self.name+ "_bytes_purged")
            h.set_ylabel("Bytes Purged (MB)")
            h.plot("3:2", directory = self.web_dir)



class SFATarRatesPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self, name, isActive=True, date=None, data_file=None):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)
        self.rate_histograms = {}
        self.rate_ntuples = {}
        self.date = date
        self.data_file = data_file

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
        self.csc = frame.get_configuration_client()
        log_server_info = self.csc.get('log_server')
        if log_server_info == None:
            sys.exit("No log server info")

        self.log_file_path = log_server_info.get('log_file_path')
        
    def fill(self, frame):
        self.log_names = []
        # enstore log file name
        if not self.date:
            tm = time.localtime()          # get the local time
            lf_name = 'LOG-%04d-%02d-%02d' % (tm.tm_year, tm.tm_mon, tm.tm_mday)
        else:
            lf_name = 'LOG-%s' % (self.date)
        path = os.path.join(self.log_file_path, lf_name)
        
        if self.data_file:
            # try to append data for the previous day
            yesterday = time.time() - 24*60*60
            date = time.strftime("%Y-%m-%d", time.localtime(yesterday))
            log_file_name = "-".join(("LOG", date))
            log_file_path = os.path.join(self.log_file_path, log_file_name)
            # check if there already are entires for this date in self.data_file
            c = "grep %s %s > /dev/null"%(log_file_name, self.data_file)
            rc = os.system(c)
            if rc != 0:
                # not found
                cmd = "grep -H 'Finished tar to' %s >> %s"%(log_file_path, self.data_file)
                os.system(cmd)
                cmd = "grep -H 'Finished tar to' %s | sed -e 's/.*LOG-//' | sed -e 's/:/ /' | awk '{print $1,$2,$7,$13,$15}' > /tmp/tar_rates"%(self.data_file,)
            else:
                cmd = None
        else:
            # create a temporay file from log log file
            cmd = "grep -H 'Finished tar to' %s | sed -e 's/.*LOG-//' | sed -e 's/:/ /' | awk '{print $1,$2,$7,$13,$15}' > /tmp/tar_rates"%(path,)
        if cmd:
            os.system(cmd)
        
        migrators = self.csc.get_migrators()
        for migrator in migrators:
            # get log names
            migrator_configuration = self.csc.get(migrator, None)
            if migrator_configuration:
                self.log_names.append(migrator_configuration['logname'])
        for log_name in self.log_names:
            cmd = "fgrep %s /tmp/tar_rates > /tmp/%s_tar_rates"%(log_name, log_name)
            os.system(cmd)

            # find min / max
            cmd = "sort -g -k 5 /tmp/%s_tar_rates | sed -n '1p;$p' | awk '{print $5}'> /tmp/%s_min_max"%(log_name, log_name)
            os.system(cmd)
            try:
                mm = open("/tmp/%s_min_max"%(log_name,), 'r')
            except OSError, IOError:
                continue
            try:
                min = float(mm.readline())
                max = float(mm.readline())
            except ValueError:
                continue
            #self.rate_histograms[log_name] = histogram.Ntuple(log_name, "%s Migrator Aggregation Rates"%(log_name,))
            self.rate_histograms[log_name] = histogram.Histogram1D(log_name+"_aggr_rates", "%s Migrator Aggregation Rates"%(log_name,),int(max-min),min,max)
            #self.rate_histograms[log_name].set_time_axis(True)
            self.rate_histograms[log_name].set_marker_type("impulses")
            self.rate_histograms[log_name].set_line_width(10)
            #self.rate_histograms[log_name].set_time_axis_format("%y-%m-%d %H:%M")
            #self.rate_histograms[log_name].set_time_axis_format("%y-%m-%d")
            self.rate_histograms[log_name].set_xlabel("Aggregation Rates [MB/s]")
            self.rate_histograms[log_name].set_ylabel("Entries")
            
            self.rate_ntuples[log_name] = histogram.Ntuple(log_name+"_aggr_rate_vs_date", log_name)
            self.rate_ntuples[log_name].set_time_axis()
            self.rate_ntuples[log_name].set_time_axis_format("%y-%m-%d")
            df= self.rate_ntuples[log_name].get_data_file()
            self.rate_ntuples[log_name].set_opt_stat()
            self.rate_ntuples[log_name].set_ylabel("Aggregation Rates [MB/s]")
            self.rate_ntuples[log_name].set_xlabel("date")

            data_file = self.rate_histograms[log_name].get_data_file()
            in_f = open("/tmp/%s_tar_rates"%(log_name,), 'r')
            ln = in_f.readline()
            while ln:
                a = ln.split(' ')
                self.rate_histograms[log_name].fill(float(a[4]))
                df.write("%s %f \n"%(a[0], float(a[4])))
                self.rate_ntuples[log_name].get_data_file().write("%s %f \n"%(a[0], float(a[4])))
                self.rate_ntuples[log_name].entries += 1
                ln = in_f.readline()
            data_file.close()
                                

    def plot(self):
        for log_name in self.log_names:
            try:
                self.rate_histograms[log_name].plot(directory = self.web_dir)
                self.rate_ntuples[log_name].get_data_file().flush()
                self.rate_ntuples[log_name].get_data_file().close()
                self.rate_ntuples[log_name].plot("1:2",directory = self.web_dir) 
            except:
                pass

