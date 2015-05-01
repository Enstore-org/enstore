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
import string

# enstore imports
import enstore_plotter_module
import enstore_constants

WEB_SUB_DIRECTORY = enstore_constants.QUOTA_PLOTS_SUBDIR

"""
In the Accounting DB to create the necessary table:
create table quotas ( time timestamp with time zone DEFAULT CURRENT_TIMESTAMP, library character varying not null, storage_group character varying not null, allocated int, blank int not null, written int not null, requested int null, authorized int null, quota int null,  PRIMARY KEY (time,library,storage_group));
"""

DAYS_AGO = 30*3  #Days in the past to start ploting data from.
DAYS_AHEAD = 30
DAY_IN_SECONDS = 60*60*24 #Seconds in a day.

class QuotasPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)


    #Write out the file that gnuplot will use to plot the data.
    # plot_filename = The file that will be read in by gnuplot containing
    #                 the gnuplot commands.
    # data_filename = The data file that will be read in by gnuplot
    #                 containing the data to be plotted.
    # ps_filename = The postscript file that will be created by gnuplot.
    def write_plot_file(self, plot_filename, data_filename, ps_filename,
                        library, storage_group, allocated, blanks, written,
                        authorized, quota, alarm = ""):

        time_val = time.localtime(time.time() - (DAYS_AGO * DAY_IN_SECONDS))
        last_quarter = time.strftime("%Y-%m-%d %H:%M:%S", time_val)

        time_val = time.localtime(time.time() + (DAYS_AHEAD * DAY_IN_SECONDS))
        next_month = time.strftime("%Y-%m-%d %H:%M:%S", time_val)

        plot_fp = open(plot_filename, "w")

        plot_fp.write("set terminal postscript color solid\n")
        plot_fp.write("set output \"%s\"\n" % (ps_filename,))
        plot_fp.write("set title \"%s %s     Authorized=%s Quota=%s " \
                      "Allocated=%s Written=%s\"\n" \
                      % (library, storage_group, authorized, quota,
                         allocated, written))
        plot_fp.write("set ylabel \"Blank Tapes Left\"\n")
        plot_fp.write("set xdata time\n")
        plot_fp.write("set timefmt \"%s\"\n" % ("%Y-%m-%d %H:%M:%S"))
        plot_fp.write("set format x \"%m-%d-%y\"\n")
        plot_fp.write("set grid\n")
        plot_fp.write("#set key left\n")
        plot_fp.write("set nokey\n")
        plot_fp.write("set label \"Plotted %s\" at graph 1.01,0 rotate " \
                      "font \"Helvetica,10\"\n" % (time.ctime(),))
        plot_fp.write("set label \"%s blanks left\" at graph .1,.85 " \
                      "font  \"Helvetica,20\"\n" % blanks)
        if alarm:
            plot_fp.write("set label \"%s\" at graph .2,.5 " \
                          "font \"Helvetica,50\"\n" % (alarm,))
        plot_fp.write("set xrange ['%s':'%s']\n"
                      % (last_quarter, next_month))
        plot_fp.write("set yrange [0:]\n")
        plot_fp.write("plot \"%s\" using 1:6 title \"Blanks Left\" with impulses\n" \
                      % (data_filename,))
        #plot_fp.write("plot \"%s\" using 1:6 title \"Blanks Left\" with impulses, \"%s\" using 1:7 title \"Tapes Used\" with impulses\n" \
        #              % (data_filename,data_filename))

        plot_fp.close()

    #######################################################################
    # The following functions must be defined by all plotting modueles.
    #######################################################################

    def book(self, frame):
        #Get cron directory information.
        cron_dict = frame.get_configuration_client().get("crons", {})

        #Pull out just the information we want.
        self.temp_dir = cron_dict.get("tmp_dir", "/tmp")
        html_dir = cron_dict.get("html_dir", "")

        #Handle the case were we don't know where to put the output.
        if not html_dir:
            sys.stderr.write("Unable to determine html_dir.\n")
            sys.exit(1)
        self.web_dir = os.path.join(html_dir, WEB_SUB_DIRECTORY)
        if not os.path.exists(self.web_dir):
            os.makedirs(self.web_dir)

    def fill(self, frame):

        #  here we create data points

        edb = frame.get_configuration_client().get("database", {})
        db = pg.DB(host   = edb.get('dbhost', "localhost"),
                   dbname = edb.get('dbname', "enstoredb"),
                   port   = edb.get('dbport', 5432),
                   user   = edb.get('dbuser', "enstore")
                   )

        #Get the current volume count and quota information.  Let's
        # also skip null and deleted volumes.
        sql_cmd = "select v1.library, v1.storage_group," \
                  "count(v1.library) as allocated," \
                  "count(v2.library) as blank," \
                  "count(v3.library) as written," \
                  "q.requested,q.authorized,q.quota,q.significance " \
                  "from volume v1 " \
                  "full join (select * from volume where eod_cookie = 'none')"\
                  "as v2 on v1.id=v2.id " \
                  "full join (select * from volume where eod_cookie != 'none')"\
                  "as v3 on v3.id=v1.id " \
                  "full join (select * from quota) as q on " \
                  "q.library=v1.library and q.storage_group=v1.storage_group" \
                  " where v1.media_type != 'null' and v1.label not like '%.deleted'" \
                  " group by v1.library,v1.storage_group,q.requested," \
                  "q.authorized,q.quota,q.significance;"

        #Get the values from the Enstore DB.
        self.db_result = db.query(sql_cmd).getresult()

        acc = frame.get_configuration_client().get(
            enstore_constants.ACCOUNTING_SERVER, {})
        acc_db = pg.DB(host   = acc.get('dbhost', "localhost"),
                       dbname = acc.get('dbname', "accounting"),
                       port   = acc.get('dbport', 5432),
                       user   = acc.get('dbuser', "enstore")
                       )

        for row in self.db_result:

            lib_sg = (row[0], row[1]) #(library, storage group)

            # 0: library
            # 1: storage group
            # 2: allocated number of volumes
            # 3: blank number of volumes
            # 4: written number of volumes
            # 5: requested number of volumes
            # 6: authorized number of volumes
            # 7: quota - hard limit of number of values allowed
            # 8: significance (y or n)

            library = row[0]
            storage_group = row[1]
            allocated = row[2]
            blank = row[3]
            written = row[4]
            requested = row[5]
            authorized = row[6]
            quota = row[7]

            if requested == None:
                requested = "NULL"
            if authorized == None:
                authorized = "NULL"
            if quota == None:
                quota = "NULL"

            #Form the statement for inserting this moments values into
            # the DB.  Time is set to the current time by default.
            sql_cmd = "insert into quotas " \
                "(library, storage_group, " \
                "allocated, blank, written, requested, authorized, quota) " \
                "values ('%s', '%s', %d, %d, %d, %s, %s, %s)" % \
                (library, storage_group,
                 allocated, blank, written, requested, authorized, quota)

            #Insert the current info into the accounting DB for historical
            # plotting purposes.
            acc_db.query(sql_cmd)

            #Form the statement for obtaining the values to plot.
            sql_cmd = "select * from quotas where " \
                      "time > CURRENT_TIMESTAMP - interval '%d days' " \
                      "and library='%s' and storage_group='%s';" \
                      % (DAYS_AGO, library, storage_group)

            #Get the data from the database.
            res = acc_db.query(sql_cmd).getresult()

            #Write out the datafile.
            pts_filename = os.path.join(self.temp_dir,
                                        "quotas_%s_%s.pts" % tuple(lib_sg))
            pts_fp = open(pts_filename, "w")

            for row in res:
                #Strip off the partial seconds information.
                row_as_list = list(row)
                #First, remove potential parts of seconds.  Then, remove
                # possible timezone adjustments.
                row_as_list[0] = row_as_list[0].split(".")[0]
                row_as_list[0] = string.join(row_as_list[0].split("-")[:3], "-")
                line = string.join(map(str, row_as_list), " ")

                #Write out to the datafile.
                pts_fp.write(line + "\n")

            pts_fp.close()

    def plot(self):

        for row in self.db_result:
            lib_sg = (row[0], row[1]) #(library, storage group)

            plot_filename = os.path.join(self.temp_dir,
                                         "quotas_%s_%s.plot" % lib_sg)
            pts_filename = os.path.join(self.temp_dir,
                                        "quotas_%s_%s.pts" % lib_sg)
            ps_filename = os.path.join(self.web_dir,
                                       "quotas_%s_%s.ps" % lib_sg)
            jpg_filename = os.path.join(self.web_dir,
                                       "quotas_%s_%s.jpg" % lib_sg)
            jpg_filename_stamp = os.path.join(self.web_dir,
                                        "quotas_%s_%s_stamp.jpg" % lib_sg)

            #Get the information.  Some may not be set, handle those
            # fields.
            library = row[0]
            storage_group = row[1]
            allocated = int(row[2])
            blank = int(row[3])
            written = int(row[4])
            #try:   #kept for future use
            #    requested = int(row[5])
            #except (TypeError, IndexError):
            #    requested = None
            try:
                authorized = int(row[6])
            except (TypeError, IndexError):
                authorized = None
            try:
                quota = int(row[7])
            except (TypeError, IndexError):
                quota = None
            #try:  #kept for future use
            #    significance = row[8]
            #except (TypeError, IndexError):
            #    significance = None

            #Is emergency or none the "correct" string?
            if storage_group.find("emergency") != -1 or \
                   storage_group.find("none") != -1:
                if blank == 0:
                    alarm = "NO BLANKS LEFT"
                elif blank == 1:
                    alarm = "1 BLANK LEFT"
                elif blank == 2:
                    alarm = "2 BLANKS LEFT"
                elif written == 0:
                    alarm = "NOTHING WRITTEN"
                elif (blank * 100 / written) <= 5:
                    alarm = "LOW BLANKS"
                else:
                    alarm = ""
            else:
                alarm = ""

            #Make the file that tells gnuplot what to do.
            self.write_plot_file(plot_filename, pts_filename, ps_filename,
                                 library, storage_group,
                                 allocated, blank, written, authorized, quota,
                                 alarm = alarm)

            # make the plot
            rtn = os.system("gnuplot < %s" % (plot_filename,))
            if rtn:
		sys.stderr.write("gnuplot failed\n")
		sys.exit(1)

            # convert to jpeg
            rtn = os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s" %
                      (ps_filename, jpg_filename))
            if rtn:
                    sys.stderr.write("convert failed\n")
                    sys.exit(1)
            rtn = os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s" %
                      (ps_filename, jpg_filename_stamp))
            if rtn:
                    sys.stderr.write("convert failed\n")
                    sys.exit(1)

            #clean up the temporary files.
            try:
                os.remove(plot_filename)
                pass
            except:
                pass
            try:
                os.remove(pts_filename)
                pass
            except:
                pass


