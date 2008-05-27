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
import string
import sys
import types

# enstore imports
import enstore_plotter_module
import enstore_constants
import tapes_burn_rate_plotter_module
import e_errors
import configuration_client

DAYS_IN_MONTH = tapes_burn_rate_plotter_module.DAYS_IN_MONTH
DAYS_IN_WEEK = tapes_burn_rate_plotter_module.DAYS_IN_WEEK

DAY_IN_SECONDS = tapes_burn_rate_plotter_module.DAY_IN_SECONDS

WEB_SUB_DIRECTORY = tapes_burn_rate_plotter_module.WEB_SUB_DIRECTORY

class SummaryBurnRatePlotterModule(#enstore_plotter_module.EnstorePlotterModule,
                                   tapes_burn_rate_plotter_module.TapesBurnRatePlotterModule):
    def __init__(self,name,isActive=True):
        tapes_burn_rate_plotter_module.TapesBurnRatePlotterModule.__init__(self,name,isActive)

        #The keys to this dictionary is media type.  The values are file
        # handles to the pts files written out in the fill() function.
        #self.PLOT_dict = {}

        #Override the tapes_burn_rate default value of empty string.
        self.output_fname_prefix = "ALL_"

    #######################################################################
    # The following functions must be defined by all plotting modules.
    #######################################################################
    
    def book(self, frame):

        #Get cron directory information.
        cron_dict = frame.get_configuration_client().get("crons", {})

        #Pull out just the information we want.
        self.temp_dir = cron_dict.get("tmp_dir", "/tmp")
        #If the output path was included on the command line, use that.
        # Failing that, use the path in the configuration file.
        html_dir = self.get_parameter("web_dir")
        if html_dir == None:
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

        #Get cron directory information.
        config_servers_dict = frame.get_configuration_client().get(
            "known_config_servers", {})
        if not e_errors.is_ok(config_servers_dict):
            sys.stderr.write("Unable to obtain configurtion server list: %s\n" \
                             % (config_servers_dict.get("status", "Unknown")))
            sys.exit(1)
        else:
            del config_servers_dict['status']  #Remove the status element.

        #For debugging only skip the cdf and d0 systems.
        #del config_servers_dict['cdfen']
        #del config_servers_dict['d0en']

        self.MT_dict = {}
        tapes = {}
        self.tape_totals = {}
        bytes_summary = {}
        self.tapes_summary_week = {}
        self.tapes_summary_month = {}
        self.extra_title_info = []
        month_ago = time.time() - DAYS_IN_MONTH * DAY_IN_SECONDS
        week_ago = time.time() - DAYS_IN_WEEK * DAY_IN_SECONDS

        #Loop over all known configuration servers and obtaining information
        # from all of them.
        for name, values in config_servers_dict.items():
            csc = configuration_client.ConfigurationClient(values)
            
            ###
            ### Get information from the Enstore Database.
            ###
            edb = csc.get("database", {})
            try:
                db = pg.DB(host  = edb.get('dbhost', "localhost"),
                           dbname= edb.get('dbname', "enstore"),
                           port  = edb.get('dbport', 5432),
                           user  = edb.get('dbuser_reader', "enstore_reader"))
            except pg.InternalError, msg:
                message = "Unable to contact (%s, %s): %s\n" % \
                          (edb['dbhost'], edb['dbport'], msg)
                sys.stderr.write(message)
                continue

            #Make sure to include a little extra information to name all
            # systems that were successful at being included.
            self.extra_title_info.append(name)

            #Get the unique library and storage group combinations.
            sql_cmd = "select distinct media_type from volume " \
                      " where label not like '%.deleted' and " \
                      " library not like '%shelf';"

            edb_res = db.query(sql_cmd).getresult() #Get the values from the DB.

            for row in edb_res:
                #row[0] is a distinct media_type
                fname = os.path.join(self.temp_dir, "ALL_%s.pts" % (row[0],))
                self.MT_dict[row[0]] = open(fname, "w")            

            #Get the library and storage group for each volume.
            sql_cmd = "select label,media_type from volume; "

            edb_res = db.query(sql_cmd).getresult() #Get the values from the DB.

            #Stuff the tape information into a dictionary.
            for row in edb_res:
                tapes[row[0]] = row[1]

            ###
            ### Get current tape information for all tapes currently in use.
            ###
            
            #Get them for each media type.
            sql_cmd = "select v1.media_type, " \
                      "count(v2.media_type) as blank," \
                      "count(v3.media_type) as written " \
                      "from volume v1 " \
                      "full join (select * from volume where eod_cookie = 'none')"\
                      " as v2 on v1.id=v2.id " \
                      "full join (select * from volume where eod_cookie != 'none')"\
                      " as v3 on v3.id=v1.id " \
                      "group by v1.media_type"

            edb_res = db.query(sql_cmd).getresult() #Get the values from the DB.

            #Store these for use later.  They are used in the plot titles.
            for row in edb_res:
                # row[0] == media_type
                # row[1] == total blank tapes
                # row[2] == total written tapes
                self.tape_totals[row[0]] = (row[1], row[2])


            ###
            ### Get the bytes written for recent write transfers from the
            ### drivestat DB.
            ###
            drs = csc.get(enstore_constants.DRIVESTAT_SERVER, {})
            try:
                db = pg.DB(host  = drs.get('dbhost', "localhost"),
                           dbname= drs.get('dbname', "drivestat"),
                           port  = drs.get('dbport', 5432),
                           user  = drs.get('dbuser_reader', "enstore_reader"))
            except pg.InternalError:
                db = None

            sql_cmd = "select time,tape_volser,mb_user_write,mb_user_read" \
                      " from status where " \
                      "date(time) between" \
                      " date(CURRENT_TIMESTAMP - interval '4 months')" \
                      " and date(CURRENT_TIMESTAMP + interval '34 days') " \
                      "and mb_user_write != 0;"
            
            try:
                #Get the values from the DB.
                drs_res = db.query(sql_cmd).getresult()
            except (pg.InternalError, AttributeError):
                drs_res = []

                
            ##Since the two tables are in seperate databases, we need to join
            ## them here.  This is done while summing the bytes written into
            ## day increments.
            for row in drs_res:
                timestamp = row[0]
                volume = row[1]
                mb_user_write = int(row[2])
                mb_user_read = int(row[3])

                #Get the tapes library and storage group.
                try:
                    media_type = tapes[volume]
                except KeyError:
                    media_type = tapes[volume + ".deleted"]
                #Get the date.
                date = timestamp.split(" ")[0]

                #Update the LM and SG byte counts into day increments.
                mt_summary = bytes_summary.get(media_type, {})
                if mt_summary == {}:
                    bytes_summary[media_type] = {} #Initial value.
                summary = mt_summary.get(date, {'mb_read':0, 'mb_write':0,})
                summary['mb_write'] = summary['mb_write'] + mb_user_write
                summary['mb_read'] = summary['mb_read'] + mb_user_read
                bytes_summary[media_type][date] = summary

                this_timestamp = time.mktime(time.strptime(timestamp,
                                                           "%Y-%m-%d %H:%M:%S"))

                #Get the tapes used values for monthly and weekly for both
                # the library and library w/ storage group.  The length of the
                # dictionaries contains the number of tapes used in a month
                # and week for each lm and lm w/ sg/
                if this_timestamp > month_ago:
                    tape_mt_summary = self.tapes_summary_month.get(media_type, {})
                    if tape_mt_summary == {}:
                        self.tapes_summary_month[media_type] = {}  #Initial value.
                    self.tapes_summary_month[media_type][volume] = 1

                if this_timestamp > week_ago:
                    tape_mt_summary = self.tapes_summary_week.get(media_type, {})
                    if tape_mt_summary == {}:
                        self.tapes_summary_week[media_type] = {}  #Initial value.
                    self.tapes_summary_week[media_type][volume] = 1

        ## Now we can write out the data to the pts files that gnuplot
        ## will plot for us.

        month_ago_date = time.strftime("%Y-%m-%d",
              time.localtime(time.time() - DAYS_IN_MONTH * DAY_IN_SECONDS))
        today_date = time.strftime("%Y-%m-%d",
                                   time.localtime())
        month_ahead_date = time.strftime("%Y-%m-%d",
              time.localtime(time.time() + DAYS_IN_MONTH * DAY_IN_SECONDS))

        for key in self.MT_dict.keys():
            #key should just be the media type here.

            pts_file = self.MT_dict[key]
            
            sum_read = 0
            sum_write = 0
            month_g_bytes = 0
            today_g_bytes = 0

            try:
                use_bytes_summary = bytes_summary[key]
            except KeyError:
                use_bytes_summary = {today_date : {'mb_read':0, 'mb_write':0}}

            #Write out the plot information to the entire librarys' data files.
            summary_keys = use_bytes_summary.keys()
            summary_keys.sort()
            for date in summary_keys:
                stats = use_bytes_summary[date]
                sum_read = sum_read + stats['mb_read']
                sum_write = sum_write + stats['mb_write']

                #Convert from MB to GB.
                current_gb = stats['mb_write'] / 1024.0
                total_gb = sum_write / 1024.0

                #For a month ago and today, plot an extra line conneting them.
                if month_ago_date == date:
                    line = "%s %s %s %s\n" % (
                        date, current_gb, total_gb, total_gb )
                    month_g_bytes = total_gb #Remember this for estimating.
                elif today_date == date:
                    line = "%s %s %s %s\n" % (
                        date, current_gb, total_gb, total_gb)
                    today_g_bytes = total_gb #Remember this for estimating.
                else:
                    line = "%s %s %s\n" % (
                        date, current_gb, total_gb)

                #Write out the information to the correct data file.
                pts_file.write(line)

            #Write out one more line to plot the estimated usage for the
            # next month.
            line = "%s %s %s %s\n" % (
                        month_ahead_date, "skip", "skip",
                        (today_g_bytes - month_g_bytes) + today_g_bytes)
            pts_file.write(line)

            pts_file.close()

            #For compatiblity with system level burn rate plots.
            self.PLOT_dict = self.MT_dict
