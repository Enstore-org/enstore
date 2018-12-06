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

SECONDS_IN_DAY = tapes_burn_rate_plotter_module.SECONDS_IN_DAY

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

    #Based on the media, get the capacity and media_type of the tapes.
    def get_capacity(self, media_type, db):

        #This is a hack for the ADIC.  Originally, the ADIC didn't know
        # what an LTO tape was, so every LTO1 and LTO2 was inserted as
        # a 3480 tape.  Thus, if we find the library is LTO OR LTO2
        # then we need to use this corrected type.  This only works if
        # the ADIC never gets LTO3 or LTO4 drives, considering the
        # end-of-life schedule for it, this shouldn't be a problem.
        if media_type == "3480":
            media_type = "LTO2"

        media_capacity = getattr(enstore_constants,
                                 "CAP_%s" % (media_type,),
                                 None)

        if media_capacity:
            #Cache this value for next time, but only for libraries we
            # care about.
            self.library_capacity[media_type] = (media_capacity, media_type)

        return media_capacity, media_type


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
        self.new_tapes_summary_week = {}
        self.new_tapes_summary_month = {}
        first_access_cache = {}
        self.full_tape_cache = {}
        self.extra_title_info = []
        month_ago = time.time() - DAYS_IN_MONTH * SECONDS_IN_DAY
        week_ago = time.time() - DAYS_IN_WEEK * SECONDS_IN_DAY

        #Loop over all known configuration servers and obtaining information
        # from all of them.
        for name, values in config_servers_dict.items():
            csc = configuration_client.ConfigurationClient(values)

            ###
            ### Get information from the Enstore Database.
            ###
            edb = csc.get("database", {})
            try:
                edb = pg.DB(host  = edb.get('dbhost', "localhost"),
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

            valid_libraries = []
            lms = csc.get_library_managers2(3, 3)
            ###
            ### Get the current list of libraries known to this Enstore system.
            ###
            for lib_info in lms:
                library = lib_info['name'].split(".")[0]
                valid_libraries.append(library)


            #Get the unique library and storage group combinations.
            sql_cmd = "select distinct media_type from volume " \
                      " where library in ('%s')"%(string.join(valid_libraries,"','"))
            sql_cmd += " and label not like '%.deleted' and media_type != 'null'"

            edb_res = edb.query(sql_cmd).getresult() #Get the values from the DB.

            if  len(edb_res) == 0 :
                edb.close()
                return

            for row in edb_res:
                #row[0] is a distinct media_type
                fname = os.path.join(self.temp_dir, "ALL_%s.pts" % (row[0],))
                self.MT_dict[row[0]] = open(fname, "w")

            #Get the library and storage group for each volume.
            sql_cmd = "select label,media_type from volume; "

            edb_res = edb.query(sql_cmd).getresult() #Get the values from the DB.

            #Stuff the tape information into a dictionary.
            for row in edb_res:
                tapes[row[0]] = row[1]

            ###
            ### Get current tape information for all tapes currently in use.
            ###

            #Get them for each media type.
            sql_cmd = (" select "
                       " v1.media_type, "
                       " count(v_blank.media_type)   as blank, "
                       " count(v_written.media_type) as written "
                       " from volume v1 "
                       " full join (select * from volume where (file_family='none' and wrapper='none') ) "
                       "     as v_blank "
                       "     on v_blank.id=v1.id "
                       " full join (select * from volume where (file_family != 'none' or wrapper != 'none') ) "
                       "     as v_written "
                       "     on v_written.id=v1.id "
                       " where v1.system_inhibit_0 != 'DELETED' "
                       "     and v1.library not like 'shelf-%' "
                       "     and v1.media_type != 'disk' "
                       "     and v1.media_type != 'null' "
                       " group by v1.media_type "
                       " order by v1.media_type "
                       )

            edb_res = edb.query(sql_cmd).getresult() #Get the values from the DB.

            #Store these for use later.  They are used in the plot titles.
            for row in edb_res:
                # row[0] == media_type
                # row[1] == total blank tapes
                # row[2] == total written tapes

                #Sum these values across all known enstore systems.
                try:
                    blank = row[1] + self.tape_totals[row[0]][0]
                except KeyError:
                    blank = row[1] #No previous value for this media_type.
                try:
                    written = row[2] + self.tape_totals[row[0]][1]
                except KeyError:
                    written = row[2] #No previous value for this media_type.

                self.tape_totals[row[0]] = (blank, written)

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
            except pg.ProgrammingError:
                message = "Unable to select from status (%s, %s): %s\n" % \
                          (edb['dbhost'], edb['dbport'], msg)
                sys.stderr.write(message)
                continue


            ##Since the two tables are in seperate databases, we need to join
            ## them here.  This is done while summing the bytes written into
            ## day increments.
            for row in drs_res:
                timestamp = row[0]
                volume = row[1]
                mb_user_write = int(row[2])
                mb_user_read = int(row[3])

                #Convert string into number of seconds since the epoch.
                this_timestamp = time.mktime(time.strptime(timestamp,
                                                           "%Y-%m-%d %H:%M:%S"))

                #Get the tapes library and storage group.
                try:
                    media_type = tapes[volume]
                    sql_cmd = "select first_access,system_inhibit_1 from volume where label = '%s'"\
                          % (volume,)
                except KeyError:
                    try:
                        media_type = tapes[volume + ".deleted"]
                        sql_cmd = "select first_access,system_inhibit_1 from volume where label = '%s'"\
                                  % (volume + ".deleted",)
                    except KeyError:
                        sys.stderr.write("No such volume in enstoredb %s(.deleted) \n" % (volume,))
                        continue
                #Get the date.
                date = timestamp.split(" ")[0]

                try:
                    #Pull the first access time and if the tape is full
                    # from the cache in memory.
                    first_access_timestamp = first_access_cache[volume]
                    is_full_tape = self.full_tape_cache[media_type][volume]
                except KeyError:
                    #Get the first access time of this volume.  We need this to
                    # determine new tapes that have been written to.
                    fa_res = edb.query(sql_cmd).getresult()
                    if len(fa_res) > 0:

                        if fa_res[0][1] == "full":
                            is_full_tape = 1
                        else:
                            is_full_tape = 0

                        try:
                            first_access_timestamp = \
                                  time.mktime(time.strptime(fa_res[0][0],
                                                            "%Y-%m-%d %H:%M:%S"))
                        except OverflowError:
                            #This error occurs for first access times set to
                            # -1 seconds since the epoch (they have not been
                            # used yet).
                            first_access_timestamp = 0
                    else:
                        first_access_timestamp = this_timestamp #punt?
                        is_full_tape = 0 #punt?

                    #Update the cache for the next time this volume comes
                    # up in the list.
                    first_access_cache[volume] = first_access_timestamp

                    #Restrict the full tape count to those filled in the
                    # last month.
                    if this_timestamp > month_ago and is_full_tape:
                        #Update the LM and SG pair for number of tapes filled.
                        ft_summary = self.full_tape_cache.get(media_type, {})
                        if ft_summary == {}:
                            self.full_tape_cache[media_type] = {}
                        self.full_tape_cache[media_type][volume] = is_full_tape

                        #Update the LM for the number of tapes filled.
                        ft_summary = self.full_tape_cache.get(media_type, {})
                        if ft_summary == {}:
                            self.full_tape_cache[media_type] = {}
                        self.full_tape_cache[media_type][volume] = is_full_tape


                #Update the media_type counts into day increments.
                mt_summary = bytes_summary.get(media_type, {})
                if mt_summary == {}:
                    bytes_summary[media_type] = {} #Initial value.
                summary = mt_summary.get(date, {'mb_read':0, 'mb_write':0,})
                summary['mb_write'] = summary['mb_write'] + mb_user_write
                summary['mb_read'] = summary['mb_read'] + mb_user_read
                bytes_summary[media_type][date] = summary


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

                #Get the new tapes that have been written to in the last month
                # and week.
                if first_access_timestamp > month_ago:
                    new_tape_mt_summary = self.new_tapes_summary_month.get(media_type, {})
                    if new_tape_mt_summary == {}:
                        self.new_tapes_summary_month[media_type] = {}  #Initial value.
                    self.new_tapes_summary_month[media_type][volume] = 1

                if first_access_timestamp > week_ago:
                    new_tape_mt_summary = self.new_tapes_summary_week.get(media_type, {})
                    if new_tape_mt_summary == {}:
                        self.new_tapes_summary_week[media_type] = {}  #Initial value.
                    self.new_tapes_summary_week[media_type][volume] = 1

        ## Now we can write out the data to the pts files that gnuplot
        ## will plot for us.

        month_ago_date = time.strftime("%Y-%m-%d",
              time.localtime(time.time() - DAYS_IN_MONTH * SECONDS_IN_DAY))
        today_date = time.strftime("%Y-%m-%d",
                                   time.localtime())

        for key in self.MT_dict.keys():
            #key should just be the media type here.

            pts_file = self.MT_dict[key]


            try:
                use_bytes_summary = bytes_summary[key]
            except KeyError:
                use_bytes_summary = {today_date : {'mb_read':0, 'mb_write':0}}
            #Extract the number of (new) tapes written last month.
            try:
                new_tapes_written_last_month = \
                                  len(self.new_tapes_summary_month[key])
            except KeyError:
                new_tapes_written_last_month = 0

            #Get the capacity in Gigabytes of the tapes belonging to this
            # library.
            capacity, media_type = self.get_capacity(key, edb)
            if capacity == None:
                #Skip media that does not have a valid library defined
                # for it anymore.
                print "DEAD LIBRARY3:", media_type
                continue

            #
            sum_read = 0
            sum_write = 0
            #Set these so that the extra last column of data is outputed at
            # the correct points for month ago, today and month ahead.
            is_month_ago_plotted = False
            month_ago_total_gb = 0

            #Write out the plot information to the entire librarys' data files.
            summary_keys = use_bytes_summary.keys()
            summary_keys.sort()
            for date in summary_keys:
                stats = use_bytes_summary[date]
                sum_read = sum_read + stats['mb_read']
                sum_write = sum_write + stats['mb_write']

                date_timestamp = time.mktime(time.strptime(date, "%Y-%m-%d"))

                #Convert from MB to GB.
                current_gb = stats['mb_write'] / 1024.0
                total_gb = sum_write / 1024.0

                #For a month ago and today, plot an extra line conneting them.
                #The line written to the data file for one month ago
                # gets an extra point.
                if month_ago_date == date:
                    is_month_ago_plotted = True
                    month_ago_total_gb = total_gb

                    line = "%s %s %s %s\n" % (
                        date, current_gb, total_gb, total_gb)
                else:
                    line = "%s %s %s\n" % (
                         date, current_gb, total_gb)

                #Slight diversion.  If we passed a month ago without
                # finding a data point, we need to insert one.
                if month_ago <= date_timestamp \
                       and not is_month_ago_plotted:
                    is_month_ago_plotted = True
                    month_ago_total_gb = total_gb

                    #Write out the information to the correct data file.
                    line2 = "%s %s %s %s\n" % (
                        date, "skip", "skip", total_gb)
                    pts_file.write(line2)##

                pts_file.write(line)

            #If there hasn't been anything written in the last month we
            # need to output the month interpolation line point now.
            if not is_month_ago_plotted:
                is_month_ago_plotted = True
                month_ago_total_gb = total_gb

                #Write out the information to the correct data file.
                line2 = "%s %s %s %s\n" % (
                    month_ago_date, "skip", "skip", total_gb)
                pts_file.write(line2)


            ## Plot the end of the interpolation line.
            ##

            #The raw value for today is the amount of tapes in GB written.
            #Where should the 90% come from?
            raw_value = (new_tapes_written_last_month * (capacity * 0.90))
            #final_value adds the raw value to GB witten up to that
            # point in time.
            final_value = raw_value + month_ago_total_gb

            #Write out the information to the correct data file.
            line2 = "%s %s %s %s\n" % (
                today_date, "skip", "skip", final_value)
            pts_file.write(line2)

            pts_file.close()

        #For compatiblity with system level burn rate plots.
        self.PLOT_dict = self.MT_dict
