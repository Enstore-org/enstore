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
import re

# enstore imports
import enstore_plotter_module
import enstore_constants

WEB_SUB_DIRECTORY = enstore_constants.TAPES_BURN_RATE_PLOTS_SUBDIR

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

class TapesBurnRatePlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)

        #The keys to this dictionary are either a library or tuple of
        # library and storage_group.  The values are file handles to the
        # pts files written out in the fill() function.
        self.PLOT_dict = {}
        #
        # This dictionay stores slopes in MB/s
        #
        self.slopes    = {}

        #This one is used by summary_burn_rate_plotter_module.py.  Appears here
        # only for compatibility.
        self.output_fname_prefix = ""

        #This is also used by summary_burn_rate_plotter_moduled.py.
        self.extra_title_info = ""

        #Cache for the capacity of each library.
        self.library_capacity = {}

    #Based on the library name, get the capacity and media_type of the tapes.
    def get_capacity(self, library, db):
        media_values = self.library_capacity.get(library, None)
        if media_values:
            return media_values

        if db == None:
            return (None, None)

        q = "select library, media_type from volume " \
            "where system_inhibit_0 != 'DELETED' and library = '%s' " \
            "group by library, media_type " \
            "order by count(*) desc;" \
            % (library,)
            # If multiple rows are returned, the predominant media_type for the
            # library is in the first row.

        edb_res = db.query(q).getresult() #Get the values from the DB.

        if len(edb_res) > 1:
            #Need to raise alarm on multiple media types defined for
            # a single library.
            pass

        try:
            media_type = edb_res[0][1]
        except IndexError:
            media_type = None
        #This is a hack for the ADIC.  Originally, the ADIC didn't know
        # what an LTO tape was, so every LTO1 and LTO2 was inserted as
        # a 3480 tape.  Thus, if we find the library is LTO OR LTO2
        # then we need to use this corrected type.  This only works if
        # the ADIC never gets LTO3 or LTO4 drives, considering the
        # end-of-life schedule for it, this shouldn't be a problem.
        if library.upper().find("LTO2") != -1:
            media_type = "LTO2"
        ### Since, LTO1s were the first type, they could be LTO or LTO1.
        elif re.compile("LTO$").match(library) != None \
                 or library.upper().find("LTO1") != -1:
            media_type = "LTO1"

        media_capacity = getattr(enstore_constants,
                                 "CAP_%s" % (media_type,),
                                 None)

        if media_capacity:
            #Cache this value for next time, but only for libraries we
            # care about.
            self.library_capacity[library] = (media_capacity, media_type)

        return media_capacity, media_type


    #Write out the file that gnuplot will use to plot the data.
    # plot_filename = The file that will be read in by gnuplot containing
    #                 the gnuplot commands.
    # data_filename = The data file that will be read in by gnuplot
    #                 containing the data to be plotted.
    # ps_filename = The postscript file that will be created by gnuplot.
    # key = The key to access information in the various instance member
    #       variables.  It is either a string containing the library
    #       or a tuple consiting of the library and storage group.
    def write_plot_file(self, plot_filename, data_filename, ps_filename, key):

        #The first an last to set the x range between.
        day_start = time.strftime("%Y-%m-%d",
                 time.localtime(time.time() - DAYS_AGO_START * SECONDS_IN_DAY))
        day_end = time.strftime("%Y-%m-%d",
                 time.localtime(time.time() + DAYS_AHEAD_END * SECONDS_IN_DAY))

        #The amount of tapes used in the last month and week.
        try:
            tapes_written_last_month = len(self.tapes_summary_month[key])
        except KeyError:
            tapes_written_last_month = 0
        try:
            tapes_written_last_week = len(self.tapes_summary_week[key])
        except KeyError:
            tapes_written_last_week = 0
        #Same for new tapes used.
        try:
            new_tapes_written_last_month = len(self.new_tapes_summary_month[key])
        except KeyError:
            new_tapes_written_last_month = 0
        try:
            new_tapes_written_last_week = len(self.new_tapes_summary_week[key])
        except KeyError:
            new_tapes_written_last_week = 0
        #Same for filled tapes.
        try:
            full_tapes = len(self.full_tape_cache[key])
        except KeyError:
            full_tapes = 0 #punt?

        #Get the blank and used number of tapes.
        try:
            blanks, written = self.tape_totals[key]
        except KeyError:
            blanks, written = 0, 0  #What can cause this???

        #Need the current time to add to the plot.
        now = time.strftime("%m-%d-%Y %H:%M:%S")

        #If the plot is for an library with storage group pair, me should
        # make the output a little easier to read.
        if type(key) == types.TupleType:
            label = string.join(key, ".")
            library = key[0]
        else:
            label = key
            library = key

        media_capacity, media_type = self.get_capacity(library, None)

        #In case there is something else we want to add.
        if hasattr(self, "extra_title_info"):
            label = "%s %s" % (label, self.extra_title_info)

        plot_fp = open(plot_filename, "w+")

        plot_fp.write('set terminal postscript color solid\n')
        plot_fp.write('set output "%s"\n' % (ps_filename,))
        plot_fp.write('set title "%s   MediaType=%s"' \
                      'font "TimesRomanBold,16"\n' % \
                      (label, media_type))
        plot_fp.write('set ylabel "Gigabytes Written"\n')
        plot_fp.write('set xdata time\n')
        plot_fp.write('set timefmt "%Y-%m-%d"\n')
        plot_fp.write('set format x "%Y-%m-%d"\n')
        plot_fp.write('set grid\n')
        plot_fp.write('set nokey\n')
        plot_fp.write('set label "Plotted %s " at graph .99,0 rotate font "Helvetica,10"\n' % (now,))
        plot_fp.write('set xrange["%s":"%s"]\n' % (day_start, day_end))
        plot_fp.write('set yrange[ 0 : ]\n')

        plot_fp.write('set label "%s tapes written last month" at graph .05,.95\n' % (int(tapes_written_last_month),))
        plot_fp.write('set label "%s tapes written last week" at graph .05,.90\n' % (int(tapes_written_last_week),))
        plot_fp.write('set label "%s new tapes drawn last month" at graph .05,.85\n' % (int(new_tapes_written_last_month),))
        plot_fp.write('set label "%s new tapes drawn last week" at graph .05,.80\n' % (int(new_tapes_written_last_week),))
        plot_fp.write('set label "%s tapes filled last month" at graph .05,.75\n' % (int(full_tapes),))
        plot_fp.write('set label "%s total tapes used" at graph .05,.70\n' % (int(written),))
        plot_fp.write('set label "%s tapes blank" at graph .05,.65\n' % (int(blanks),))
        plot_fp.write('set label "%sGB media capacity" at graph .05,.60\n' % (media_capacity,))
        try :
            plot_fp.write('set label "slope %5.1f MB/s " at graph .05,.55\n' % (self.slopes[key],))
        except KeyError, msg:
            pass


        ## The first plot is the summation of bytes written over the last 4
        ## months.  The second plot is the daily bytes written.  The third
        ## plot is a line connecting the summation bars together in case of
        ## a hole in the data.  The last plot contains three points
        ## representing the bytes written at a month ago to the present and
        ## to the estimated amount a month from now.

        plot_fp.write('plot "%s" using 1:3 with impulses linewidth 10, ' \
                      '"%s" using 1:2 with impulses linewidth 10, ' \
                      '"%s" using 1:3 with lines, ' \
                      '"%s" using 1:4 title "estimated usage" w lp lt 3 lw 5 pt 5\n' % \
                      (data_filename, data_filename, data_filename, data_filename))

        plot_fp.close()

    #######################################################################
    # The following functions must be defined by all plotting modules.
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

        edb_info = frame.get_configuration_client().get("database", {})
        edb = pg.DB(host  = edb_info.get('dbhost', "localhost"),
                   dbname= edb_info.get('dbname', "enstore"),
                   port  = edb_info.get('dbport', 5432),
                   user  = edb_info.get('dbuser_reader', "enstore_reader"))

        ###
        ### Get the current list of libraries known to this Enstore system.
        ###
        self.LM_dict = {}
        lms = frame.get_configuration_client().get_library_managers2(3, 3)
        for lib_info in lms:
            library = lib_info['name'].split(".")[0]

            #Get the capacity in Gigabytes of the tapes belonging to this
            # library.
            capacity, media_type = self.get_capacity(library, edb)
            if capacity == None:
                #Skip media that does not have a valid media type
                # for it.  (This usually excludes null and disk libraries.)
                #print "DEAD LIBRARY1:", library
                continue

            #Open the file to output the data points to plot.
            fname = os.path.join(self.temp_dir,
                                 "burn_rate_%s.pts" % (library,))
            self.LM_dict[library] = open(fname, "w")

        ###
        ### Get information from the Enstore Database.
        ###

        #Get the unique library and storage group combinations.
        sql_cmd = "select distinct library,storage_group from volume;"

        edb_res = edb.query(sql_cmd).getresult() #Get the values from the DB.

        self.LM_SG_dict = {}
        for row in edb_res:
            library = row[0]
            storage_group = row[1]

            #Don't worry about plotting old libraries no longer in the
            # configuration file.
            if library not in self.LM_dict.keys():
                continue
            #Get the capacity in Gigabytes of the tapes belonging to this
            # library.
            capacity, media_type = self.get_capacity(library, edb)
            if capacity == None:
                #Skip media that does not have a valid media type
                # for it.  (This usually excludes null and disk libraries.)
                #print "DEAD LIBRARY2:", library
                continue

            #Open the file to output the data points to plot.
            fname = os.path.join(self.temp_dir,
                                 "burn_rate_%s.%s.pts" % (library,
                                                          storage_group))
            self.LM_SG_dict[(library, storage_group)] = open(fname, "w")

        #For compatiblity with system summary level burn rate plots; combine
        # these two dictionaries into one.
        for key in self.LM_dict.keys():
            self.PLOT_dict[key] = self.LM_dict[key]
        for key in self.LM_SG_dict.keys():
            self.PLOT_dict[key] = self.LM_SG_dict[key]

        #Get the library and storage group for each volume.  It is possible
        # that a file gets written and the tape in is on gets recycled; thus
        # we need to include the .deleted volumes valid rate for information.
        sql_cmd = "select label,library,storage_group from volume; "

        edb_res = edb.query(sql_cmd).getresult() #Get the values from the DB.

        #Stuff the tape information into a dictionary.
        tapes = {}
        for row in edb_res:
            tapes[row[0]] = (row[1], row[2])

        ###
        ### Get current tape information for all tapes currently in use.
        ###

        #First get them for the LM and SG pair.
        sql_cmd = "select v1.library, v1.storage_group," \
                  "count(v2.library) as blank," \
                  "count(v3.library) as written " \
                  "from volume v1 " \
                  "full join (select * from volume where (eod_cookie = 'none' or eod_cookie = '0000_000000000_0000001') and system_inhibit_0!='DELETED' and wrapper='none' and file_family='none')"\
                  " as v2 on v1.id=v2.id " \
                  "full join (select * from volume where system_inhibit_0!='DELETED' and ((eod_cookie != 'none' and eod_cookie != '0000_000000000_0000001') or (wrapper!='none' or file_family!='none')))"\
                  " as v3 on v3.id=v1.id " \
                  "group by v1.library,v1.storage_group"

        edb_res = edb.query(sql_cmd).getresult() #Get the values from the DB.

        #Store these for use later.  They are used in the plot titles.
        self.tape_totals = {}
        for row in edb_res:
            # row[0] == library
            # row[1] == storage_group
            # row[2] == total blank tapes
            # row[3] == total written tapes
            self.tape_totals[(row[0], row[1])] = (row[2], row[3])

        #Second get them for the LMs.
        sql_cmd = "select v1.library," \
                  "count(v2.library) as blank," \
                  "count(v3.library) as written " \
                  "from volume v1 " \
                  "full join (select * from volume where (eod_cookie = 'none' or eod_cookie = '0000_000000000_0000001') and system_inhibit_0!='DELETED' and wrapper='none' and file_family='none')"\
                  " as v2 on v1.id=v2.id " \
                  "full join (select * from volume where system_inhibit_0!='DELETED' and ((eod_cookie != 'none' and eod_cookie != '0000_000000000_0000001') or (wrapper!='none' or file_family!='none')))"\
                  " as v3 on v3.id=v1.id " \
                  "group by v1.library"

        edb_res = edb.query(sql_cmd).getresult() #Get the values from the DB.

        #Store these for use later.  They are used in the plot titles.
        for row in edb_res:
            # row[0] == library
            # row[1] == total blank tapes
            # row[2] == total written tapes
            self.tape_totals[row[0]] = (row[1], row[2])



        ###
        ### Get the bytes written for recent write transfers from the
        ### drivestat DB.
        ###
        drs = frame.get_configuration_client().get(enstore_constants.DRIVESTAT_SERVER, {})
        try:
            db = pg.DB(host  = drs.get('dbhost', "localhost"),
                       dbname= drs.get('dbname', "drivestat"),
                       port  = drs.get('dbport', 5432),
                       user  = drs.get('dbuser_reader', "enstore_reader"))
        except pg.InternalError:
            db = None

        sql_cmd = "select time,tape_volser,mb_user_write,mb_user_read " \
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

        bytes_summary = {}
        self.tapes_summary_week = {}
        self.tapes_summary_month = {}
        self.new_tapes_summary_week = {}
        self.new_tapes_summary_month = {}
        first_access_cache = {}
        self.full_tape_cache = {}
        month_ago = time.time() - DAYS_IN_MONTH * SECONDS_IN_DAY
        week_ago = time.time() - DAYS_IN_WEEK * SECONDS_IN_DAY
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
                lm, sg = tapes[volume]
                sql_cmd = "select first_access,system_inhibit_1 from volume where label = '%s'"\
                          % (volume,)
            except KeyError:
                try:
                    lm, sg = tapes[volume + ".deleted"]
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
                is_full_tape = self.full_tape_cache[lm][volume]
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
                    ft_summary = self.full_tape_cache.get((lm, sg), {})
                    if ft_summary == {}:
                        self.full_tape_cache[(lm, sg)] = {}
                    self.full_tape_cache[(lm, sg)][volume] = is_full_tape

                    #Update the LM for the number of tapes filled.
                    ft_summary = self.full_tape_cache.get(lm, {})
                    if ft_summary == {}:
                        self.full_tape_cache[lm] = {}
                    self.full_tape_cache[lm][volume] = is_full_tape


            #Update the LM and SG byte counts into day increments.
            lm_sg_summary = bytes_summary.get((lm, sg), {})
            if lm_sg_summary == {}:
                bytes_summary[(lm, sg)] = {} #Initial value.
            summary = lm_sg_summary.get(date, {'mb_read':0, 'mb_write':0,})
            summary['mb_write'] = summary['mb_write'] + mb_user_write
            summary['mb_read'] = summary['mb_read'] + mb_user_read
            bytes_summary[(lm, sg)][date] = summary

            #Update the LM byte counts into day increments.
            lm_summary = bytes_summary.get(lm, {})
            if lm_summary == {}:
                bytes_summary[lm] = {} #Initial value.
            summary = lm_summary.get(date, {'mb_read':0, 'mb_write':0,})
            summary['mb_write'] = summary['mb_write'] + mb_user_write
            summary['mb_read'] = summary['mb_read'] + mb_user_read
            bytes_summary[lm][date] = summary


            #Get the tapes used values for monthly and weekly for both
            # the library and library w/ storage group.  The length of the
            # dictionaries contains the number of tapes used in a month
            # and week for each lm and lm w/ sg.
            if this_timestamp > month_ago:
                tape_lm_summary = self.tapes_summary_month.get(lm, {})
                if tape_lm_summary == {}:
                    self.tapes_summary_month[lm] = {}  #Initial value.
                self.tapes_summary_month[lm][volume] = 1

                tape_lm_summary = self.tapes_summary_month.get((lm, sg), {})
                if tape_lm_summary == {}:
                    self.tapes_summary_month[(lm, sg)] = {}
                self.tapes_summary_month[(lm, sg)][volume] = 1
            if this_timestamp > week_ago:
                tape_lm_summary = self.tapes_summary_week.get(lm, {})
                if tape_lm_summary == {}:
                    self.tapes_summary_week[lm] = {}  #Initial value.
                self.tapes_summary_week[lm][volume] = 1

                tape_lm_summary = self.tapes_summary_week.get((lm, sg), {})
                if tape_lm_summary == {}:
                    self.tapes_summary_week[(lm, sg)] = {}
                self.tapes_summary_week[(lm, sg)][volume] = 1

            #Get the new tapes that have been written to in the last month
            # and week.
            if first_access_timestamp > month_ago:
                new_tape_lm_summary = self.new_tapes_summary_month.get(lm, {})
                if new_tape_lm_summary == {}:
                    self.new_tapes_summary_month[lm] = {}  #Initial value.
                self.new_tapes_summary_month[lm][volume] = 1

                new_tape_lm_summary = self.new_tapes_summary_month.get((lm, sg), {})
                if new_tape_lm_summary == {}:
                    self.new_tapes_summary_month[(lm, sg)] = {}
                self.new_tapes_summary_month[(lm, sg)][volume] = 1
            if first_access_timestamp > week_ago:
                new_tape_lm_summary = self.new_tapes_summary_week.get(lm, {})
                if new_tape_lm_summary == {}:
                    self.new_tapes_summary_week[lm] = {}  #Initial value.
                self.new_tapes_summary_week[lm][volume] = 1

                new_tape_lm_summary = self.new_tapes_summary_week.get((lm, sg), {})
                if new_tape_lm_summary == {}:
                    self.new_tapes_summary_week[(lm, sg)] = {}
                self.new_tapes_summary_week[(lm, sg)][volume] = 1

        ## Now we can write out the data to the pts files that gnuplot
        ## will plot for us.

        month_ago_date = time.strftime("%Y-%m-%d",
                 time.localtime(time.time() - DAYS_IN_MONTH * SECONDS_IN_DAY))
        today_date = time.strftime("%Y-%m-%d",
                 time.localtime())

        #for key in self.LM_dict.keys() + self.LM_SG_dict.keys():
        for key in self.PLOT_dict.keys():
            #Key at this point is either, the library or a tuple consisting
            # of the library and storage group.

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
            if type(key) == types.TupleType:
                lm = key[0]
            else:
                lm = key
            capacity, media_type = self.get_capacity(lm, edb)
            if capacity == None:
                #Skip media that does not have a valid library defined
                # for it anymore.
                #print "DEAD LIBRARY3:", lm
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

                #Convert from MB to GB.
                current_gb = stats['mb_write'] / 1024.0
                total_gb = sum_write / 1024.0

                date_timestamp = time.mktime(time.strptime(date, "%Y-%m-%d"))

                #The line written to the data file for one month ago
                # gets an extra point.
                if month_ago_date == date:

                    #fdodamo = First Day Of Data After Month Ago
                    # We need this for groups that don't write data
                    # every single day.
                    #fdodamo_date = date

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
                    #fdodamo = First Day Of Data After Month Ago
                    # We need this for groups that don't write data
                    # every single day.
                    #fdodamo_date = date

                    is_month_ago_plotted = True
                    month_ago_total_gb = total_gb

                    #Write out the information to the correct data file.
                    line2 = "%s %s %s %s\n" % (
                        date, "skip", "skip", total_gb)
                    self.PLOT_dict[key].write(line2)

                #Write out the information to the correct data file.
                self.PLOT_dict[key].write(line)

            #If there hasn't been anything written in the last month we
            # need to output the month interpolation line point now.
            if not is_month_ago_plotted:
                #fdodamo = First Day Of Data After Month Ago
                # We need this for groups that don't write data
                # every single day.
                #fdodamo_date = month_ago_date  #punt with nothing to plot

                is_month_ago_plotted = True
                month_ago_total_gb = total_gb

                #Write out the information to the correct data file.
                line2 = "%s %s %s %s\n" % (
                    month_ago_date, "skip", "skip", total_gb)
                self.PLOT_dict[key].write(line2)

            ##
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
            self.slopes[key] = ( final_value - month_ago_total_gb ) * 1024. / ( DAYS_IN_MONTH * SECONDS_IN_DAY )
            self.PLOT_dict[key].write(line2)

        #Cleanup after ourselves.
        for pts_file in self.PLOT_dict.values():
            pts_file.close()

    def plot(self):
        #for key in self.LM_dict.keys() + self.LM_SG_dict.keys():
        for key in self.PLOT_dict.keys():
            #Key at this point is either, the library or a tuple consisting
            # of the library and storage group.

            #If the plot is for an library with storage group pair, me should
            # make the output a little easier to read.
            if type(key) == types.TupleType:
                label = string.join(key, ".")
            else:
                label = key

            #Get some filenames for the various files that get created.
            plot_filename = os.path.join(self.temp_dir,
                                         "burn_rate.%s.plot" % (label,))

            if self.PLOT_dict.has_key(key):
                pts_filename = self.PLOT_dict[key].name
            else:
                sys.stderr.write("No pts file for %s.\n" % (key,))
                continue
            ps_filename = os.path.join(self.web_dir,
                                       "%s%s.ps" % (self.output_fname_prefix,
                                                    label,))
            jpg_filename = os.path.join(self.web_dir,
                                        "%s%s.jpg" % (self.output_fname_prefix,
                                                      label,))
            jpg_stamp_filename = os.path.join(self.web_dir,
                                        "%s%s_stamp.jpg" % (self.output_fname_prefix,
                                                            label,))

            #Write the gnuplot command file(s).
            self.write_plot_file(plot_filename, pts_filename, ps_filename, key)

            #Make the plot and convert it to jpg.
            os.system("gnuplot < %s" % plot_filename)
            os.system("convert -rotate 90  %s %s\n"
                      % (ps_filename, jpg_filename))
            os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                      % (ps_filename, jpg_stamp_filename))

            #Cleanup the temporary files for this loop.
            try:
                os.remove(plot_filename)
            except:
                pass
            try:
                os.remove(pts_filename)
            except:
                pass
