#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import pg
import os
import sys
import time

# enstore imports
import enstore_plotter_module
import enstore_constants

WEB_SUB_DIRECTORY = enstore_constants.MIGRATION_SUMMARY_PLOTS_SUBDIR

"""
In the enstore DB to create the necessary view:
CREATE VIEW remaining_blanks AS
enstoredb-#     SELECT volume.media_type, count(*) AS blanks FROM volume WHERE ((((volume.storage_group)::text = 'none'::text) AND ((volume.file_family)::text = 'none'::text)) AND ((volume.wrapper)::text = 'none'::text)) GROUP BY volume.media_type;
"""

###
### SQL command to return the number of closed and in progress volumes
### being migrated.
###

# Note: This sql command is similar to the daily Migrated/Duplicated tables
# in the sbin/migration_summary and sbin/duplication_summary scripts.  Be
# sure to modify them when you modify this sql statement.
SQL_COMMAND = \
"""
/*This outer select just sorts the merged s1, s2 and s3 'day' columns into
  a unified sorted order. */
select * from
(
/* This inner select combines three sub selects sorted by day and
   media type. */
select CASE WHEN s1.day is not null THEN s1.day
            WHEN s2.day is not null THEN s2.day
            WHEN s3.day is not null THEN s3.day
            ELSE NULL
       END as day,
       CASE WHEN s1.media_type is not null THEN s1.media_type
            WHEN s2.media_type is not null THEN s2.media_type
            WHEN S3.media_type is not null THEN s3.media_type
            ELSE NULL
       END as media_type,
       CASE WHEN s2.started is not NULL THEN s2.started
            ELSE 0
       END as started,
       CASE WHEN s1.completed is not NULL THEN s1.completed
            ELSE 0
       END as completed,
       CASE WHEN s3.closed is not NULL THEN s3.closed
            ELSE 0
       END as closed
from

/*Three sub selects get the count for each day and media for number of
  volumes started, migrated/duplicated and closed. */

/****  s1  ****/
(
select date(time) as day,
       /* It should be as simple as just using the media_type. However,
          LTO1 and LTO2 at FNAL are both set as 3480 do to limitations
          in the AML/2 when it was first put into use. */
       /*media_type,*/
       CASE WHEN media_type = '3480' and capacity_bytes = '107374182400'
            THEN 'LTO1'
            WHEN media_type = '3480' and capacity_bytes = '214748364800'
            THEN 'LTO2'
            WHEN media_type = '3480' and capacity_bytes < 100
            THEN NULL    --Cleaning tape; skip it.
            ELSE media_type
       END as media_type,
       count(distinct CASE WHEN system_inhibit_1 in ('migrated',
                                                     'duplicated')
                            THEN label
                            ELSE NULL
                      END) as completed

from volume,state
where volume.id = state.volume
      and volume.media_type != 'null'
      and volume.system_inhibit_1 in ('migrated', 'duplicated')
      /* This time sub-query is needed to limit test volumes migrated
         multiple times to be counted only once. */
      /* Note: It is important that this time value come from the state
         table and not the migration_history table.  If an empty or
         only-containing-skipped-deleted-files tapes are migrated and entry
         will make it into the state table but not the migration_history
         table. */
      and time = (select max(time)
                  from state s2
                  where s2.volume = volume.id
                  and s2.value in ('migrated', 'duplicated'))
      and capacity_bytes > 500  --Skip cleaning tapes.
group by day,media_type,capacity_bytes
order by day,media_type
) as s1
/****  s1  ****/

/****  s2  ****/
full join (
select date(state.time) as day,
       /* It should be as simple as just using the media_type. However,
          LTO1 and LTO2 at FNAL are both set as 3480 do to limitations
          in the AML/2 when it was first put into use. */
       /*media_type,*/
       CASE WHEN media_type = '3480' and capacity_bytes = '107374182400'
            THEN 'LTO1'
            WHEN media_type = '3480' and capacity_bytes = '214748364800'
            THEN 'LTO2'
            WHEN media_type = '3480' and capacity_bytes < 100
            THEN NULL    --Cleaning tape; skip it.
            ELSE media_type
       END as media_type,
       count(distinct volume.label) as started
from volume,state
where volume.id = state.volume
      and volume.media_type != 'null'
      and volume.system_inhibit_1 in ('migrating', 'duplicating',
                                      'migrated', 'duplicated')
      /* Hopefully, setting state.time like this will correctly handle
         all vintages of the migration process.  The migrating and duplicating
         stages were added September of 2008. */
      /* Note: It is important that this time value come from the state
         table and not the migration_history table.  If an empty or
         only-containing-skipped-deleted-files tapes are migrated an entry
         will make it into the state table but not the migration_history
         table. */
      /* The use of max(s2.time) instead of min(s2.time) are for migrated,
         then recycled, then migrated (again) tapes to report the correct
         date for XXOO00 and XXOO00.deleted, since XXOO00 has the combined
         volume history for XXOO00 and XXOO00.deleted. */
      and state.time = (select min(s5.time) from (
                        select CASE WHEN s2.value in ('migrating',
                                                      'duplicating')
                                    THEN max(s2.time)
                                    WHEN s2.value in ('readonly')
                                         and time > current_timestamp - interval '30 days'
                                    THEN max(s2.time)
                                    WHEN s2.value in ('migrated',
                                                      'duplicated')
                                    THEN max(s2.time)
                                    ELSE NULL
                               END as time
                        from state s2
                        where s2.volume = volume.id
                              and s2.value in ('migrating', 'duplicating',
                                               'migrated', 'duplicated',
                                               'readonly')
                        group by s2.value, time
                        order by s2.value, time
                        ) as s5)
      and capacity_bytes > 500  --Skip cleaning tapes.
group by day, volume.media_type,capacity_bytes
order by day, volume.media_type
) as s2 on (s1.day, s1.media_type) = (s2.day, s2.media_type)
/****  s2  ****/

/****  s3  ****/
full join (
select date(closed_time) as day,
       /* It should be as simple as just using the media_type. However,
          LTO1 and LTO2 at FNAL are both set as 3480 do to limitations
          in the AML/2 when it was first put into use. */
       /*media_type,*/
       CASE WHEN media_type = '3480' and capacity_bytes = '107374182400'
            THEN 'LTO1'
            WHEN media_type = '3480' and capacity_bytes = '214748364800'
            THEN 'LTO2'
            WHEN media_type = '3480' and capacity_bytes < 100
            THEN NULL    --Cleaning tape; skip it.
            ELSE media_type
       END as media_type,
       count(distinct label) as closed
from volume,migration_history
where volume.id = migration_history.src_vol_id
      and volume.media_type != 'null'
      and volume.system_inhibit_1 in ('migrated', 'duplicated')
      /* This time sub-query is needed to limit test volumes migrated
         multiple times to be counted only once. */
      /* We use migration_history table here, instead of state table, because
         we need to report source tapes where all the destination tapes
         have been scanned.  For blank or only-containing-skipped-deleted-files
         tapes, there are no destination tapes to scan and thus a more
         accurate number of source tapes 'closed'. */
      and closed_time = (select max(closed_time)
                         from migration_history m2
                         where m2.src_vol_id = volume.id)
      /* Make sure that at least one destination has been scanned. */
      and (select count(*)
           from migration_history
           left join volume v3 on migration_history.dst_vol_id = v3.id
           where migration_history.src_vol_id = volume.id
             and migration_history.closed_time is not NULL
             and v3.system_inhibit_0 != 'DELETED'
           limit 1) > 0
      /* Make sure all the destination volumes have been scanned. */
      and (select count(*)
           from migration_history
           left join volume v3 on migration_history.dst_vol_id = v3.id
           where migration_history.src_vol_id = volume.id
             and migration_history.closed_time is NULL
             and v3.system_inhibit_0 != 'DELETED') = 0
      and capacity_bytes > 500  --Skip cleaning tapes.
group by day,media_type,capacity_bytes
order by day,media_type
) as s3 on (s2.day, s2.media_type) = (s3.day, s3.media_type)
/****  s3  ****/

group by s1.day,s2.day,s3.day,s1.media_type,s1.completed,s2.media_type,s2.started,s3.media_type,s3.closed
order by s1.day,s2.day,s3.day
) as inner_result order by day;
"""


#SQL command to obtain the number of tapes still needing to be migrated.
SQL_COMMAND3 = \
"""
select /* It should be as simple as just using the media_type. However,
          LTO1 and LTO2 at FNAL are both set as 3480 do to limitations
          in the AML/2 when it was first put into use. */
       /*media_type,*/
       CASE WHEN media_type = '3480' and capacity_bytes = '107374182400'
            THEN 'LTO1'
            WHEN media_type = '3480' and capacity_bytes = '214748364800'
            THEN 'LTO2'
            WHEN media_type = '3480' and capacity_bytes < 100
            THEN NULL    --Cleaning tape; skip it.
            ELSE media_type
       END as media_type,
       count(distinct label) as remaining_volumes
from volume
left join migration_history on migration_history.src_vol_id = volume.id
where ( /* We obviously need to include un-migrated tapes in the robot. */
       (system_inhibit_1 not in ('migrated', 'duplicated', 'cloned', 'cloning')
        and
        library not like '%shelf%')
       or
       (( /* Include migrated tapes without any entries in the
              migration_history table. */
          (select count(*)
              from migration_history mh2
              left join volume v3 on mh2.dst_vol_id = v3.id
              where volume.id = mh2.src_vol_id
                and v3.system_inhibit_0 != 'DELETED') = 0
             and
              volume.system_inhibit_1 in ('migrated', 'duplicated', 'cloned'))
           or /* Include migrated tapes with missing closed_time entries
                 in the migration_history table. */
            ((select count(*)
              from migration_history mh2
              left join volume v3 on mh2.dst_vol_id = v3.id
               where volume.id = mh2.src_vol_id
                 and v3.system_inhibit_0 != 'DELETED'
                 and mh2.closed_time is NULL) > 0
             and
              volume.system_inhibit_1 in ('migrated', 'duplicated', 'cloned'))))
      and system_inhibit_0 != 'DELETED'
      and media_type != 'null'
      and volume.sum_wr_access - volume.sum_wr_err > 0
      and capacity_bytes > 500  --Skip cleaning tapes.
group by media_type,capacity_bytes;
"""

#SQL command to obtain the number of non-empty tapes.
SQL_COMMAND4 = \
"""
select /* It should be as simple as just using the media_type. However,
          LTO1 and LTO2 at FNAL are both set as 3480 do to limitations
          in the AML/2 when it was first put into use. */
       /*media_type,*/
       CASE WHEN media_type = '3480' and capacity_bytes = '107374182400'
            THEN 'LTO1'
            WHEN media_type = '3480' and capacity_bytes = '214748364800'
            THEN 'LTO2'
            WHEN media_type = '3480' and capacity_bytes < 100
            THEN NULL    --Cleaning tape; skip it.
            ELSE media_type
       END as media_type,
       count(distinct label) as non_empty_volumes
from volume
where system_inhibit_0 != 'DELETED'
      and library not like '%shelf%'
      and media_type != 'null'
      and sum_wr_access - sum_wr_err > 0 --Skip tapes with only write failures.
      and capacity_bytes > 500  --Skip cleaning tapes.
group by media_type,capacity_bytes;
"""


ACCUMULATED = "accumulated"
ACCUMULATED_NG = "accumulated_ng"
ACCUMULATED_LG = "accumulated_log"
DAILY = "daily"

class MigrationSummaryPlotterModule(enstore_plotter_module.EnstorePlotterModule):
    def __init__(self,name,isActive=True):
        enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)


    #Write out the file that gnuplot will use to plot the data.
    # plot_filename = The file that will be read in by gnuplot containing
    #                 the gnuplot commands.
    # data_filename = The data file that will be read in by gnuplot
    #                 containing the data to be plotted.
    # ps_filename = The postscript file that will be created by gnuplot.
    # key = The key to access information in the various instance member
    #       variables.  Here it is the media_type.
    def write_plot_file(self, plot_filename, data_filename, ps_filename,
                        key, plot_type, columns, titles):
        plot_fp = open(plot_filename, "w+")

        if self.enstore_name:
            use_title = "Migration/Duplication summary %s for %s on %s" % \
                        (plot_type, key, self.enstore_name)
        else:
            use_title = "Migration/Duplication summary %s for %s" % \
                        (plot_type, key)

        plot_fp.write('set terminal postscript color solid\n')
        plot_fp.write('set output "%s"\n' % (ps_filename,))
        plot_fp.write('set title "%s" font "TimesRomanBold,16"\n' % \
                      (use_title,))
        plot_fp.write('set ylabel "Volumes Done"\n')
        plot_fp.write('set xlabel "Year-month-day"\n')
        plot_fp.write('set xdata time\n')
        plot_fp.write('set timefmt "%Y-%m-%d"\n')
        plot_fp.write('set format x "%Y-%m-%d"\n')
        plot_fp.write('set xtics rotate\n')
        plot_fp.write('set grid\n')
        #plot_fp.write('set nokey\n')
        plot_fp.write('set label "Plotted %s " at graph 1.01,0 rotate font "Helvetica,10"\n' % (time.ctime(),))

        if plot_type in ( ACCUMULATED, ACCUMULATED_NG, ACCUMULATED_LG):
            started_count = self.summary_started.get(key, 0L)
            #done_count = self.summary_done.get(key, 0L)
            closed_count = self.summary_closed.get(key, 0L)
            remaining_count = self.summary_remaining.get(key, 0L)
            total_count = self.summary_total.get(key, 0L)
            #This is possibly used for adjusting the goal line.  See comment
            # below for possible reasons.
            if started_count > total_count:
                goal_adjust = started_count - total_count
            else:
                goal_adjust = 0
            #Determine the number of volumes that is the goal.  Basically
            # this takes the sum of the number of tapes closed added to the
            # number of tapes remaining.  However, it is far more complicated.
            #
            # Take 9940A tapes.  They can be migrated as 9940As then
            # clobbered into 9940B tapes.  Thus, more tapes can be migrated
            # than what summary_total will contain at this point in time
            # for the total number of 9940As.  This is where goal_adjust
            # comes into play
            total_volumes = max(total_count + goal_adjust,
                                (closed_count + remaining_count))
            #This is possibly used for adjusting the goal line.  See comm
            if plot_type == ACCUMULATED_LG:
                plot_fp.write('set logscale y\n')
                miny=1.0
            else:
                miny=0
            if total_volumes:
                 percent = min(100,100*(float(closed_count)/total_volumes))
                 #Don't set yrange if total_volumes is zero, otherwise the
                 # plot will fail from the error:
                 # "line 0: Can't plot with an empty y range!"

                 if plot_type != ACCUMULATED_NG:
                    plot_fp.write('set yrange [ %f : %f ]\n' % (miny,total_volumes * 1.1))
            else:
                percent=100
            plot_fp.write('set label "Remaining %s " at graph .05,.95\n' \
                          % (self.summary_remaining.get(key, 0L),))
            plot_fp.write('set label "Started %s" at graph .05,.90\n' \
                          % (self.summary_started.get(key, 0L),))
            plot_fp.write('set label "Migrated %s" at graph .05,.85\n' \
                          % (self.summary_done.get(key, 0L),))
            plot_fp.write('set label "Closed %s (%.2f%% done)" at graph .05,.80\n' \
                          % (self.summary_closed.get(key, 0L),percent))
            plot_fp.write('set label "Total %s" at graph .05,.75\n' \
                          % (total_volumes,))
        else: #Daily
            plot_fp.write('set yrange [ 0 : ]\n')

        #Build the plot command line.
        plot_line = "plot "
        for i in range(len(columns)):
            column = columns[i]
            if plot_line != "plot ":
                #If we are on the first plot, don't append the comma.
                plot_line = "%s, " % (plot_line,)

            #Add the next set of plots.
            plot_line = plot_line + '"%s" using 1:%d title "%s" with impulses lw 10' % (data_filename, column, titles[i])
        #If the plot is accumulated, plot the total to migrate.
        if plot_type in ( ACCUMULATED, ACCUMULATED_LG):
            plot_line = '%s, x,%s title "goal" with lines lw 4' % (plot_line, total_volumes)
            pass
        #Put the whole thing together.
        plot_line = "%s\n" % (plot_line,)

        #Write out the plot line.
        plot_fp.write(plot_line)

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

        self.enstore_name = cron_dict.get("enstore_name", None)

    def fill(self, frame):

        #  here we create data points

        edb = frame.get_configuration_client().get("database", {})
        db = pg.DB(host   = edb.get('dbhost', "localhost"),
                   dbname = edb.get('dbname', "enstoredb"),
                   port   = edb.get('dbport', 5432),
                   user   = edb.get('dbuser', "enstore")
                   )

        ###
        ### Lets get the daily values.
        ###
        print "Starting SQL_COMMAND:", time.ctime()

        #This query is for volumes that are all done.
        res = db.query(SQL_COMMAND).getresult()

        #Open the datafiles.
        self.pts_files = {}
        self.summary_started = {}
        self.summary_done = {}
        self.summary_closed = {}
        for row in res:
            #row[0] is the date (YYYY-mm-dd)
            #row[1] is the media type
            #row[2] is the number of migrated tapes started
            #row[3] is the number of migrated tapes migrated/duplicated
            #row[4] is the number of migrated tapes closed
            if not self.pts_files.get(row[1], None):
                fname = os.path.join(self.temp_dir,
                                     "migration_summary_%s.pts" % (row[1],))
                self.pts_files[row[1]] = open(fname, "w")
                self.summary_started[row[1]] = 0L
                self.summary_done[row[1]] = 0L #Set the key values to zeros.
                self.summary_closed[row[1]] = 0L


        ###
        ### Now that the information for each day is obtained, lets output
        ### the data to the data file.

        #Output to temporary files the data that gnuplot needs to plot.
        for row in res:
            try:
                self.summary_started[row[1]] = self.summary_started[row[1]] + row[2]
            except TypeError:
                pass
            try:
                self.summary_done[row[1]] = self.summary_done[row[1]] + row[3]
            except TypeError:
                pass
            try:
                self.summary_closed[row[1]] = self.summary_closed[row[1]] + row[4]
            except TypeError:
                pass

            #Get the daily values.  If there isn't one, use zero.
            if row[2] == None:
                today_started = 0L
            else:
                today_started = row[2]
            if row[3] == None:
                today_completed = 0L    #completed = migrated/duplicated
            else:
                today_completed = row[3]
            if row[4] == None:
                today_closed = 0L
            else:
                today_closed = row[4]

            # Here we write the contents to the file.
            self.pts_files[row[1]].write("%s %s %s %s %s %s %s\n" % (
                row[0], today_started, today_completed, today_closed,
                self.summary_started[row[1]], self.summary_done[row[1]],
                self.summary_closed[row[1]]))

        #Avoid resource leaks.
        for key in self.pts_files.keys():
            self.pts_files[key].close()


        ###
        ### Now lets get some totals about what is left to go.
        ###
        print "Starting SQL_COMMAND3:", time.ctime()

        #This query is for volumes that are all done.
        res3 = db.query(SQL_COMMAND3).getresult()

        self.summary_remaining = {}
        for row3 in res3:
            #row3[0] is the media type
            #row3[1] is the remaing tapes to migrate
            self.summary_remaining[row3[0]] = row3[1]


        ###
        ### Now lets get some totals about how many tapes there are.
        ###
        print "Starting SQL_COMMAND4:", time.ctime()

        #This query is for volumes that are all done.
        res4 = db.query(SQL_COMMAND4).getresult()

        self.summary_total = {}
        for row4 in res4:
            #row3[0] is the media type
            #row3[1] is the number of tapes
            self.summary_total[row4[0]] = row4[1]


    def plot(self):
        for key in self.pts_files.keys():
            #Key at this point is unique media types (that have been migrated).

            print "Plotting %s accumulated" % (key,)
            self._plot(key, ACCUMULATED)
            print "Plotting %s accumulated_ng" % (key,)
            self._plot(key,ACCUMULATED_NG)
            print "Plotting %s accumulated_lg" % (key,)
            self._plot(key,ACCUMULATED_LG)
            print "Plotting %s daily" % (key,)
            self._plot(key, DAILY)

            #Cleanup the temporary files for this loop.
            try:
                #os.remove(self.pts_files[key].name)
                pass
            except:
                pass

    #Plot type is either ACCUMULATED ACCUULATED_NG ACCUMULATED_LG or DAILY.
    def _plot(self, key, plot_type):

            #Get some filenames for the various files that get created.
            ps_filename = os.path.join(self.web_dir,
                      "migration_%s_%s.ps" % (plot_type, key,))
            jpeg_filename = os.path.join(self.web_dir,
                      "migration_%s_%s.jpg" % (plot_type, key,))
            jpeg_filename_stamp = os.path.join(self.web_dir,
                      "migration_%s_%s_stamp.jpg" % (plot_type, key,))
            plot_filename = os.path.join(self.temp_dir,
                      "migration_%s_%s.plot" % (plot_type, key,))

            titles = ['started', 'migrated/duplicated', 'closed']
            if plot_type in ( ACCUMULATED, ACCUMULATED_NG, ACCUMULATED_LG):
                columns = [5, 6, 7]
            elif plot_type == DAILY:
                columns = [2, 3, 4]
            else:
                columns = [0]  #What happens when we get here?

            #Write the gnuplot command file(s).
            self.write_plot_file(plot_filename, self.pts_files[key].name,
                                 ps_filename, key, plot_type, columns, titles)

            #Make the plot and convert it to jpg.
            os.system("gnuplot < %s" % plot_filename)
            os.system("convert -flatten -background lightgray -rotate 90  %s %s\n"
                      % (ps_filename, jpeg_filename))
            os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                      % (ps_filename, jpeg_filename_stamp))

            #Cleanup the temporary files for this loop.
            try:
                os.remove(plot_filename)
            except:
                pass
