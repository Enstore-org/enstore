###############################################################################
# $Id$
# $Author$
#
# Routines to create the inquisitor plots.
#
###############################################################################

# system import
import threading
import string
import os

# enstore imports
import Trace
import e_errors
import enstore_files
import enstore_plots
import enstore_functions
import enstore_functions2
import enstore_constants
import accounting_query
import configuration_client

LOGFILE_DIR = "logfile_dir"
XFER_SIZE = "xfer-size"


class InquisitorPlots:

    def open_db_connection(self):
        if not self.acc_db:
            # open a connection to the accounting db, the data is there
            acc = self.config_d.get(enstore_constants.ACCOUNTING_SERVER, {})
            if acc:
                self.acc_db = accounting_query.accountingQuery(host=acc.get('dbhost', "localhost"),
                                                               port=acc.get(
                                                                   'dbport', 5432),
                                                               dbname=acc.get(
                                                                   'dbname', ""),
                                                               user=acc.get('dbuser', "enstore"))

    def close_db_connection(self):
        if self.acc_db:
            self.acc_db.close()
            self.acc_db = None

    # create the htl file with the inquisitor plot information
    def make_plot_html_page(self):
        for plotfile_t in self.plotfile_l:
            if len(plotfile_t) > 2:
                plotfile = plotfile_t[0]
                html_dir = plotfile_t[1]
                links_l = plotfile_t[2:]
            else:
                (plotfile, html_dir) = plotfile_t
                links_l = None
                nav_link = ""
            plotfile.open()
            # get the list of stamps and jpg files
            (jpgs, stamps, pss) = enstore_plots.find_jpg_files(html_dir)
            plotfile.write(jpgs, stamps, pss, self.mount_label, links_l)
            plotfile.close()
            plotfile.install()

    # figure out how much information to get out of the db
    # if start_time specified : do start_time -> latest
    # if stop_time specified  : do 30 days ago -> stop_time
    # if start_time and stop_time are specified :
    #                           do start_time -> stop_time
    def make_time_query(self, column):
        time_q = "%s to_char(%s, 'YYYY-MM-DD') %s" % (accounting_query.WHERE, column,
                                                      accounting_query.GREATER)
        if not self.start_time:
            # only use the last 30 days
            self.start_time = self.acc_db.days_ago(30)
        time_q = "%s '%s'" % (time_q, self.start_time)

        if self.stop_time:
            time_q = "%s %s %s %s '%s'" % (time_q, accounting_query.AND, column, accounting_query.LESS,
                                           self.stop_time)
        return time_q

    # make the mount plots (mounts per hour and mount latency)
    def mount_plot(self, prefix):
        self.open_db_connection()

        # get the config file so we can not plot null mover data
        config = enstore_functions.get_config_dict()
        if config:
            config = config.configdict
        else:
            config = {}

        # get the mount information out of the accounting db. it is in
        # the tape_mounts table
        db_table = "tape_mounts"
        start_col = "start"
        finish_col = "finish"
        type_col = "type"
        table = "tape_mounts"

        time_q = self.make_time_query(start_col)

        # only need mount requests
        time_q = "%s %s state%s'M'" % (
            time_q, accounting_query.AND, accounting_query.EQUALS)

        # get the total mounts/day.  the results are in a list
        # where each element looks like -
        #  {'start': '2003-03-27', 'total': '345'}
        query = "select to_char(start , 'YYYY-MM-DD') as %s, count(*) as total from %s %s group by to_char(start, 'YYYY-MM-DD') order by to_char(start, 'YYYY-MM-DD')" % (start_col, table, time_q)
        total_mounts = self.acc_db.query(query)

        # get the mounts for each drive type.  first get a list of the drive
        # types
        query = "select distinct %s from %s" % (type_col, table)
        drive_types = self.acc_db.query(query)
        total_mounts_type = {}
        # for each drive type get a list of the mounts/day
        # the results are in a dict which holds a list where each element
        # looks like -
        #  {'start': '2003-03-27 00:05:55', 'latency': '00:00:25'}
        for type_d in drive_types.dictresult():
            aType = type_d[type_col]
            query = "select to_char(start , 'YYYY-MM-DD') as %s, count(*) as total from %s %s %s %s='%s' group by to_char(start, 'YYYY-MM-DD') order by to_char(start, 'YYYY-MM-DD')" % (start_col, table, time_q,
                                                                                                                                                                                         accounting_query.AND, type_col,
                                                                                                                                                                                         aType)
            total_mounts_type[aType] = self.acc_db.query(query).dictresult()

        # only do the plotting if we have some data
        if total_mounts:
            # now save any new mount count data for the continuing total
            # count. and create the overall total mount plot
            mpdfile = enstore_plots.MpdDataFile(
                self.output_dir, self.mount_label)
            mpdfile.open()
            total_mounts_l = total_mounts.dictresult()
            mpdfile.plot(total_mounts_l)
            mpdfile.close()
            mpdfile.install(self.html_dir)

            mmpdfile = enstore_plots.MpdMonthDataFile(
                self.output_dir, self.mount_label)
            mmpdfile.open()
            mmpdfile.plot(total_mounts_l, total_mounts_type)
            mmpdfile.close()
            mmpdfile.install(self.html_dir)
            mmpdfile.cleanup(self.keep, self.keep_dir)

        # close db connection
        self.close_db_connection()

    # make the total transfers per unit of time and the bytes moved per day
    # plot
    def encp_plot(self, prefix):
        ofn = self.output_dir + "/bytes_moved.txt"

        # open connection to db.
        self.open_db_connection()

        # Dmitry:
        # Kludge: this seems like  the only way I can get storage groups efficiently
        #
        res = self.acc_db.query(
            "select distinct(storage_group) from encp_xfer_average_by_storage_group")
        storage_groups = []
        for row in res.getresult():
            if not row:
                continue
            storage_groups.append(row[0])

        # always add /dev/null to the end of the list of files to search thru
        # so that grep always has > 1 file and will always print the name of
        # the file at the beginning of the line. do not count any null moves.

        encpfile = enstore_files.EnEncpDataFile("%s* /dev/null" % (prefix,), ofn,
                                                "-e %s" % (Trace.MSG_ENCP_XFER,),
                                                self.logfile_dir,
                                                "| grep -v %s" % (enstore_constants.NULL_DRIVER,))
        # only extract the information from the newly created file that is
        # within the requested timeframe.
        encpfile.open('r')
        os.system("cp %s %s_1" % (encpfile.file_name, encpfile.file_name,))
        encpfile.read_and_parse(
            self.start_time,
            self.stop_time,
            prefix,
            self.media_changer)
        encpfile.close()
        encpfile.cleanup(self.keep, self.keep_dir)

        #
        # This is a test, this is a test. I am testing retrieval of data from DB. I replaces encpfile.data -> self.data
        #
        self.data = []
        self.start_time = self.acc_db.days_ago(30)

        encp_q = "select to_char(date,'YYYY-MM-DD:hh24:mi:ss'), size, rw, mover, drive_id, storage_group from encp_xfer where date > '%s' and driver != '%s'" % (
            self.start_time, enstore_constants.NULL_DRIVER,)
        res = self.acc_db.query(encp_q)
        for row in res.getresult():
            if not row:
                continue
            self.data.append([row[0], row[1], row[2], row[3], row[4], row[5]])

        # only do the plotting if we have some data
        if self.data:
            # overall bytes/per/day count
            bpdfile = enstore_plots.BpdDataFile(self.output_dir)
            bpdfile.open()
            bpdfile.plot(self.data)
            bpdfile.close()
            bpdfile.install(self.html_dir)

            mbpdfile = enstore_plots.BpdMonthDataFile(self.output_dir)
            mbpdfile.open()
            mbpdfile.plot(bpdfile.write_ctr)
            mbpdfile.close()
            mbpdfile.install(self.html_dir)

            xferfile = enstore_plots.XferDataFile(self.output_dir,
                                                  mbpdfile.ptsfile)
            xferfile.open()
            xferfile.plot(self.data)
            xferfile.close()
            xferfile.install(self.html_dir)
            xferfile.cleanup(self.keep, self.keep_dir)

            #
            # check if there are other storage groups
            #

            if (len(storage_groups) > 1):
                for sg in storage_groups:

                    xferfile = enstore_plots.XferDataFile(self.output_dir,
                                                          mbpdfile.ptsfile, sg)
                    xferfile.open()
                    xferfile.plot(self.data)
                    xferfile.close()
                    xferfile.install(
                        self.html_dir + "/" + XFER_SIZE)  # Kludge (Dmitry)
                    xferfile.cleanup(self.keep, self.keep_dir)

            # delete any extraneous files. do it here because the xfer file
            # plotting needs the bpd data file
            bpdfile.cleanup(self.keep, self.keep_dir)
            mbpdfile.cleanup(self.keep, self.keep_dir)

        self.close_db_connection()

    # make the plot showing queue movement for different storage groups plot
    def sg_plot(self, prefix):
        ofn = self.output_dir + "/sg_lmq.txt"

        # open connection to db.
        self.open_db_connection()

        # always add /dev/null to the end of the list of files to search thru
        # so that grep always has > 1 file and will always print the name of
        # the file at the beginning of the line.
        sgfile = enstore_files.EnSgDataFile("%s* /dev/null" % (prefix,), ofn,
                                            "-e %s" % (Trace.MSG_ADD_TO_LMQ,),
                                            self.logfile_dir)
        # only extract the information from the newly created file that is
        # within the requested timeframe.
        sgfile.open('r')
        sgfile.timed_read(self.start_time, self.stop_time, prefix)
        # now pull out the info we are going to plot from the lines
        sgfile.parse_data(prefix)
        sgfile.close()
        sgfile.cleanup(self.keep, self.keep_dir)

        # only do the plotting if we have some data
        if sgfile.data:
            sgplot = enstore_plots.SgDataFile(self.output_dir)
            sgplot.open()
            sgplot.plot(sgfile.data)
            sgplot.close()
            sgplot.install(self.html_dir)

            # delete any extraneous files. do it here because the xfer file
            # plotting needs the bpd data file
            sgplot.cleanup(self.keep, self.keep_dir)
        # close db connection
        self.close_db_connection()

    def get_bpd_files(self):
        #
        # Dmitry is hacking below, getting info from config server
        #   *I assume that config server and points are on the same node!!!!*
        #
        servers = self.config_d.get('known_config_servers', [])
        nodes_and_dirs = {}
        for server in servers:
            if (server == 'status'):
                continue
            server_name, server_port = servers.get(server)
            #
            # access config server to get locatin of web directory
            #
            csc = configuration_client.ConfigurationClient(
                (server_name, server_port))
            inq_d = csc.get(enstore_constants.INQUISITOR, {})
            nodes_and_dirs[server_name.split('.')[0]] = inq_d.get(
                "html_file", "/fnal/ups/prd/www_pages/enstore")
        files_l = []
        pts_file_only = "%s%s" % (
            enstore_constants.BPD_FILE, enstore_plots.PTS)
        this_node = enstore_functions2.strip_node(os.uname()[1])
        for node in nodes_and_dirs.keys():
            destination_directory = nodes_and_dirs.get(node)
            pts_file = "%s/%s" % (destination_directory, pts_file_only)
            if enstore_functions2.ping(node) == enstore_constants.IS_ALIVE:
                new_file = "/tmp/%s.%s" % (pts_file_only, node)
                if node == this_node:
                    rtn = os.system("cp %s %s" % (pts_file, new_file))
                else:
                    rtn = enstore_functions2.get_remote_file(
                        node, pts_file, new_file)
                if rtn == 0:
                    files_l.append((new_file, node))
                else:
                    Trace.log(e_errors.WARNING,
                              "could not copy %s from %s for total bytes plot" % (pts_file, node))
            else:
                Trace.log(e_errors.WARNING,
                          "could not ping,  %s from %s for total bytes plot" % (pts_file, node))
        return files_l

    def fill_in_bpd_d(self, bpdfile, data_d, node):
        if bpdfile.lines:
            for line in bpdfile.lines:
                fields = string.split(string.strip(line))
                # this is the date
                if fields[0] not in data_d:
                    data_d[fields[0]] = {}
                if len(fields) > 2:
                    # we have data , need date, total, ctr, writes
                    data_d[fields[0]][node] = {enstore_plots.TOTAL: float(fields[1]),
                                               enstore_plots.CTR: enstore_plots.get_ctr(fields),
                                               enstore_plots.WRITES: float(fields[3])}
                else:
                    data_d[fields[0]][node] = {enstore_plots.TOTAL: 0.0,
                                               enstore_plots.CTR: 0,
                                               enstore_plots.WRITES: 0.0}

    def read_bpd_data(self, files_l):
        data_d = {}
        for filename, node in files_l:
            bpdfile = enstore_files.EnstoreBpdFile(filename)
            bpdfile.open('r')
            # only extract the info that is within the requested time frame
            bpdfile.timed_read(self.start_time, self.stop_time)
            bpdfile.close()
            # now fill in the data dictionary with the data
            self.fill_in_bpd_d(bpdfile, data_d, node)
        return data_d

    def total_bytes_plot(self):
        # we need to copy the enplot_bpd.pts files from all the nodes that are not
        # our own.
        files_l = self.get_bpd_files()
        # for each file we have, open it and read the requested data
        data_d = self.read_bpd_data(files_l)
        # now write out the data and plot it
        if data_d:
            tbpdfile = enstore_plots.TotalBpdDataFile(self.output_dir)
            tbpdfile.open()
            tbpdfile.plot(data_d)
            tbpdfile.close()
            tbpdfile.install(self.html_dir)
            tbpdfile.cleanup(self.keep, self.keep_dir)

    #  make all the plots
    def plot(self, encp, mount, sg, total_bytes):
        ld = {}
        self.acc_db = None
        # find out where the log files are located
        if self.logfile_dir is None:
            ld = self.config_d.get("log_server")
            self.logfile_dir = ld["log_file_path"]

        if self.output_dir is None:
            self.output_dir = self.logfile_dir

        # get the list of alternate log files.  we may need to grep these instead of the
        # normal ones
        if not ld:
            ld = self.config_d.get("log_server")
        alt_logs = ld.get("msg_type_logs", {})

        if encp:
            enstore_functions.inqTrace(enstore_constants.PLOTTING,
                                       "Creating inq transfer plots")
            alt_key = string.strip(Trace.MSG_ENCP_XFER)
            self.encp_plot(alt_logs.get(alt_key, enstore_constants.LOG_PREFIX))
        if mount:
            enstore_functions.inqTrace(enstore_constants.PLOTTING,
                                       "Creating inq mount plots")
            alt_key = string.strip(Trace.MSG_MC_LOAD_REQ)
            self.mount_plot(
                alt_logs.get(
                    alt_key,
                    enstore_constants.LOG_PREFIX))
        if sg:
            enstore_functions.inqTrace(enstore_constants.PLOTTING,
                                       "Creating inq storage group plots")
            alt_key = string.strip(Trace.MSG_ADD_TO_LMQ)
            self.sg_plot(alt_logs.get(alt_key, enstore_constants.LOG_PREFIX))
        if total_bytes:
            enstore_functions.inqTrace(enstore_constants.PLOTTING,
                                       "Creating inq total bytes summary plot")
            self.total_bytes_plot()

        # update the inquisitor plots web page
        if not self.no_plot_html:
            Trace.trace(11, "Creating the inq plot web page")
            self.make_plot_html_page()
