#!/usr/bin/env python
###############################################################################
# $Author$
# $Date$
# $Id$
#
#  public | xfer_by_day                        | table | enstore
#  public | xfer_by_month                      | table | enstore
#
###############################################################################
from __future__ import print_function
import sys
import os
import string
import time
import math
import configuration_client
import pg
import enstore_constants
import histogram
import thread
PB = 1024. * 1024. * 1024. * 1024. * 1024.
TB = 1024. * 1024. * 1024. * 1024.
GB = 1024. * 1024. * 1024.
MB = 1024. * 1024.
KB = 1024.

SELECT_STMT = "select date,sum(read),sum(write) from xfer_by_day group by date order by date"
SELECT_STMT1 = "select date,sum(read),sum(write) from xfer_by_month group by date order by date"

SELECT_DELETED_BYTES = "select to_char(state.time, 'YYYY-MM-DD HH:MM:SS'), sum(file.size)::bigint from file, state where state.volume=file.volume and state.value='DELETED' group by state.time"
SELECT_WRITTEN_BYTES = "select substr(bfid,5,10), size from file, volume  where file.volume = volume.id and not label like '%.deleted' and media_type != 'null'"


def showError(msg):
    sys.stderr.write("Error: " + msg)


def usage():
    print("Usage: %s  <file_family> " % (sys.argv[0],))


def decorate(h, color, ylabel, marker):
    h.set_time_axis(True)
    h.set_ylabel(ylabel)
    h.set_xlabel("Date (year-month-day)")
    h.set_line_color(color)
    h.set_line_width(20)
    h.set_marker_text(marker)
    h.set_marker_type("impulses")


exitmutexes = []


def fill_histograms(i, server_name, server_port, hlist):
    config_server_client = configuration_client.ConfigurationClient(
        (server_name, server_port))
    acc = config_server_client.get("database", {})
    db_server_name = acc.get('db_host')
    db_name = acc.get('dbname')
    db_port = acc.get('db_port')
    name = db_server_name.split('.')[0]
    name = db_server_name.split('.')[0]
    print("we are in thread ", i, db_server_name, db_name, db_port)

    h = hlist[4 * i]
    h1 = hlist[4 * i + 1]
    h2 = hlist[4 * i + 2]
    h3 = hlist[4 * i + 3]

    if db_port:
        db = pg.DB(
            host=db_server_name,
            user=acc.get('dbuser'),
            dbname=db_name,
            port=db_port)
    else:
        db = pg.DB(host=db_server_name, user=acc.get('dbuser'), dbname=db_name)
    res = db.query(SELECT_DELETED_BYTES)
    for row in res.getresult():
        if not row:
            continue
        h2.fill(
            time.mktime(
                time.strptime(
                    row[0],
                    '%Y-%m-%d %H:%M:%S')),
            row[1] / TB)
        h3.fill(
            time.mktime(
                time.strptime(
                    row[0],
                    '%Y-%m-%d %H:%M:%S')),
            row[1] / TB)
    res = db.query(SELECT_WRITTEN_BYTES)
    for row in res.getresult():
        if not row:
            continue
        h.fill(float(row[0]), row[1] / TB)
        h1.fill(float(row[0]), row[1] / TB)
    db.close()
    exitmutexes[i] = 1


def plot_bpd():
    #
    # this function creates plots of bytes transferred per day and per month
    # based on data on accounting database (*ensrv6)
    #
    intf = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc = configuration_client.ConfigurationClient(
        (intf.config_host, intf.config_port))
    if (0):
        acc = csc.get(enstore_constants.ACCOUNTING_SERVER)
        inq = csc.get('inquisitor')
        inq_host = inq.get('www_host').split('/')[2]
    servers = []
    servers = []
    servers = csc.get('known_config_servers')
    histograms = []

    now_time = time.time()
    t = time.ctime(time.time())
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)

    now_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
    start_time = now_time - 31 * 3600 * 24
    start_day = time.mktime((2002, 12, 31, 23, 59, 59, 0, 0, 0))
    now_day = time.mktime((Y + 1, 12, 31, 23, 59, 59, wd, jd, dst))
    nbins = int((now_day - start_day) / (24. * 3600.) + 0.5)
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(start_time)
    start_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))

    color = 1

    s = histogram.Histogram1D(
        "xfers_total_by_day",
        "Total Bytes Transferred per Day By Enstore",
        31,
        float(start_time),
        float(now_time))
    s1 = histogram.Histogram1D(
        "xfers_write_by_day",
        "Total Bytes Written per Day By Enstore",
        31,
        float(start_time),
        float(now_time))
    s2 = histogram.Histogram1D(
        "xfers_read_by_da",
        "Total Bytes Read per Day By Enstore",
        31,
        float(start_time),
        float(now_time))

    s3 = histogram.Histogram1D(
        "xfers_total_by_month",
        "Total Bytes Transferred per Month By",
        nbins,
        float(start_day),
        float(now_day))
    s4 = histogram.Histogram1D(
        "xfers_write_by_month",
        "Total Bytes Written per Month By",
        nbins,
        float(start_day),
        float(now_day))
    s5 = histogram.Histogram1D(
        "xfers_read_by_month",
        "Total Bytes Read per Month By",
        nbins,
        float(start_day),
        float(now_day))

    s.set_time_axis(True)
    s1.set_time_axis(True)
    s2.set_time_axis(True)
    s3.set_time_axis(True)
    s4.set_time_axis(True)
    s5.set_time_axis(True)

    plotter = histogram.Plotter(
        "xfers_total_by_day",
        "Total TBytes Transferred per Day By Enstore")
    plotter1 = histogram.Plotter(
        "xfers_total_by_month",
        "Total TBytes Transferred per Month By Enstore")

    plotter = histogram.Plotter(
        "xfers_total_by_day",
        "Total TBytes Transferred per Day By Enstore")
    plotter1 = histogram.Plotter(
        "xfers_total_by_month",
        "Total TBytes Transferred per Month By Enstore")

    s_i = histogram.Histogram1D(
        "integrated_xfers_total_by_day",
        "Integrated total Bytes transferred per Day By Enstore",
        31,
        float(start_time),
        float(now_time))
    s3_i = histogram.Histogram1D(
        "integrated_xfers_total_by_month",
        "Integarted total Bytes transferred per Month By Enstore",
        nbins,
        float(start_day),
        float(now_day))

    s_i.set_time_axis(True)
    s3_i.set_time_axis(True)

    iplotter = histogram.Plotter(
        "integrated_xfers_total_by_day",
        "Integrated total Bytes transferred per Day By Enstore")
    iplotter1 = histogram.Plotter(
        "integrated_xfers_total_by_month",
        "Integarted total Bytes transferred per Month By Enstore")

    w_day = 0.
    r_day = 0.
    t_day = 0.

    w_month = 0.
    r_month = 0.
    t_month = 0.

    n_day = 0
    n_month = 0

    for server in servers:
        server_name, server_port = servers.get(server)
        if (server_port is not None):
            config_server_client = configuration_client.ConfigurationClient(
                (server_name, server_port))
            acc = config_server_client.get(enstore_constants.ACCOUNTING_SERVER)
            db_server_name = acc.get('dbhost')
            db_name = acc.get('dbname')
            db_port = acc.get('dbport')
            name = db_server_name.split('.')[0]
            name = db_server_name.split('.')[0]

            h = histogram.Histogram1D(
                "xfers_total_by_day_%s" %
                (name,), "Total Bytes Transferred per Day By  %s" %
                (server,), 31, float(start_time), float(now_time))
            h1 = histogram.Histogram1D(
                "xfers_write_by_day_%s" %
                (name,), "Total Bytes Written per Day By  %s" %
                (server,), 31, float(start_time), float(now_time))
            h2 = histogram.Histogram1D(
                "xfers_read_by_day_%s" %
                (name,), "Total Bytes Read per Day By  %s" %
                (server,), 31, float(start_time), float(now_time))

            h3 = histogram.Histogram1D(
                "xfers_total_by_month_%s" %
                (name,), "Total Bytes Transferred per Month By %s" %
                (server,), nbins, float(start_day), float(now_day))
            h4 = histogram.Histogram1D(
                "xfers_write_by_month_%s" %
                (name,), "Total Bytes Written per Month By %s" %
                (server,), nbins, float(start_day), float(now_day))
            h5 = histogram.Histogram1D(
                "xfers_read_by_month_%s" %
                (name,), "Total Bytes Read per Month By %s" %
                (server,), nbins, float(start_day), float(now_day))

            h.set_time_axis(True)
            h.set_ylabel("Bytes")
            h.set_xlabel("Date (year-month-day)")
            h.set_line_color(color)
            h.set_line_width(20)

            h1.set_time_axis(True)
            h1.set_ylabel("Bytes")
            h1.set_xlabel("Date (year-month-day)")
            h1.set_line_color(color)
            h1.set_line_width(20)

            h2.set_time_axis(True)
            h2.set_ylabel("Bytes")
            h2.set_xlabel("Date (year-month-day)")
            h2.set_line_color(color)
            h2.set_line_width(20)

            h3.set_time_axis(True)
            h3.set_ylabel("Bytes")
            h3.set_xlabel("Date (year-month-day)")
            h3.set_line_color(color)
            h3.set_line_width(20)

            h4.set_time_axis(True)
            h4.set_ylabel("Bytes")
            h4.set_xlabel("Date (year-month-day)")
            h4.set_line_color(color)
            h4.set_line_width(20)

            h5.set_time_axis(True)
            h5.set_ylabel("Bytes")
            h5.set_xlabel("Date (year-month-day)")
            h5.set_line_color(color)
            h5.set_line_width(20)
            color = color + 1
            if db_port:
                db = pg.DB(
                    host=db_server_name,
                    user=acc.get('dbuser'),
                    dbname=db_name,
                    port=db_port)
            else:
                db = pg.DB(
                    host=db_server_name,
                    user=acc.get('dbuser'),
                    dbname=db_name)
            res = db.query(SELECT_STMT)
            for row in res.getresult():
                if not row:
                    continue
                n_day = n_day + 1
                h.fill(
                    time.mktime(
                        time.strptime(
                            row[0],
                            '%Y-%m-%d')),
                    (row[1] + row[2]) / TB)
#                h1.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d')),row[2])
#                h2.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d')),row[1])
            res = db.query(SELECT_STMT1)
            for row in res.getresult():
                if not row:
                    continue
                h3.fill(
                    time.mktime(
                        time.strptime(
                            row[0],
                            '%Y-%m-%d')),
                    (row[1] + row[2]) / TB)
#                h4.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d')),row[2])
#                h5.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d')),row[1])
            db.close()

            tmp = s + h
            tmp.set_name("xfer_%s" % (server,))
            tmp.set_data_file_name(server)
            tmp.set_marker_text(server)
            tmp.set_time_axis(True)
            tmp.set_ylabel("TByte/day")
            tmp.set_marker_type("impulses")
            tmp.set_line_color(color)
            tmp.set_line_width(20)
            plotter.add(tmp)
            s = tmp

            tmp = s3 + h3
            tmp.set_name("xfer_%s" % (server,))
            tmp.set_data_file_name(server)
            tmp.set_marker_text(server)
            tmp.set_time_axis(True)
            tmp.set_ylabel("TByte/month")
            tmp.set_marker_type("impulses")
            tmp.set_line_color(color)
            tmp.set_line_width(20)
            plotter1.add(tmp)
            s3 = tmp

            integral = h.integral()
            integral3 = h3.integral()

            integral.set_marker_text(server)
            integral.set_marker_type("impulses")
            integral.set_ylabel("TB")

            integral3.set_marker_text(server)
            integral3.set_marker_type("impulses")
            integral3.set_ylabel("TB")

            tmp = s_i + integral
            tmp.set_name(
                "integrated_xfers_daily_%s" %
                (integral.get_marker_text(),))
            tmp.set_data_file_name(
                "integrated_xfers_daily_%s" %
                (integral.get_marker_text(),))
            tmp.set_marker_text(integral.get_marker_text())
            tmp.set_time_axis(True)
            tmp.set_ylabel(integral.get_ylabel())
            tmp.set_marker_type(integral.get_marker_type())
            tmp.set_line_color(color)
            tmp.set_line_width(20)
            iplotter.add(tmp)
            s_i = tmp

            tmp = s3_i + integral3
            tmp.set_name(
                "integrated_xfers_monthly_%s" %
                (integral3.get_marker_text(),))
            tmp.set_data_file_name(
                "integrated_xfers_monthly_%s" %
                (integral3.get_marker_text(),))
            tmp.set_marker_text(integral3.get_marker_text())
            tmp.set_time_axis(True)
            tmp.set_ylabel(integral3.get_ylabel())
            tmp.set_marker_type(integral3.get_marker_type())
            tmp.set_line_color(color)
            tmp.set_line_width(20)
            iplotter1.add(tmp)
            s3_i = tmp

    plotter.reshuffle()
    tmp = plotter.get_histogram_list()[0]

    t_day_max = 0.
    i_day_max = 0

    t_day_min = 1.e+32
    i_day_min = 0

    for i in range(tmp.n_bins()):
        t_day = t_day + tmp.get_bin_content(i)
        if (tmp.get_bin_content(i) > t_day_max):
            t_day_max = tmp.get_bin_content(i)
            i_day_max = i
        if (tmp.get_bin_content(i) < t_day_min and tmp.get_bin_content(i) > 0):
            t_day_min = tmp.get_bin_content(i)
            i_day_min = i

    tmp.set_line_color(1)

    delta = tmp.binarray[i_day_max] * 0.05

    tmp.add_text("set label \"%5d\" at \"%s\",%f right rotate font \"Helvetica,12\"\n" % (tmp.binarray[i_day_max] + 0.5,
                                                                                          time.strftime(
                                                                                              "%Y-%m-%d %H:%M:%S", time.localtime(tmp.get_bin_center(i_day_max))),
                                                                                          tmp.binarray[i_day_max] + delta,))

    tmp.add_text("set label \"%5d\" at \"%s\",%f right rotate font \"Helvetica,12\"\n" % (tmp.binarray[i_day_min] + 0.5,
                                                                                          time.strftime(
                                                                                              "%Y-%m-%d %H:%M:%S", time.localtime(tmp.get_bin_center(i_day_min))),
                                                                                          tmp.binarray[i_day_min] + delta,))

    tmp.add_text(
        "set label \"Total :  %5d TB  \" at graph .8,.8  font \"Helvetica,13\"\n" %
        (t_day + 0.5,))
    tmp.add_text("set label \"Max   :  %5d TB (on %5s) \" at graph .8,.75  font \"Helvetica,13\"\n" % (t_day_max + 0.5,
                                                                                                       time.strftime("%m-%d", time.localtime(tmp.get_bin_center(i_day_max))),))
    tmp.add_text("set label \"Min   :  %5d TB (on %5s) \" at graph .8,.70  font \"Helvetica,13\"\n" % (t_day_min + 0.5,
                                                                                                       time.strftime("%m-%d", time.localtime(tmp.get_bin_center(i_day_min))),))
    tmp.add_text("set label \"Mean  :  %5d TB \" at graph .8,.65  font \"Helvetica,13\"\n" % (
        t_day / (tmp.n_bins() - 1) + 0.5,))

    r_day = float(r_day) / float(n_day)
    w_day = float(w_day) / float(n_day)
    t_day = float(t_day) / float(n_day)

    plotter.plot()

    plotter1.reshuffle()
    tmp = plotter1.get_histogram_list()[0]

    t_month_max = 0.
    i_month_max = 0

    t_month_min = 1.e+32
    i_month_min = 0

    for i in range(tmp.n_bins()):
        if (tmp.get_bin_content(i) > 0):
            n_month = n_month + 1
        t_month = t_month + tmp.get_bin_content(i)
        if (tmp.get_bin_content(i) > t_month_max):
            t_month_max = tmp.get_bin_content(i)
            i_month_max = i
        if (tmp.get_bin_content(i) < t_month_min and tmp.get_bin_content(i) > 0):
            t_month_min = tmp.get_bin_content(i)
            i_month_min = i

    tmp.set_line_color(1)

    delta = tmp.binarray[i_month_max] * 0.05

    tmp.add_text("set label \"%10d\" at \"%s\",%f right rotate font \"Helvetica,12\"\n" % (tmp.binarray[i_month_max] + 0.5,
                                                                                           time.strftime(
                                                                                               "%Y-%m-%d %H:%M:%S", time.localtime(tmp.get_bin_center(i_month_max))),
                                                                                           tmp.binarray[i_month_max] + delta,))

    tmp.add_text("set label \"%10d\" at \"%s\",%f right rotate font \"Helvetica,12\"\n" % (tmp.binarray[i_month_min] + 0.5,
                                                                                           time.strftime(
                                                                                               "%Y-%m-%d %H:%M:%S", time.localtime(tmp.get_bin_center(i_month_min))),
                                                                                           tmp.binarray[i_month_min] + delta,))

    tmp.add_text(
        "set label \"Total :  %5d TB  \" at graph .05,.9  font \"Helvetica,13\"\n" %
        (t_month + 0.5,))
    tmp.add_text("set label \"Max   :  %5d TB (on %5s) \" at graph .05,.85  font \"Helvetica,13\"\n" % (t_month_max + 0.5,
                                                                                                        time.strftime("%Y-%m", time.localtime(tmp.get_bin_center(i_month_max))),))
    tmp.add_text("set label \"Min   :  %5d TB (on %5s) \" at graph .05,.80  font \"Helvetica,13\"\n" % (t_month_min + 0.5,
                                                                                                        time.strftime("%Y-%m", time.localtime(tmp.get_bin_center(i_month_min))),))
    tmp.add_text(
        "set label \"Mean  :  %5d TB \" at graph .05,.75  font \"Helvetica,13\"\n" %
        (t_month / n_month + 0.5,))

    plotter1.plot()

    iplotter.reshuffle()
    tmp = iplotter.get_histogram_list()[0]
    tmp.set_line_color(1)
    tmp.set_marker_type("impulses")
    iplotter.plot()

    iplotter1.reshuffle()
    tmp = iplotter1.get_histogram_list()[0]
    tmp.set_line_color(1)
    tmp.set_marker_type("impulses")
    iplotter1.plot()


def plot_bytes():
    #
    # This function plots bytes written/deleted to/from Enstore base on data in file and volume tables
    # from *ensrv0 postgress databases damn slow
    #
    intf = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc = configuration_client.ConfigurationClient(
        (intf.config_host, intf.config_port))
    servers = []
    servers = []
    servers = csc.get('known_config_servers')
    histograms = []

    now_time = time.time()
    t = time.ctime(time.time())
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)

    now_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
    start_time = now_time - 31 * 3600 * 24
    start_day = time.mktime((2001, 12, 31, 23, 59, 59, 0, 0, 0))
    now_day = time.mktime((Y, 12, 31, 23, 59, 59, wd, jd, dst))
    nbins = int((now_day - start_day) / (24. * 3600.) + 0.5)
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(start_time)
    start_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))

    s = histogram.Histogram1D(
        "writes_total_by_month",
        "Total bytes written per month to Enstore",
        nbins,
        float(start_day),
        float(now_day))
    s1 = histogram.Histogram1D(
        "writes_total_by_day",
        "Total bytes written per day by Enstore",
        31,
        float(start_time),
        float(now_time))
    s2 = histogram.Histogram1D(
        "deletes_total_by_month",
        "Total bytes deleted per month from Enstore",
        nbins,
        float(start_day),
        float(now_day))
    s3 = histogram.Histogram1D(
        "deletes_total_by_day",
        "Total bytes deleted  per day from Enstore",
        31,
        float(start_time),
        float(now_time))

    s.set_time_axis(True)
    s1.set_time_axis(True)
    s2.set_time_axis(True)
    s3.set_time_axis(True)

    plotter = histogram.Plotter(
        "writes_total_by_month",
        "Total TBytes written per month by Enstore")
    plotter1 = histogram.Plotter(
        "writes_total_by_day",
        "Total TBytes written per day by Enstore")
    plotter2 = histogram.Plotter(
        "deletes_total_by_month",
        "Total TBytes deleted per month from Enstore")
    plotter3 = histogram.Plotter(
        "deletes_total_by_day",
        "Total TBytes deleted per day from Enstore")

    s_i = histogram.Histogram1D(
        "writes_total_by_month",
        "Integrated Total bytes written per month to Enstore",
        nbins,
        float(start_day),
        float(now_day))
    s1_i = histogram.Histogram1D(
        "writes_total_by_day",
        "Integrated Total bytes written per day by Enstore",
        31,
        float(start_time),
        float(now_time))
    s2_i = histogram.Histogram1D(
        "deletes_total_by_month",
        "Integrated Total bytes deleted per month from Enstore",
        nbins,
        float(start_day),
        float(now_day))
    s3_i = histogram.Histogram1D(
        "deletes_total_by_day",
        "Integrated Total bytes deleted  per day from Enstore",
        31,
        float(start_time),
        float(now_time))

    s_i.set_time_axis(True)
    s1_i.set_time_axis(True)
    s2_i.set_time_axis(True)
    s3_i.set_time_axis(True)

    iplotter = histogram.Plotter(
        "integrated_writes_total_by_month",
        "Integrated Total TBytes written per month by Enstore")
    iplotter1 = histogram.Plotter(
        "integrated_writes_total_by_day",
        "Integrated Total TBytes written per day by Enstore")
    iplotter2 = histogram.Plotter(
        "integrated_deletes_total_by_month",
        "Integrated Total TBytes deleted per month from Enstore")
    iplotter3 = histogram.Plotter(
        "integrated_deletes_total_by_day",
        "Integrated Total TBytes deleted per day from Enstore")

    i = 0
    color = 1
    for server in servers:
        server_name, server_port = servers.get(server)
#        if (server == "stken") : continue
        if (server_port is not None):

            h = histogram.Histogram1D(
                "writes_by_month_%s" %
                (server,), "Total Bytes Written by Month By %s" %
                (server,), nbins, float(start_day), float(now_day))
            decorate(h, color, "TB/month", server)
            histograms.append(h)

            h = histogram.Histogram1D(
                "writes_by_day_%s" %
                (server,), "Total Bytes Written by Day By %s" %
                (server,), 31, float(start_time), float(now_time))
            decorate(h, color, "TB/day", server)
            histograms.append(h)

            h = histogram.Histogram1D(
                "deletes_by_month_%s" %
                (server,), "Total Bytes Deleted by Month By %s" %
                (server,), nbins, float(start_day), float(now_day))
            decorate(h, color, "TB/month", server)
            histograms.append(h)

            h = histogram.Histogram1D(
                "deletes_by_day_%s" %
                (server,), "Total Bytes Deleted by Day By %s" %
                (server,), 31, float(start_time), float(now_time))
            decorate(h, color, "TB/day", server)
            histograms.append(h)

            exitmutexes.append(0)
            thread.start_new(
                fill_histograms, (i, server_name, server_port, histograms))
            i = i + 1
            color = color + 1

    while 0 in exitmutexes:
        pass

    i = 0
    for i in range(len(histograms) / 4):
        h = histograms[i * 4]
        h1 = histograms[4 * i + 1]
        h2 = histograms[4 * i + 2]
        h3 = histograms[4 * i + 3]
        color = int(i / 4) + 1

        tmp = s + h
        tmp.set_name("writes_monthly_%s" % (h.get_marker_text(),))
        tmp.set_data_file_name("writes_monthly_%s" % (h.get_marker_text(),))
        tmp.set_marker_text(h.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(h.get_ylabel())
        tmp.set_marker_type(h.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(20)
        plotter.add(tmp)
        s = tmp

        tmp = s1 + h1
        tmp.set_name("writes_daily_%s" % (h1.get_marker_text(),))
        tmp.set_data_file_name("writes_daily_%s" % (h1.get_marker_text(),))
        tmp.set_marker_text(h1.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(h1.get_ylabel())
        tmp.set_marker_type(h1.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(20)
        plotter1.add(tmp)
        s1 = tmp

        tmp = s2 + h2
        tmp.set_name("deletes_monthly_%s" % (h2.get_marker_text(),))
        tmp.set_data_file_name("deletes_monthly_%s" % (h2.get_marker_text(),))
        tmp.set_marker_text(h2.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(h2.get_ylabel())
        tmp.set_marker_type(h2.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(20)
        plotter2.add(tmp)
        s2 = tmp

        tmp = s3 + h3
        tmp.set_name("deletes_daily_%s" % (h3.get_marker_text(),))
        tmp.set_data_file_name("deletes_daily_%s" % (h3.get_marker_text(),))
        tmp.set_marker_text(h3.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(h3.get_ylabel())
        tmp.set_marker_type(h3.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(20)
        plotter3.add(tmp)
        s3 = tmp

        integral = h.integral()
        integral1 = h1.integral()
        integral2 = h2.integral()
        integral3 = h3.integral()

        integral.set_marker_text(h.get_marker_text())
        integral.set_marker_type("impulses")
        integral.set_ylabel("TB")

        integral1.set_marker_text(h1.get_marker_text())
        integral1.set_marker_type("impulses")
        integral1.set_ylabel("TB")

        integral2.set_marker_text(h2.get_marker_text())
        integral2.set_marker_type("impulses")
        integral2.set_ylabel("TB")

        integral3.set_marker_text(h3.get_marker_text())
        integral3.set_marker_type("impulses")
        integral3.set_ylabel("TB")

        tmp = s_i + integral
        tmp.set_name(
            "integrated_writes_monthly_%s" %
            (integral.get_marker_text(),))
        tmp.set_data_file_name(
            "integrated_writes_monthly_%s" %
            (integral.get_marker_text(),))
        tmp.set_marker_text(integral.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(integral.get_ylabel())
        tmp.set_marker_type(integral.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(20)
        iplotter.add(tmp)
        s_i = tmp

        tmp = s1_i + integral1
        tmp.set_name(
            "integrated_writes_daily_%s" %
            (integral1.get_marker_text(),))
        tmp.set_data_file_name(
            "integrated_writes_daily_%s" %
            (integral1.get_marker_text(),))
        tmp.set_marker_text(integral1.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(integral1.get_ylabel())
        tmp.set_marker_type(integral1.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(20)
        iplotter1.add(tmp)
        s1_i = tmp

        tmp = s2_i + integral2
        tmp.set_name("integrated_deletes_monthly_%s" % (h2.get_marker_text(),))
        tmp.set_data_file_name(
            "integrated_deletes_monthly_%s" %
            (h2.get_marker_text(),))
        tmp.set_marker_text(h2.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(h2.get_ylabel())
        tmp.set_marker_type(h2.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(20)
        iplotter2.add(tmp)
        s2_i = tmp

        tmp = s3_i + integral3
        tmp.set_name(
            "integrated_deletes_daily_%s" %
            (integral3.get_marker_text(),))
        tmp.set_data_file_name(
            "integrated_deletes_daily_%s" %
            (integral3.get_marker_text(),))
        tmp.set_marker_text(integral3.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(integral3.get_ylabel())
        tmp.set_marker_type(integral3.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(20)
        iplotter3.add(tmp)
        s3_i = tmp
        i = i + 1

    plotters = []
    plotters.append(plotter)
    plotters.append(plotter1)
    plotters.append(plotter2)
    plotters.append(plotter3)
    plotters.append(iplotter)
    plotters.append(iplotter1)
    plotters.append(iplotter2)
    plotters.append(iplotter3)

    for p in plotters:
        p.reshuffle
        tmp = p.get_histogram_list()[0]
        tmp.set_line_color(1)
        tmp.set_marker_type("impulses")
        p.plot()


if __name__ == "__main__":
    plot_bpd()
    plot_bytes()
    sys.exit(0)
