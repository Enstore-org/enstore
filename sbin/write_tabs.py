#!/usr/bin/env python
###############################################################################
# $Author$
# $Date$
# $Id$
#
# This script is originally written by Alexander Moibenko. I just added
# more stuff to it (Dmitry Litvintsev 05/10)
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
import enstore_constants
import histogram

SELECT_STMT = "select  to_char(date, 'YYYY-MM-DD HH:MM:SS'), total, should, not_yet, done from write_protect_summary order by date desc"
SELECT_STMT1 = "select date,total, should, not_yet, done from write_protect_summary where date(time) between date(%s%s%s) and date(%s%s%s) and mb_user_write != 0 order by date desc"
SELECT_STMT3 = "select to_char(date, 'YYYY-MM-DD HH:MM:SS'), total, should, not_yet, done  from write_protect_summary_by_library  where library='%s' order by date desc"
SELECT_STMT4 = "select distinct library from write_protect_summary_by_library"


def showError(msg):
    sys.stderr.write("Error: " + msg)


def usage():
    print("Usage: %s  <file_family> " % (sys.argv[0],))


def main():
    intf = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc = configuration_client.ConfigurationClient(
        (intf.config_host, intf.config_port))
    if (0):
        acc = csc.get(enstore_constants.ACCOUNTING_SERVER)
        inq = csc.get('inquisitor')
        inq_host = inq.get('www_host').split('/')[2]
    servers = []
    servers = csc.get('known_config_servers')

    html_file = open("write_tabs_by_library.html", 'w')
    html_file.write(
        "<html><head><title>Tape Write Tabs per Library</title></head>")
    html_file.write(
        "<body text=\"#000066\" bgcolor=\"#FFFFFF\" link=\"#0000EF\" vlink=\"#55188A\" alink=\"#FF0000\" background=\"enstore.gif\">")

    html_host = None
    html_dir = None

    for server in servers:
        if server != "stken":
            continue
        server_name, server_port = servers.get(server)
        if (server_port is not None):
            config_server_client = configuration_client.ConfigurationClient(
                (server_name, server_port))
            inq_d = config_server_client.get(enstore_constants.INQUISITOR, {})
            if "html_file" in inq_d:
                html_dir = inq_d["html_file"]
            else:
                html_dir = enstore_files.default_dir
            if "host" in inq_d:
                html_host = inq_d["host"]
            else:
                html_host = enstore_files.default_dir

    if html_host is None or html_dir is None:
        print("Failed to find html host or html dir")
        sys.exit(1)

    for server in servers:
        server_name, server_port = servers.get(server)
        if (server_port is not None):
            config_server_client = configuration_client.ConfigurationClient(
                (server_name, server_port))
            acc = config_server_client.get(enstore_constants.ACCOUNTING_SERVER)
            db = pg.DB(host=acc.get('dbhost', "localhost"),
                       dbname=acc.get('dbname', "accounting"),
                       port=acc.get('dbport', 5432),
                       user=acc.get('dbuser_reader', "enstore_reader"))
            name = server
#            name           = db_server_name.split('.')[0]
#            name=db_server_name.split('.')[0]
            now_time = time.time()
            t = time.ctime(time.time())
            Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)
            now_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
            start_time = now_time - 30 * 3600 * 24 - 7 * 3600 * 24

            h = histogram.Histogram1D(
                "write_tabs_%s" %
                (name,), "Write tab states %s" %
                (name,), 37, float(start_time), float(now_time))
            h1 = histogram.Histogram1D(
                "write_tabs_not_done_%s" %
                (name,), "Number of tabs to be flipped  %s" %
                (name,), 37, float(start_time), float(now_time))
            h2 = histogram.Histogram1D(
                "write_tabs_done_%s" %
                (name,), "Number of tabs flipped per day %s" %
                (name,), 37, float(start_time), float(now_time))

            h.set_time_axis(True)
            h.set_profile(True)
            h.set_ylabel("# of tapes that should have write tabs ON")
            h.set_xlabel("Date (year-month-day)")

            h1.set_time_axis(True)
            h1.set_profile(True)
            h1.set_ylabel("# of tapes that  have write tabs OFF")
            h1.set_xlabel("Date (year-month-day)")

            h2.set_time_axis(True)
            h2.set_profile(True)
            h2.set_ylabel("# of tapes that should have write tabs ON")
            h2.set_xlabel("Date (year-month-day)")

            #
            # now do by library
            #
            libraries = []
            res = db.query(SELECT_STMT4)
            for row in res.getresult():
                if not row:
                    continue
                libraries.append(row[0])

            ncols = 3
            cols = 0

            html_file.write(
                "<p>&nbsp;<p><center><font size=5 color=\"#AA0000\"><b>%s</b></font></center>\n" %
                (server,))
            html_file.write("<TABLE align=\"CENTER\" cols=\"%s\" " % (ncols,))
            html_file.write(
                "cellpadding=\"0\" cellspacing=\"0\" width=\"100%\">\n<TR>")

            for library in libraries:
                cols = cols + 1
                h_1 = histogram.Histogram1D(
                    "write_tabs_%s_%s" %
                    (name, library,), "Write tab states for %s library" %
                    (library,), 37, float(start_time), float(now_time))
                h_2 = histogram.Histogram1D(
                    "write_tabs_not_done_%s_%s" %
                    (name, library,), "Number of tabs to be flipped  in %s library" %
                    (library,), 37, float(start_time), float(now_time))
                h_3 = histogram.Histogram1D(
                    "write_tabs_done_%s_%s" %
                    (name, library,), "Number of tabs flipped per day in %s library" %
                    (library,), 37, float(start_time), float(now_time))

                h_1.set_time_axis(True)
                h_1.set_profile(True)
                h_1.set_ylabel("# of tapes that should have write tabs ON")
                h_1.set_xlabel("Date (year-month-day)")

                h_2.set_time_axis(True)
                h_2.set_profile(True)
                h_2.set_ylabel("# of tapes that  have write tabs OFF")
                h_2.set_xlabel("Date (year-month-day)")

                h_3.set_time_axis(True)
                h_3.set_profile(True)
                h_3.set_ylabel("# of tapes that should have write tabs ON")
                h_3.set_xlabel("Date (year-month-day)")

                res = db.query(SELECT_STMT3 % (library,))
                should = res.getresult()[0][2]
                not_yet = res.getresult()[0][3]
                done = res.getresult()[0][4]
                date = ""
                for row in res.getresult():
                    if not row:
                        continue
                    tmp = row[0].split(' ')[0]
                    if (date != tmp):
                        date = tmp
                        h_1.fill(
                            time.mktime(
                                time.strptime(
                                    row[0],
                                    '%Y-%m-%d %H:%M:%S')),
                            row[2])
                        h_2.fill(
                            time.mktime(
                                time.strptime(
                                    row[0],
                                    '%Y-%m-%d %H:%M:%S')),
                            row[3])
                        h_3.fill(
                            time.mktime(
                                time.strptime(
                                    row[0],
                                    '%Y-%m-%d %H:%M:%S')),
                            row[4])

                h_1.set_line_color(2)
                h_1.set_line_width(20)
                h_1.set_marker_type("impulses")
                h_1.set_marker_text("ON")

                h_1.add_text(
                    "set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n" %
                    (t,))
                h_1.add_text(
                    "set label \"Should %s, Done %s(%3.1f%%), Not Done %s.\" at graph .05,.90\n" %
                    (should, done, 100. * done / should, not_yet,))

                h_2.set_line_color(1)
                h_2.set_line_width(20)
                h_2.set_marker_type("impulses")
                h_2.set_marker_text("OFF")

                h_3.set_line_color(2)
                h_3.set_line_width(20)
                h_3.set_marker_type("impulses")
                h_3.set_marker_text("OFF")

                h_1.plot2(h_2)

                derivative2 = h_3.derivative(
                    "write_tabs_done_%s_%s" %
                    (name, library,), "Number of tabs flipped/to be flipped per day %s" %
                    (library,))
                derivative1 = h_2.derivative(
                    "write_tabs_not_done_%s_%s" %
                    (name, library,), "Number of tabs to be flipped  %s" %
                    (library,))

                derivative2.set_ylabel(
                    "log10(N) of tabs flipped(green)/to be flipped(red) per day")
                derivative2.set_xlabel("Date (year-month-day)")
                derivative2.set_marker_text("added to done")
                derivative2.add_text(
                    "set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n" %
                    (t,))

                derivative1.set_ylabel("# of tabs to be flipped per day")
                derivative1.set_xlabel("Date (year-month-day)")
                derivative1.set_marker_text("added to not yet")
                derivative1.add_text(
                    "set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n" %
                    (t,))

                for i in range(derivative1.n_bins()):
                    if (derivative1.binarray[i] > 0):
                        derivative2.add_text("set label \"%5d \" at \"%s\",%f right rotate font \"Helvetica,12\"\n" % (derivative1.binarray[i],
                                                                                                                       time.strftime(
                                                                                                                           "%Y-%m-%d %H:%M:%S", time.localtime(derivative1.get_bin_center(i))),
                                                                                                                       -math.log10(derivative1.binarray[i]),))
                        derivative1.binarray[i] = math.log10(
                            derivative1.binarray[i])

                for i in range(derivative2.n_bins()):
                    if (derivative2.binarray[i] > 0):
                        derivative2.add_text("set label \"%5d\" at \"%s\",%f left rotate font \"Helvetica,12\"\n" % (derivative2.binarray[i],
                                                                                                                     time.strftime(
                                                                                                                         "%Y-%m-%d %H:%M:%S", time.localtime(derivative2.get_bin_center(i))),
                                                                                                                     math.log10(derivative2.binarray[i]),))
                        derivative2.binarray[i] = math.log10(
                            derivative2.binarray[i])

                derivative2.plot2(derivative1, True)

                html_file.write(
                    "<td align=center nosave><b><font color=\"#000066\" size=\"+2\">\n")
                html_file.write(
                    "<a href=\"%s.jpg\"><img src=\"%s_stamp.jpg\"></a><br>\n" %
                    (h_1.get_name(), h_1.get_name(),))
                html_file.write(
                    "<a href=\"%s.jpg\"><img src=\"%s_stamp.jpg\"></a><br> %s \n" %
                    (h_3.get_name(), h_3.get_name(), library))
                html_file.write("</font></b></td>")

                cmd = "$ENSTORE_DIR/sbin/enrcp %s.jpg %s.ps %s_stamp.jpg %s:%s/write_tabs/" % (
                    h_1.get_name(), h_1.get_name(), h_1.get_name(), html_host, html_dir)
                os.system(cmd)
                cmd = "$ENSTORE_DIR/sbin/enrcp %s.jpg %s.ps %s_stamp.jpg %s:%s/write_tabs/" % (
                    h_3.get_name(), h_3.get_name(), h_3.get_name(), html_host, html_dir)
                os.system(cmd)
                cmd = "rm %s.jpg %s.ps %s_stamp.jpg" % (
                    h_1.get_name(), h_1.get_name(), h_1.get_name(),)
                os.system(cmd)
                cmd = "rm %s.jpg %s.ps %s_stamp.jpg" % (
                    h_3.get_name(), h_3.get_name(), h_3.get_name(),)
                os.system(cmd)
                if (cols % ncols == 0):
                    html_file.write("</tr><tr>")

            res = db.query(SELECT_STMT)
            should = res.getresult()[0][2]
            not_yet = res.getresult()[0][3]
            done = res.getresult()[0][4]
            date = ""
            for row in res.getresult():
                if not row:
                    continue
                tmp = row[0].split(' ')[0]
                if (date != tmp):
                    date = tmp
                    h.fill(
                        time.mktime(
                            time.strptime(
                                row[0],
                                '%Y-%m-%d %H:%M:%S')),
                        row[2])
                    h1.fill(
                        time.mktime(
                            time.strptime(
                                row[0],
                                '%Y-%m-%d %H:%M:%S')),
                        row[3])
                    h2.fill(
                        time.mktime(
                            time.strptime(
                                row[0],
                                '%Y-%m-%d %H:%M:%S')),
                        row[4])

            db.close()

            html_file.write("</tr>")
            html_file.write("</table><p>&nbsp;<p>")

            h.set_line_color(2)
            h.set_line_width(20)
            h.set_marker_type("impulses")
            h.set_marker_text("ON")

            h.add_text(
                "set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n" %
                (t,))
            h.add_text(
                "set label \"Should %s, Done %s(%3.1f%%), Not Done %s.\" at graph .05,.90\n" %
                (should, done, 100. * done / should, not_yet,))

            h1.set_line_color(1)
            h1.set_line_width(20)
            h1.set_marker_type("impulses")
            h1.set_marker_text("OFF")

            h2.set_line_color(2)
            h2.set_line_width(20)
            h2.set_marker_type("impulses")
            h2.set_marker_text("OFF")

            h.plot2(h1)

            derivative2 = h2.derivative(
                "write_tabs_done_%s" %
                (name,), "Number of tabs flipped/to be flipped per day %s" %
                (name,))
            derivative1 = h1.derivative(
                "write_tabs_not_done_%s" %
                (name,), "Number of tabs to be flipped  %s" %
                (name,))

            derivative2.set_ylabel(
                "log10(N) of tabs flipped(green)/to be flipped(red) per day")
            derivative2.set_xlabel("Date (year-month-day)")
            derivative2.set_marker_text("added to done")
            derivative2.add_text(
                "set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n" %
                (t,))

            derivative1.set_ylabel("# of tabs to be flipped per day")
            derivative1.set_xlabel("Date (year-month-day)")
            derivative1.set_marker_text("added to not yet")
            derivative1.add_text(
                "set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n" %
                (t,))

            for i in range(derivative1.n_bins()):
                if (derivative1.binarray[i] > 0):
                    derivative2.add_text("set label \"%5d \" at \"%s\",%f right rotate font \"Helvetica,12\"\n" % (derivative1.binarray[i],
                                                                                                                   time.strftime(
                                                                                                                       "%Y-%m-%d %H:%M:%S", time.localtime(derivative1.get_bin_center(i))),
                                                                                                                   -math.log10(derivative1.binarray[i]),))
                    derivative1.binarray[i] = math.log10(
                        derivative1.binarray[i])

            for i in range(derivative2.n_bins()):
                if (derivative2.binarray[i] > 0):
                    derivative2.add_text("set label \"%5d\" at \"%s\",%f left rotate font \"Helvetica,12\"\n" % (derivative2.binarray[i],
                                                                                                                 time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(derivative2.get_bin_center(i))),
                        math.log10(derivative2.binarray[i]),))
                    derivative2.binarray[i] = math.log10(
                        derivative2.binarray[i])

            derivative2.plot2(derivative1, True)
            cmd = "$ENSTORE_DIR/sbin/enrcp %s.jpg %s.ps %s_stamp.jpg %s:%s" % (
                h.get_name(), h.get_name(), h.get_name(), html_host, html_dir)
            os.system(cmd)
            cmd = "$ENSTORE_DIR/sbin/enrcp %s.jpg %s.ps %s_stamp.jpg %s:%s" % (
                h2.get_name(), h2.get_name(), h2.get_name(), html_host, html_dir)
            os.system(cmd)
            cmd = "rm %s.jpg %s.ps %s_stamp.jpg" % (
                h.get_name(), h.get_name(), h.get_name(),)
            os.system(cmd)
            cmd = "rm %s.jpg %s.ps %s_stamp.jpg" % (
                h2.get_name(), h2.get_name(), h2.get_name(),)
            os.system(cmd)

    html_file.write("</body></html>")
    html_file.close()
    cmd = "$ENSTORE_DIR/sbin/enrcp write_tabs_by_library.html %s:%s/write_tabs/" % (
        html_host, html_dir)
    os.system(cmd)
    os.system("rm write_tabs_by_library.html")
    sys.exit(0)


if __name__ == "__main__":
    main()
