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
import sys
import os
import string
import time
import math
import pg
import thread
import socket
import configuration_client
import enstore_constants
import histogram
import enstore_files

KB=1024.
MB=KB*KB
GB=KB*MB
TB=KB*GB
PB=KB*TB


SELECT_STMT="select date,sum(read),sum(write) from xfer_by_day where date between %s and %s group by date order by date desc"
SELECT_STMT1="select date,sum(read),sum(write) from xfer_by_day group by date order by date" # was xferby_month

SELECT_DELETED_BYTES ="select to_char(state.time, 'YYYY-MM-DD HH:MM:SS'), sum(file.size)::bigint from file, state where state.volume=file.volume and state.value='DELETED' group by state.time"

SELECT_WRITTEN_BYTES ="select  substr(bfid,5,10), size, deleted  from file where  file.deleted = 'n' and file.volume in (select volume.id from volume where volume.media_type != 'null' and system_inhibit_0 != 'DELETED' ) "

def bfid2time(bfid):
    if bfid[-1] == "L":
        e = -6
    else:
        e = -5

    if bfid[0].isdigit():
        i = 0
    elif bfid[3].isdigit():
        i = 3
    else:
        i = 4

    t = int(bfid[i:e])
    if t > 1500000000:
        t = t - 619318800

    return t

def showError(msg):
    sys.stderr.write("Error: " + msg)

def usage():
    print "Usage: %s  <file_family> "%(sys.argv[0],)

def decorate(h,color,ylabel,marker):
    h.set_time_axis(True)
    h.set_ylabel(ylabel);
    h.set_xlabel("Date (year-month-day)")
    h.set_line_color(color)
    h.set_line_width(20)
    h.set_marker_text(marker)
    h.set_marker_type("impulses")

def get_min_max(h) :
    y_max   =  0
    i_max = 0
    y_min   = 1.e+32
    i_min = 0
    for i in range(h.n_bins()) :
        if (  h.get_bin_content(i) > y_max ) :
            y_max = h.get_bin_content(i)
            i_max = i
        if ( h.get_bin_content(i) < y_min  and  h.get_bin_content(i) > 0 ) :
            y_min     = h.get_bin_content(i)
            i_min = i
    return y_min,i_min,y_max,i_max

def get_sum(h) :
    sum=0.
    for i in range(h.n_bins()) :
        sum = sum + h.get_bin_content(i)
    return sum

exitmutexes=[]

def fill_histograms(i,server_name,server_port,hlist):
    config_server_client   = configuration_client.ConfigurationClient((server_name, server_port))
    acc            = config_server_client.get("database", {})
    db = pg.DB(host  = acc.get('db_host', "localhost"),
               dbname= acc.get('dbname', "enstoredb"),
               port  = acc.get('db_port', 5432),
               user  = acc.get('dbuser_reader', "enstore_reader"))
    h   = hlist[i]
    res=db.query(SELECT_DELETED_BYTES)
    for row in res.getresult():
        if not row:
            continue
        h.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d %H:%M:%S')),row[1]/TB)
    db.close()
    exitmutexes[i]=1

def fill_tape_histograms(i,server_name,server_port,hlist):
    config_server_client   = configuration_client.ConfigurationClient((server_name, server_port))
    acc            = config_server_client.get("database", {})
    db = pg.DB(host  = acc.get('db_host', "localhost"),
               dbname= acc.get('dbname', "enstoredb"),
               port  = acc.get('db_port', 5432),
               user  = acc.get('dbuser_reader', "enstore_reader"))

    now_time    = time.time()
    t           = time.ctime(time.time())
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)
    start_day   = time.mktime((2000, 12, 31, 23, 59, 59, 0, 0, 0))
    now_day     = time.mktime((Y, 12, 31, 23, 59, 59, wd, jd, dst))
    nbins = int((now_day-start_day)/(24.*3600.)+0.5)

    q="select date,active_bytes,storage_group from historic_tape_bytes order by date asc"
    res = db.query(q)
    for row in res.getresult():
        sg = row[2]
        if not hlist.has_key(sg):
            hlist[sg]=histogram.Histogram1D("on_tape_by_month_%s"%(sg,),"Total Bytes On Tape by Month By %s"%(sg,),nbins,float(start_day),float(now_day))
        h = hlist[sg]
        h.set_marker_text(sg)
        h.set_line_color(0)
        h.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d %H:%M:%S')),row[1]/TB)
        h.set_marker_type("impulses")
        h.set_time_axis(True)
        h.set_line_width(8)
    db.close()

def plot_bpd():
    #
    # this function creates plots of bytes transferred per day and per month
    # based on data on accounting database (*ensrv6)
    #
    intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc   = configuration_client.ConfigurationClient((intf.config_host,
                                                      intf.config_port))
    if ( 0 ) :
        acc = csc.get(enstore_constants.ACCOUNTING_SERVER)
        inq = csc.get('inquisitor')
        inq_host=inq.get('www_host').split('/')[2]
    servers=[]
    servers=[]
    servers=csc.get('known_config_servers')
    histograms=[]

    now_time    = time.time()
    t           = time.ctime(time.time())
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)

    start_day   = time.mktime((2001, 12, 31, 23, 59, 59, 0, 0, 0))
    now_day     = time.mktime((Y+1, 12, 31, 23, 59, 59, wd, jd, dst))
    nbins       = int((now_day-start_day)/(30.*24.*3600.)+0.5)

    color=1

    s  = histogram.Histogram1D("xfers_total_by_month",
                               "Total Bytes Transferred per Month By Enstore",
                               nbins,float(start_day),float(now_day))
    s.set_time_axis(True)
    plotter=histogram.Plotter("xfers_total_by_month",
                              "Total TBytes Transferred per Month By Enstore")
    s_i  = histogram.Histogram1D("integrated_xfers_total_by_month",
                                 "Integarted total Bytes transferred per Month By Enstore",
                                 nbins,float(start_day),float(now_day))
    s_i.set_time_axis(True)
    iplotter=histogram.Plotter("integrated_xfers_total_by_month",
                               "Integarted total Bytes transferred per Month By Enstore")

    s1  = histogram.Histogram1D("writes_total_by_month",
                                "Total bytes written per month to Enstore",
                                nbins,float(start_day),float(now_day))
    s1.set_time_axis(True)
    plotter1=histogram.Plotter("writes_total_by_month",
                               "Total TBytes written per month by Enstore")
    s1_i  = histogram.Histogram1D("writes_total_by_month",
                                  "Integrated Total bytes written per month to Enstore",
                                  nbins,float(start_day),float(now_day))
    s1_i.set_time_axis(True)

    iplotter1=histogram.Plotter("integrated_writes_total_by_month",
                                "Integrated Total TBytes written per month by Enstore")

    w_month=0.
    r_month=0.
    t_month=0.
    n_month=0;

    for server in servers:
        server_name,server_port = servers.get(server)
        if ( server_port != None ):
            config_server_client   = configuration_client.ConfigurationClient((server_name, server_port))
            acc = config_server_client.get(enstore_constants.ACCOUNTING_SERVER)

            db_server_name = acc.get('dbhost','localhost')
            name           = db_server_name.split('.')[0]

            h  = histogram.Histogram1D("xfers_total_by_month_%s"%(name,),
                                       "Total Bytes Transferred per Month By %s"
                                       %(server,),
                                       nbins,float(start_day),float(now_day))

            h.set_time_axis(True)
            h.set_ylabel("Bytes");
            h.set_xlabel("Date (year-month-day)")
            h.set_line_color(color)
            h.set_line_width(5)

            h1   = histogram.Histogram1D("writes_by_month_%s"%(server,),
                                         "Total Bytes Written by Month By %s"
                                         %(server,),
                                         nbins,float(start_day),float(now_day))
            decorate(h1,color,"TiB/month",server)
            histograms.append(h1)
            color=color+1
            db = pg.DB(host  = acc.get('dbhost', 'localhost'),
                       dbname= acc.get('dbname', 'accounting'),
                       port  = acc.get('dbport', 5432),
                       user  = acc.get('dbuser_reader', 'enstore_reader'))
            res=db.query(SELECT_STMT1)
            for row in res.getresult():
                if not row:
                    continue
                h.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d')),
                       (row[1]+row[2])/TB)
                h1.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d')),row[2]/TB)
            db.close()

            tmp=s+h
            tmp.set_name("xfer_%s"%(server,))
            tmp.set_data_file_name(server)
            tmp.set_marker_text(server)
            tmp.set_time_axis(True)
            tmp.set_ylabel("TiB/month")
            tmp.set_marker_type("impulses")
            tmp.set_line_color(color)
            tmp.set_line_width(5)
            plotter.add(tmp)
            s=tmp

            integral  = h.integral()

            integral.set_marker_text(server)
            integral.set_marker_type("impulses")
            integral.set_ylabel("TiB");

            tmp=s_i+integral
            tmp.set_name("integrated_xfers_monthly_%s"%(integral.get_marker_text(),))
            tmp.set_data_file_name("integrated_xfers_monthly_%s"%
                                   (integral.get_marker_text(),))
            tmp.set_marker_text(integral.get_marker_text())
            tmp.set_time_axis(True)
            tmp.set_ylabel(integral.get_ylabel())
            tmp.set_marker_type(integral.get_marker_type())
            tmp.set_line_color(color)
            tmp.set_line_width(5)
            iplotter.add(tmp)
            s_i=tmp

            tmp=s1+h1
            tmp.set_name("deletes_monthly_%s"%(h1.get_marker_text(),))
            tmp.set_data_file_name("deletes_monthly_%s"%(h1.get_marker_text(),))
            tmp.set_marker_text(h1.get_marker_text())
            tmp.set_time_axis(True)
            tmp.set_ylabel(h1.get_ylabel())
            tmp.set_marker_type(h1.get_marker_type())
            tmp.set_line_color(color)
            tmp.set_line_width(5)
            plotter1.add(tmp)
            s1=tmp


            integral1 = h1.integral()

            integral1.set_marker_text(h1.get_marker_text())
            integral1.set_marker_type("impulses")
            integral1.set_ylabel("TiB");

            tmp=s1_i+integral1
            tmp.set_name("integrated_deletes_monthly_%s"%(h1.get_marker_text(),))
            tmp.set_data_file_name("integrated_deletes_monthly_%s"%
                                   (h1.get_marker_text(),))
            tmp.set_marker_text(h1.get_marker_text())
            tmp.set_time_axis(True)
            tmp.set_ylabel(h1.get_ylabel())
            tmp.set_marker_type(h1.get_marker_type())
            tmp.set_line_color(color)
            tmp.set_line_width(5)
            iplotter1.add(tmp)
            s1_i=tmp

    plotter.reshuffle()
    tmp=plotter.get_histogram_list()[0]

    t_month_min,i_month_min,t_month_max,i_month_max = get_min_max(tmp)
    t_month = get_sum(tmp)
    tmp.set_line_color(1)

    delta =  tmp.binarray[i_month_max]*0.05

    tmp.add_text("set label \"%10d\" at \"%s\",%f right rotate font \"Helvetica,12\"\n"%(tmp.binarray[i_month_max]+0.5,
        time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(tmp.get_bin_center(i_month_max))),
        tmp.binarray[i_month_max]+delta,))

    tmp.add_text("set label \"Total :  %5d TiB  \" at graph .05,.9  font \"Helvetica,13\"\n"%(t_month+0.5,))
    tmp.add_text("set label \"Max   :  %5d TiB (on %s) \" at graph .05,.85  font \"Helvetica,13\"\n"%(t_month_max+0.5,
                                                                                                 time.strftime("%Y-%m",time.localtime(tmp.get_bin_center(i_month_max))),))
    plotter.plot()

    iplotter.reshuffle()
    tmp=iplotter.get_histogram_list()[0]
    t_month_min,i_month_min,t_month_max,i_month_max = get_min_max(tmp)
    tmp.add_text("set label \"Total Transferred :  %5d TiB  \" at graph .1,.8  font \"Helvetica,13\"\n"%(t_month_max+0.5,))
    tmp.set_line_color(1)
    tmp.set_marker_type("impulses")
    iplotter.plot()


    plotter1.reshuffle()
    tmp=plotter1.get_histogram_list()[0]

    t_month_min,i_month_min,t_month_max,i_month_max = get_min_max(tmp)
    t_month = get_sum(tmp)
    tmp.set_line_color(1)

    delta =  tmp.binarray[i_month_max]*0.05

    tmp.add_text("set label \"%10d\" at \"%s\",%f right rotate font \"Helvetica,12\"\n"%(tmp.binarray[i_month_max]+0.5,
        time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(tmp.get_bin_center(i_month_max))),
        tmp.binarray[i_month_max]+delta,))

    tmp.add_text("set label \"Total :  %5d TiB  \" at graph .05,.9  font \"Helvetica,13\"\n"%(t_month+0.5,))
    tmp.add_text("set label \"Max   :  %5d TiB (on %s) \" at graph .05,.85  font \"Helvetica,13\"\n"%(t_month_max+0.5,
                                                                                                 time.strftime("%Y-%m",time.localtime(tmp.get_bin_center(i_month_max))),))

    plotter1.plot()

    iplotter1.reshuffle()
    tmp=iplotter1.get_histogram_list()[0]
    t_month_min,i_month_min,t_month_max,i_month_max = get_min_max(tmp)
    tmp.add_text("set label \"Total Written :  %5d TiB  \" at graph .1,.8  font \"Helvetica,13\"\n"%(t_month_max+0.5,))
    tmp.set_line_color(1)
    tmp.set_marker_type("impulses")
    iplotter1.plot()


def plot_bytes():
    #
    # This function plots bytes written/deleted to/from Enstore base on data in file and volume tables
    # from *ensrv0 postgres databases damn slow
    #
    intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc   = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
    servers=[]
    servers=[]
    servers=csc.get('known_config_servers')
    histograms=[]

    now_time    = time.time()
    t           = time.ctime(time.time())
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)
    start_day   = time.mktime((2001, 12, 31, 23, 59, 59, 0, 0, 0))
    now_day     = time.mktime((Y+1, 12, 31, 23, 59, 59, wd, jd, dst))
    nbins       = int((now_day-start_day)/(30.*24.*3600.)+0.5)

    s1 = histogram.Histogram1D("deletes_total_by_month","Total bytes deleted per month from Enstore",nbins,float(start_day),float(now_day))
    s1.set_time_axis(True)
    plotter1=histogram.Plotter("deletes_total_by_month","Total TiB deleted per month from Enstore")
    s1_i = histogram.Histogram1D("deletes_total_by_month","Integrated Total bytes deleted per month from Enstore",nbins,float(start_day),float(now_day))
    s1_i.set_time_axis(True)
    iplotter1=histogram.Plotter("integrated_deletes_total_by_month","Integrated Total TiB deleted per month from Enstore")

    i = 0
    color=1
    for server in servers:
        server_name,server_port = servers.get(server)
        if ( server_port != None ):

            h   = histogram.Histogram1D("deletes_by_month_%s"%(server,),"Total Bytes Deleted by Month By %s"%(server,),nbins,float(start_day),float(now_day))
            decorate(h,color,"TiB/month",server)
            histograms.append(h)

            exitmutexes.append(0)
            thread.start_new(fill_histograms, (i,server_name,server_port,histograms))
            i=i+1
            color=color+1

    while 0 in exitmutexes:
        time.sleep(60)
        pass

    i = 0
    for i in range(len(histograms)):
        h1  = histograms[i]
        color = i + 2

        tmp=s1+h1
        tmp.set_name("deletes_monthly_%s"%(h1.get_marker_text(),))
        tmp.set_data_file_name("deletes_monthly_%s"%(h1.get_marker_text(),))
        tmp.set_marker_text(h1.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(h1.get_ylabel())
        tmp.set_marker_type(h1.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(5)
        plotter1.add(tmp)
        s1=tmp


        integral1 = h1.integral()

        integral1.set_marker_text(h1.get_marker_text())
        integral1.set_marker_type("impulses")
        integral1.set_ylabel("TiB");

        tmp=s1_i+integral1
        tmp.set_name("integrated_deletes_monthly_%s"%(h1.get_marker_text(),))
        tmp.set_data_file_name("integrated_deletes_monthly_%s"%(h1.get_marker_text(),))
        tmp.set_marker_text(h1.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(h1.get_ylabel())
        tmp.set_marker_type(h1.get_marker_type())
        tmp.set_line_color(color)
        tmp.set_line_width(5)
        iplotter1.add(tmp)
        s1_i=tmp

        i=i+1

    plotters=[]
    plotters.append(plotter1)

    iplotters=[]
    iplotters.append(iplotter1)



    for p in plotters:
        p.reshuffle()
        tmp=p.get_histogram_list()[0]
        tmp.set_line_color(1)

        t_day_min,i_day_min,t_day_max,i_day_max = get_min_max(tmp)
        t_day = get_sum(tmp)

        delta =  tmp.binarray[i_day_max]*0.05

        tmp.add_text("set label \"%5d\" at \"%s\",%f right rotate font \"Helvetica,12\"\n"%(tmp.binarray[i_day_max]+0.5,
                                                                                             time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(tmp.get_bin_center(i_day_max))),
                                                                                             tmp.binarray[i_day_max]+delta,))


        tmp.add_text("set label \"Total :  %5d TiB  \" at graph .8,.8  font \"Helvetica,13\"\n"%(t_day+0.5,))
        tmp.add_text("set label \"Max   :  %5d TiB (on %s) \" at graph .8,.75  font \"Helvetica,13\"\n"%(t_day_max+0.5,
                                                                                                        time.strftime("%m-%d",time.localtime(tmp.get_bin_center(i_day_max))),))

        tmp.set_marker_type("impulses")
        p.plot()

    for p in iplotters:
        p.reshuffle()
        tmp=p.get_histogram_list()[0]
        tmp.set_line_color(1)

        t_day_min,i_day_min,t_day_max,i_day_max = get_min_max(tmp)
        tmp.add_text("set label \"Total :  %5d TiB  \" at graph .1,.8  font \"Helvetica,13\"\n"%(t_day_max+0.5,))

        tmp.set_marker_type("impulses")
        p.plot()

def plot_tape_bytes():
    #
    # This function plots bytes written/deleted to/from Enstore base on data in file and volume tables
    # from *ensrv0 postgres databases damn slow
    #
    intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc   = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
    servers=[]
    servers=csc.get('known_config_servers')
    histograms={}

    now_time    = time.time()
    t           = time.ctime(time.time())
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)
    start_day   = time.mktime((2000, 12, 31, 23, 59, 59, 0, 0, 0))
    now_day     = time.mktime((Y, 12, 31, 23, 59, 59, wd, jd, dst))
    nbins       = int((now_day-start_day)/(24.*3600.)+0.5)


    s1 = histogram.Histogram1D("on_tape_total_by_month","Active bytes on tape in Enstore",nbins,float(start_day),float(now_day))
    s1.set_time_axis(True)

    plotter1=histogram.Plotter("on_tape_total_by_month","Active TiB on tape in Enstore")

    i = 0
    for server in servers:
        server_name,server_port = servers.get(server)
        if ( server_port != None ):
            fill_tape_histograms(i,server_name,server_port,histograms)
            i=i+1

    values = histograms.values()
    values.sort()
    for h1 in values:
        tmp=s1+h1
        tmp.set_name("on_tape_monthly_%s"%(h1.get_marker_text(),))
        tmp.set_data_file_name("on_tape_monthly_%s"%(h1.get_marker_text(),))
        tmp.set_marker_text(h1.get_marker_text())
        tmp.set_time_axis(True)
        tmp.set_ylabel(h1.get_ylabel())
        tmp.set_marker_type(h1.get_marker_type())
        tmp.set_line_color(0)
        tmp.set_line_width(8)
        plotter1.add(tmp)
        s1=tmp

        i=i+1

    plotters=[]
    plotters.append(plotter1)

    for p in plotters:
        p.reshuffle()
        p.add_command("set key outside width 2")
        p.add_command("set xtics border nomirror rotate by 90")
        tmp=p.get_histogram_list()[0]

        t_day_min,i_day_min,t_day_max,i_day_max = get_min_max(tmp)
        t_day = get_sum(tmp)

        delta =  tmp.binarray[i_day_max]*0.05

        tmp.add_text("set label \"Total   :  %5d TiB \" at graph .05,.95  font \"Helvetica,13\"\n"%(t_day_max+0.5))

        tmp.set_marker_type("impulses")
        p.plot()

        p.name = p.name  + "_log"
        p.title="Active TiB on tape in Enstore (log scale)"
        tmp.set_logy()
        p.plot()


if __name__ == "__main__":
    plot_bpd()
    plot_bytes()
    plot_tape_bytes()
    intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc   = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
    retry=0
    timeout=1
    system_name = csc.get_enstore_system(1,retry)
    config_dict={}
    if system_name:
        config_dict = csc.dump(timeout, retry)
        config_dict = config_dict['dump']
    else:
        configfile = os.environ.get('ENSTORE_CONFIG_FILE')
        f = open(configfile,'r')
        code = string.join(f.readlines(),'')
        configdict={}
        exec(code)
        config_dict=configdict
        ret =configdict['known_config_servers']
        def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
                    int(os.environ['ENSTORE_CONFIG_PORT']))
        for item in ret.items():
            if socket.getfqdn(item[1][0]) == socket.getfqdn(def_addr[0]):
                system_name = item[0]

    inq_d = config_dict.get(enstore_constants.INQUISITOR, {})

    html_dir=None
    if inq_d.has_key("html_file"):
        html_dir=inq_d["html_file"]
    else:
        html_dir = enstore_files.default_dir


    html_host=None
    if inq_d.has_key("host"):
        html_host=inq_d["host"]
    else:
        html_host = enstore_files.default_dir



    cmd = "$ENSTORE_DIR/sbin/enrcp *.jpg  %s:%s/bytes_statistics/"%(html_host,html_dir)
    os.system(cmd)
    cmd = "$ENSTORE_DIR/sbin/enrcp *.ps   %s:%s/bytes_statistics/"%(html_host,html_dir)
    os.system(cmd)


    sys.exit(0)
