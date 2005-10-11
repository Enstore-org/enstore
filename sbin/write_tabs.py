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
import sys
import os
import string
import time
import configuration_client
import pg
import enstore_constants
import histogram

SELECT_STMT="select  to_char(date, 'YY-MM-DD HH:MM:SS'), total, should, not_yet, done from write_protect_summary order by date desc"
SELECT_STMT1="select date,total, should, not_yet, done from write_protect_summary where date(time) between date(%s%s%s) and date(%s%s%s) and mb_user_write != 0 order by date desc"

def showError(msg):
    sys.stderr.write("Error: " + msg)

def usage():
    print "Usage: %s  <file_family> "%(sys.argv[0],)
def main():
    intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc   = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
    if ( 0 ) :
        acc = csc.get(enstore_constants.ACCOUNTING_SERVER)
        inq = csc.get('inquisitor')
        inq_host=inq.get('www_host').split('/')[2]
    servers=[]
    servers=csc.get('known_config_servers')

    for server in servers:
        server_name,server_port = servers.get(server)
        if ( server_port != None ):
            config_server_client   = configuration_client.ConfigurationClient((server_name, server_port))
            acc = config_server_client.get(enstore_constants.ACCOUNTING_SERVER)
            db_server_name = acc.get('dbhost')
            db_name        = acc.get('dbname')
            db_port        = acc.get('dbport')
            name           = db_server_name.split('.')[0]
            name=db_server_name.split('.')[0]
            now_time    = time.time()
            Y, M, D, h, m, s, wd, jd, dst = time.localtime(now_time)
            now_time = time.mktime((Y, M, D, 23, 59, 59, wd, jd, dst))
            start_time  = now_time-30*3600*24-7*3600*24
            
            h  = histogram.Histogram1D("write_tabs_%s"%(name,),"Write tab states %s"%(name,),37,float(start_time),float(now_time))
            h1 = histogram.Histogram1D("write_tabs_not_done_%s"%(name,),"Number of tabs to be flipped  %s"%(name,),37,float(start_time),float(now_time))
            h2 = histogram.Histogram1D("write_tabs_done_%s"%(name,),"Number of tabs flipped per day %s"%(name,),37,float(start_time),float(now_time))

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

            if db_port:
                db = pg.DB(host=db_server_name, dbname=db_name, port=db_port);
            else:
                db = pg.DB(host=db_server_name, dbname=db_name);

            res=db.query(SELECT_STMT)
            should  =  res.getresult()[0][2]
            not_yet =  res.getresult()[0][3]
            done    =  res.getresult()[0][4]
            date=""
            for row in res.getresult():
                if not row:
                    continue
                tmp = row[0].split(' ')[0]
                if ( date  != tmp ) :
                    date =  tmp
                    h.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d %H:%M:%S')),row[2])
                    h1.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d %H:%M:%S')),row[3])
                    h2.fill(time.mktime(time.strptime(row[0],'%Y-%m-%d %H:%M:%S')),row[4])
            db.close()

            h.set_line_color(2)
            h.set_line_width(20)
            h.set_marker_type("impulses")
            h.set_marker_text("ON")

            t = time.ctime(time.time())

            h.add_text("set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n"% (t,))
            h.add_text("set label \"Should %s, Done %s(%3.1f%%), Not Done %s.\" at graph .05,.90\n" % (should,done,100.*done/should,not_yet,))

            h1.set_line_color(1)
            h1.set_line_width(20)
            h1.set_marker_type("impulses")
            h1.set_marker_text("OFF")

            h2.set_line_color(2)
            h2.set_line_width(20)
            h2.set_marker_type("impulses")
            h2.set_marker_text("OFF")

            h.plot2(h1)

            derivative2=h2.derivative("write_tabs_done_%s"%(name,),"Number of tabs flipped/to be flipped per day %s"%(name,))
            derivative1=h1.derivative("write_tabs_not_done_%s"%(name,),"Number of tabs to be flipped  %s"%(name,))

            derivative2.set_ylabel("# of tabs flipped(green)/to be flipped(red) per day")
            derivative2.set_xlabel("Date (year-month-day)")
            derivative2.set_marker_text("flipped")
            derivative2.add_text("set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n"% (t,))

            derivative1.set_ylabel("# of tabs to be flipped per day")
            derivative1.set_xlabel("Date (year-month-day)")
            derivative1.set_marker_text("to be flipped")
            derivative1.add_text("set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n"% (t,))

            derivative2.plot2(derivative1, True)


#            h2.set_ylabel("# of tabs flipped per day")
#            h2.set_xlabel("Date (year-month-day)")
#            h2.set_marker_text("")
#            h2.add_text("set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n"% (t,))
#            h2.plot_derivative()

            cmd = "source /home/enstore/gettkt; $ENSTORE_DIR/sbin/enrcp %s.jpg %s.ps %s_stamp.jpg stkensrv2.fnal.gov:/fnal/ups/prd/www_pages/enstore"%(h.get_name(),h.get_name(),h.get_name(),)
            os.system(cmd)
            cmd = "source /home/enstore/gettkt; $ENSTORE_DIR/sbin/enrcp %s.jpg %s.ps %s_stamp.jpg stkensrv2.fnal.gov:/fnal/ups/prd/www_pages/enstore"%(h2.get_name(),h2.get_name(),h2.get_name(),)
            os.system(cmd)
            cmd = "rm %s.jpg %s.ps %s_stamp.jpg"%(h.get_name(),h.get_name(),h.get_name(),)
            os.system(cmd)
            cmd = "rm %s.jpg %s.ps %s_stamp.jpg"%(h2.get_name(),h2.get_name(),h2.get_name(),)
            os.system(cmd)


    sys.exit(0)
if __name__ == "__main__":
    main()
