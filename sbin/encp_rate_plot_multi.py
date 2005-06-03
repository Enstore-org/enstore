#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# This script produces histograms perinent to encp rate:
#
# Requires no input arguments 
#
###############################################################################

import sys
import time
import popen2
import os
import string
# import getopt, string
# import math

MB=1024*1024.
ENCP_RATE = "encp-rates"


def showError(msg):
    sys.stderr.write("Error: " + msg)

def usage():
    print "Usage: %s  <starting date> <ending date> <node> <storage_group> <rw> "%(sys.argv[0],)
    print "date format: YYYY-MM-DD"
    print "example: 2005-04-01 2005-04-02 cmsstor01.fnal.gov cms w"
def main():

    if len(sys.argv)<1:
        usage()
        sys.exit(0)

       
#    start = sys.argv[1]
#    stop  = sys.argv[2]
#    storage_group = sys.argv[4]
#    node = sys.argv[3]
#    rw = sys.argv[5]

#    node          = "cmsstor01.fnal.gov"
#    storage_group = "cms"
#    rw            = "w"
    

    now_time   =  int(time.time())
    delta_time =  30*24*60*60

    cmd = "psql   enstore -h stkensrv6 -t -q -c \"select max(unix_time) from encp_xfer_average_by_storage_group\""

    inp,out = os.popen2 (cmd, 'r')
    inp.write (cmd)
    inp.close ()

    max_time = now_time
    
    for line in out.readlines() :
       if not line:
           continue
       if line.isspace():
           continue
       max_time = int(line.strip(' '))
    out.close()

#
# Get list of storage groups
#

    cmd = "psql   enstore -h stkensrv6 -t -q -c \"select distinct(storage_group) from encp_xfer_average_by_storage_group\""

    inp,out = os.popen2 (cmd, 'r')
    inp.write (cmd)
    inp.close ()

    storage_groups = []
    for line in out.readlines() :
       if not line:
           continue
       if line.isspace():
           continue
       column = line.strip(' ')
       column = column.strip('\n')
       storage_groups.append(column)
    out.close()

    for storage_group in storage_groups:
       sg =  storage_group.strip(' ')
       cmd = "rm -f rate_%s_w.dat"%(sg)
       os.system(cmd)
       cmd = "rm -f rate_%s_r.dat"%(sg)
       os.system(cmd)

#
# extract values for a month
# 
    
    cmd = "psql   enstore -h stkensrv6 -t -q  -c \"select  "
    cmd = cmd + "date,unix_time,storage_group,rw,"
    cmd = cmd + "avg_overall_rate,stddev_overall_rate,"
    cmd = cmd + "avg_network_rate,stddev_network_rate,"
    cmd = cmd + "avg_disk_rate,stddev_disk_rate,"
    cmd = cmd + "avg_transfer_rate,stddev_transfer_rate,"
    cmd = cmd + "avg_drive_rate,stddev_drive_rate,"
    cmd = cmd + "avg_size,stddev_size,"
    cmd = cmd + "counter "
    cmd = cmd + "from encp_xfer_average_by_storage_group "
    cmd = cmd + "where unix_time between "
    cmd = cmd + str(now_time-delta_time)
    cmd = cmd + " and "
    cmd = cmd + str(now_time)
    cmd = cmd + ";\""
    inp,out = os.popen2 (cmd, 'r')
    inp.write (cmd)
    inp.close ()

    for line in out.readlines() :
       if not line:
           continue
       if line.isspace():
           continue
       try:
         (d,ut,sg,rw,a_or,s_o_r,a_nr,s_n_r,a_dsk_r,s_dsk_r,a_t_r,s_t_r,a_drv_r,s_drv_r,a_s,s_s,counter) = line.split('|')
       except:
         print 'can not parse', line,len(line)
         continue
       for storage_group in storage_groups:
           cmd = "echo \"%s %s %s %s %s %s %s %s %s %s %s %s %s %s \""%(d,a_or,s_o_r,a_nr,s_n_r,a_dsk_r,s_dsk_r,a_t_r,s_t_r,a_drv_r,s_drv_r,a_s,s_s,counter)
           if ( storage_group.strip(' ') == sg.strip(' ') ):
               if ( rw.strip(' ') == "w" ):
                   cmd = "%s >> rate_%s_w.dat"%(cmd,sg.strip(' '))
               elif ( rw.strip(' ') == "r"):
                   cmd = "%s >> rate_%s_r.dat"%(cmd,sg.strip(' '))
               os.system(cmd)
    out.close()


    for sg in storage_groups:

        postscript_output="%s/encp_rates_%s_r.ps"%(ENCP_RATE,sg.strip(' '))
        jpeg_output="%s/encp_rates_%s_r.jpg"%(ENCP_RATE,sg.strip(' '))
        jpeg_output_stamp="%s/encp_rates_%s_r_stamp.jpg"%(ENCP_RATE,sg.strip(' '))

        plot_data_file_r="rate_%s_r.dat"%(sg.strip(' '))
        plot_data_file_w="rate_%s_w.dat"%(sg.strip(' '))

        if (os.path.exists(plot_data_file_r)) : 
            lower = "%s"%(time.strftime("%Y-%m-%d",time.localtime(now_time-delta_time)))
            file_name = "tmp_%s_r_gnuplot.cmd"%(sg.strip(' '))
            gnu_cmd = open(file_name,'w')
            upper = "%s"%(time.strftime("%Y-%m-%d",time.localtime(now_time)))
            gnu_cmd.write("set terminal postscript color solid\n")
            gnu_cmd.write("set output '%s'\n"%(postscript_output,))
            gnu_cmd.write("set xlabel 'Date'\n")
            tf='"%Y-%m-%d %H:%M"'
            gnu_cmd.write("set timefmt %s\n"%(tf,))
            gnu_cmd.write("set yrange [0 : ]\n")
            gnu_cmd.write("set xdata time\n")
            gnu_cmd.write("set xrange [ '%s':'%s' ]\n"%(lower, upper))
            tf='"%m-%d %H"'
            gnu_cmd.write("set format x %s\n"%(tf,))
            gnu_cmd.write("set ylabel 'Rate MB/s'\n")
            gnu_cmd.write("set grid\n")
            gnu_cmd.write("set size 1.0, 2.0\n")
            gnu_cmd.write("set origin 0.0, 0.0\n");
            gnu_cmd.write("set multiplot\n")
            gnu_cmd.write("set size 0.5, 0.6\n")
            gnu_cmd.write("set origin 0.25,1.2\n")
            gnu_cmd.write("set title '%s: overall encp read rate generated at %s'\n"%(sg.strip(' '),(time.ctime(time.time())),))
            gnu_cmd.write("plot '%s' using 1:3 t '' with impulses lw 5\n"%(plot_data_file_r,))
            gnu_cmd.write("set origin 0.0,0.6\n")
            gnu_cmd.write("set title '%s: network encp read'\n"%(sg.strip(' '),))
            gnu_cmd.write("plot '%s' using 1:5 t '' with impulses lw 5\n"%(plot_data_file_r,))
            gnu_cmd.write("set origin 0.5,0.6\n")
            gnu_cmd.write("set title '%s: disk encp read rate'\n"%(sg.strip(' '),))
            gnu_cmd.write("plot '%s' using 1:7 t '' with impulses lw 5\n"%(plot_data_file_r,))
            gnu_cmd.write("set origin 0.,0.0\n")
            gnu_cmd.write("set title '%s: transfer encp read rate'\n"%(sg.strip(' '),))
            gnu_cmd.write("plot '%s' using 1:9 t '' with impulses lw 5\n"%(plot_data_file_r,))
            gnu_cmd.write("set origin 0.5,0.0\n")
            gnu_cmd.write("set title '%s: driver encp read rate'\n"%(sg.strip(' '),))
            gnu_cmd.write("plot '%s' using 1:11 t '' with impulses lw 5\n"%(plot_data_file_r,))
            gnu_cmd.close()
            
            os.system("gnuplot %s"%(file_name))
            os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_output, jpeg_output))
            os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_output, jpeg_output_stamp))
            os.system("rm -f %s"%file_name)


        postscript_output="%s/encp_rates_%s_w.ps"%(ENCP_RATE,sg.strip(' '))
        jpeg_output="%s/encp_rates_%s_w.jpg"%(ENCP_RATE,sg.strip(' '))
        jpeg_output_stamp="%s/encp_rates_%s_w_stamp.jpg"%(ENCP_RATE,sg.strip(' '))

        if (os.path.exists(plot_data_file_w)) : 
            lower = "%s"%(time.strftime("%Y-%m-%d",time.localtime(now_time-delta_time)))
            file_name = "tmp_%s_w_gnuplot.cmd"%(sg.strip(' '))
            gnu_cmd = open(file_name,'w')
            upper = "%s"%(time.strftime("%Y-%m-%d",time.localtime(now_time)))
            gnu_cmd.write("set terminal postscript color solid\n")
            gnu_cmd.write("set output '%s'\n"%(postscript_output,))
            gnu_cmd.write("set xlabel 'Date'\n")
            tf='"%Y-%m-%d %H:%M"'
            gnu_cmd.write("set timefmt %s\n"%(tf,))
            gnu_cmd.write("set yrange [0 : ]\n")
            gnu_cmd.write("set xdata time\n")
            gnu_cmd.write("set xrange [ '%s':'%s' ]\n"%(lower, upper))
            tf='"%m-%d %H"'
            gnu_cmd.write("set format x %s\n"%(tf,))
            gnu_cmd.write("set ylabel 'Rate MB/s'\n")
            gnu_cmd.write("set grid\n")
            gnu_cmd.write("set size 1.0, 2.0\n")
            gnu_cmd.write("set origin 0.0, 0.0\n");
            gnu_cmd.write("set multiplot\n")
            gnu_cmd.write("set size 0.5, 0.6\n")
            gnu_cmd.write("set origin 0.25,1.2\n")
            gnu_cmd.write("set title '%s: overall encp write rate generated at %s'\n"%(sg.strip(' '),(time.ctime(time.time())),))
            gnu_cmd.write("plot '%s' using 1:3 t '' with impulses lw 5\n"%(plot_data_file_w,))
            gnu_cmd.write("set origin 0.0,0.6\n")
            gnu_cmd.write("set title '%s: network encp write rate'\n"%(sg.strip(' '),))
            gnu_cmd.write("plot '%s' using 1:5 t '' with impulses lw 5\n"%(plot_data_file_w,))
            gnu_cmd.write("set origin 0.5,0.6\n")
            gnu_cmd.write("set title '%s: disk encp write rate'\n"%(sg.strip(' '),))
            gnu_cmd.write("plot '%s' using 1:7 t '' with impulses lw 5\n"%(plot_data_file_w,))
            gnu_cmd.write("set origin 0.,0.0\n")
            gnu_cmd.write("set title '%s: transfer encp write rate'\n"%(sg.strip(' '),))
            gnu_cmd.write("plot '%s' using 1:9 t '' with impulses lw 5\n"%(plot_data_file_w,))
            gnu_cmd.write("set origin 0.5,0.0\n")
            gnu_cmd.write("set title '%s: driver encp write rate'\n"%(sg.strip(' '),))
            gnu_cmd.write("plot '%s' using 1:11 t '' with impulses lw 5\n"%(plot_data_file_w,))
            gnu_cmd.close()
            
            os.system("gnuplot %s"%(file_name))
            os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_output, jpeg_output))
            os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_output, jpeg_output_stamp))
            os.system("rm -f %s"%file_name)


#    for sg in storage_groups:
#
#        postscript_output="encp_rates_%s_r.ps"%(sg.strip(' '))
#        jpeg_output="encp_rates_%s_r.jpg"%(sg.strip(' '))
#        jpeg_output_stamp="encp_rates_%s_r_stamp.jpg"%(sg.strip(' '))
#
#
#        os.system("rcp %s enstore@stkensrv2.fnal.gov:/diska/www_pages/%s"%(postscript_output,postscript_output))
#        os.system("rcp %s enstore@stkensrv2.fnal.gov:/diska/www_pages/%s"%(jpeg_output,jpeg_output))
#        os.system("rcp %s enstore@stkensrv2.fnal.gov:/diska/www_pages/%s"%(jpeg_output_stamp,jpeg_output_stamp))
#
#        os.system("rcp -f %s"%(postscript_output))
#        os.system("rcp -f %s"%(jpeg_output))
#        os.system("rcp -f %s"%(jpeg_output_stamp))
#
#        postscript_output="encp_rates_%s_w.ps"%(sg.strip(' '))
#        jpeg_output="encp_rates_%s_w.jpg"%(sg.strip(' '))
#        jpeg_output_stamp="encp_rates_%s_w_stamp.jpg"%(sg.strip(' '))
#
#
#        os.system("rcp %s enstore@stkensrv2.fnal.gov:/diska/www_pages/%s"%(postscript_output,postscript_output))
#        os.system("rcp %s enstore@stkensrv2.fnal.gov:/diska/www_pages/%s"%(jpeg_output,jpeg_output))
#        os.system("rcp %s enstore@stkensrv2.fnal.gov:/diska/www_pages/%s"%(jpeg_output_stamp,jpeg_output_stamp))
#
#        os.system("rcp -f %s"%(postscript_output))
#        os.system("rcp -f %s"%(jpeg_output))
#        os.system("rcp -f %s"%(jpeg_output_stamp))


    for storage_group in storage_groups:
       sg =  storage_group.strip(' ')
       cmd = "rm -f rate_%s_w.dat"%(sg)
       os.system(cmd)
       cmd = "rm -f rate_%s_r.dat"%(sg)
       os.system(cmd)
        


    sys.exit(0)


    

if __name__ == "__main__":
    main()
