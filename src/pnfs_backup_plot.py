#!/usr/bin/env python

import sys
import os
import string
import time

data = '/tmp/pnfsFastBackupHISTOGRAM'
tmp_gnuplot_cmd = '/tmp/pnfsFastBackup.cmd'
plot_data_file = '/tmp/pnfsFastBackup.pts'
good_points='/tmp/pnfsFastBackup_good.pts'
bad_points='/tmp/pnfsFastBackup_bad.pts'
output_file = 'pnfs_backup_time'
install_dir = '/fnal/ups/prd/www_pages/enstore'
postscript_output = os.path.join(install_dir, output_file+'.ps')
jpeg_output = os.path.join(install_dir, output_file+'.jpg')
jpeg_output_stamp = os.path.join(install_dir, output_file+'_stamp.jpg')

if __name__ == '__main__':
   plot_data = []
   d = []
   f = open(data)
   l = f.readline()
   started=finished=0
   
   while l:
      #the line format is as YYYY-MM-DD:hh:mm:ss code
      t0,code = l.split()
      code=int(code)
      t1 = t0.split('-')
      t2 = t1[2].split(':')
      tm = []
      
      for i in (t1[0],t1[1]):
         tm.append(int(i))
      for i in t2:
         tm.append(int(i))
      tm.append(0)
      tm.append(0)
      tm.append(-1)
      t = time.mktime(tm)
      if code == 10 and started == 0:
	 started = 1
	 st = t
      else:
         if finished == 0 and started ==1:
            finished = 1
            ft = t
      if started & finished:
	 dt = ft - st
	 started = finished = 0
	 d.append((st,dt, code))
	 
      l = f.readline()

   
   for i in d:
      if i[2] != 10:
         # data should not contain a beginnig code
         plot_data.append(i)
   del(d)
   pts = open(plot_data_file,'w')
   good_pts=open(good_points,'w')
   bad_pts=open(bad_points,'w')
   timefmt = "%Y-%m-%d:%H:%M"
   #timefmt = "%Y-%m-%d"
   for i in plot_data:
      pts.write("%s %s\n"%(time.strftime(timefmt,time.localtime(i[0])), i[1]))
      if i[2] == 0:
         good_pts.write("%s %s\n"%(time.strftime(timefmt,time.localtime(i[0])), i[1]))
      else:
         bad_pts.write("%s %s\n"%(time.strftime(timefmt,time.localtime(i[0])), i[1]))
   pts.close()
   good_pts.close()
   bad_pts.close()
   # generate plot for a month period of time
   t = time.time()
   t0 = t - 30*24*60*60
   lower = "%s"%(time.strftime("%Y-%m-%d",time.localtime(t0)))
   upper = "%s"%(time.strftime("%Y-%m-%d",time.localtime(t)))
   cmd = open(tmp_gnuplot_cmd,'w')
   cmd.write("set terminal postscript color solid\n")
   cmd.write("set xlabel 'Date'\n")
   tf='"%Y-%m-%d:%H:%M"'
   cmd.write("set timefmt %s\n"%(tf,))
   cmd.write("set yrange [0 : ]\n")
   cmd.write("set xdata time\n")
   cmd.write("set xrange [ '%s':'%s' ]\n"%(lower, upper))
   #cmd.write("set xrange [ : ]\n")
   #tf='"%y-%m-%d:%H:%M"'
   #tf='"%y-%m-%d"'
   tf='"%m-%d"'
   cmd.write("set format x %s\n"%(tf,))
   cmd.write("set ylabel 'Backup time'\n")
   cmd.write("set grid\n")
   cmd.write("set output '%s'\n"%(postscript_output,))
   cmd.write("set title 'pnfs backup time generated on %s'\n"%((time.ctime(time.time())),))
   cmd.write("plot '%s' using 1:2 t '' with impulses lw 10\n"%(plot_data_file,))
   cmd.close()
   os.system("gnuplot %s"%(tmp_gnuplot_cmd))
   os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_output, jpeg_output))
   os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_output, jpeg_output_stamp))

