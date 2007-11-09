#!/usr/bin/env python

import pg
import sys
import os
import string
import time
import sys
import enstore_plotter_framework
import enstore_plotter_module
import enstore_constants

tmp_data        = '/tmp/mounts'
tmp_gnuplot_cmd = '/tmp/gnuplot.cmd'
install_dir     = '/fnal/ups/prd/www_pages/enstore'
output_file     = "mounts"


class MountsPlot(enstore_plotter_module.EnstorePlotterModule):
	def __init__(self,name,isActive=True):
		enstore_plotter_module.EnstorePlotterModule.__init__(self,name,isActive)
		self.low_water_mark=2000
		self.high_water_mark=5000
		self.step=100
		self.yoffset=200
		self.ystep=500
		self.tol=0
		self.toh=0
		self.mts=[]
		self.mtsg={}
		self.mtsh={}
		self.hist_keys=[]
		self.library=""

	def book(self,frame):
		if self.get_parameter("low_water_mark"):
			self.low_water_mark=self.get_parameter("low_water_mark")
		if self.get_parameter("high_water_mark"):
			self.high_water_mark=self.get_parameter("high_water_mark")
		if self.get_parameter("step"):
			self.step=self.get_parameter("step")
		if self.get_parameter("yoffset"):
			self.yoffset=self.get_parameter("yoffset")
		if self.get_parameter("ystep"):
			self.ystep=self.get_parameter("ystep")
		if self.get_parameter("library"):
			self.library=self.get_parameter("library")

	def hist_key(self,n):
		if n >= self.high_water_mark:
			return 'Over '+str(self.high_water_mark)
		elif n >= self.low_water_mark:
			return str(self.low_water_mark)+'-'+str(self.high_water_mark-1)
		else:
			nb = n/self.step*self.step
			return str(nb)+'-'+str(nb+self.step-1)

	def fill(self,frame):
		acc = frame.get_configuration_client().get('database', {})
		db = pg.DB(host  = acc.get('db_host', "localhost"),
			   dbname= acc.get('dbname', "enstoredb"),
			   port  = acc.get('db_port', 5432),
			   user  = acc.get('dbuser', "enstore"))

		for i in range(0, self.low_water_mark, self.step):
			k = self.hist_key(i)
			self.mtsh[k] = 0
			self.hist_keys.append(k)
		k = self.hist_key(self.low_water_mark)
		self.hist_keys.append(k)
		self.mtsh[k] = 0
		k = self.hist_key(self.high_water_mark)
		self.hist_keys.append(k)
		self.mtsh[k] = 0

		
		db.query("begin");
		db.query("declare volume_cursor cursor for select label,sum_mounts,storage_group,file_family,wrapper,library from volume where system_inhibit_0!='DELETED' and media_type!='null'")
		while True:
			res =  db.query("fetch 10000 from volume_cursor").getresult()
			for row in res:
				sg=row[2]
				ff=row[3]
				wp=row[4]
				m=row[1]
				lib=row[5]
				if not self.library or (lib == self.library):
					if m == -1:
						m = 0
					self.mts.append(m)
					if self.mtsg.has_key(sg):
						self.mtsg[sg].append(m)
					else:
						self.mtsg[sg] = [m]
					if m >= self.high_water_mark:
						self.toh = self.toh + 1
					if m >= self.low_water_mark:
						self.tol = self.tol + 1
					k = self.hist_key(m)
					self.mtsh[k] = self.mtsh[k]+1
			l=len(res)
			if (l < 10000):
				break
		db.close()

		self.mts.sort()
		self.html_dir=frame.get_configuration_client().get('inquisitor')['html_file']

	def plot(self):

		output_file="mounts"
		if self.library:
			output_file = output_file + '-' +self.library

		output_file_logy = output_file + '_logy'
	
		install_dir=self.html_dir
		# postscript_output = '/tmp/mounts.ps'
		postscript_output = os.path.join(install_dir, output_file+'.ps')
		# postscript_output_logy = '/tmp/mounts_logy.ps'
		postscript_output_logy = os.path.join(install_dir, output_file_logy+'.ps')
		jpeg_output = os.path.join(install_dir, output_file+'.jpg')
		jpeg_output_logy = os.path.join(install_dir, output_file_logy+'.jpg')
		jpeg_output_stamp = os.path.join(install_dir, output_file+'_stamp.jpg')
		jpeg_output_logy_stamp = os.path.join(install_dir, output_file_logy+'_stamp.jpg')

		hist_out = output_file+'_hist'
		postscript_hist_out = os.path.join(install_dir, hist_out+'.ps')
		jpeg_hist_out = os.path.join(install_dir, hist_out+'.jpg')
		jpeg_hist_out_stamp = os.path.join(install_dir, hist_out+'_stamp.jpg')

		for i in self.mtsg.keys():
			self.mtsg[i].sort()
		count = 0
		outf = open(tmp_data, "w")
		for i in self.mts:
			count = count + 1
			outf.write("%d %d\n"%(count, i))
		outf.close()
		if count == 0 :
			return

		outf = open(tmp_gnuplot_cmd, "w")
		outf.write("set grid\n")
		outf.write("set ylabel 'Mounts'\n")
		outf.write("set terminal postscript color solid\n")
		outf.write("set output '"+postscript_output+"'\n")
		outf.write("set title '%s Tape Mounts per Volume (plotted at %s)'\n"%(self.library, time.ctime(time.time())))
		if self.toh > 0:
			outf.write("set arrow 1 from %d,%d to %d,%d head\n"%(count-self.toh-500, self.high_water_mark-500, count-self.toh, self.high_water_mark))
			outf.write("set label 1 '%d' at %d,%d right\n"%(self.toh, count-self.toh-500, self.high_water_mark-500))
		if self.tol > 0:
			outf.write("set arrow 2 from %d,%d to %d,%d head\n"%(count-self.tol-500, self.low_water_mark+500, count-self.tol, self.low_water_mark))
			outf.write("set label 2 '%d' at %d,%d right\n"%(self.tol, count-self.tol-500, self.low_water_mark+500))
		outf.write("set label 3 '%d' at 500,%d left\n"%(self.high_water_mark, self.high_water_mark))
		outf.write("set label 4 '%d' at 500,%d left\n"%(self.low_water_mark, self.low_water_mark))
		outf.write("plot '%s' notitle with impulses, %d notitle, %d notitle\n"%(tmp_data, self.low_water_mark, self.high_water_mark))

		outf.write("set logscale y\n")
		outf.write("set output '"+postscript_output_logy+"'\n")
		if self.toh > 0:
			outf.write("set arrow 1 from %d,%d to %d,%d head\n"%(count-self.toh-500, self.high_water_mark-2000, count-self.toh, self.high_water_mark))
			outf.write("set label 1 '%d' at %d,%d right\n"%(self.toh, count-self.toh-500, self.high_water_mark-2000))
		outf.write("plot '%s' notitle with impulses, %d notitle, %d notitle\n"%(tmp_data, self.low_water_mark, self.high_water_mark))
		outf.close()

		os.system("gnuplot %s"%(tmp_gnuplot_cmd))

		# convert to jpeg
		os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_output, jpeg_output))
		os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_output_logy, jpeg_output_logy))
		os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_output, jpeg_output_stamp))
		os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_output_logy, jpeg_output_logy_stamp))

		# The histogram

		count = 0
		set_label = ""
		set_xtics = "set xtics rotate ("
		outf = open(tmp_data, "w")
		maxy = 0
		for i in self.hist_keys:
			count = count + 1
			outf.write("%d %d\n"%(count, self.mtsh[i]))
			if self.mtsh[i] > maxy:
				maxy = self.mtsh[i]
			set_xtics = set_xtics + '"%s" %d,'%(i, count)
			set_label = set_label+'set label %d "%d" at %d,%d center\n'%(
				count, self.mtsh[i], count, self.mtsh[i]+self.yoffset)
		outf.close()
		set_xtics = set_xtics[:-1]+')'
		set_xtics = set_xtics+'\n'

		outf = open(tmp_gnuplot_cmd, "w")
		outf.write("set grid\n")
		outf.write("set ylabel 'Volumes'\n")
		outf.write("set xlabel 'Mounts'\n")
		outf.write("set xrange [0:%d]\n"%(count+1))
		outf.write("set yrange [0:%d]\n"%(((maxy+self.yoffset*2)/self.ystep+1)*self.ystep))
		outf.write(set_label)
		outf.write(set_xtics)
		outf.write("set tics out\n")
		if os.uname()[1] == 'cdfensrv2.fnal.gov':
			outf.write("set arrow from 21,2000 to 21,500\n")
			outf.write("set label \"Bakken's Tape\" at 21,2250 center\n")
		outf.write("set terminal postscript color solid\n")
		outf.write("set output '"+postscript_hist_out+"'\n")
		outf.write("set title '%s Tape Mounts (plotted at %s)'\n"%(self.library, time.ctime(time.time())))
		outf.write("plot '%s' notitle with impulse lw 20\n"%(tmp_data))
		outf.close()

		os.system("gnuplot %s"%(tmp_gnuplot_cmd))

		# convert to jpeg

		os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_hist_out, jpeg_hist_out))
		os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_hist_out, jpeg_hist_out_stamp))

		# clean up
		os.unlink(tmp_data)
		os.unlink(tmp_gnuplot_cmd)
