#!/usr/bin/env python

import sys
import os
import string
import time

vf_file = '/diska/tape-inventory/VOLUMES_DEFINED'
tmp_data = '/tmp/mounts'
tmp_gnuplot_cmd = '/tmp/gnuplot.cmd'
install_dir = '/fnal/ups/prd/www_pages/enstore'
output_file = 'mounts'
output_file_logy = 'mounts_logy'
# postscript_output = '/tmp/mounts.ps'
postscript_output = os.path.join(install_dir, output_file+'.ps')
# postscript_output_logy = '/tmp/mounts_logy.ps'
postscript_output_logy = os.path.join(install_dir, output_file_logy+'.ps')
jpeg_output = os.path.join(install_dir, output_file+'.jpg')
jpeg_output_logy = os.path.join(install_dir, output_file_logy+'.jpg')

jpeg_output_stamp = os.path.join(install_dir, output_file+'_stamp.jpg')
jpeg_output_logy_stamp = os.path.join(install_dir, output_file_logy+'_stamp.jpg')

low_water_mark = 1000
high_water_mark = 5000

tol = 0
toh = 0

mts = []
mtsg = {}

if __name__ == '__main__':
	f = open(vf_file)
	l = f.readline()
	while l:
		t = string.split(l)
		if len(t) > 8 and t[0] != 'Date':
			m = int(string.split(t[-2], '<')[0])
			if m == -1:
				m = 0
			mts.append(m)
			sg = string.split(t[-1], '.')[0]
			if mtsg.has_key(sg):
				mtsg[sg].append(m)
			else:
				mtsg[sg] = [m]
			if m > low_water_mark:
				tol = tol + 1
			if m > high_water_mark:
				toh = toh + 1

		l = f.readline()

	mts.sort()
	for i in mtsg.keys():
		mtsg[i].sort()

	count = 0
	outf = open(tmp_data, "w")
	for i in mts:
		count = count + 1
		outf.write("%d %d\n"%(count, i))
	outf.close()

	outf = open(tmp_gnuplot_cmd, "w")
	outf.write("set grid\n")
	outf.write("set ylabel 'Mounts'\n")
	outf.write("set terminal postscript color solid\n")
	outf.write("set output '"+postscript_output+"'\n")
	outf.write("set title 'Tape Mounts per Volume (plotted at %s)'\n"%(time.ctime(time.time())))
	outf.write("set arrow 1 from %d,%d to %d,%d head\n"%(count-toh-500, high_water_mark-500, count-toh, high_water_mark))
	outf.write("set arrow 2 from %d,%d to %d,%d head\n"%(count-tol-500, low_water_mark+500, count-tol, low_water_mark))
	outf.write("set label 1 '%d' at %d,%d right\n"%(toh, count-toh-500, high_water_mark-500))
	outf.write("set label 2 '%d' at %d,%d right\n"%(tol, count-tol-500, low_water_mark+500))
	outf.write("set label 3 '%d' at -50,%d right\n"%(high_water_mark, high_water_mark))
	outf.write("set label 4 '%d' at -50,%d right\n"%(low_water_mark, low_water_mark))
	outf.write("plot '%s' notitle with impulses, %d notitle, %d notitle\n"%(tmp_data, low_water_mark, high_water_mark))

	outf.write("set logscale y\n")
	outf.write("set output '"+postscript_output_logy+"'\n")
	outf.write("set arrow 1 from %d,%d to %d,%d head\n"%(count-toh-500, high_water_mark-2000, count-toh, high_water_mark))
	outf.write("set label 1 '%d' at %d,%d right\n"%(toh, count-toh-500, high_water_mark-2000))
	outf.write("plot '%s' notitle with impulses, %d notitle, %d notitle\n"%(tmp_data, low_water_mark, high_water_mark))
	outf.close()

	os.system("gnuplot %s"%(tmp_gnuplot_cmd))

	# convert to jpeg
	os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_output, jpeg_output))
	os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_output_logy, jpeg_output_logy))
	os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_output, jpeg_output_stamp))
	os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_output_logy, jpeg_output_logy_stamp))

	# clean up
	os.unlink(tmp_data)
	os.unlink(tmp_gnuplot_cmd)
