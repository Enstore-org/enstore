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

hist_out = output_file+'_hist'
postscript_hist_out = os.path.join(install_dir, hist_out+'.ps')
latex_hist_out = os.path.join(install_dir, hist_out+'.tex')
jpeg_hist_out = os.path.join(install_dir, hist_out+'.jpg')
jpeg_hist_out_stamp = os.path.join(install_dir, hist_out+'_stamp.jpg')

low_water_mark = 1000
high_water_mark = 5000
step = 100

tol = 0
toh = 0

mts = []
mtsg = {}
mtsh = {}

def hist_key(n):
	if n >= high_water_mark:
		return '>='+str(high_water_mark)
	elif n >= low_water_mark:
		return str(low_water_mark)+'-'+str(high_water_mark-1)
	else:
		nb = n/step*step
		return str(nb)+'-'+str(nb+step-1)
		
if __name__ == '__main__':

	# create bins
	hist_keys = [] # to preserve the order
	for i in range(0, low_water_mark, step):
		k = hist_key(i)
		mtsh[k] = 0
		hist_keys.append(k)
	k = hist_key(low_water_mark)
	hist_keys.append(k)
	mtsh[k] = 0
	k = hist_key(high_water_mark)
	hist_keys.append(k)
	mtsh[k] = 0

	f = open(vf_file)
	l = f.readline()
	while l:
		t = string.split(l)
		if len(t) > 8 and t[0] != 'Date':
			sg, ff, wp = string.split(t[-1], '.')
			if wp != 'null':
				m = int(string.split(t[-2], '<')[0])
				if m == -1:
					m = 0
				mts.append(m)
				if mtsg.has_key(sg):
					mtsg[sg].append(m)
				else:
					mtsg[sg] = [m]
				if m >= high_water_mark:
					toh = toh + 1
				if m >= low_water_mark:
					tol = tol + 1
				k = hist_key(m)
				mtsh[k] = mtsh[k]+1
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
	if toh > 0:
		outf.write("set arrow 1 from %d,%d to %d,%d head\n"%(count-toh-500, high_water_mark-500, count-toh, high_water_mark))
		outf.write("set label 1 '%d' at %d,%d right\n"%(toh, count-toh-500, high_water_mark-500))
	if tol > 0:
		outf.write("set arrow 2 from %d,%d to %d,%d head\n"%(count-tol-500, low_water_mark+500, count-tol, low_water_mark))
		outf.write("set label 2 '%d' at %d,%d right\n"%(tol, count-tol-500, low_water_mark+500))
	outf.write("set label 3 '%d' at 500,%d left\n"%(high_water_mark, high_water_mark))
	outf.write("set label 4 '%d' at 500,%d left\n"%(low_water_mark, low_water_mark))
	outf.write("plot '%s' notitle with impulses, %d notitle, %d notitle\n"%(tmp_data, low_water_mark, high_water_mark))

	outf.write("set logscale y\n")
	outf.write("set output '"+postscript_output_logy+"'\n")
	if toh > 0:
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

	# The histogram

	count = 0
	set_xtics = "set xtics rotate ("
	outf = open(tmp_data, "w")
	for i in hist_keys:
		count = count + 1
		outf.write("%d %d\n"%(count, mtsh[i]))
		set_xtics = set_xtics + '"%s" %d,'%(i, count)
	outf.close()
	set_xtics = set_xtics[:-1]+')'
	set_xtics = set_xtics+'\n'

	outf = open(tmp_gnuplot_cmd, "w")
	outf.write("set grid\n")
	outf.write("set ylabel 'Volumes'\n")
	outf.write("set xlabel 'Mounts'\n")
	outf.write("set xrange [0:%d]\n"%(count+1))
	outf.write(set_xtics)
	outf.write("set terminal postscript color solid\n")
	outf.write("set output '"+postscript_hist_out+"'\n")
	outf.write("set title 'Tape Mounts (plotted at %s)'\n"%(time.ctime(time.time())))
	outf.write("plot '%s' notitle with impulse lw 5\n"%(tmp_data))
	outf.close()

	os.system("gnuplot %s"%(tmp_gnuplot_cmd))

	# convert to jpeg

	os.system("convert -rotate 90 -modulate 80 %s %s"%(postscript_hist_out, jpeg_hist_out))
	os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s %s"%(postscript_hist_out, jpeg_hist_out_stamp))

	# clean up
	os.unlink(tmp_data)
	os.unlink(tmp_gnuplot_cmd)
