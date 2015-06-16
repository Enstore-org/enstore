#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import pg
import sys
import os
import time

# enstore imports
import enstore_plotter_module
import enstore_constants
import enstore_stop

WEB_SUB_DIRECTORY = enstore_constants.MOUNT_PLOTS_SUBDIR

class MountsPlotterModule(enstore_plotter_module.EnstorePlotterModule):
	def __init__(self,name,isActive=True):
		enstore_plotter_module.EnstorePlotterModule.__init__(
			self, name, isActive)
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

		self.libraries=None

	def hist_key(self,n):
		if n >= self.high_water_mark:
			return 'Over '+str(self.high_water_mark)
		elif n >= self.low_water_mark:
			return str(self.low_water_mark)+'-'+str(self.high_water_mark-1)
		else:
			nb = n/self.step*self.step
			return str(nb)+'-'+str(nb+self.step-1)

	#Write out the file that gnuplot will use to plot the data.
	# plot_filename = The file that will be read in by gnuplot containing
	#                 the gnuplot commands.
	# data_filename = The data file that will be read in by gnuplot
	#                 containing the data to be plotted.
	# ps_filename = The postscript file that will be created by gnuplot.
	# ps_filename_logy = The logy postscript file that will be created.
	# library = Name of the Library Manager these tapes belong to.
	# count = ???
	def write_plot_files(self, plot_filename, pts_filename,
			     ps_filename, ps_filename_logy, library, count):
		outf = open(plot_filename, "w")

		#Write out the commands for both files.
		outf.write("set grid\n")
		outf.write("set ylabel 'Mounts'\n")
		outf.write("set terminal postscript color solid\n")

		outf.write("set title '%s Tape Mounts per Volume (plotted at %s)'\n"
			   % (library, time.ctime(time.time())))
		if self.toh > 0:
			outf.write("set arrow 1 from %d,%d to %d,%d head\n"
				   % (count-self.toh-500,
				      self.high_water_mark-500,
				      count-self.toh, self.high_water_mark))
			outf.write("set label 1 '%d' at %d,%d right\n"
				   %(self.toh, count-self.toh-500,
				     self.high_water_mark-500))
		if self.tol > 0:
			outf.write("set arrow 2 from %d,%d to %d,%d head\n"
				   % (count-self.tol-500,
				      self.low_water_mark+500, count-self.tol,
				      self.low_water_mark))
			outf.write("set label 2 '%d' at %d,%d right\n"
				   % (self.tol, count-self.tol-500,
				      self.low_water_mark+500))
		outf.write("set label 3 '%d' at 500,%d left\n"
			   % (self.high_water_mark, self.high_water_mark))
		outf.write("set label 4 '%d' at 500,%d left\n"
			   % (self.low_water_mark, self.low_water_mark))

		#Write out regular plot creating commands.
		outf.write("set output '" + ps_filename + "'\n")
		outf.write("plot '%s' notitle with impulses, %d notitle, %d notitle\n"
			   % (pts_filename, self.low_water_mark,
			      self.high_water_mark))

		#Write out logy plot creating commands.
		outf.write("set logscale y\n")
		outf.write("set output '" + ps_filename_logy + "'\n")
		if self.toh > 0:
			outf.write("set arrow 1 from %d,%d to %d,%d head\n"
				   % (count-self.toh-500,
				      self.high_water_mark-2000,
				      count-self.toh, self.high_water_mark))
			outf.write("set label 1 '%d' at %d,%d right\n"
				   %(self.toh, count-self.toh-500,
				     self.high_water_mark-2000))
		outf.write("plot '%s' notitle with impulses, %d notitle, %d notitle\n"
			   % (pts_filename, self.low_water_mark,
			      self.high_water_mark))

		outf.close()

	def write_hist_plot_file(self, plot_filename, pts_filename,
				 ps_filename, library, count, set_xtics,
				 maxy, set_label):
		outf = open(plot_filename, "w")
		outf.write("set grid\n")
		outf.write("set ylabel 'Volumes'\n")
		outf.write("set xlabel 'Mounts'\n")
		outf.write("set xrange [0:%d]\n" % (count+1))
		outf.write("set yrange [0:%d]\n"
			   % (((maxy+self.yoffset*2)/self.ystep+1)*self.ystep))
		outf.write(set_label)
		outf.write(set_xtics)
		outf.write("set tics out\n")
		if os.uname()[1] == 'cdfensrv2.fnal.gov':
			outf.write("set arrow from 21,2000 to 21,500\n")
			outf.write("set label \"Bakken's Tape\" at 21,2250 center\n")
		outf.write("set terminal postscript color solid\n")
		outf.write("set output '" + ps_filename + "'\n")
		outf.write("set title '%s Tape Mounts (plotted at %s)'\n"
			   % (library, time.ctime(time.time())))
		outf.write("plot '%s' notitle with impulse lw 20\n"
			   % (pts_filename,))
		outf.close()


    #######################################################################
    # The following functions must be defined by all plotting modueles.
    #######################################################################

	def book(self, frame):
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
		#if self.get_parameter("library"):
		#	self.library=self.get_parameter("library")


		#Get cron directory information.
		cron_dict = frame.get_configuration_client().get("crons", {})

		#Pull out just the information we want.
		self.temp_dir = cron_dict.get("tmp_dir", "/tmp")
		html_dir = cron_dict.get("html_dir", "")

		#Handle the case were we don't know where to put the output.
		if not html_dir:
			sys.stderr.write("Unable to determine html_dir.\n")
			sys.exit(1)

		self.web_dir = os.path.join(html_dir, WEB_SUB_DIRECTORY)
		if not os.path.exists(self.web_dir):
			os.makedirs(self.web_dir)

	def fill(self, frame):

		#Specify the x ranges.
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


		edb = frame.get_configuration_client().get('database', {})
		db = pg.DB(host  = edb.get('db_host', "localhost"),
			   dbname= edb.get('dbname', "enstoredb"),
			   port  = edb.get('db_port', 5432),
			   user  = edb.get('dbuser', "enstore"))

		# get list of library managers available in config and then select only those to plot
		libraries = enstore_stop.find_servers_by_type(frame.get_configuration_client(), enstore_constants.LIBRARY_MANAGER)
		q="select distinct library from volume where media_type!='null'"
		if len(libraries) > 0 :
			q = q + " and library in ('"+libraries[0].split('.')[0] +"'"
			for l in libraries[1:]:
				q = q + ",'"+l.split('.')[0]+"'"
			q=q+")"
		res_lib = db.query(q).getresult()

		self.libraries = {}
		for row in res_lib:
			if not row:
				continue
			#Get the data for the plot.
			pts_filename = os.path.join(self.temp_dir,
						    "mounts_%s.pts" % row[0])
			count = self.__fill(row[0], pts_filename, db)
			self.libraries[row[0]] = {"fname" : pts_filename,
						  "count" : count,
						  }

		#One last one for the entire system.
		pts_filename = os.path.join(self.temp_dir, "mounts.pts")
		count = self.__fill(None, pts_filename, db)
		self.libraries["All"] = {"fname" : pts_filename,
					"count" : count,
					}
		db.close()

	def __fill(self, library, pts_filename, db):

		sql_stm = "declare volume_cursor cursor for " \
			  "select label,sum_mounts,storage_group,file_family,wrapper,library " \
			  "from volume " \
			  "where system_inhibit_0!='DELETED' " \
			  "and media_type!='null'"
		if library:
			  sql_stm = "%s and library = '%s'" \
				    % (sql_stm, library)

		db.query("begin")
		db.query(sql_stm)
		while True:
			res =  db.query("fetch 10000 from volume_cursor").getresult()
			for row in res:
				sg=row[2]
				#ff=row[3]
				#wp=row[4]
				m=row[1]
				#lib=row[5]

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
		db.query("rollback") #Close off the "begin".

		self.mts.sort()

		#Write out the data files.
		for i in self.mtsg.keys():
			self.mtsg[i].sort()
		count = 0
		outf = open(pts_filename, "w")
		for i in self.mts:
			count = count + 1
			outf.write("%d %d\n" % (count, i))

		#If there are no valid data points, we need to insert one zero
		# valued point for gnuplot to plot.  If this file is empty,
		# gnuplot throws and error.  This will happen, most likely,
		# when the pnfs backup cronjob has not run at all in the
		# last month.
		if not self.mts:
			outf.write("%d %d\n" % (0, 0))

		outf.close()

		#if count == 0 :
		return count


	def plot(self):

	    for library, lib_dict in self.libraries.items():
	        pts_filename = lib_dict['fname']
		count = lib_dict['count']

		#
		### The mount plots by library (and one total).
		#

	        #Some preliminary variables to be used in filenames.
		output_file="mounts"
		if library:
			output_file = output_file + '-' + library
		output_file_logy = output_file + '_logy'

		#The final output
		ps_filename = os.path.join(self.web_dir, output_file + '.ps')
		ps_filename_logy = os.path.join(self.web_dir,
						output_file_logy + '.ps')

		#The jpeg output.
		jpeg_filename = os.path.join(self.web_dir,
					     output_file + '.jpg')
		jpeg_filename_logy = os.path.join(self.web_dir,
						  output_file_logy + '.jpg')
		jpeg_filename_stamp = os.path.join(self.web_dir,
						   output_file + '_stamp.jpg')
		jpeg_filename_logy_stamp = os.path.join(self.web_dir,
					      output_file_logy + '_stamp.jpg')

		#The commands that gnuplot will execute will go here.
		plot_filename = os.path.join(self.temp_dir,
					     output_file + ".plot")
		self.write_plot_files(plot_filename, pts_filename,
				      ps_filename, ps_filename_logy,
				      library, count)

		os.system("gnuplot %s" % (plot_filename))

		# convert to jpeg
		os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
			  % (ps_filename, jpeg_filename))
		os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
			  % (ps_filename_logy, jpeg_filename_logy))
		os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
			  % (ps_filename, jpeg_filename_stamp))
		os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
			  % (ps_filename_logy, jpeg_filename_logy_stamp))

		#Cleanup the temporary files.
		try:
			os.remove(plot_filename)
			pass
		except:
			pass
		try:
			os.remove(pts_filename)
			pass
		except:
			pass

		#
		### The histogram.
		#

		#Filenames for various values.
		hist_out = output_file + '_hist'
		ps_hist_filename = os.path.join(self.web_dir,
						hist_out + '.ps')
		jpeg_hist_filename = os.path.join(self.web_dir,
						  hist_out + '.jpg')
		jpeg_hist_filename_stamp = os.path.join(self.web_dir,
						   hist_out + '_stamp.jpg')

		if library:
			pts_filename = os.path.join(self.temp_dir,
						    "%s.pts" % hist_out)
		else:
			pts_filename = os.path.join(self.temp_dir,
						    "%s.pts" % hist_out)


		count = 0
		set_label = ""
		set_xtics = "set xtics rotate ("
		outf = open(pts_filename, "w")
		maxy = 0
		for i in self.hist_keys:
			count = count + 1
			###This does some filling to.  Oh well.
			outf.write("%d %d\n" % (count, self.mtsh[i]))
			if self.mtsh[i] > maxy:
				maxy = self.mtsh[i]
			set_xtics = set_xtics + '"%s" %d,'%(i, count)
			set_label = set_label + \
				    'set label %d "%d" at %d,%d center\n' \
				    % (count, self.mtsh[i], count,
				       self.mtsh[i] + self.yoffset)
		outf.close()
		set_xtics = set_xtics[:-1]+')'
		set_xtics = set_xtics+'\n'


		#The commands that gnuplot will execute will go here.
		plot_filename = os.path.join(self.temp_dir,
					     hist_out + ".plot")
		self.write_hist_plot_file(plot_filename, pts_filename,
					  ps_hist_filename, library,
					  count, set_xtics, maxy, set_label)

		#Make the plot and convert it to jpg.
		os.system("gnuplot %s" % (plot_filename,))
		os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
			  % (ps_hist_filename, jpeg_hist_filename))
		os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
			  % (ps_hist_filename, jpeg_hist_filename_stamp))

		#Cleanup the temporary files.
		try:
			os.remove(plot_filename)
			pass
		except:
			pass
		try:
			os.remove(pts_filename)
			pass
		except:
			pass
