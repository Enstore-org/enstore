#
# Classes to create the enstore plots.
#
##############################################################################
# system import
import string
import os
import time
import calendar
import stat
import sys

# enstore imports
import enstore_functions2
import enstore_files
import enstore_constants
import Trace


YES = 1
NO = 0
WRITE = "w"
#WRITE = "to"
TOTAL = "total"
READS = "reads"
WRITES = "writes"
CTR = "ctr"
MEAN = "mean"
USE_SUBDIR = "use_subdir"
DRIVE_ID = "drive_id"
LARGEST = "largest"
SMALLEST = "smallest"

# file extensions
PTS = ".pts"
TMP = ".tmp"
GNU = ".gnuplot"

HOURS_IN_DAY = ["00", "01", "02", "03", "04", "05", "06", "07", "08", \
                "09", "10", "11", "12", "13", "14", "15", "16", \
                "17", "18", "19", "20", "21", "22", "23"]

STAMP_JPG = "%s%s"%(enstore_constants.STAMP, enstore_constants.JPG)

LTO = "LTO"
MAMMOTH = "Mammoth"

DRIVE_IDS = { "ULT3580-TD1" : LTO,
	      "ULTRIUM-TD1" : LTO,
	      "EXB-89008E000112" : MAMMOTH,
	      "EXB-8900" : MAMMOTH
	      }

# write out the mount data
def write_mount_data(openfile, data):
    total = 0
    for mount_d in data:
        openfile.write("%s %s\n"%(mount_d['start'], mount_d['total']))
        total = total + int(mount_d['total'])
    return total

# there may be several different drive ids that correspond to one type of drive.
def translate_drive_id(drive_id):
    # if the drive_id has a '/' in it make it a '-'
    drive_id = string.replace(drive_id, '/', '-')
    return DRIVE_IDS.get(drive_id, drive_id)

def get_ctr(fields):
    return long(float(fields[1])/float(fields[6]))

# init the following hash from the first date given to the last date
def init_date_hash(sdate, edate):
    ndate = {}
    ndate[sdate[0:10]] = 0.0
    ndate[edate[0:10]] = 0.0
    imon = string.atoi(sdate[5:7])
    iday = string.atoi(sdate[8:10])
    iday = iday - 1 # we will increment this at the beginning of the loop
    iyr = string.atoi(sdate[0:4])
    is_leap = calendar.isleap(iyr)
    emon = string.atoi(edate[5:7])
    eday = string.atoi(edate[8:10])
    # there is a possible problem if the start date is after the end date
    while 1:
	if (imon == emon) and (iday == eday):
	    break
	iday = iday + 1
	if imon == 2:
	    mday = calendar.mdays[imon] + is_leap
	else:
	    mday = calendar.mdays[imon]
	if iday <= mday:
	    tmp = "%i-%02i-%02i" % (iyr, imon, iday)
	    ndate[tmp] = {TOTAL: 0.0}
	    ndate[tmp][READS] = 0.0
	    ndate[tmp][WRITES] = 0.0
	    ndate[tmp][LARGEST] = 0.0
	    ndate[tmp][SMALLEST] = -1
	    ndate[tmp][CTR] = 0
	    continue
	else:
	    imon = imon + 1
	    iday = 0
	    if imon > 12:
		imon = 1
		iyr = iyr + 1
		is_leap = calendar.isleap(iyr)
    return ndate

def sort_stamp_files(tmp_stamps):
    # sort the stamp files so that all mount per hour stamps are at the end in
    # descending date order.  first get the other plots to the front.
    jpg_stamp_files = []
    tmp_stamps.sort()
    # now move all list elements that are not mph stamps to new list, & delete them
    # from the old
    i = 0
    num_stamps = len(tmp_stamps)
    while i < num_stamps:
	if string.find(tmp_stamps[i][0], enstore_constants.MPH_FILE) == -1:
	    # this is not an mph stamp
	    jpg_stamp_files.append(tmp_stamps.pop(i))
	    num_stamps = num_stamps - 1
	else:
	    # this is mph file, leave it here for later sorting, skip to next stamp
	    i = i + 1
    # now we should have jpg_stamp_files full of non mph stamps and only mph stamps
    # left in tmp_stamps.  reverse sort these and add them at end of other stamps
    tmp_stamps.reverse()
    jpg_stamp_files = jpg_stamp_files + tmp_stamps
    return (jpg_stamp_files)

def ignore_file(file, ignore):
    for ignore_string in ignore:
	if not string.find(file, ignore_string) == -1:
	    # this file has a string meaning 'ignore this file' in it
		return YES
    else:
	return NO

def find_files(files, dir, ignore):
    # find all files with ".jpg" in them. fill
    # in the lists above with those files with and without the "_stamp"
    # string from this group. also find the ps files
    tmp_stamps = []
    jpg_files = []
    ps_files = []
    for file in files:
	if ignore_file(file, ignore) == YES:
	    continue
	if not string.find(file, enstore_constants.JPG) == -1:
	    # this file has '.jpg' in it
	    if not string.find(file, STAMP_JPG) == -1:
		# this is a postage stamp file
		tmp_stamps.append((file,
				   os.stat("%s/%s"%(dir, file))[stat.ST_MTIME]))
	    else:
		jpg_files.append((file,
				  os.stat("%s/%s"%(dir, file))[stat.ST_MTIME]))
	elif not string.find(file, enstore_constants.PS) == -1:
	    # this file has '.ps' in it
            ps_files.append((file, os.stat("%s/%s"%(dir, file))[stat.ST_MTIME]))
    return (jpg_files, tmp_stamps, ps_files)

def find_jpg_files(dir):
    # given the directory to look in, find all files with ".jpg" in them. fill
    # in the lists above with those files with and without the "_stamp"
    # string from this group. also find the ps files
    files = os.listdir(dir)
    ignore = [enstore_constants.MPH,]
    jpg_files, stamp_files, ps_files = find_files(files, dir, ignore)
    jpg_files.sort()
    ps_files.sort()
    jpg_stamp_files = sort_stamp_files(stamp_files)
    return (jpg_files, jpg_stamp_files, ps_files)

def convert_to_jpg(psfile, file_name):
    os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s%s"%(psfile, file_name,
                                                              STAMP_JPG))
    #JPG_STAMP_FILES.append("%s%s"%(file_name, STAMP_JPG))
    os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s%s"%(psfile, file_name,
                                                         enstore_constants.JPG))
    #JPG_FILES.append("%s%s"%(file_name, enstore_constants.JPG))

# return the time to be included in the title of the plots
def plot_time():
    return "(Plotted: %s)"%(enstore_functions2.format_time(time.time()),)

class EnPlot(enstore_files.EnFile):

    def __init__(self, dir, name):
	enstore_files.EnFile.__init__(self, dir+"/"+name)
	self.name = name
	self.dir = dir
	self.ptsfile = self.file_name+PTS
	self.tmpptsfile = self.file_name+TMP+PTS
	self.psfile = self.file_name+enstore_constants.PS
	self.gnufile = self.file_name+GNU

    def install(self, dir):
        Trace.trace(enstore_constants.PLOTTING,
                    "installing %s in %s"%(self.psfile, dir,))
        # create the ps file, copy it to the users dir
	os.system("gnuplot %s;cp %s %s;"%(self.gnufile, self.psfile, dir))
	# make a jpg version of the file including a postage stamp sized one
	convert_to_jpg(self.psfile, "%s/%s"%(dir, self.name))

    def open(self, mode='w'):
	Trace.trace(enstore_constants.PLOTTING,
                    "enfile open "+self.file_name)
	self.openfile = open(self.ptsfile, mode)

    def cleanup(self, keep, pts_dir):
        if not keep:
            # delete the gnu command file and the data points file
            os.system("rm %s;rm %s*"%(self.gnufile, self.ptsfile))
        else:
            if pts_dir:
                # move these files somewhere
                os.system("mv %s %s;mv %s* %s"%(self.gnufile, pts_dir,
                                                self.ptsfile, pts_dir))


class MpdGnuFile(enstore_files.EnFile):

    def write(self, outfile, ptsfile, total_mounts, mount_label, lw=20):
	if mount_label is None:
	    mount_label = ""
	self.openfile.write("set terminal postscript color solid\n"+ \
			    "set xlabel 'Date (year-month-day)'\n"+\
			    "set timefmt \"%Y-%m-%d\"\n"+ \
			    "set yrange [0 : ]\n"+ \
			    "set xrange [ : ]\n"+ \
			    "set xdata time\n"+ \
                            "set size 1.5,1\n"+ \
			    "set format x \"%y-%m-%d\"\n"+ \
			    "set ylabel 'Mounts'\n"+\
			    "set grid\n"+ \
			    "set output '"+outfile+"'\n"+\
			    "set title '%s Mounts/Day (Total = "%(mount_label,)+\
			    total_mounts+") "+plot_time()+"'\n"+\
			    "plot '"+ptsfile+"' using 1:2 t '' with impulses lw "+\
			    repr(lw)+"\n")

class MpdDriveTypeDataFile(EnPlot):

    def __init__(self, dir, drive_type):
        file = "%s-%s"%(drive_type, enstore_constants.MPD_FILE)
	EnPlot.__init__(self, dir, file)
        self.drive_type = drive_type
	self.lw = 10

    # make the mounts per day per drive type plot file
    def plot(self, total_mounts):
	# the data passed is of the form -
        # {'start': '2003-03-27', 'total': '345'}
        total = write_mount_data(self.openfile, total_mounts)

        self.openfile.close()
        gnucmds = MpdGnuFile(self.gnufile)
        gnucmds.open('w')
        gnucmds.write(self.psfile, self.ptsfile, repr(total), self.drive_type, self.lw)
        gnucmds.close()


class MpdDataFile(EnPlot):

    def __init__(self, dir, mount_label=None):
	EnPlot.__init__(self, dir, enstore_constants.MPD_FILE)
	self.mount_label = mount_label
	self.lw = 10

    def get_all_mounts(self, new_mounts_l):
	mounts_l = []
	updated_mounts_l = []
	total_mounts = 0
        # we can more easily use a dict of totals with the date as the key
        new_mounts_d = {}
        for new_mount in new_mounts_l:
            new_mounts_d[new_mount['start']] = int(new_mount['total'])
	if self.openfile:
	    mounts_l = self.openfile.readlines()
	    for line in mounts_l:
		[day, count] = string.split(string.strip(line))
                if new_mounts_d.has_key(day):
                    # replace the old count with the new value
                    updated_mounts_l.append("%s %s\n"%(day, new_mounts_d[day]))
                    total_mounts = total_mounts + new_mounts_d[day]
                    del new_mounts_d[day]
                else:
                    updated_mounts_l.append(line)
                    total_mounts = total_mounts + int(count)

	# now add to list any of the new days that were not present in the
        # old list
	days = new_mounts_d.keys()
	days.sort()
	for day in days:
	    updated_mounts_l.append("%s %s\n"%(day, new_mounts_d[day]))
	    total_mounts = total_mounts + new_mounts_d[day]
	return updated_mounts_l, total_mounts

    def open(self):
	if os.path.isfile(self.ptsfile):
	    EnPlot.open(self, 'r')

    # make the mounts per day plot file
    def plot(self, new_mounts_l):
	# the data passed to us is a list where each element is -
        # {'start': '2003-03-27', 'total': '345'}
        # in effect this is new data that
        # must be merged with the data currently in the total mount count
        # file.  will read in current data and overrite any old data with
        # the data that we have which is more recent.
        # NOTE: by default, the new data is for only 30 days.  if this saved
        # data has not been updated in longer than that, then there will be
        # incomplete/missing days.
	mounts_l, total_mounts = self.get_all_mounts(new_mounts_l)
	if self.openfile:
	    self.openfile.close()
	self.openfile = open(self.tmpptsfile, 'w')
	mounts_l.sort()
	for line in mounts_l:
	    self.openfile.write(line)
        else:
            self.openfile.close()
            # now move the temp file to the real one
            os.system("mv %s %s"%(self.tmpptsfile, self.ptsfile))

	# now create the gnuplot command file
	gnucmds = MpdGnuFile(self.gnufile)
	gnucmds.open('w')
	gnucmds.write(self.psfile, self.ptsfile, repr(total_mounts), self.mount_label,
		      self.lw)
	gnucmds.close()


class MpdMonthDataFile(EnPlot):

    def __init__(self, dir, mount_label=None):
	EnPlot.__init__(self, dir, enstore_constants.MPD_MONTH_FILE)
	self.mount_label = mount_label

    def open(self):
	if os.path.isfile(self.ptsfile):
	    EnPlot.open(self, 'r')

    # make the mounts per day plot file
    def plot(self, total_mounts, total_mounts_type):
        # total_mounts has really only 30 days, or what the user asked
        # for mounts
	if self.openfile:
	    self.openfile.close()
	self.openfile = open(self.ptsfile, 'w')
        total = write_mount_data(self.openfile, total_mounts)
	# don't need to close the file as it is closed by the caller

	# now create the gnuplot command file
	gnucmds = MpdGnuFile(self.gnufile)
	gnucmds.open('w')
	gnucmds.write(self.psfile, self.ptsfile, repr(total), self.mount_label)
	gnucmds.close()

        # now create the mounts/day/drive type plots
        self.files_to_install = {}
        for drive_type in total_mounts_type.keys():
            # do not generate plot if there is no data
            if total_mounts_type[drive_type]:
                self.files_to_install[drive_type] = MpdDriveTypeDataFile(self.dir, drive_type)
                self.files_to_install[drive_type].open()
                self.files_to_install[drive_type].plot(total_mounts_type[drive_type])
                self.files_to_install[drive_type].close()

    def install(self, html_dir):
        EnPlot.install(self, html_dir)
        for file in self.files_to_install.keys():
            self.files_to_install[file].install(html_dir)


class XferGnuFile(enstore_files.EnFile):

    def write(self, outfile1, outfile2, ptsfile1, ptsfile2,extra=None):
        long_string="set output '"+outfile2+"'\n"+ \
                     "set terminal postscript color solid\n"
        if ( extra == None ) :
            long_string = long_string + "set title 'Individual Transfer Activity (no null mvs)"
        else:
            long_string = long_string + "set title '"+extra+": Individual Transfer Activity (no null mvs)"
        long_string = long_string + plot_time()+"'\n"+ \
                      "set xlabel 'Date (year-month-day)'\n"+ \
                      "set timefmt \"%Y-%m-%d:%H:%M:%S\"\n"+ \
                      "set xdata time\n"+ \
                      "set xrange [ : ]\n"+ \
                      "set size 1.5,1\n"+ \
                      "set ylabel 'Bytes per Transfer'\n"+ \
                      "set grid\n"+ \
                      "set format x \"%y-%m-%d\"\n"+ \
                      "set logscale y\n"+ \
                      "plot '"+ptsfile1+\
                      "' using 1:4 t 'reads' with points pt 1 lw 1 lt 3, "+\
                      "'"+ptsfile1+\
                      "' using 1:3 t 'writes' with points pt 1 lw 1 lt 1\n"+\
                      "set output '"+outfile1+"'\n"+ \
                      "set pointsize 2\n"+ \
                      "set nologscale y\n"+ \
                      "set yrange [0: ]\n"+ \
                      "plot '"+ptsfile1+"' using 1:2 t '' w impulses, "+\
                      "'"+ptsfile2+\
                      "' using 1:7 t 'mean file size' w points 3 5\n"
        self.openfile.write(long_string)

class XferDataFile(EnPlot):

    def __init__(self, dir, bpdfile,sg=None):
	self.bpdfile = bpdfile
	EnPlot.__init__(self, dir, enstore_constants.XFER_FILE)
        self.sg = sg
        if ( self.sg != None ) :
            self.name = self.name+"_"+self.sg
            self.dir   = dir
            self.ptsfile    = self.file_name+"_"+self.sg+PTS
            self.tmpptsfile = self.file_name+"_"+self.sg+TMP+PTS
            self.psfile     = self.file_name+"_"+self.sg+enstore_constants.PS
            self.gnufile    = self.file_name+GNU



	self.logfile = "%s/%s%s"%(dir, enstore_constants.XFERLOG_FILE,
				  enstore_constants.PS)

    # make the file with the plot points in them
    def plot(self, data):
	# write out the data points
	for [xpt, ypt, type, mover, drive_id, sg ] in data:
            if ( self.sg == None or self.sg == sg ) :
                if type == WRITE:
                    # this was a write request
                    self.openfile.write("%s %s %s\n"%(xpt, ypt, ypt))
                else:
                    # this was a read request
                    self.openfile.write("%s %s 0 %s\n"%(xpt, ypt, ypt))

	# we must create our gnu plot command file too
	gnucmds = XferGnuFile(self.gnufile)
	gnucmds.open('w')
        gnucmds.write(self.psfile, self.logfile, self.ptsfile, self.bpdfile,self.sg)

	gnucmds.close()

    def install(self, dir):
        EnPlot.install(self, dir)
	os.system("cp %s %s"%(self.logfile, dir))
	convert_to_jpg(self.logfile, "%s/%s%s"%(dir, self.name,
						enstore_constants.LOG))


class BpdGnuFile(enstore_files.EnFile):

    def write(self, outfile, ptsfile, total, meansize, xfers, read_xfers, write_xfers,
	      total_written, lw=20):
	psfiler = string.replace(outfile, enstore_constants.BPD_FILE,
				 enstore_constants.BPD_FILE_R)
	psfilew = string.replace(outfile, enstore_constants.BPD_FILE,
				 enstore_constants.BPD_FILE_W)
	self.openfile.write("set output '"+outfile+"'\n"+ \
	                   "set terminal postscript color solid\n"+ \
	                   "set title 'Total Bytes Transferred Per Day (no null mvs) "+\
			    plot_time()+"'\n"+ \
	                   "set xlabel 'Date (year-month-day)'\n"+ \
	                   "set timefmt \"%Y-%m-%d\"\n"+ \
	                   "set xdata time\n"+ \
                           "set size 1.5,1\n"+ \
	                   "set xrange [ : ]\n"+ \
	                   "set ylabel 'Bytes'\n"+ \
	                   "set grid\n"+ \
	                   "set yrange [0: ]\n"+ \
	                   "set format x \"%y-%m-%d\"\n"+ \
			   "set key right top Right samplen 1 title \"Total Bytes : "+\
			      "%.2e"%(total,)+"\\nMean Xfer Size : "+
			      "%.2e"%(meansize,)+"\\n Number of Xfers : "+
			      repr(xfers)+"\"\n"+\
			   "plot '"+ptsfile+\
			   "' using 1:2 t 'reads' w impulses lw "+repr(lw)+" lt 3 , '"+ptsfile+\
			   "' using 1:4 t 'writes' w impulses lw "+repr(lw)+" lt 1 \n"+ \
			   #       "' using 1:4 t 'writes' w impulses lw 20 1 1\n"+
			   # "set output '"+psfiler+"'\n"+ \
			   # "set title 'Total Bytes Read Per Day (no null mvs) "+plot_time()+"'\n"+ \
			   # "set pointsize 2\n"+ \
			   # "set key right top Right samplen 1 title \"Total Bytes : "+\
			   #   "%.2e"%(total,)+"\\n Number of Xfers : "+\
			   #   repr(read_xfers)+"\"\n"+\
			   # "plot '"+ptsfile+"' using 1:2 t 'total' w points 4 7, '"+ptsfile+\
			   #       "' using 1:3 t 'reads' w impulses lw 20 1 1\n"+
			   "set output '"+psfilew+"'\n"+ \
			   "set title 'Total Bytes Written Per Day (no null mvs) "+plot_time()+"'\n"+ \
			   "set key right top Right samplen 1 title \"Total Bytes : "+\
			         "%.2e"%(total_written,)+"\\n Number of Xfers : "+\
			    repr(write_xfers)+"\"\n"+\
			   "plot '"+ptsfile+\
			    "' using 1:4  t '' w impulses lw "+repr(lw)+" lt 1 \n"
			    )


class BpdMoverGnuFile(enstore_files.EnFile):

    def write(self, mover, outfile, ptsfile, total, meansize, xfers, lw=20):
	self.openfile.write("set output '"+outfile+"'\n"+ \
	                   "set terminal postscript color solid\n"+ \
	                   "set title 'Total Bytes Transferred Per Day for %s "%(mover,)+ \
			    plot_time()+"'\n"+ \
	                   "set xlabel 'Date (year-month-day)'\n"+ \
	                   "set timefmt \"%Y-%m-%d\"\n"+ \
	                   "set xdata time\n"+ \
                           "set size 1.5,1\n"+ \
	                   "set xrange [ : ]\n"+ \
	                   "set ylabel 'Bytes'\n"+ \
	                   "set grid\n"+ \
	                   "set yrange [0: ]\n"+ \
	                   "set format x \"%y-%m-%d\"\n"+ \
			   "set key right top Right samplen 1 title \"Total Bytes : "+\
			      "%.2e"%(total,)+"\\nMean Xfer Size : "+
			      "%.2e"%(meansize,)+"\\n Number of Xfers : "+
			      repr(xfers)+"\"\n"+\
	                   "plot '"+ptsfile+\
			   "' using 1:2 t 'reads' w impulses lw "+repr(lw)+" lt 3 , '"+ptsfile+\
			   "' using 1:4 t 'writes' w impulses lw "+repr(lw)+" lt 1 \n"
			    )


class BpdMoverDataFile(EnPlot):

    def __init__(self, dir, mover):
	EnPlot.__init__(self, dir, "%s-%s"%(enstore_constants.BPD_FILE, mover))
	self.do_delete_ps = NO

    def cleanup(self, keep, pts_dir):
        if not keep:
            # delete the gnu command file
            os.system("rm %s"%(self.gnufile,))
	    if self.do_delete_ps == YES:
		os.system("rm %s"%(self.psfile,))
	else:
            if pts_dir:
                # move these files somewhere
                os.system("mv %s %s"%(self.gnufile, pts_dir))
		if self.do_delete_ps == YES:
		    os.system("mv %s %s"%(self.psfile, pts_dir))

    def install(self, dir, use_subdir):
	# see if there is a subdir to 'dir' that is bpd_per_mover.  if so, install
	# our files there. only do this if told to.
	if use_subdir:
	    new_dir = enstore_functions2.get_bpd_subdir(dir)
	else:
	    new_dir = dir
	EnPlot.install(self, new_dir)
	if not dir == new_dir:
	    # we moved the files to the sub dir so on cleanup, delete them from here
	    self.do_delete_ps = YES

class BpdDataFile(EnPlot):

    def __init__(self, dir):
	EnPlot.__init__(self, dir, enstore_constants.BPD_FILE)
	self.per_mover_files_d = {}
	self.movers_d = {}
	self.lw = 10

    def cleanup(self, keep, pts_dir):
        if not keep:
            # delete the gnu command file
            os.system("rm %s"%(self.gnufile,))
        else:
            if pts_dir:
                # move these files somewhere
                os.system("mv %s %s"%(self.gnufile, pts_dir))
	for mover in self.per_mover_files_d.keys():
	    self.per_mover_files_d[mover].cleanup(keep, pts_dir)

    # write out the files for each movers' bytes/day
    def per_mover(self):
	movers_l = self.movers_d.keys()
	dates = self.ndata.keys()
	dates.sort()
	for mover in movers_l:
	    mover_d = self.movers_d[mover]
	    # open a file to write the data to
	    openfile = BpdMoverDataFile(self.dir, mover)
	    openfile.open()
	    for day in dates:
		if mover_d.has_key(day):
		    # add the reads and the writes
		    total = mover_d[day][0] + mover_d[day][1]
		    openfile.write(day+" "+repr(total)+" "+\
				   repr(mover_d[day][0])+" "+\
				   repr(mover_d[day][1])+" "+"\n")
		else:
		    # no writes were done on this day for this mover
		    openfile.write(day+"\n")
	    openfile.close()
	    self.per_mover_files_d[mover] = openfile

    def sum_movers(self, mover_d):
	drive_id = mover_d[DRIVE_ID]
	if not self.movers_d.has_key(drive_id):
	    self.movers_d[drive_id] = {TOTAL : 0.0, CTR : 0, USE_SUBDIR : NO}
	drive_id_d = self.movers_d[drive_id]
	for date in mover_d.keys():
	    if date in [TOTAL, CTR, DRIVE_ID, USE_SUBDIR]:
		continue
	    # now we have only dates
	    if not drive_id_d.has_key(date):
		drive_id_d[date] = [0.0, 0.0]
	    drive_id_d[date][0] = drive_id_d[date][0] + mover_d[date][0]
	    drive_id_d[date][1] = drive_id_d[date][1] + mover_d[date][1]
	else:
	    drive_id_d[TOTAL] = drive_id_d[TOTAL] + mover_d[TOTAL]
	    drive_id_d[CTR] = drive_id_d[CTR] + mover_d[CTR]

    def sum_data(self, data):
	self.read_ctr = 0
	self.write_ctr = 0
	for [xpt, ypt, type, mover, drive_id, sg] in data:
	    adate = xpt[0:10]
            fypt = string.atof(ypt)
            try:
                day = self.ndata[adate]
            except(KeyError):
                continue

	    day[CTR] = day[CTR] + 1
	    if fypt > day[LARGEST]:
		day[LARGEST] = fypt
	    if day[SMALLEST] == -1:
		day[SMALLEST] = fypt
	    else:
		if fypt < day[SMALLEST]:
		    day[SMALLEST] = fypt
	    day[TOTAL] = day[TOTAL] + fypt

	    # save the data for the movers' bytes/day plots too
	    if self.movers_d.has_key(mover):
		if not self.movers_d[mover].has_key(adate):
		    self.movers_d[mover][adate] = [0.0, 0.0]
	    else:
		# translate the drive_id if necessary
		drive_id_lcl = translate_drive_id(drive_id)
		self.movers_d[mover] = {adate : [0.0, 0.0],
					TOTAL : 0.0, CTR : 0,
					DRIVE_ID : drive_id_lcl,
					USE_SUBDIR : YES}

	    if type == WRITE:
		day[WRITES] = day[WRITES] + fypt
		self.movers_d[mover][adate][1] = self.movers_d[mover][adate][1] + fypt
		self.write_ctr = self.write_ctr + 1
	    else:
		day[READS] = day[READS] + fypt
		self.movers_d[mover][adate][0] = self.movers_d[mover][adate][0] + fypt
		self.read_ctr = self.read_ctr + 1
	    self.movers_d[mover][TOTAL] = self.movers_d[mover][TOTAL] + fypt
	    self.movers_d[mover][CTR] = self.movers_d[mover][CTR] + 1
	else:
	    # now sum the data based on mover type
	    for mover in self.movers_d.keys():
		self.sum_movers(self.movers_d[mover])

    def get_all_bpd(self):
	if self.openfile:
	    bpd_l = self.openfile.readlines()
	    for line in bpd_l:
		fields = string.split(string.strip(line))
		if not self.ndata.has_key(fields[0]):
		    # we do not have data for this day yet
		    if len(fields) > 1:
			self.ndata[fields[0]] = { TOTAL : float(fields[1]),
						  READS : float(fields[2]),
						  WRITES : float(fields[3]),
						  SMALLEST : float(fields[4]),
						  LARGEST : float(fields[5]),
						  CTR : get_ctr(fields) }
		    else:
			self.ndata[fields[0]] = {TOTAL : 0,
						 READS : 0,
						 WRITES : 0,
						 SMALLEST : 0,
						 LARGEST : 0,
						 CTR : 0 }

    def open(self):
	if os.path.isfile(self.ptsfile):
	    EnPlot.open(self, 'r')

    # make the file with the bytes per day format, first we must sum the data
    # that we have based on the day
    def plot(self, data):
	# initialize the new data hash
	self.ndata = init_date_hash(data[0][0], data[len(data)-1][0])
	# sum the data together based on day boundaries. also save the largest
	# smallest and average sizes and sum up reads and writes separately
	self.sum_data(data)
	# merge the old data in with the new data
	self.get_all_bpd()
	# write out the data points, open file for write, not read
	if self.openfile:
	    self.openfile.close()
	EnPlot.open(self, 'w')
	keys = self.ndata.keys()
	keys.sort()
	numxfers = 0
	total = 0.0
	total_written = 0.0
	for key in keys:
	    day = self.ndata[key]
	    if not day[TOTAL] == 0:
	        self.openfile.write(key+" "+repr(day[TOTAL])+" "+\
				    repr(day[READS])+" "+\
				    repr(day[WRITES])+" "+\
				    repr(day[SMALLEST])+" "+\
				    repr(day[LARGEST])+" "+\
				    repr(day[TOTAL]/day[CTR])+"\n")
	    else:
		# all data is 0
	        self.openfile.write(key+"\n")
	    # now find the total bytes transferred over all days and the mean
	    # size of all transfers.
	    total = total + day[TOTAL]
	    total_written = total_written + day[WRITES]
	    # there may not be any transfers on a certain date, so check the key
	    # first.  above ndata has all dates initialized to 0 so no check is
	    # necessary.
	    numxfers = numxfers + day[CTR]

	# we must create our gnu plot command file too
	gnucmds = BpdGnuFile(self.gnufile)
	gnucmds.open('w')
	gnucmds.write(self.psfile, self.ptsfile, total, total/numxfers, numxfers,
		      self.read_ctr, self.write_ctr, total_written, self.lw)
	gnucmds.close()

	# now output the data to make the bytes/day/mover plots
	self.per_mover()
	for mover in self.movers_d.keys():
	    mover_d = self.movers_d[mover]
	    datafile = self.per_mover_files_d[mover]
	    gnucmds = BpdMoverGnuFile(datafile.gnufile)
	    gnucmds.open('w')
	    gnucmds.write(mover, datafile.psfile, datafile.ptsfile, mover_d[TOTAL],
			  mover_d[TOTAL]/mover_d[CTR], mover_d[CTR])
	    gnucmds.close()

    def install(self, dir):
	EnPlot.install(self, dir)

	# now make the plots for bytes/day/mover
	for mover in self.per_mover_files_d.keys():
            Trace.trace(enstore_constants.PLOTTING,
                        "installing %s in %s"%(self.per_mover_files_d[mover].name,
                                               dir,))
	    self.per_mover_files_d[mover].install(dir,
                                                  self.movers_d[mover][USE_SUBDIR])

	#filer = string.replace(self.name, enstore_constants.BPD_FILE,
	#		       enstore_constants.BPD_FILE_R)
	#psfiler = "%s/%s%s"%(self.dir, filer, enstore_constants.PS)
	filew = string.replace(self.name, enstore_constants.BPD_FILE,
			       enstore_constants.BPD_FILE_W)
	psfilew = "%s/%s%s"%(self.dir, filew, enstore_constants.PS)
	os.system("cp %s %s"%(psfilew, dir))
	#os.system("cp %s %s %s"%(psfiler, psfilew, dir))
	#convert_to_jpg(psfiler, "%s/%s"%(dir, filer))
	convert_to_jpg(psfilew, "%s/%s"%(dir, filew))


class BpdMonthDataFile(EnPlot):

    def __init__(self, dir):
	EnPlot.__init__(self, dir, enstore_constants.BPD_MONTH_FILE)

    def cleanup(self, keep, pts_dir):
        if not keep:
            # delete the gnu command file
            os.system("rm %s"%(self.gnufile,))
        else:
            if pts_dir:
                # move these files somewhere
                os.system("mv %s %s"%(self.gnufile, pts_dir))

    def open(self):
	if os.path.isfile(self.ptsfile):
	    EnPlot.open(self, 'r')

    def get_30_bpd(self):
	bpdfile = BpdDataFile(self.dir)
	bpdfile.open()
	bpd_l = bpdfile.openfile.readlines()
	bpdfile.openfile.close()
	bpd_l.sort()
	return bpd_l[-30:]

    def get_total_bpd(self, bpd_l):
	total_bpd = 0.0
	num_xfers = 0
	total_w_bpd = 0.0
	for bpd in bpd_l:
	    fields = string.split(string.strip(bpd))
            if len(fields) > 1:
                total_bpd = total_bpd + float(fields[1])
                num_xfers = num_xfers + long(float(fields[1])/float(fields[6]))
                total_w_bpd = total_w_bpd + float(fields[3])
	return total_bpd, num_xfers, total_w_bpd

    # make the bytes per day plot file
    def plot(self, write_ctr):
	# we must plot the last 30 days of  data in the total bpd file
	bpd_l = self.get_30_bpd()
	total_bpd, num_xfers, total_w_bpd = self.get_total_bpd(bpd_l)
	if self.openfile:
	    self.openfile.close()
	self.openfile = open(self.ptsfile, 'w')
	for line in bpd_l:
	    self.openfile.write(line)
	# don't need to close the file as it is closed by the caller

	# now create the gnuplot command file
	gnucmds = BpdGnuFile(self.gnufile)
	gnucmds.open('w')
	gnucmds.write(self.psfile, self.ptsfile, total_bpd, total_bpd/num_xfers,
		      num_xfers, 0, write_ctr, total_w_bpd)
	gnucmds.close()


    def install(self, dir):
	EnPlot.install(self, dir)

	filew = string.replace(self.name, enstore_constants.BPD_MONTH_FILE,
			       enstore_constants.BPD_MONTH_FILE_W)
	psfilew = "%s/%s%s"%(self.dir, filew, enstore_constants.PS)
	os.system("cp %s %s"%(psfilew, dir))
	convert_to_jpg(psfilew, "%s/%s"%(dir, filew))


class SgGnuFile(enstore_files.EnFile):

    def write(self, outfile, ptsfiles):
	plot_command = "plot "
	for ptsfile in ptsfiles:
	    plot_command = plot_command + "'%s' using 1:2 t '%s(%s)' with points,"%(ptsfile[0],
										    ptsfile[1],
										    ptsfile[2])
	else:
	    # remove the final ','
	    plot_command = plot_command[0:-1]
	self.openfile.write("set output '"+outfile+"\n"+ \
			    "set terminal postscript color solid\n"+ \
			    "set title 'Pending->Active Jobs By Storage Group "+\
			    plot_time()+"'\n"+ \
			    "set xlabel 'Date (year-month-day  hour)'\n"+ \
			    "set timefmt \"%Y-%m-%d:%H:%M:%S\"\n"+ \
			    "set xdata time\n"+ \
                            "set size 1.5,1\n"+ \
			    "set xrange [ : ]\n"+ \
                            "set yrange [0: ]\n"+ \
			    "set ylabel 'Storage Group'\n"+ \
			    "set grid\n"+ \
			    "set key outside\n" + \
			    "set format x \"%y-%m-%d\\n%H\"\n"+ \
			    plot_command)


class SgDataFile(EnPlot):

    def __init__(self, dir):
	EnPlot.__init__(self, dir, enstore_constants.SG_FILE)
	self.ptsfiles = []

    def plot(self, data):
	sgs = data.keys()
	sgs.sort()
	num_sgs = len(sgs)
	index = num_sgs
	decr = 1
	offset = .3
	# create the data files
	for sg in sgs:
            if sg is None : continue
	    self.ptsfile = "%s%s"%(sg, PTS)
	    self.ptsfiles.append([self.ptsfile, sg, index])
	    self.open()
	    for point in data[sg]:
		if point[1] is None:
		    # this is a wam operation
		    pt = index + offset
		else:
		    pt = index
		self.openfile.write("%s  %s\n"%(point[0], pt))
	    else:
		self.openfile.close()
		index = index - decr
	else:
	    # now creat the gnu command file
	    gnucmds = SgGnuFile(self.gnufile)
	    gnucmds.open('w')
	    gnucmds.write(self.psfile, self.ptsfiles)
	    gnucmds.close()


class TotalBpdGnuFile(enstore_files.EnFile):

    def write(self, outfile, ptsfile, total, meansize, xfers, max_nodes, total_written,
	      lw=20):
	psfilew = string.replace(outfile, enstore_constants.TOTAL_BPD_FILE,
				 enstore_constants.TOTAL_BPD_FILE_W)
	self.openfile.write("set output '"+outfile+"'\n"+ \
	                   "set terminal postscript color solid\n"+ \
	                   "set title 'Total Bytes Transferred Per Day By Enstore "+\
			    plot_time()+"'\n"+ \
	                   "set xlabel 'Date (year-month-day)'\n"+ \
	                   "set timefmt \"%Y-%m-%d\"\n"+ \
	                   "set xdata time\n"+ \
                           "set size 1.5,1\n"+ \
	                   "set xrange [ : ]\n"+ \
	                   "set ylabel 'Bytes'\n"+ \
	                   "set grid\n"+ \
	                   "set yrange [0: ]\n"+ \
	                   "set format x \"%y-%m-%d\"\n"+ \
			   "set key right top Right samplen 1 title \"Total Bytes : "+\
			      "%.2e"%(total,)+"\\nMean Xfer Size : "+
			      "%.2e"%(meansize,)+"\\n Number of Xfers : "+
			      repr(xfers)+"\"\n")
	len_max_nodes = len(max_nodes)
	if len_max_nodes > 0:
	    self.openfile.write("plot '%s' using 1:2 t '%s' w impulses lw %s lt 1 "%(ptsfile,
									    max_nodes[0],
									    lw))
	    color = 3
	    column = 3
	    for node in max_nodes[1:]:
		self.openfile.write(", '%s' using 1:%s t '%s' w impulses lw %s lt %s "%(ptsfile,
										    column,
										    node, lw,
										    color))
		color = color + 2
		column = column + 1
	    # now plot the total bytes written
	    self.openfile.write("\nset output '"+psfilew+"'\n"+ \
				"set title 'Total Bytes Written Per Day By Enstore "+\
				plot_time()+"'\n"+ \
				"set key right top Right samplen 1 title \"Total Bytes : "+\
				"%.2e"%(total_written,)+"\"\n")
	    self.openfile.write("plot '%s' using 1:5 t '%s' w impulses lw %s lt 1 "%(ptsfile,
									    max_nodes[0],
									    lw))
	    color = 3
	    column = 6
	    for node in max_nodes[1:]:
		self.openfile.write(", '%s' using 1:%s t '%s' w impulses lw %s lt %s "%(ptsfile,
										    column,
										    node, lw,
										    color))
		color = color + 2
		column = column + 1
	    self.openfile.write("\n")


class TotalBpdDataFile(EnPlot):

    def __init__(self, dir):
	EnPlot.__init__(self, dir, enstore_constants.TOTAL_BPD_FILE)
	self.lw = 20

    # make the file with the bytes per day format, first we must sum the data
    # that we have based on the day.  we will also make the plot of total writes/day
    def plot(self, data_d):
	keys = data_d.keys()
	keys.sort()
	numxfers = 0
	total = 0.0
	total_writes = 0.0
	max_nodes = []
	# first
	for key in keys:
	    day = data_d[key]
	    nodes = day.keys()
	    # keep track of the max  nodes we will be plotting. each node
	    # corresponds to a column in the gnuplot file
	    nodes.sort()
	    if len(nodes) > len(max_nodes):
		max_nodes = nodes
	    total_l = []
	    total_w_l = []
	    i = 0
	    total_so_far = 0.0
	    total_writes_so_far = 0.0
	    for node in nodes:
		t = total_so_far + day[node][TOTAL]
		t_w = total_writes_so_far + day[node][WRITES]
		total_l.append(t)
		total_w_l.append(t_w)
		total_so_far = t
		total_writes_so_far = t_w
		i = i + 1
	        numxfers = numxfers + day[node][CTR]
	    else:
		# keep a running total of everything
		total = total + total_so_far
		total_writes = total_writes + total_writes_so_far

	    # write out the data
	    if not total_so_far == 0.0:
		line = "%s "%(key,)
		# output the largest first as we will plot that column first
		total_l.reverse()
		for amt in total_l:
		    line = "%s %s"%(line, amt)
		else:
		    total_w_l.reverse()
		    for amt in total_w_l:
			line = "%s %s"%(line, amt)
		    else:
			self.openfile.write("%s\n"%(line,))
	    else:
		# all data is 0
	        self.openfile.write("%s\n"%(key,))

	# we must create our gnu plot command file too
	gnucmds = TotalBpdGnuFile(self.gnufile)
	gnucmds.open('w')
	# reverse the order of the nodes as we did the columns we wrote to the
	# data file
	max_nodes.reverse()
	gnucmds.write(self.psfile, self.ptsfile, total, total/numxfers,
		      numxfers, max_nodes, total_writes, self.lw)
	gnucmds.close()

    def install(self, dir):
	EnPlot.install(self, dir)

	filew = string.replace(self.name, enstore_constants.TOTAL_BPD_FILE,
			       enstore_constants.TOTAL_BPD_FILE_W)
	psfilew = "%s/%s%s"%(self.dir, filew, enstore_constants.PS)
	os.system("cp %s %s"%(psfilew, dir))
	convert_to_jpg(psfilew, "%s/%s"%(dir, filew))

