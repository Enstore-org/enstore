#
# Classes to create the enstore plots.
#
##############################################################################
# system import

# enstore imports
import enstore_status

START_TIME = "start_time"
STOP_TIME = "stop_time"

DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
HOURS_IN_DAY = ["00", "01", "02", "03", "04", "05", "06", "07", "08", \
                "09", "10", "11", "12", "13", "14", "15", "16", \
                "17", "18", "19", "20", "21", "22", "23"]

class EnPlot(enstore_status.EnFile):

    def close(self):
	Trace.trace(10,"{enplot close "+self.file_name)
	self.close()
	# now move the file to the html area
	if 
	Trace.trace(10,"}enplot close ")

class gnuFile(EnPlot):

    def __init__(self, file_name):
	Trace.trace(10,"{gnuplot init "+file_name)
	enstore_status.EnFile.__init__(self, file_name)
	# we need to check if the file exists.  if not create it.
	Trace.trace(10,"}gnuplot init ")

    def close(self):
	Trace.trace(10,"{enplot close "+self.file_name)
	self.close()
	Trace.trace(10,"}enplot close ")

class mlatPlot(EnPlot):



class InquisitorMethods:

    suffix = ".new"

    # check the line to see if the date and timestamp on the beginning of it
    # is between the given start and end values
    def check_line(self, line, start_time, stop_time):
	# split the line into the date/time and all the rest
	[datetime, rest] = string.split(line, None, 1)
	# remove the beginning LOG_PREFIX
	l = regsub.gsub(LOG_PREFIX, "", datetime)
	# now see if the date/time is between the start time and the end time
	time_ok = TRUE
	if not start_time == "":
	    if l < start_time:
	        time_ok = FALSE
	if time_ok and (not stop_time == ""):
	    if l > stop_time:
	        time_ok = FALSE
	return time_ok

    # read in the given file and return a list of lines that are between a
    # given start and end time
    def extract_lines(self, filename, ticket):
	do_all = FALSE
	matched_lines = []
	if ticket.has_key(START_TIME):
	    start_time = ticket[START_TIME]
	else:
	    start_time = ""
	if ticket.has_key(STOP_TIME):
	    stop_time = ticket[STOP_TIME]
	else:
	    stop_time = ""
	    if start_time == "":
	        do_all = TRUE
	# open the file and read it in.  only save the lines that match the
	# desired time frame
	try:
	    theFile = open(filename, 'r')
	    while TRUE:
	        line = theFile.readline()
	        if not line:
	            break
	        else:
	            if do_all or self.check_line(line, start_time, stop_time):
	                matched_lines.append(line)
	except:
	    pass
	return matched_lines

    # make the mount plots (mounts per hour and mount latency
    def mount_plot(self, ticket, lfd):
        Trace.trace(11,"{mount_plot "+repr(ticket))
	ofn = lfd+"/mount_lines.txt"

	# always add /dev/null to the end of the list of files to search thru 
	# so that grep always has > 1 file and will always print the name of 
	# the file at the beginning of the line.
	mountfile = enstore_status.EnDataFile(LOG_PREFIX+"* /dev/null",\
	                                      ofn, "edia changer load", lfd)

	# only extract the information from the newly created file that is
	# within the requested timeframe.
	lines = self.extract_lines(ofn, ticket)
	self.enprint("Lines found to plot = "+repr(len(lines)), 
	             generic_cs.SERVER, self.verbose)
	# now pull out the info we are going to plot from the lines
	data = []
	for line in lines:
	    minfo = enstore_status.parse_mount_line(line)
	    # the time info may contain the file directory which we must
	    # strip off
	    self.strip_file_dir(minfo)
	    data.append([minfo[enstore_status.MDEV],
                        string.replace(minfo[enstore_status.ETIME], \
	                LOG_PREFIX, ""), minfo[enstore_status.MSTART]])
	mph_file = lfd+"/mph.pts"
	mlat_file = lfd+"/mlat.pts"
	gnu_file = lfd+"/mph.gnuplot"
	if not len(data) == 0:
	    self.make_mph_plot_files(mph_file, data, gnu_file)
	    self.gnuplot(lfd, gnu_file, mph_file, self.html_dir)
	    self.make_mlat_plot_file(mlat_file, data)
	    self.gnuplot(lfd, lfd+"/mlat.gnuplot", mlat_file, self.html_dir)
        Trace.trace(11,"}mount_plot ")

    # make the total transfers per unit of time and the bytes moved per day
    # plot
    def encp_plot(self, ticket, lfd):
        Trace.trace(11,"{encp_plot "+repr(ticket))
	ofn = lfd+"/bytes_moved.txt"

	# always add /dev/null to the end of the list of files to search thru 
	# so that grep always has > 1 file and will always print the name of 
	# the file at the beginning of the line.
	encpfile = enstore_status.EnDataFile(LOG_PREFIX+"* /dev/null",\
	                                     ofn, "ENCP", lfd)

	# only extract the information from the newly created file that is
	# within the requested timeframe.
	lines = self.extract_lines(ofn, ticket)
	self.enprint("Lines found to plot = "+repr(len(lines)), 
	             generic_cs.SERVER, self.verbose)
	# now pull out the info we are going to plot from the lines
	data = []
	for line in lines:
	    einfo = enstore_status.parse_encp_line(line)
	    if einfo[enstore_status.ESTATUS] == \
	       log_client.sevdict[log_client.INFO]:
	        # the time info may contain the file directory which we must
	        # strip off
	        self.strip_file_dir(einfo)
	        data.append([string.replace(einfo[enstore_status.ETIME], \
	                     LOG_PREFIX, ""), einfo[enstore_status.EBYTES]])
	pts_file = lfd+"/bpt.pts"
	bpd_file = lfd+"/bytes.pts"
	if not len(data) == 0:
	    self.make_xfer_plot_file(pts_file, data)
	    # we really created 2 files in the above so need to cp both
	    pts_file = pts_file+"*"
	    self.gnuplot(lfd, lfd+"/bpt.gnuplot", pts_file, self.html_dir)
	    self.make_bytes_per_day_plot_file(bpd_file, data)
	    self.gnuplot(lfd, lfd+"/bytes.gnuplot", bpd_file, self.html_dir)
        Trace.trace(11,"}encp_plot ")

    # parse out the file directory , a remnant from the grep in the time field
    def strip_file_dir(self, minfo):
        ind = string.rfind(minfo[enstore_status.ETIME], "/")
	if not ind == -1:
	    minfo[enstore_status.ETIME] = \
	                         minfo[enstore_status.ETIME][(ind+1):]

    # mount data (mount requests and the actual mounting) are not necessarily
    # time ordered when read from the files.  2 or more mount requests may
    # occur before any actual volume has been mounted.  also, since log files
    # are closed/opened on day boundaries with no respect as to whats going on
    # in the system, a log file may begin with several mount satisfied messages
    # which have no matching requests in the file.  also the file may end with
    # several requests that are not satisifed until the next day or the next
    # log file. to make things simpler for us to plot, the data will be 
    # ordered so that each mount request is immediately followed by the actual
    # mount satisfied message.
    def order_mount_data(self, data):
	# order data so that all the requests for each device are grouped
	# together and in addition are time ordered
	data.sort()

    # run gnuplot on the file to create a plot
    def gnuplot(self, dir, cmds, pts_file, output_dir):
	os.system("cd "+dir+"; gnuplot "+cmds+";cp "+pts_file+".ps "+\
	          output_dir)

    # make the mounts per hour plot file
    def make_mph_plot_files(self, filename, data, gnu_file):
	Trace.trace(12,"{make_mph_plot_file ")
	# sum the data together based on hour boundaries. we will only plot 1
	# day per plot though.
	date_only = {}
	ndata = {}
	for [dev, time, strt] in data:
	    if strt == enstore_status.MMOUNT:
	        # this was the record of the mount having been done
	        adate = time[0:13]
	        date_only[time[0:10]] = 0
	        try:
	            ndata[adate] = ndata[adate] + 1
	        except:
	            ndata[adate] = 1
	# open the file for each day and write out the data points
	days = date_only.keys()
	days.sort()
	gfile = 0
	for day in days:
	    # we will only create one plot file, so output the first bit of
	    # info to it only once
	    fn = filename+"."+day
	    if gfile == 0:
	        gfile = open(gnu_file, 'w')
	        gfile.write("set output '"+filename+".ps'\nset terminal postscript color\nset xlabel 'Hour'\nset yrange [0 : ]\nset xrange [ : ]\nset ylabel 'Mounts'\nset grid\n")

	    pfile = open(fn, 'w')
	    total = 0
	    for hour in HOURS_IN_DAY:
	        tm = day+":"+hour
	        try:
	            pfile.write(hour+" "+repr(ndata[tm])+"\n")
	            total = total + ndata[tm]
	        except:
	            pfile.write(hour+" 0\n")
	    else:
	        # now we must make the gnuplot file with instructions to
	        # process this file in particular
	        gfile.write("set title 'Mount Count For "+day+" (Total = "+repr(total)+")'\nplot '"+fn+"' using 1:2 t '' with boxes\n")
	        pfile.close()
	else:
	    gfile.close()
	Trace.trace(12,"}make_mph_plot_file ")

    # make the mount latency plot file
    def make_mlat_plot_file(self, filename, data):
	Trace.trace(12,"{make_mlat_plot_file ")
	self.order_mount_data(data)
	last_mount_req = ""
	# open the file and write out the data points
	pfile = open(filename, 'w')
	for [dev, time, strt] in data:
	    if strt == enstore_status.MMOUNT:
	        # this was the record of the mount having been done
	        if not last_mount_req == "":
	            # we have recorded a mount req 
	            ltnc = self.latency(last_mount_req, time)
	            pfile.write(time+" "+repr(ltnc)+"\n")

	            # initialize so any trailing requests are not plotted
	            last_mount_req == ""
	    else:
	        # this was the mount request
	        last_mount_req = time
	else:
	    pfile.close()
	Trace.trace(12,"}make_mlat_plot_file ")

    # subtract two times and return their difference
    def latency(self, time1, time2):
	# first convert each time into a tuple of the form -
	#  (year, month, day, hour, minutes, seconds, 0, 0, -1)
	# then convert the tuple into a seconds value and subtract the
	# two to get the latency in seconds
	t1 = (string.atoi(time1[0:4]), string.atoi(time1[5:7]), \
	      string.atoi(time1[8:10]), string.atoi(time1[11:13]), \
	      string.atoi(time1[14:16]), string.atoi(time1[17:]), 0, 0, -1)
	t2 = (string.atoi(time2[0:4]), string.atoi(time2[5:7]), \
	      string.atoi(time2[8:10]), string.atoi(time2[11:13]), \
	      string.atoi(time2[14:16]), string.atoi(time2[17:]), 0, 0, -1)
	return (time.mktime(t2) - time.mktime(t1))

    # make the file with the plot points in them
    def make_xfer_plot_file(self, filename, data):
	Trace.trace(12,"{make_xfer_plot_file ")
	# open the file and write out the data points
	pfile = open(filename, 'w')
	if len(data[0]) == 2:
	    for [xpt, ypt] in data:
	        pfile.write(xpt+" "+ypt+"\n")
	elif len(data[0]) == 3:
	    for [xpt, ypt, zpt] in data:
	        pfile.write(xpt+" "+ypt+" "+zpt+"\n")
	pfile.close()
	Trace.trace(12,"}make_xfer_plot_file ")

    # make the file with the bytes per day format, first we must sum the data
    # that we have based on the day
    def make_bytes_per_day_plot_file(self, filename, data):
	Trace.trace(12,"{make_bytes_per_day_plot_file ")
	# initialize the new data hash
	ndata = self.init_date_hash(data[0][0], data[len(data)-1][0])
	# sum the data together based on day boundaries. also save the largest
	# smallest and average sizes
	mean = {}
	smallest = {}
	largest = {}
	ctr = {}
	for [xpt, ypt] in data:
	    adate = xpt[0:10]
	    fypt = string.atof(ypt)
	    if mean.has_key(adate):
	        mean[adate] = mean[adate] + fypt
	        ctr[adate] = ctr[adate] + 1
	    else:
	        mean[adate] = fypt
	        ctr[adate] = 1
	    if largest.has_key(adate):
	        if fypt > largest[adate]:
	            largest[adate] = fypt
	    else:
	        largest[adate] = fypt
	    if smallest.has_key(adate):
	        if fypt < smallest[adate]:
	            smallest[adate] = fypt
	    else:
	        smallest[adate] = fypt
	    ndata[adate] = ndata[adate] + fypt
	# open the file and write out the data points
	pfile = open(filename, 'w')
	keys = ndata.keys()
	keys.sort()
	for key in keys:
	    if not ndata[key] == 0:
	        pfile.write(key+" "+repr(ndata[key])+" "+\
	                            repr(smallest[key])+" "+\
	                            repr(largest[key])+" "+\
	                            repr(mean[key]/ctr[key])+"\n")
	    else:
	        pfile.write(key+" "+repr(ndata[key])+"\n")
	pfile.close()
	Trace.trace(12,"}make_bytes_per_day_plot_file ")

    # init the following hash from the first date given to the last date
    def init_date_hash(self, sdate, edate):
	Trace.trace(12,"{init_date_hash "+sdate+" "+edate)
	ndate = {}
	ndate[sdate[0:10]] = 0.0
	ndate[edate[0:10]] = 0.0
	imon = string.atoi(sdate[5:7])
	iday = string.atoi(sdate[8:10])
	iyr = string.atoi(sdate[0:4])
	emon = string.atoi(edate[5:7])
	eday = string.atoi(edate[8:10])
	while 1:
	    if (imon == emon) and (iday == eday):
	        break
	    iday = iday + 1
	    if iday <= DAYS_IN_MONTH[imon-1]:
	        tmp = "%i-%02i-%02i" % (iyr, imon, iday)
	        ndate[tmp] = 0.0
	        continue
	    else:
	        imon = imon + 1
	        iday = 0
	        if imon > 12:
	            imon = 1
	            iyr = iyr + 1
	Trace.trace(12,"}init_date_hash ")
	return ndate
