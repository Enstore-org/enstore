#!/usr/bin/env python

import sys
import string
import os
import tempfile

import configuration_client


#Conversion from "bytes per 15 seconds" to "terabytes per day"
BP15S_TO_TBPD = 5.7e-09

def print_usage():
    print "Usage:", sys.argv[0],
    print "[-t <num>] [-n <name>] [--help]"
    print "   -t     number of 15 second intervals smoothed.  (default=40)"
    print "   -n     name of the system to plot." \
          "  (default=$ENSTORE_CONFIG_HOST)"
    print "  --help  print this message"
    print "See configuration dictionary entry \"ratekeeper\" for defaults."

#############################################################################
#############################################################################
#This is the "magic" class to use when filtering out elements that have the
# same external label in a list.
class file_filter:
    def __call__(self, list_element):
        #find() return -1 on failure and 0 through n for success.  By adding
        # one to this return value the value for failure becomes 0 and
        # success is 1 through (n + 1).  Thus we get what we want.
        return string.find(list_element, self.compare_value) + 1
    def __init__(self, thresh):
        self.compare_value = thresh


############################################################################
############################################################################


#Function to recover the directory that the rate log files are in.
def get_rate_info():
    #Get the configuration from the configuration server.
    csc = configuration_client.ConfigurationClient()
    ratekeep = csc.get('ratekeeper', timeout=15, retry=3)
    
    dir  = ratekeep.get('dir', 'MISSING')
    tmp_dir  = ratekeep.get('tmp', 'MISSING')
    ratekeeper_name = ratekeep.get('name','MISSING')
    rate_nodes = ratekeep.get('nodes','MISSING')
    gif_filename = ratekeep.get('gif','MISSING')
    
    if dir == 'MISSING':
        print "Unable to determine the log directory."
        sys.exit(1)
    if tmp_dir == 'MISSING':
        tmp_dir = None
    if ratekeeper_name == 'MISSING' or ratekeeper_name == "":
        ratekeeper_name = os.environ.get("ENSTORE_CONFIG_HOST")
    if rate_nodes == 'MISSING':
        rate_nodes = ''
    if gif_filename == 'MISSING':
        print "Unable to determine the gif output filename."
        sys.exit(1)

    if dir[-1] != "/":
        dir = dir + "/"
    if tmp_dir[-1] != "/":
        tmp_dir = tmp_dir + "/"

    return dir, tmp_dir, ratekeeper_name, rate_nodes, gif_filename

###########################################################################
###########################################################################

#Use the file_filter class to filter out the files with rate data that we
# are looking for.  sys_name is the name to look for inside the filenames,
# and log_dir is the directory to look in.
def filter_out_files(sys_name, log_dir):
    all_log_files = os.listdir(log_dir)

    #criteria is a callable class that is used inside of filter for pulling
    # specific filenames out of a list.
    criteria = file_filter(".RATES.")
    rate_log_files = filter(criteria, all_log_files)

    criteria.compare_value = sys_name
    sys_name_rate_log_files = filter(criteria, rate_log_files)

    return sys_name_rate_log_files

##########################################################################
##########################################################################

#Take all of the data in all of the files (that we care about), and merge
# them together.  At the same time convert the data from bytes per 15 seconds
# to terabytes per day.
#rate_log_files is the list of files and scaled_filename is the file that
# the merged data is written to.
def write_scale_file(log_dir, rate_log_files, scaled_file):
    for file in rate_log_files:
        in_file = open(log_dir + file, "r")
        while 1:
            line = in_file.readline()
            if not line:
                break
            date, time, read, write = string.split(line)
            scaled_file.write("%s %s %s %s\n" %
                            (date, time, BP15S_TO_TBPD * float(read),
                             BP15S_TO_TBPD * float(write)))


#Take the file writen out in write_scale_file and smooth the data.
# smooth_filename = file to write out
# scaled_filename = file to read in
# smooth_num = number of points that get smoothed into one.
def write_smooth_file(smooth_file, scaled_file, smooth_num = 40):
    n = smooth_num
    tr, tw = 0.0, 0.0
    c = 0

    while 1:
        line = string.strip(scaled_file.readline())
        if not line:
            break
        date, time, r, w = string.split(line)
        tr = tr + float(r)
        tw = tw + float(w)
        c = (c+1)%n
        if not c:
            smooth_file.write("%s %s %s %s\n" % (date, time, tr/n, tw/n))
            tr, tw = 0.0, 0.0



#Write out the file that gnuplot will use to plot the data.
# sys_name = The node name that will be the plot title.
# smooth_file = the file that contains the data to be ploted.
# plot_file = the file that will be read in by gnuplot.
def write_plot_file(sys_name, smooth_filename, plot_file, graphic_filename):

    plot_file.write("set title \"%s\"\n" % (sys_name))
    plot_file.write("set ylabel \"Terabytes/day\"\n")
    plot_file.write("set xdata time\n")
    plot_file.write("set timefmt \"%s\"\n" % ("%m-%d-%Y %H:%M:%S"))
    plot_file.write("set format x \"%s\"\n" % ("%m-%d-%Y\\n%H:%M:%S"))
    plot_file.write("set grid ytics\n")
    plot_file.write("set terminal pbm small color\n")
    plot_file.write("set size 1.4,1.2\n")
    plot_file.write("set output \"%s\"\n" % graphic_filename)
    plot_file.write("plot \"%s\" using 1:3 title \"read\" with lines," \
                    "\"%s\" using 1:4 title \"write\" with lines\n" %
                    (smooth_filename, smooth_filename))

##########################################################################
##########################################################################

if __name__ == "__main__":
    if "--help" in sys.argv:
        print_usage()
        sys.exit(1)
    
    log_dir, tmp_dir, sys_name, rate_nodes, gif_filename = get_rate_info()

    #Check the command line arguments.
    #-t stands for Time smoothing, which is the number of points that get
    # averaged together.
    if "-t" in sys.argv:
        smooth_num = int(sys.argv[sys.argv.index("-t") + 1])
    else:
        smooth_num = 40
    #-n stands for Name, which is the name of the system to plot.
    if "-n" in sys.argv:
        sys_name = sys.argv[sys.argv.index("-n") + 1]
        
        if rate_nodes != 'MISSING':
            for short_name in rate_nodes.keys():
                if sys_name[:len(short_name)] == short_name:
                    sys_name = rate_nodes[short_name][1]
                    break

    #If they put an * in the filename, replace it with the system name.
    gif_filename = string.replace(gif_filename, '*', sys_name)

    rate_log_files = filter_out_files(sys_name, log_dir)

    if not rate_log_files:
        print "No %s rate files found." % (sys_name)
        sys.exit(1)

    #Create temporary files.
    tempfile.tempdir = tmp_dir
    scaled_filename = tempfile.mktemp(".plot")
    smooth_filename = tempfile.mktemp(".plot")
    plot_filename = tempfile.mktemp(".plot")
    graphic_filename = tempfile.mktemp(".ppm")

    #Open these files for writing.
    scaled_file = open(scaled_filename, "w")
    smooth_file = open(smooth_filename, "w")
    plot_file = open(plot_filename, "w")

    write_scale_file(log_dir, rate_log_files, scaled_file)  #Scale the data.
    scaled_file.close()   #Start reading from beginning of file.
    scaled_file = open(scaled_filename, "r")
    write_smooth_file(smooth_file, scaled_file, smooth_num)  #Smooth the data.
    smooth_file.close()   #Start reading from beginning of file.
    smooth_file = open(smooth_filename, "r")
    #Write the gnuplot command file.
    write_plot_file(sys_name, smooth_filename, plot_file, graphic_filename)
    smooth_file.close()
    plot_file.close()     #Close this file so gnuplot can read it.

    os.system("gnuplot < %s" % plot_filename)

    os.system("ppmtogif %s > %s\n" % (graphic_filename, gif_filename))
    
    os.remove(scaled_filename)
    os.remove(smooth_filename)
    os.remove(plot_filename)
    os.remove(graphic_filename)
