#!/usr/bin/env python

import sys
import string
import os

import configuration_client

config_port = string.atoi(os.environ.get('ENSTORE_CONFIG_PORT', 7500))
config_host = os.environ.get('ENSTORE_CONFIG_HOST', "localhost")
config=(config_host,config_port)
timeout=15
tries=3

#Conversion from "bytes per 15 seconds" to "terabytes per day"
BP15S_TO_TBPD = 5.7e-09

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
    csc = configuration_client.ConfigurationClient(config)
    ratekeep = csc.get('ratekeeper', timeout, tries)
    
    dir  = ratekeep.get('dir', 'MISSING')
    scaled_file = ratekeep.get('scaled_plot', 'MISSING')
    smooth_file = ratekeep.get('smooth_plot', 'MISSING')
    ratekeeper_name = ratekeep.get('name','MISSING')
    plot_file = ratekeep.get('plot_file','MISSING')
    rate_nodes = ratekeep.get('nodes','MISSING')
    
    if dir == 'MISSING':
        print "Unable to determine the log directory."
        sys.exit(1)
    if scaled_file == 'MISSING':
        print "Unable to determine scale file."
        sys.exit(1)
    if smooth_file == 'MISSING':
        print "Unable to determine smooth file."
        sys.exit(1)
    if ratekeeper_name == 'MISSING' or ratekeeper_name == "":
        ratekeeper_name = os.environ.get("ENSTORE_CONFIG_HOST")
    if rate_nodes == 'MISSING':
        rate_nodes = ''

    if dir[-1] != "/":
        dir = dir + "/"

    return dir, scaled_file, smooth_file, ratekeeper_name, plot_file, \
           rate_nodes

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
def write_scale_file(rate_log_files, scaled_filename):

    rate_file = open(scaled_filename, "w")
    
    for file in rate_log_files:
        in_file = open(log_dir + file, "r")
        while 1:
            line = in_file.readline()
            if not line:
                break
            date, time, read, write = string.split(line)
            rate_file.write("%s %s %s %s\n" %
                            (date, time, BP15S_TO_TBPD * float(read),
                             BP15S_TO_TBPD * float(write)))

    rate_file.close()


#Take the file writen out in write_scale_file and smooth the data.
# smooth_filename = file to write out
# scaled_filename = file to read in
# smooth_num = number of points that get smoothed into one.
def write_smooth_file(smooth_filename, scaled_filename, smooth_num = 40):
    n = smooth_num
    tr, tw = 0.0, 0.0
    c = 0

    rate_file = open(scaled_filename, "r")
    smooth_file = open(smooth_filename, "w")
    
    while 1:
        line = string.strip(rate_file.readline())
        if not line:
            break
        date, time, r, w = string.split(line)
        tr = tr + float(r)
        tw = tw + float(w)
        c = (c+1)%n
        if not c:
            smooth_file.write("%s %s %s %s\n" % (date, time, tr/n, tw/n))
            tr, tw = 0.0, 0.0

    rate_file.close()
    smooth_file.close()


#Write out the file that gnuplot will use to plot the data.
# sys_name = The node name that will be the plot title.
# smooth_file = the file that contains the data to be ploted.
# plot_file = the file that will be read in by gnuplot.
def write_plot_file(sys_name, smooth_file, plot_file):
    plot_file = open(plot_file, "w")

    plot_file.write("set title \"%s\"\n" % (sys_name))
    plot_file.write("set ylabel \"Terabytes/day\"\n")
    plot_file.write("set xdata time\n")
    plot_file.write("set timefmt \"%s\"\n" % ("%m-%d-%Y %H:%M:%S"))
    plot_file.write("set format x \"%s\"\n" % ("%m-%d-%Y\\n%H:%M:%S"))
    plot_file.write("set grid ytics\n")
    plot_file.write("plot \"%s\" using 1:3 title \"read\" with lines," \
                    "\"%s\" using 1:4 title \"write\" with lines" %
                    (smooth_file, smooth_file))

##########################################################################
##########################################################################

if __name__ == "__main__":
    log_dir, scaled_filename, smooth_filename, sys_name, plot_filename, \
             rate_nodes = get_rate_info()

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

    rate_log_files = filter_out_files(sys_name, log_dir)

    if not rate_log_files:
        print "No %s rate files found." % (sys_name)
        sys.exit(1)

    write_scale_file(rate_log_files, scaled_filename)
    write_smooth_file(smooth_filename, scaled_filename, smooth_num)

    write_plot_file(sys_name, smooth_filename, plot_filename)

    os.system("gnuplot -persist < %s" % plot_filename)

    os.system("rm " + smooth_filename)
    os.system("rm " + scaled_filename)
    os.system("rm " + plot_filename)
