#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################

# system imports
from __future__ import print_function
import sys
import string
import os
import tempfile
import time

# enstore imports
import configuration_client
import enstore_functions2


# Conversion from "bytes per 15 seconds" to "terabytes per day"
BP15S_TO_TBPD = 5.7e-09
# Seconds in a day
SEC_IN_DAY = 60 * 60 * 24


def print_usage():
    print("Usage:", sys.argv[0], end=' ')
    print("[-s <num>] [-t <num>] [-n <name>] [--help]")
    print("   -t     number of days to plot (default=2)")
    print("   -s     number of 15 second intervals smoothed.  (default=40)")
    print("  --help  print this message")
    print("See configuration dictionary entry \"ratekeeper\" for defaults.")

#############################################################################
#############################################################################
# This is the "magic" class to use when filtering out elements that have the
# same external label in a list.


class file_filter:
    def __call__(self, list_element):
        # find() return -1 on failure and 0 through n for success.  By adding
        # one to this return value the value for failure becomes 0 and
        # success is 1 through (n + 1).  Thus we get what we want.
        return string.find(list_element, self.compare_value) + 1

    def __init__(self, thresh):
        self.compare_value = thresh

# This is the "magic" class to use when filtering out elements that have the
# same external label in a list.


class file_timestamp_filter:
    def __call__(self, list_element):
        return list_element[list_element.rfind(
            ".") + 1:] > self.compare_value[self.compare_value.rfind(".") + 1:]

    def __init__(self, thresh):
        self.compare_value = thresh
############################################################################
############################################################################


# Function to recover the directory that the rate log files are in.
def get_rate_info():
    # Get the configuration from the configuration server.
    csc = configuration_client.ConfigurationClient()
    ratekeep = csc.get('ratekeeper', timeout=15, retry=3)

    dname = ratekeep.get('dir', 'MISSING')
    tmp_dir = ratekeep.get('tmp', 'MISSING')
    ratekeeper_name = ratekeep.get('name', 'MISSING')
    ratekeeper_host = ratekeep.get('host', 'MISSING')
    rate_nodes = ratekeep.get('nodes', 'MISSING')
    ps_filename = ratekeep.get('ps', 'MISSING')
    jpg_filename = ratekeep.get('jpg', 'MISSING')
    jpg_stamp_filename = ratekeep.get('stamp', 'MISSING')

    if dname == 'MISSING':
        print("Unable to determine the log directory.")
        sys.exit(1)
    if tmp_dir == 'MISSING':
        tmp_dir = None
    if ratekeeper_name == 'MISSING' or ratekeeper_name == "":
        if ratekeeper_host == 'MISSING' or ratekeeper_name == "":
            ratekeeper_name = os.environ.get("ENSTORE_CONFIG_HOST")
            ratekeeper_name = enstore_functions2.strip_node(ratekeeper_name)
        else:
            ratekeeper_name = enstore_functions2.strip_node(ratekeeper_host)
    if rate_nodes == 'MISSING':
        rate_nodes = ''

    if ps_filename == 'MISSING':
        print("Unable to determine the ps output filename.")
        sys.exit(1)
    if jpg_filename == 'MISSING':
        print("Unable to determine the jpg output filename.")
        sys.exit(1)
    if jpg_stamp_filename == 'MISSING':
        print("Unable to determine the jpg stamp output filename.")
        sys.exit(1)

    if dname[-1] != "/":
        dname = dname + "/"
    if tmp_dir[-1] != "/":
        tmp_dir = tmp_dir + "/"

    return dname, tmp_dir, ratekeeper_name, rate_nodes, ps_filename, \
        jpg_filename, jpg_stamp_filename


###########################################################################
###########################################################################

# Use the file_filter class to filter out the files with rate data that we
# are looking for.  sys_name is the name to look for inside the filenames,
# and log_dir is the directory to look in.
def filter_out_files(sys_name, log_dir, time_in_days):
    all_log_files = os.listdir(log_dir)

    # criteria is a callable class that is used inside of filter for pulling
    # specific filenames out of a list.
    criteria = file_filter(".RATES.")
    rate_log_files = filter(criteria, all_log_files)

    criteria.compare_value = sys_name
    sys_name_rate_log_files = filter(criteria, rate_log_files)

    # determine the first day of plots to plot
    startday = time.localtime(time.time() - (SEC_IN_DAY * time_in_days))[:3]
    startday_string = "%s.RATES.%02d%02d%02d" % (sys_name, startday[0],
                                                 startday[1],
                                                 startday[2])
    timestamp_criteria = file_timestamp_filter(startday_string)
    recent_rate_log_files = filter(timestamp_criteria, sys_name_rate_log_files)

    return recent_rate_log_files

##########################################################################
##########################################################################

# Take all of the data in all of the files (that we care about), and merge
# them together.  At the same time convert the data from bytes per 15 seconds
# to terabytes per day.
# rate_log_files is the list of files and scaled_filename is the file that
# the merged data is written to.


def write_scale_file(log_dir, rate_log_files, scaled_file):
    for fname in rate_log_files:
        in_file = open(log_dir + fname, "r")
        while True:
            line = in_file.readline()
            if not line:
                break
            split_line = string.split(line)
            if len(split_line) == 4:  # Older ratekeepers.
                date, time, read, write = split_line
                null_read, null_write = (0, 0)
            elif len(split_line) == 6:
                date, time, read, write, null_read, null_write = split_line
            scaled_file.write("%s %s %s %s %s %s\n" %
                              (date, time,
                               BP15S_TO_TBPD * float(read),
                               BP15S_TO_TBPD * float(write),
                               BP15S_TO_TBPD * float(null_read),
                               BP15S_TO_TBPD * float(null_write),
                               ))


# Take the file writen out in write_scale_file and smooth the data.
# smooth_filename = file to write out
# scaled_filename = file to read in
# smooth_num = number of points that get smoothed into one.
def write_smooth_file(smooth_file, scaled_file, smooth_num=40):
    n = smooth_num
    tr, tw = 0.0, 0.0
    tnr, tnw = 0.0, 0.0
    c = 0

    while True:
        line = string.strip(scaled_file.readline())
        if not line:
            break
        split_line = string.split(line)
        if len(split_line) == 4:  # Older ratekeepers.
            date, time, r, w = split_line
            n_r, n_w = (0.0, 0.0)
        elif len(split_line) == 6:
            date, time, r, w, n_r, n_w = split_line
        tr = tr + float(r)
        tw = tw + float(w)
        tnr = tnr + float(n_r)
        tnw = tnw + float(n_w)
        c = (c + 1) % n
        if not c:
            smooth_file.write("%s %s %s %s %s %s %s %s\n" %
                              (date, time,
                               tr / n, tw / n, (tr + tw) / n,
                               tnr / n, tnw / n, (tnr + tnw) / n,))
            # Reset these.
            tr, tw = 0.0, 0.0
            tnr, tnw = 0.0, 0.0


# Write out the file that gnuplot will use to plot the data.
# sys_name = The node name that will be the plot title.
# smooth_file = the file that contains the data to be ploted.
# plot_file = the file that will be read in by gnuplot.
def write_plot_file(sys_name, smooth_filename, plot_file, graphic_filename,
                    group, group_n, supplemental_title_text=""):

    group_title = string.upper(group[0]) + group[1:]

    plot_file.write("set title \"%s Data Rates on %s (Plotted: %s) %s\"\n" %
                    (group_title, sys_name, time.ctime(time.time()),
                     supplemental_title_text))
    plot_file.write("set ylabel \"Terabytes/day\"\n")
    plot_file.write("set xdata time\n")
    plot_file.write("set timefmt \"%s\"\n" % ("%m-%d-%Y %H:%M:%S"))
    plot_file.write("set format x \"%s\"\n" % ("%m-%d-%Y\\n%H:%M:%S"))
    plot_file.write("set grid ytics\n")
    plot_file.write("set terminal postscript color solid\n")
    plot_file.write("set size 1.4,1.2\n")
    plot_file.write("set output \"%s\"\n" % graphic_filename)
    plot_file.write("plot \"%s\" using 1:%d title \"read\" with lines,"
                    "\"%s\" using 1:%d title \"write\" with lines,"
                    "\"%s\" using 1:%d title \"both\" with lines"
                    % ((smooth_filename, group_n * 3 + 3,
                        smooth_filename, group_n * 3 + 4,
                        smooth_filename, group_n * 3 + 5,)))

##########################################################################
##########################################################################


if __name__ == "__main__":
    if "--help" in sys.argv:
        print_usage()
        sys.exit(1)

    # Make sure the gnuplot is available.
    if os.system("echo exit | gnuplot > /dev/null 2>&1"):
        try:
            sys.stderr.write("Unable to find gnuplot.  Aborting.\n")
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)

    log_dir, tmp_dir, sys_name, rate_nodes, ps_filename_template, \
        jpg_filename_template, jpg_stamp_filename_template = get_rate_info()
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Check the command line arguments.
    # -t stands for Time smoothing, which is the number of points that get
    # averaged together.
    if "-s" in sys.argv:
        smooth_num = int(sys.argv[sys.argv.index("-s") + 1])
    else:
        smooth_num = 40
    if "-t" in sys.argv:
        time_in_days = int(sys.argv[sys.argv.index("-t") + 1])
    else:
        time_in_days = 2

    rate_log_files = filter_out_files(sys_name, log_dir, time_in_days)

    if not rate_log_files:
        print("No %s rate files found." % (sys_name))
        sys.exit(1)

    groups = ["real", "null"]  # Order matters here!
    sst = {'null': "(only null movers)", 'real': "(no null movers)"}
    ps_filename = {}
    jpg_filename = {}
    jpg_stamp_filename = {}
    plot_filename = {}

    # Replace the *s with text for the filenames.
    for group in groups:
        ps_filename[group] = string.replace(ps_filename_template, '*',
                                            group + "_")
        jpg_filename[group] = string.replace(jpg_filename_template, '*',
                                             group + "_")
        jpg_stamp_filename[group] = string.replace(jpg_stamp_filename_template,
                                                   '*', group + "_")

    # Create temporary filenames.
    tempfile.tempdir = tmp_dir
    scaled_filename = tempfile.mktemp(".plot")
    smooth_filename = tempfile.mktemp(".plot")
    for group in groups:
        plot_filename[group] = tempfile.mktemp(".plot")

    # Write the scaled file
    try:
        scaled_file = open(scaled_filename, "w+")
        write_scale_file(log_dir, rate_log_files, scaled_file)
        scaled_file.seek(0, 0)  # Go to beginning of the file.
    except (OSError, IOError):
        exc, msg = sys.exc_info()[:2]
        try:
            sys.stderr.write("Unable to write scaled file: %s: %s\n" %
                             (str(exc), str(msg)))
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)

    # Write the smoothed file
    try:
        smooth_file = open(smooth_filename, "w+")
        write_smooth_file(smooth_file, scaled_file, smooth_num)
        smooth_file.seek(0, 0)  # Go to beginning of the file.
    except (OSError, IOError):
        exc, msg = sys.exc_info()[:2]
        try:
            sys.stderr.write("Unable to write smoothed file: %s: %s\n" %
                             (str(exc), str(msg)))
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit(1)

    for group in groups:
        # Write the gnuplot command file(s).
        try:
            plot_file = open(plot_filename[group], "w+")
            write_plot_file(sys_name, smooth_filename, plot_file,
                            ps_filename[group], group, groups.index(group),
                            sst[group])
            plot_file.close()  # Close this file so gnuplot can read it.
        except (OSError, IOError):
            exc, msg = sys.exc_info()[:2]
            try:
                sys.stderr.write("Unable to write plot file: %s: %s\n" %
                                 (str(exc), str(msg)))
                sys.stderr.flush()
            except IOError:
                pass
            sys.exit()

        os.system("gnuplot < %s" % plot_filename[group])

        os.system("convert -flatten -background lightgray -rotate 90  %s %s\n" % (ps_filename[group],
                                                                                  jpg_filename[group]))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s\n"
                  % (ps_filename[group], jpg_stamp_filename[group]))

    # Close the files.
    try:
        scaled_file.close()
        smooth_file.close()
    except (OSError, IOError):
        exc, msg = sys.exc_info()[:2]
        try:
            sys.stderr.write("Unable to write scaled file: %s: %s\n" %
                             (str(exc), str(msg)))
            sys.stderr.flush()
        except IOError:
            pass
        sys.exit()

    os.remove(scaled_filename)
    os.remove(smooth_filename)
    for group in groups:
        os.remove(plot_filename[group])
