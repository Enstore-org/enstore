#! /usr/bin/env python
#
from __future__ import print_function
import sys
import os
import time

# Parameter description:
#
#     <filename>     : name of output file.  we will save this file to a timestamped
#                        version.
#     [num_of_files] : number of timestamped output files to keep around.  the oldest
#                        ones will be deleted.
#

DEFAULTNUMOFFILES = 5

YEARFMT = "%Y-%m-%d"
TIMEFMT = "%H:%M:%S"


def format_time(theTime, sep="_"):
    return time.strftime("%s%s%s" %
                         (YEARFMT, sep, TIMEFMT), time.localtime(theTime))


def print_usage(me):
    print("   ")
    print("USAGE: %s <filename> [num_of_files]")


# Parse parameters
if len(sys.argv) < 2:
    # we must have at least the filename entered
    print("ERROR: filename argument missing")
    print_usage(sys.argv[0])
else:
    filename = sys.argv[1]
    if len(sys.argv) > 2:
        num_of_files = int(sys.argv[2])
    else:
        num_of_files = DEFAULTNUMOFFILES

# cp the current output file to a timestamped one
now = format_time(time.time())
os.system("cp %s %s.%s" % (filename, filename, now))

# get the list of output files that currently exist and delete the oldest
# if needed.
files = os.popen("ls -1 %s.*" % (filename,), 'r').readlines()
if len(files) > num_of_files:
    files.sort()
    files_to_delete = files[:-num_of_files]
    for file in files_to_delete:
        os.system("rm %s" % (file,))
