#!/bin/sh

# clean all files older than 1 day that are in the passed in directory

set -u  
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

print_usage() {
    echo "USAGE: ${0} <directory>"
}

file_name="*.dcache_page_*"

# get directory to pare down
if [ "${1:-q}" = "q" ] ; then 
    # we need a directory, and do not have one, error and exit
    echo "ERROR: No directory entered"
    print_usage
    exit 1
else
    directory=${1};     
    # find all files that are more than 1 day old and that have a
    # file name of one of the following forms, and then remove them
    #
    #  *.dcache_page_*
    find $directory -maxdepth 0 -name "$file_name" -user enstore -mtime +1 -exec rm {} \;
fi
