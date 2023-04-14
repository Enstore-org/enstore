#!/usr/bin/env python

import duplication_util
import sys
import os

def usage():
	print "Make files from original volumes as duplicate of the migrated files"
	print "i.e. The migrated (new) file is primary and the original is its copy"
	print
	print "Usage:"
	print
	print "%s <vol> ..."%(sys.argv[0])
	print
	print "where <vol> ... are the list of original volumes from which"
	print "                the files have been migrated"

if __name__ == "__main__":
	if len(sys.argv) < 2:
		usage()
		sys.exit(0)

	if sys.argv[1] == '--help':
		usage()
		sys.exit(0)

	if os.geteuid() != 0:
		sys.stderr.write("Must run as user root.\n")
		sys.exit(1)

	duplication_util.make_original_as_duplicate(sys.argv[1:])
