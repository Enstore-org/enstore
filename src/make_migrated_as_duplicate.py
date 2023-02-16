#!/usr/bin/env python

import duplication_util
import sys
import os

def usage():
	print "Make files on the migrated-to volumes as duplicate of the original files"
	print "i.e. The original file is primary and the migrated (new) is its copy"
	print
	print "Usage:"
	print
	print "%s <vol> ..."%(sys.argv[0])
	print
	print "where <vol> ... are the list of new volumes to which"
	print "                the files have been migrated"

if __name__ == "__main__":   # pragma: no cover
	if len(sys.argv) < 2:
		usage()
		sys.exit(0)

	if sys.argv[1] == '--help':
		usage()
		sys.exit(0)

	if os.geteuid() != 0:
		sys.stderr.write("Must run as user root.\n")
		sys.exit(1)

	duplication_util.make_migrated_as_duplicate(sys.argv[1:])
