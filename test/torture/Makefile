SHELL=/bin/sh
#############################################################
#
#  $Id$
#
#############################################################
all: gauss createfile

gauss: gauss.cc
	g++ -o $@ gauss.cc
createfile: createfile.cc
	g++ -o $@ createfile.cc

clean:
	-rm gauss createfile
