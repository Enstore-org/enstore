#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
set -u  # force better programming and ability to use check for not set
. /usr/local/etc/setups.sh
setup enstore
echo "installing enstore_html"
rpm -U --force ftp://ssasrv1.fnal.gov/en/enstore_related/enstore_html-1.0-0.i386.rpm
