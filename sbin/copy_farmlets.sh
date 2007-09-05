#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set
. /usr/local/etc/setups.sh
setup enstore
this_host=`uname -n`
if [ $this_host != $ENSTORE_CONFIG_HOST ];
then
    if [ ! -d /usr/local/etc/farmlets ]
    then
	echo "Copying farmlets from $ENSTORE_CONFIG_HOST"
	mkdir -p /usr/local/etc/farmlets
	enrcp $ENSTORE_CONFIG_HOST:/usr/local/etc/farmlets/* /usr/local/etc/farmlets
    fi
else
    echo "You need to create farmlets in $FARMLETS_DIR"
fi
