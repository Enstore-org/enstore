#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set
. /usr/local/etc/setups.sh
setup enstore
echo "Copying farmlets from $ENSTORE_CONFIG_HOST"
enrcp $ENSTORE_CONFIG_HOST:/usr/local/etc/farmlets/* /usr/local/etc/farmlets
