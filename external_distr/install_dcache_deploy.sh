#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

. /usr/local/etc/setups.sh
if [ $? -ne 0 ]
then 
    echo setup failed
    exit 1
fi

cp -rp $ENSTORE_DIR/dcache_deploy $ENSTORE_HOME
chown -R enstore.enstore $ENSTORE_DIR/dcache_deploy






