#!/bin/bash 
##############################################################################
#
# $Id$ 
#
# script that monitors pmountd and starts it if it is missing
#
##############################################################################
setup=/usr/etc/pnfsSetup
if [ ! -f $setup ] ; then
   echo " Sorry, can't find $setup "
   exit 1
fi
. $setup
tools=$pnfs/tools
. $tools/special

pid=`spps | $AWK '{ if(($NF=="pmountd"))printf "%s ",$1; }'`
if [ -z $pid ]
then
    enstore alarm  --raise --client-name PMOUNTD_NANNY --severity W --root-error "PMOUNTD GONE" --message "pmountd not running, restarting "
    $tools/pnfs.server start pmountd 
fi