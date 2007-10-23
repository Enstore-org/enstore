#!/bin/sh
###############################################################################
#
# $Id:
#
###############################################################################

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

place="${1:-ftp://ssasrv1.fnal.gov/en/enstore_related}"

echo "Installing enstore rpm and required products from $place"
if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

if [ -x /etc/rc.d/init.d/enstore-boot ]
then
    echo "stopping enstore"
    /etc/rc.d/init.d/enstore-boot stop
fi
if [ -x /etc/rc.d/init.d/monitor_server-boot ]
then
    /etc/rc.d/init.d/monitor_server-boot stop
fi


#echo "Installing ftt"
#rpm -U --force ${place}/ftt-2.26-1.i386.rpm 
echo "Installing tcl"
yum install tcl
echo "Installing tk"
yum install tk
echo "Installing python"
rpm -U --force ${place}/Python-enstore-1.0.0-3.i386.rpm
echo "Installing enstore"
rpm -Uvh --force --nodeps /usr/src/redhat/RPMS/i386/enstore-1.0.1-8.i386.rpm
ENSTORE_DIR=`rpm -ql enstore | head -1`



$ENSTORE_DIR/external_distr/create_demo_enstore_environment.sh -x
$ENSTORE_DIR/external_distr/finish_demo_server_install.sh -x
# install crons
#$ENSTORE_DIR/tools/install_crons.py
