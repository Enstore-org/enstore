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


rpm -q ftt > /dev/null
if [ $? -ne 0 ]; then
    echo "Installing ftt"
    rpm -U --force ${place}/ftt-2.26-1.i386.rpm
fi
 
rpm -q tcl > /dev/null
if [ $? -ne 0 ]; then
    echo "Installing tcl"
    yum install tcl
fi
rpm -q tk > /dev/null
if [ $? -ne 0 ]; then
    echo "Installing tk"
    yum install tk
fi
rpm -q Python-enstore > /dev/null
if [ $? -ne 0 ]; then
    echo "Installing python"
    rpm -U --force ${place}/Python-enstore-1.0.0-3.i386.rpm
fi
echo "Installing enstore"

#rpm -Uvh --force --nodeps ${place}/enstore-1.0b.1-10.i386.rpm
rpm -Uvh --force --nodeps ${place}/enstore_sa-1.0.1-10.i386.rpm
rpm -q enstore > /dev/null
if [ $? -eq 0 ]; 
then
    ENSTORE_DIR=`rpm -ql enstore | head -1`
else
    ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
fi

$ENSTORE_DIR/external_distr/create_demo_enstore_environment.sh -x
$ENSTORE_DIR/external_distr/finish_demo_server_install.sh -x
# install crons
$ENSTORE_DIR/tools/install_crons.py
# create database
$ENSTORE_DIR/external_distr/install_database.sh
