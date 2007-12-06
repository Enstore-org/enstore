#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi
if [ "${1:-}" = "64" ] ; then export x_64="_x64"; shift; else x_64=""; fi
if [ "${1:-}" = "server" ] ; then export server=1; shift; else server=0; fi
if [ "${1:-}" = "fnal" ]; then export fnal=$1; shift; else fnal="";fi
usage() {
echo "$0 [server] [fnal] [url]"
} 
if [ "${1:-}" = "-h" ];then usage;fi
 
place="${1:-ftp://ssasrv1.fnal.gov/en/enstore_related}"


echo "Installing enstore rpm and required products from $place"
echo "This is a fermilab specific installation"
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


echo "Installing ftt"
rpm -U --force ${place}/ftt-2.26-1.i386.rpm 

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
if [ "${fnal:-x}" = "fnal" ]
then
    echo "installing swig"
    rpm -U --force ${place}/swig-enstore-1_883-1.i386.rpm 
    if [ $server -eq 1 ]
    then
	echo "installing aci"
	rpm -U --force ${place}/aci-3.1.2-1.i386.rpm
    fi
    echo "Installing enstore"
    rpm -Uvh --force ${place}/enstore-1.0.1-10.i386.rpm
    ENSTORE_DIR=`rpm -ql enstore | head -1`
else
    echo "Installing enstore"
    rpm -Uvh --force ${place}/enstore_sa-1.0.1-10.i386.rpm
    ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
fi


$ENSTORE_DIR/external_distr/create_enstore_environment.sh $fnal
$ENSTORE_DIR/sbin/copy_farmlets.sh
if [ $server -eq 1 ]
then
    $ENSTORE_DIR/sbin/finish_server_install.sh $place
fi
unset ENSTORE_DIR
source /usr/local/etc/setups.sh
setup enstore
# bring up alais intrefaces
echo Intializing service IPs
$ENSTORE_DIR/tools/service_ips 
# install crons
echo Installing crons
$ENSTORE_DIR/tools/install_crons.py
chmod 644 /etc/cron.d/*
