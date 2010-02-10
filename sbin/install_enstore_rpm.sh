#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set
quiet=0
server=0
fnal=""
place="ftp://ssasrv1.fnal.gov/en/lts44"
force=""

usage() {
echo "$0 [-c config_server] [-hqx] [force] [server] [fnal] [url]"
}

# parse command line 
while [ $# -gt 0 ];
do
	case $1 in
		-c) shift; ENSTORE_CONFIG_HOST=$1;
		    export ENSTORE_CONFIG_HOST;
		    shift; ;;
		-x) set -xv; shift;	;;
		-q) export quiet=1; shift;	;;
		-h) usage; exit 0;	;;
		server) export server=1; shift;	;;
		fnal) export fnal=$1; shift;	;;
		force)  force="--${1}"; shift;	;;
		*) place=$1; shift;	;;

	esac;
done
export place
processor=`uname -p`

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
rpm -U $force ${place}/${processor}/ftt-2.26-4.${processor}.rpm 

echo "Installing tcl"
yum install tcl
echo "Installing tk"
yum install tk
echo "Installing python"
rpm -U $force ${place}/${processor}/Python-enstore2.6-3.0.0-1.${processor}.rpm
echo "installing swig"
    rpm -U $force ${place}/${processor}/swig-enstore-1_883-1.${processor}.rpm 
echo "Installing enstore"
rpm -U $force ${place}/${processor}/enstore-2.0.0-0.${processor}.rpm
ENSTORE_DIR=`rpm -ql enstore | head -1`

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
