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
echo "Installing tcl"
yum install tcl
echo "Installing tk"
yum install tk
echo "Installing python"
rpm -U --force ${place}/Python-enstore-1.0.0-3.i386.rpm
if [ "${fnal:-x}" = "fnal" ]
then
    export ENSSH=/usr/bin/ssh
    export ENSCP=/usr/bin/scp
    export ENSTORE_USER_DEFINED_CONFIG_HOST=d0ensrv2n.fnal.gov
    export ENSTORE_USER_DEFINED_CONFIG_FILE=/home/enstore/enstore/etc/d0en_sde_test.conf
    export ENSTORE_USER_DEFINED_MAIL=moibenko@fnal.gov
    echo "installing swig"
    rpm -U --force ${place}/swig-enstore-1_883-1.i386.rpm 
    echo "installing aci"
    rpm -U --force ${place}/aci-3.1.2-1.i386.rpm
    echo "Installing enstore"
    rpm -Uvh --force ${place}/enstore-1.0.1-9.i386.rpm
    ENSTORE_DIR=`rpm -ql enstore | head -1`
else
    echo "Installing enstore"
    rpm -Uvh --force ${place}/enstore_sa-1.0.1-9.i386.rpm
    ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
fi


$ENSTORE_DIR/external_distr/create_enstore_environment.sh $fnal
$ENSTORE_DIR/sbin/copy_farmlets.sh
if [ $server -eq 1 ]
then
    $ENSTORE_DIR/sbin/finish_server_install.sh -x $place
fi
# install crons
source /usr/local/etc/setups.sh
setup enstore
$ENSTORE_DIR/tools/install_crons.py
