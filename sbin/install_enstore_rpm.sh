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
redhat_release=`sed -e 's/ /\n/g' /etc/redhat-release | while read i;do grep '[0-9'] | cut -d "." -f 1;done`
echo "redhat_release ${redhat_release}"
if [ $redhat_release = "5" ]; then
    release_dir="slf5x"
else
    # default to 4
    release_dir="lts44"
fi
echo "release dir ${release_dir}"
place="ftp://ssasrv1.fnal.gov/en/${release_dir}"
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
		-x) set -xv; shift;export ENSTORE_VERBOSE="y";	;;
		-q) export quiet=1; shift;	;;
		-h) usage; exit 0;	;;
		server) export server=1; shift;	;;
		fnal) export fnal=$1; shift;	;;
		force)  export force=$1;force="--${1}"; shift;	;;
		*) place=$1; shift;	;;

	esac;
done
export place
processor=`uname -i`

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


echo "Installing tcl"
yum -y install tcl.${processor}
echo "Installing tk"
yum -y install tk.${processor}
echo "Installing enstore"
rpm -U $force ${place}/${processor}/enstore-2.0.0-2.${processor}.rpm
ENSTORE_DIR=`rpm -ql enstore | head -1`

$ENSTORE_DIR/external_distr/create_enstore_environment.sh $fnal
$ENSTORE_DIR/sbin/copy_farmlets.sh
if [ $server -eq 1 ]
then
    $ENSTORE_DIR/sbin/finish_server_install.sh $place $force
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
