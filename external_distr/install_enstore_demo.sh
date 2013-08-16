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
		force)  export force; force="--${1}"; shift;	;;
		*) place=$1; shift;	;;

	esac;
done
export place
processor=`uname -i`

echo "Installing enstore rpm and required products from $place"
echo "This is a demo installation"
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
# enstore depends on postgresql-libs beginning with version 2.0.1-0
echo "Installing postgresql-libs"
rpm -U $force ${place}/${processor}/postgresql-libs*
echo "Installing enstore"
rpm -U $force ${place}/${processor}/enstore-2.2.2-3.${processor}.rpm
ENSTORE_DIR=`rpm -ql enstore | head -1`

$ENSTORE_DIR/external_distr/create_demo_enstore_environment.sh
$ENSTORE_DIR/sbin/finish_server_install.sh $place $force
unset ENSTORE_DIR
source /usr/local/etc/setups.sh
setup enstore

ln -s $ENSTORE_DIR $ENSTORE_HOME/enstore
#start pnfs
echo "Starting pnfs"
/etc/init.d/pnfs start


echo Installing crons
$ENSTORE_DIR/tools/install_crons.py
chmod 644 /etc/cron.d/*

# create database
echo "creating enstore databases"
python $ENSTORE_DIR/sbin/create_database.py enstoredb
if [ $? -ne 0 ]
then
    echo "Failed to create enstore DB"
    exit 1
fi

python $ENSTORE_DIR/sbin/create_database.py accounting
if [ $? -ne 0 ]
then
    echo "Failed to create accounting DB"
    exit 1
fi

python $ENSTORE_DIR/sbin/create_database.py drivestat
if [ $? -ne 0 ]
then
    echo "Failed to create drivestat DB"
    exit 1
fi

#python $ENSTORE_DIR/sbin/create_database.py operation
#if [ $? -ne 0 ]
#then
#    echo "Failed to create operation DB"
#    exit 1
#fi

# make directories for disk movers
mkdir -p /srv2/data/d1
mkdir -p /srv2/data/d2

#start enstore
echo "Starting enstore"
/etc/init.d/enstore-boot start
# checking if enstore has started
enstore conf --timeout 3 --alive
if [ $? -ne 0 ];
then
    echo "Please check the installation output for any possible errors"
    exit 1
fi

# add null vols
#echo "Adding null volumes"
#source /usr/local/etc/setups.sh
#enstore vol --add NUL000 null none none none null 400G
#enstore vol --add NUL001 null none none none null 400G
echo "enstore started on this machine.

Please remove all firewalls.
You should be able to see http://localhost/enstore web page
from this page the link to 'Enstore Server Status' shows the
status of installed system. All components must be 'alive'
The documentation url is: http://www-ccf.fnal.gov/enstore/documentation.html
And has a link on http://localhost/enstore called 'Mass Storage System Documentation Page'

Login as user enstore and try toransfer files as:
Writes:
encp --verbose 4 some_file /pnfs/fs/usr/data1/disk
encp --verbose 4 some_file /pnfs/fs/usr/data1/NULL 
Note!!! Transfers to null movers fail if client is on the same node with mover.
Until this is resolved please use disk movers only!


Reads:
encp --verbose 4 /pnfs/fs/usr/data1/disk/some_file .
encp --verbose 4 /pnfs/fs/usr/data1/NULL/some_file /dev/null
"
echo "Now I will try to start entv and make enstore transfers"
cd 
if [ ! -f ".entvrc" ];then
    echo "$ENSTORE_CONFIG_HOST 471x549+0+0          #ffcc66    animate" > .entvrc
fi
echo "Starting entv"
entv&
sleep 10


echo "Making initial transfer. Watch entv!"
echo "Copy ${ENSTORE_DIR}/bin/encp to enstore disk"
set -xv
mkdir -p /pnfs/fs/usr/data1/disk
cd /pnfs/fs/usr/data1/disk
# check if disk library manager exists
enstore conf --show disk.library_manager
if [ $? -ne 0 ]
then
    echo "disk.library_manager does not exist in configuration. Can not proceed with testing"
    exit 1
fi
enstore pnfs --storage_group TEST
enstore pnfs --library disk
enstore pnfs --file_family TEST
enstore pnfs --file_family_width 1
enstore pnfs --file_family_wrapper null
encp --verbose 4 ${ENSTORE_DIR}/bin/encp /pnfs/fs/usr/data1/disk/encp_0.$$
if [ $? -ne 0 ]; then
    echo "encp failed. Check the output. Do not forget to remove firewalls"
    exit 1
fi

echo "Copy /pnfs/fs/usr/data1/disk/encp_0 from enstore disk to /tmp"
rm -rf /tmp/encp_0
encp --verbose 4 /pnfs/fs/usr/data1/disk/encp_0.$$ /tmp
if [ $? -ne 0 ]; then
    echo "encp failed. Check the output. Do not forget to remove firewalls"
    exit 1
fi

echo "Compare ${ENSTORE_DIR}/bin/encp to /tmp/encp_0"
diff ${ENSTORE_DIR}/bin/encp /tmp/encp_0.$$
exit 0
 
