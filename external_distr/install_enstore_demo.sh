#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

place="${1:-ftp://ssasrv1.fnal.gov/en/lts44/i386}"

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
    rpm -U --force ${place}/ftt-2.26-2.i386.rpm
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
rpm -Uvh --force --nodeps ${place}/enstore_sa-1.0.1-11.i386.rpm
rpm -q enstore > /dev/null
if [ $? -eq 0 ]; 
then
    ENSTORE_DIR=`rpm -ql enstore | head -1`
else
    ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
fi

$ENSTORE_DIR/external_distr/create_demo_enstore_environment.sh
# install crons
echo "installing crons"
unset ENSTORE_DIR
source /usr/local/etc/setups.sh
$ENSTORE_DIR/tools/install_crons.py

$ENSTORE_DIR/external_distr/finish_demo_server_install.sh

# create database
echo "creating enstore databases"
$ENSTORE_DIR/external_distr/install_database.sh

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
"Now I will try to start entv and make enstore transfers"
cd 
if [ ! -f ".entvrc" ];then
    echo "$ENSTORE_CONFIG_HOST 471x549+0+0          #ffcc66    animate" > .entvrc
fi
echo "Starting entv"
entv&
sleep 10


echo "Making initial transfer. Watch entv"
echo "Copy ${ENSTORE_DIR}/bin/encp to enstore disk"
set -xv
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
 
