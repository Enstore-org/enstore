#!/bin/sh
###############################################################################
#
# $Id:
#
###############################################################################
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
. /usr/etc/pnfsSetup
PATH=$PATH:$pnfs/tools
dbdir=`dirname $database`
mdb create admin ${dbdir}/admin
mdb create data1 ${dbdir}/data1


echo "Starting pnfs"   # do not start pnfs as it will crash if there is no database
/etc/init.d/pnfs start
#create pnfs directory
if [ ! -d /pnfs/fs ];
then
    mkdir -p /pnfs/fs
    chmod -R 777 /pnfs/fs
fi

mount -o intr,hard,rw localhost:/fs   /pnfs/fs
sleep 10
df


echo "Creating enstore tags"

mkdir /pnfs/fs/usr/data1 
cd /pnfs/fs/usr/data1

enstore pnfs --file_family=test
enstore pnfs --file_family_wrapper=null
enstore pnfs --file_family_width=2
enstore pnfs --storage_group=NULL
mkdir /pnfs/fs/usr/data1/disk
mkdir /pnfs/fs/usr/data1/NULL
chmod -R 777 /pnfs/fs/usr/
chmod -R 777 /pnfs/fs/usr/data1/*
cd /pnfs/fs/usr/data1/NULL
enstore pnfs --library=null
cd ../disk
enstore pnfs --library=disk
enstore pnfs --storage_group=DISK

