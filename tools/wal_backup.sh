#!/bin/sh -f
##############################################################################
#
# $Id$
#
# This script performs postgresql live backup
#
##############################################################################
setup=/usr/etc/pnfsSetup
#
if [ ! -f $setup ] ; then
  echo " Can't locate $setup "
  exit 1
fi
. $setup

db=$database
db_psql=$database_postgres
backdir=$database/../backup
genFile=$backdir/backupGeneration
archive_dir=$backdir/xlogs

in=${1}
out=${archive_dir}/${2}.Z

if [ -f ${out} ] ; then
        exit 100
fi

compress <$in >${out}
if [ "$?" != "0" ] ; then
        rm -f ${out}
        exit 1
fi

source /usr/local/etc/setups.sh
setup enstore

${ENSTORE_DIR}/sbin/enrcp ${out} ${remotebackup}/../pnfs-backup.xlogs

if [ "$?" != "0" ] ; then
        rm -f ${out}
        exit 3
fi

exit 0
