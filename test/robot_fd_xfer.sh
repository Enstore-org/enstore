#! /bin/sh
#   This file (robot_fd_xfer.sh) was created by Ron Rechenmacher <ron@fnal.gov> on
#   May 14, 1999. "TERMS AND CONDITIONS" governing this file are in the README
#   or COPYING file. If you do not have such a file, one can be obtained by
#   contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#   $RCSfile$
#   $Revision$
#   $Date$

set -u
if [ "${1-}" = "-x" ];then set -x; shift; fi

USAGE="`basename $0` \
<lps> <emass_tape_drive_type> <emass_drive_spec> <emass_vol_spec> <file> <tape_dev>

examples:
 `basename $0` 30 DECDLT DE01 CA2508 /raid/enstore/random/1GB.trand /dev/rmt/tps2d1
or
 `basename $0` 30 DECDLT DE02 CA2513 /raid/enstore/random/1GB.trand /dev/rmt/tps2d2
"

if [ $# != 6 ];then
    echo "$USAGE"
    exit
fi

if [ ! "${ENSTORE_DIR-}" ];then
    echo "ENSTORE_DIR not defined; setup enstore"
    exit
fi

loops=`expr $1 + 1`             # this (also) will make sure it is a number
emtype=$2
emdriv=$3
emvolm=$4
infile=$5
device=$6

eod=None

x=0
while loops=`expr $loops - 1`;do
    echo "start loop $x at `date`"
    echo -n "ejecting... "
    mt -f $device offline
    echo -n "dismounting... "
    rsh rip10 ". /usr/local/etc/setups.sh;setup enstore;\
              dasadmin dismount -d $emdriv"
    echo -n "mounting... "
    rsh rip10 ". /usr/local/etc/setups.sh;setup enstore;\
              dasadmin mount -t $emtype $emvolm $emdriv"
    echo -n "xferring... "
    eod=`$ENSTORE_DIR/test/fd_xfer.py $infile $device $eod`
    x=`expr $x + 1`
done

