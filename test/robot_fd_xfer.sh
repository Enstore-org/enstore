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

opts_wo_args='no_robot'
USAGE="\
usage:    `basename $0` [--$opts_wo_args] \
<lps> <emass_tape_drive_type> <emass_drive_spec> <emass_vol_spec> <file> <tape_dev>
examples: `basename $0` 30 DECDLT DE01 CA2508 /raid/enstore/random/1GB.trand /dev/rmt/tps2d1 rip10
       or `basename $0` 30 DECDLT DE02 CA2513 /raid/enstore/random/1GB.trand /dev/rmt/tps2d2 rip10
"
while opt=`expr "${1-}" : '--\(.*\)'`;do
    shift
    eval "case \$opt in
    \\?) echo \"$USAGE\"; exit 0;;
    $opts_wo_args)
        eval opt_\$opt=1;;
    *)  echo \"invalid option: \$opt\"; echo \"$USAGE\"; exit 1;;
    esac"
done


if [ $# != 7 ];then
    echo "invalid number of parameters; got $#, expected 6"; echo "$USAGE"
    exit
fi

if [ ! "${ENSTORE_DIR-}" ];then
    echo "ENSTORE_DIR not defined; setup enstore"
    exit
fi

echo "my pid is $$"

loops=`expr $1 + 1`             # this (also) will make sure it is a number
emtype=$2
emdriv=$3
emvolm=$4
infile=$5
device=$6
dasnod=$7

exec </dev/null

eod=None
x=0
while loops=`expr $loops - 1`;do
    echo "`date`: start loop $x"
    if [ "${opt_no_robot-}" ];then
        echo -n "`date`:     rewinding... "
        mt -f $device rewind
    else
        echo -n "`date`:     ejecting... "
        mt -f $device offline
        echo -n '\n`date`:     dismounting... '
        rsh $dasnod ". /usr/local/etc/setups.sh;setup enstore;\
                   dasadmin dismount -d $emdriv; echo \$?"
        echo -n '`date`:     mounting... '
        rsh $dasnod ". /usr/local/etc/setups.sh;setup enstore;\
                  dasadmin mount -t $emtype $emvolm $emdriv; echo \$?"
    fi
    echo -n '`date`:     xferring... '
    # fd_xfer.py prints lines of info...
    eod=`$ENSTORE_DIR/test/fd_xfer.py $infile $device $eod`
    x=`expr $x + 1`
    if [ -f pause ];then
        echo 'paused - waiting for "pause" file to be removed'
        while [ -f pause ];do sleep 1;done
        echo 'resuming'
    fi
done

