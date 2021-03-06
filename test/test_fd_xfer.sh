#! /bin/sh
#   This file (t.sh) was created by Ron Rechenmacher <ron@fnal.gov> on
#   May 18, 1999. "TERMS AND CONDITIONS" governing this file are in the README
#   or COPYING file. If you do not have such a file, one can be obtained by
#   contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#   $RCSfile$
#   $Revision$
#   $Date$

ctlr=3

cmd()
{   echo "$@"
    "$@"
}


if [ `hostname` != rip2.fnal.gov ];then
    echo "must be run from specific node (rip2 in this case)"
    exit
fi

backup() # $1 is file to be backed up
{   file=${1-}
    if [ -f "$file" ];then
        # need to save
        if x=`ls | grep "$file\.~[0-9]*~$"`;then
            x=`echo "$x" | sed -e 's/.*\.~//' -e 's/~$//' | sort -rn | head -1`
            x=`expr $x + 1`
        else x=1
        fi
        mv $file $file.~$x~
    fi
}

for dd in 1 2 3 4;do
    if [ ! -d ${ctlr}d$dd ];then mkdir ${ctlr}d$dd; fi
    cd ${ctlr}d$dd
    backup *.log
    cd ..
done

rsh rip10 ". /usr/local/etc/setups.sh;setup enstore;\
           dasadmin list rip3;\
           dasadmin list rip10"

cmd mt -f /dev/rmt/tps${ctlr}d1n status
cmd mt -f /dev/rmt/tps${ctlr}d2n status
cmd mt -f /dev/rmt/tps${ctlr}d3n status
cmd mt -f /dev/rmt/tps${ctlr}d4n status

cd ${ctlr}d1
../fd_xfer.py /raid/enstore/random/200MB.trand /dev/rmt/tps${ctlr}d1n None 1000 DECDLT DE01 CA2502 rip10 >${ctlr}d1.log 2>&1 &
cd ../${ctlr}d2
../fd_xfer.py /raid/enstore/random/300MB.trand /dev/rmt/tps${ctlr}d2n None 1000 DECDLT DE02 CA2504 rip10 >${ctlr}d2.log 2>&1 &
cd ../${ctlr}d3
#../fd_xfer.py /raid/enstore/random/400MB.trand /dev/rmt/tps${ctlr}d3n None  800 DECDLT DE14 CA2508 rip3 >${ctlr}d3.log 2>&1 &
../fd_xfer.py /raid/enstore/random/400MB.trand /dev/rmt/tps${ctlr}d3n None  800 >${ctlr}d3.log 2>&1 &
cd ../${ctlr}d4
#../fd_xfer.py /raid/enstore/random/200MB.trand /dev/rmt/tps${ctlr}d4n None  800 DECDLT DE13 CA2506 rip3 >${ctlr}d4.log 2>&1 &


