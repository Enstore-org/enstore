#! /bin/sh
#   This file (t.sh) was created by Ron Rechenmacher <ron@fnal.gov> on
#   May 18, 1999. "TERMS AND CONDITIONS" governing this file are in the README
#   or COPYING file. If you do not have such a file, one can be obtained by
#   contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#   $RCSfile$
#   $Revision$
#   $Date$

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

for dd in 1 2 3;do
    if [ ! -d 2d$dd ];then mkdir 2d$dd; fi
    cd 2d$dd
    backup *.log
    cd ..
done

rsh rip10 ". /usr/local/etc/setups.sh;setup enstore;\
           dasadmin list rip3;\
           dasadmin list rip10"

mt -f /dev/rmt/tps2d1 status
mt -f /dev/rmt/tps2d2 status
mt -f /dev/rmt/tps2d3 status
mt -f /dev/rmt/tps2d4 status

cd 2d1
#../robot_fd_xfer.sh 1000 DECDLT DE01 CA2502 /raid/enstore/random/200MB.trand  /dev/rmt/tps2d1 rip10 >2d1.log 2>&1 &
cd ../2d2
../robot_fd_xfer.sh 1000 DECDLT DE02 CA2504 /raid/enstore/random/300MB.trand /dev/rmt/tps2d2 rip10 >2d2.log 2>&1 &
cd ../2d3
../robot_fd_xfer.sh  800 DECDLT DE14 CA2508 /raid/enstore/random/400MB.trand /dev/rmt/tps2d3 rip3 >2d3.log 2>&1 &
cd ../2d4
../robot_fd_xfer.sh  800 DECDLT DE13 CA2506 /raid/enstore/random/200MB.trand /dev/rmt/tps2d4 rip3 >2d4.log 2>&1 &


