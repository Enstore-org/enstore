#! /bin/sh
#  This file (mem_check.sh) was created by Ron Rechenmacher <ron@fnal.gov> on
#  Jul 16, 1998. "TERMS AND CONDITIONS" governing this file are in the README
#  or COPYING file. If you do not have such a file, one can be obtained by
#  contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#  $RCSfile$
#  $Revision$
#  $Date$

set -u

pfind () 
{ 
    ps auxw | ( ( IFS=;
    read hdr;
    echo "$hdr" );
    egrep "$@" )
}

size_file=/tmp/mem_check

rm -f $size_file
# remember to filter out encp
pfind "python.*/src/[^e]" | sed -e 's|pytho.*/src/||' -e 's| --conf.*||' | cut -c8- |
(   read ln;echo "$ln";
    while read ln;do
        pid=`expr "$ln" : '\([^ ]*\)'`
        siz=`expr "$ln" : '[^ ]* *[^ ]* *[^ ]* *\([^ ]*\)'`
        echo "$pid=$siz" >>$size_file
        echo "$ln"
    done
)

for s in `cat $size_file`;do
    pid=`expr "$s" : '\(.*\)='`
    siz=`expr "$s" : '.*=\(.*\)'`
    eval "SIZE$pid=$siz; export SIZE$pid"
    eval printenv SIZE$pid
done
rm -f $size_file

sleep_secs=1
total_sleep=1; sleep 1  # this makes numbers turn out better anyway
while true; do
    echo "`date`   seconds since inital reading: $total_sleep"
    pfind "python.*/src/[^e]" | sed -e 's|pytho.*/src/||' -e 's| --conf.*||' | cut -c8-70 |
    (   read ln;echo "$ln";
        while read ln;do
            pid=`expr "$ln" : '\([^ ]*\)'`
            siz=`expr "$ln" : '[^ ]* *[^ ]* *[^ ]* *\([^ ]*\)'`
            osiz=`printenv SIZE$pid`;
            if [ "$osiz" ];then
                change=`expr $siz - $osiz`
                echo "$ln   	change=$change"
            fi
        done
    )
    echo
    sleep $sleep_secs
    total_sleep=`expr $total_sleep + $sleep_secs`
    if [ $sleep_secs -lt 8192 ];then
        sleep_secs=`expr $sleep_secs + $sleep_secs`
    fi
done

