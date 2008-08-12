#!/bin/bash 
#############################################################
#
#  $Id$
#
#############################################################
pnfs_path="${1:-/pnfs/cdfen/test/sl8500/test/}";shift;
n_cycles="${1:-300}"
node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
mail="${ENSTORE_MAIL:-litvinse@fnal.gov}"
suffix="1 2 3 4"
RANDOM=$$$(date +"%s")
rm -f tmp_$$.data
echo $n_cycles

i=0
while [ $i -le $n_cycles ] 
do
  for s in $suffix 
  do 
    #files=`find ${pnfs_path}/$s/$node -name '*.data'`
    files=`find ${pnfs_path}/$s -name '*.data'`
    j=0
    lfiles=()
    for file in ${files}
    do
      lfiles[j]=$file
      let j=j+1
    done
    if [ $j -ne 0 ] 
    then 
       r=`expr \( $RANDOM % $j \)`
       f=${lfiles[$r]}
       if [ -e $f ] 
       then
          s=`stat -t $f | awk '{ print $2 }'`
          if [ $s -ne 0 ] 
          then
             encp --threaded $f tmp_$$.data
             if [ $? -ne 0 ]
             then
               /bin/mail -s "message from ${node}: encp ${f} tmp_$$.data failed ( $r $s $node )" $mail
               rm tmp_$$.data
               exit 1
             fi
             rm tmp_$$.data
          else
          echo "file $f has zero length"
         fi
       fi
    fi
  done
let i=i+1
echo $i
done 
echo "sending mail"
echo "READ TEST COMPLETED ON NODE ${node}" | /bin/mail -s "READ TEST COMPLETED ON NODE ${node}: " $mail
exit 0

