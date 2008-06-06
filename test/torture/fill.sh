#!/bin/bash 
#############################################################
#
#  $Id$
#
#############################################################

pnfs_path="${1:-/pnfs/cdfen/test/sl8500/test/}"
node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
suffix="10"
mail="${ENSTORE_MAIL:-litvinse@fnal.gov}"
#suffix="6 7 8 9 10"
#suffix="1 2 3 4"

i=0
while [ $i -le 100 ] 
do
for s in ${suffix}
do
  file_family=${node}_${s}
  # destination=/pnfs/cdfen/test/sl8500/${node}/${s}
  destination=${pnfs_path}${node}/${s}
  if [ ! -e ${destination} ] 
  then 
      mkdir -p ${destination}
      cd $destination
      enstore pnfs --file_family=${file_family}
      enstore pnfs --file_family_width=1
      cd -  > /dev/null 2>&1
  fi
  name=${node}_4000_`date +"%s"`.data
  sz=`gauss 4000|awk '{ print $1}'`
  createfile $sz $name 
  encp --threaded $name $destination
  if [ $? -ne 0 ]
      then
      /bin/mail -s "message from ${node}: encp ${name} ${destination} failed" $mail
      rm $name
      exit 1
  fi
  rm ${name}
  sleep 1
done
let i=i+1
done 
/bin/mail -s "WRITE TEST COMPLETED ON NODE ${node}: " $mail
exit 0
