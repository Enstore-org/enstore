#!/bin/bash 
#############################################################
#
#  $Id$
#
#############################################################

pnfs_path="${1:-/pnfs/cdfen/test/sl8500/test/}"
sizes="1 10 50 100 200 500 800 1000 2000 3000 4000 5000"

for s in $sizes
do
  i=0
  while [ $i -le 10 ] 
  do
    name=test_${s}_`date +"%s"`.data
    sz=`./gauss ${s}|awk '{ print $1}'`
    echo "$s $sz "
    ./createfile $sz $name 
    let i=i+1
    sleep 1 
  done
  files=`ls *.data`
  if [ ! -e $pnfs_path/$s ] 
  then 
      mkdir $pnfs_path/$s
  fi
  cd $pnfs_path/$s 
  enstore pnfs --file_family=test
  enstore pnfs --file_family_width=1 
  cd - > /dev/null 2>&1
  encp --threaded $files $pnfs_path/$s
  rm $files
done 
