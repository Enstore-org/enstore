#!/bin/bash 
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

pnfs_path="${1:-/pnfs/cdfen/test/sl8500/test/}"
#sizes="1 10 50 100 200 500 800 1000 2000 3000 4000 5000"
sizes="1 2 3 4 5 6 7 8 9 10 11 12"


for s in $sizes
do
  if [ ! -e $pnfs_path/$s ] 
  then 
      mkdir $pnfs_path/$s
      cd $pnfs_path/$s 
      enstore pnfs --file_family=test
      enstore pnfs --file_family_width=3
      cd - > /dev/null 2>&1
  fi

  i=0
  while [ $i -lt 10 ] 
  do
    #name=test_${s}_`date +"%s"`.data
    name=`hostname | cut -f 1 -d \.`_test_${s}_`date +"%s"`_$$.data
    sz=`./gauss ${s}|awk '{ print $1}'`
    echo "$$ $s $sz "
    ./createfile $sz $name 
    let i=i+1
    sleep 1 
  done
  files=`ls *$$.data`
  for f in $files
  do
    encp --threaded  $f $pnfs_path/$s &
  done
  wait
  rm $files
done 
