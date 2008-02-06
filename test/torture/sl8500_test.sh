#!/bin/bash 

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
  if [ ! -e /pnfs/cdfen/test/sl8500/test/$s ] 
  then 
      mkdir /pnfs/cdfen/test/sl8500/test/$s
  fi
  cd /pnfs/cdfen/test/sl8500/test/$s 
  enstore pnfs --file_family=test
  enstore pnfs --file_family_width=1 
  cd - > /dev/null 2>&1
  encp --threaded $files /pnfs/cdfen/test/sl8500/test/$s
  rm $files
done 
