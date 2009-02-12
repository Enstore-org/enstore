#!/bin/bash
#############################################################
#
#  $Id$
#
#############################################################
bfids=""
n_cycles=""
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-p" ] ; then shift; pnfs_path=$1; shift; fi
if [ "${1:-}" = "-b" ] ; then shift; bfids=1; bfid_list_file=$1; shift; fi
if [ "${1:-}" = "-c" ] ; then shift; n_cycles=$1; shift; fi

if [ -z $pnfs_path ]; then pnfs_path="/pnfs/cdfen/test/sl8500/test"; fi
if [ -z $n_cycles ]; then n_cycles="${1:-300}";fi

read_by_filename() { 
  node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
  #mail="${ENSTORE_MAIL:-litvinse@fnal.gov}"
  suffix="3 4 5"
  RANDOM=$$$(date +"%s")
  rm -f tmp_$$.data
  echo $n_cycles

  i=0
  while [ $i -le $n_cycles ] 
  do
    for s in $suffix 
    do 
      files=`find ${pnfs_path}/$s -name '*.data*'`
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
	       #encp --threaded $f tmp_$$.data &
	       encp --threaded $f /dev/null &
	    else
	    echo "file $f has zero length"
	    fi
	fi
      fi
    done
    let i=i+1
    echo $i
  done 
  wait
}

read_by_bf_id() {
  echo $n_cycles
  i=0
  for entry in $(cat $bfid_list_file)
  do
    echo $entry
    let i=i+1
    echo $i
    encp --get_bfid $entry /dev/null
    if [ $i -gt $n_cycles ]; then break;fi
  done
  wait
}

if [ -z $bfids ]
then
  read_by_filename
else
  read_by_bf_id
fi

#echo "sending mail"
#echo "READ TEST COMPLETED ON NODE ${node}" | /bin/mail -s "READ TEST COMPLETED ON NODE ${node}: " $mail
exit 0

