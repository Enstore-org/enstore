#!/bin/bash 
#############################################################
#
#  $Id$
#
#############################################################
if [ "${OSG_APP:-x}" != "x" ]
then
    . $OSG_APP/moibenko/config/setup-enstore 
fi
pnfs_path=''
if [ "${OSG_WN_TMP:-x}" = "x" ];
then 
    data='.'
else
data=${OSG_WN_TMP}/enstore_w_test
fi
ff_width=3
sg='TEST'
lib='null1'
wrap='null'
ff='test'

batch=10
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-p" ] ; then shift; pnfs_path=$1; shift; fi
if [ "${1:-}" = "-d" ] ; then shift; data=$1; shift; fi
if [ "${1:-}" = "-f" ] ; then shift; ff_width=$1; shift; fi
if [ "${1:-}" = "-g" ] ; then shift; sg=$1; shift; fi
if [ "${1:-}" = "-l" ] ; then shift; lib=$1; shift; fi
if [ "${1:-}" = "-f" ] ; then shift; ff=$1; shift; fi
if [ "${1:-}" = "-w" ] ; then shift; ff_width=$1; shift; fi
if [ "${1:-}" = "-b" ] ; then shift; batch=$1; shift; fi

if [ -z $pnfs_path ]; then pnfs_path="/pnfs/cdfen/test/sl8500/test"; fi
if [ ! -z $data ]; then data=${data}/; fi
#sizes="1 10 50 100 200 500 800 1000 2000 3000 4000 5000"
sizes="3 4 5 6 7 8 9 10 11 12"
#sizes="3 4 5 6 7 8"

if [ ! -d $data ]; then mkdir -p $data;fi
#echo  $data*$$.data
files=`ls $data*.data`
host=`hostname | cut -f 1 -d \.`
fsz=${#files}
date=`date +%F`
#echo fsz $fsz
for s in $sizes
  do
    # check anf if needed create pnfs path
    if [ "${OSG_APP:-x}" = "x" ]
	then
	    $ENSTORE_DIR/test/torture/make_pnfs_dir.py $pnfs_path/$date/${host}/${s} $sg $lib $ff $ff_width $wrap
     else	    
	$OSG_APP/moibenko/bin/make_pnfs_dir $pnfs_path/$date/${host}/${s} $sg $lib $ff $ff_width $wrap 
     fi
     cd - > /dev/null 2>&1
done

if [ $fsz -eq 0 ]; then
      for s in $sizes
      do
	i=1
	while [ $i -le $batch ] 
	do
	  #name=test_${s}_`date +"%s"`.data
	  name=${host}_test_${s}_`date +"%s"`_$$.data
	  if [ "${OSG_APP:-x}" = "x" ]
	  then
	    sz=`$ENSTORE_DIR/test/torture/gauss ${s}|awk '{ print $1}'`
	    $ENSTORE_DIR/test/torture/createfile $sz $data$name 
	  else
	    sz=`$OSG_APP/moibenko/bin/gauss ${s}|awk '{ print $1}'`
	    $OSG_APP/moibenko/bin/createfile $sz $data$name 
	  fi
	  let i=i+1
	  sleep 1
	done 
      done
fi
files=`ls $data*.data`

for s in $sizes
  do
  files=`ls $data/${host}_test_${s}*.data`
  for f in $files
    do
	if [ "${OSG_APP:-x}" = "x" ]
	then
	    $ENSTORE_DIR/bin/encp --threaded  $f $pnfs_path/$date/${host}/$s/`basename $f`.$$ &
	else
	    $OSG_APP/moibenko/bin/encp --threaded  $f $pnfs_path/$date/${host}/$s/`basename $f`.$$ &
	fi
    done
done
wait
#  rm $files
