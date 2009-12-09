#!/bin/bash 
#############################################################
#
#  $Id$
#
#############################################################
. $OSG_GRID/setup.sh
echo "$@"
#echo OSG_GRID $OSG_GRID
#echo `ls -1 $OSG_GRID/globus/bin`
#echo `ls -1 $OSG_APP`
#echo "-------------"
#echo `ls -1 $OSG_APP/moibenko`

if [ ! -d $OSG_APP/moibenko/config ]; then mkdir -p $OSG_APP/moibenko/config;fi
if [ ! -f $OSG_APP/moibenko/config/setup-enstore ]; 
then 
    #scp fnpcsrv1:/grid/app/moibenko/config/setup-enstore $OSG_APP/moibenko/config;
    $OSG_GRID/globus/bin/globus-url-copy gsiftp://fnpcsrv1/grid/app/moibenko/config/setup-enstore file://$OSG_APP/moibenko/config/
fi
if [ ! -d $OSG_APP/moibenko/bin ]; 
then 
    mkdir -p $OSG_APP/moibenko/bin
    $OSG_GRID/globus/bin/globus-url-copy gsiftp://fnpcsrv1/grid/app/moibenko/bin/ file://$OSG_APP/moibenko/bin/
    #scp fnpcsrv1:/grid/app/moibenko/bin/* $OSG_APP/moibenko/bin
    chmod 755 $OSG_APP/moibenko/bin/*
fi
$OSG_GRID/globus/bin/globus-url-copy gsiftp://fnpcsrv1/grid/app/moibenko/bin/ file://$OSG_APP/moibenko/bin/ 
#scp fnpcsrv1:/grid/app/moibenko/bin/* $OSG_APP/moibenko/bin
echo `ls -l $OSG_APP/moibenko/bin`

. $OSG_APP/moibenko/config/setup-enstore
pnfs_path=''
#if [ "${OSG_DATA:-x}" = "x" ];
if [ "${OSG_WN_TMP:-x}" = "x" ];
then 
    data=''
else
    data=${OSG_WN_TMP}/enstore_w_test
    #data=${OSG_DATA}/enstore_w_test
fi
#echo OSG_APP is ${OSG_APP}
#echo OSG_DATA is ${OSG_DATA}
ff_width=3
sg='TEST'
lib='null1'
wrap='null'
ff='test'

batch=10
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-p" ] ; then shift; pnfs_path=$1; shift; fi
if [ "${1:-}" = "-d" ] ; then shift; data=$1; shift; fi
if [ "${1:-}" = "-g" ] ; then shift; sg=$1; shift; fi
if [ "${1:-}" = "-l" ] ; then shift; lib=$1; shift; fi
if [ "${1:-}" = "-f" ] ; then shift; ff=$1; shift; fi
if [ "${1:-}" = "-w" ] ; then shift; ff_width=$1; shift; fi
if [ "${1:-}" = "-b" ] ; then shift; batch=$1; shift; fi

if [ -z $pnfs_path ]; then pnfs_path="/pnfs/cdfen/test/sl8500/test"; fi
if [ ! -z $data ]; then data=${data}/; fi
#sizes="1 10 50 100 200 500 800 1000 2000 3000 4000 5000"
#sizes="3 4 5 6 7 8 9 10 11 12"
sizes="3 4 5 6 7 8"
echo data ${data}
echo sg $sg
echo ff $ff
echo ff width $ff_width

if [ ! -d $data ]; then mkdir -p $data;fi
#echo  $data*$$.data
files=`ls $data*.data`
host=`hostname | cut -f 1 -d \.`
fsz=${#files}
date=`date +%F`
#echo fsz $fsz
for s in $sizes
  do
     # check and if needed create pnfs path
     #$ENSTORE_DIR/test/torture/make_pnfs_dir.py $pnfs_path/$date/$sg/${host}/${s} $sg $lib $ff $ff_width $wrap 
     $OSG_APP/moibenko/bin/make_pnfs_dir $pnfs_path/$date/$sg/${host}/${s} $sg $lib $ff $ff_width $wrap
     sleep .5
     cd - > /dev/null 2>&1
done

if [ $fsz -eq 0 ]; then
      for s in $sizes
      do
	i=1
	while [ $i -lt $batch ] 
	do
	  #name=test_${s}_`date +"%s"`.data
	  name=${host}_test_${s}_`date +"%s"`_$$.data
	  #sz=`~/enstore/test/torture/gauss ${s}|awk '{ print $1}'`
	  sz=`$OSG_APP/moibenko/bin/gauss ${s}|awk '{ print $1}'`
	  #echo "$$ $s $sz "
	  #~/enstore/test/torture/createfile $sz $data$name

	  $OSG_APP/moibenko/bin/createfile $sz $data$name 
	  let i=i+1
	  sleep 1
	done 
      done
fi
files=`ls $data*.data`
#uname -a
#ls -l $data*
#ls -l $data*.data

for s in $sizes
  do
  files=`ls $data/${host}_test_${s}*.data`
  for f in $files
    do
      $OSG_APP/moibenko/bin/encp --threaded  $f $pnfs_path/$date/$sg/${host}/$s/`basename $f`.$$ &
      sleep 0.5
    done
done
wait
#  rm $files
