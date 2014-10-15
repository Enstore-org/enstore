#!/bin/sh

set -u

OVERRIDE_PATH=1  # 1 to enable, 0 to disable the --override-path
version=m

# get example
# ~enstore/dcache-deploy/scripts/real-encp.sh  get 000200000000000000007A80  /tmp/x1 '-si=size=312;new=true;stored=false;sClass=test.dcache;cClass=-;hsm=enstore;alloc-size=309155;onerror=default;timeout=-1;flag-c=1:34677ad2;uid=5744;;path=<Unknown>;group=test;family=dcache;bfid=<Unknown>;volume=<unknown>;location=<unknown>;' -pnfs=/pnfs/fs -command=/home/enstore/dcache-deploy/scripts/real-encp2.sh

# '~enstore/dcache-deploy/scripts/real-encp.sh get 000063ADB93D23C84969B4C51C3DC3BB2CDB /diska/read-pool-1/data/000063ADB93D23C84969B4C51C3DC3BB2CDB -si=size=1073741824;new=false;stored=true;sClass=test.dcache;cClass=-;hsm=enstore;accessLatency=NEARLINE;retentionPolicy=CUSTODIAL;gid=3200;uid=8637;enstore://enstore/?volume=VOO534&location_cookie=0000_000000000_0002267&size=1073741824&file_family=dcache&original_name=/pnfs/fnal.gov/usr/test/litvinse/zero_data_lqcdsrm_dccp_1.data&map_file=&pnfsid_file=000063ADB93D23C84969B4C51C3DC3BB2CDB&pnfsid_map=&bfid=CDMS136605186900000&origdrive=stkenmvr216a:/dev/rmt/tps4d0n:1310065470&crc=0;;path=/pnfs/fnal.gov/usr/test/litvinse/zero_data_lqcdsrm_dccp_1.data;group=test;family=dcache;bfid=CDMS136605186900000;volume=VOO534;location=0000_000000000_0002267; -pnfs=/pnfs/fs -command=/usr/local/bin/real-encp.sh -uri=enstore://enstore/?volume=VOO534&location_cookie=0000_000000000_0002267&size=1073741824&file_family=dcache&original_name=/pnfs/fnal.gov/usr/test/litvinse/zero_data_lqcdsrm_dccp_1.data&map_file=&pnfsid_file=000063ADB93D23C84969B4C51C3DC3BB2CDB&pnfsid_map=&bfid=CDMS136605186900000&origdrive=stkenmvr216a:/dev/rmt/tps4d0n:1310065470&crc=0'

# put example
# /usr/local/bin/real-encp.sh put 001400000000000000B177C0 /data/write-pool-1/data/001400000000000000B177C0 '-si=size=4274832;new=true;stored=false;sClass=cms.cms4;cClass=-;hsm=enstore;path=/pnfs/fnal.gov/usr/cms/WAX/4/pnfs/fnal.gov/cms/PCP04/Digi/eg03_jets_2g_pt50170/jon.test.103;;path=<Unknown>;group=cms;family=cms4;bfid=<Unknown>;volume=<unknown>;location=<unknown>;' -pnfs=/pnfs/fs -command=/usr/local/bin/real-encp.sh

# /usr/local/bin/real-encp.sh put 00005FBD8F37A25941CBA3AB3AE25873B5E0 /diska/write-pool-1/data/00005FBD8F37A25941CBA3AB3AE25873B5E0 -si=size=83886080;new=true;stored=false;sClass=test.dcache;cClass=-;hsm=enstore;accessLatency=NEARLINE;retentionPolicy=CUSTODIAL;uid=-1;path=/pnfs/fnal.gov/usr/test/litvinse/go/fnisd1_c6f54a2e493411e2a3460019b9037377.data;gid=-1;StoreName=sql;;path=<Unknown>;group=test;family=dcache;bfid=<Unknown>;volume=<unknown>;location=<unknown>; -pnfs=/pnfs/fs -command=/usr/local/bin/real-encp.sh'
# remove example
#/usr/local/bin/real-encp.sh  remove -uri=enstore://enstore/?volume=VON589&location_cookie=0000_000000000_0075148&size=1024&file_family=dcache&original_name=/pnfs/fnal.gov/usr/eagle/dcache-tests/yujun/1kfile.1.2013Mar19145325&map_file=&pnfsid_file=0000AB4A74D3B1694EC49AA30D50211140C7&pnfsid_map=&bfid=CDMS136374786500000&origdrive=enmvr035:/dev/rmt/tps4d0n:1310260228&crc=0 -pnfs=/pnfs/fs -command=/usr/local/bin/real-encp.sh

f=/tmp/tmpOK$$
touch $f
if [ $? -eq  0 ];then
  out=/tmp/real-encp/`date +'%Y-%m-%d:%H:%M:%S'`.$$.$1.$2
  rm $f
else
  out=$E_H/tmp/real-encp/`date +'%Y-%m-%d:%H:%M:%S'`.$$.$1.$2
  mkdir $E_H/tmp 2>/dev/null
fi

dir=`dirname $out`
if [ ! -d "$dir" ]; then mkdir $dir; fi
export out
exec >>$out 2>&1 <&-

set -xv

if [ -z "${E_H:-}" ]; then
   f=/usr/local/bin/ENSTORE_HOME
   if [ -r $f ]; then
      . $f
   else
      echo "ABORT: Cannot figure out E_H path to enstore home"
      exit 1
  fi
fi

if [ -d "${LOG_DIR:-}" ]; then
    LOGFILE=$LOG_DIR/real-encp.log
    ERROR=$LOG_DIR/real-encp-error.log
    SUCCESS=$LOG_DIR/real-encp-success.log
else
    LOGFILE=$E_H/dcache-log/real-encp.log
    ERROR=$E_H/dcache-log/real-encp-error.log
    SUCCESS=$E_H/dcache-log/real-encp-success.log
fi

args="$*"
say() { if [ -n "${LOGFILE-}" ]; then  echo $version `date` ${node:-nonode} ${command:-nocmd} ${pnfsid:-noid} ${filepath:-nofilepath} $* >> $LOGFILE; fi
                                       echo $version `date` ${node:-nonode} ${command:-nocmd} ${pnfsid:-noid} ${filepath:-nofilepath} $*
      }
sayE() { if [ -n "${ERROR}" ];   then  echo $version `date` ${node:-nonode} ${command:-nocmd} ${pnfsid:-noid} ${filepath:-nofilepath} $* >> $ERROR; fi
      }
sayS() { if [ -n "${ERROR}" ];   then  echo $version `date` ${node:-nonode} ${command:-nocmd} ${pnfsid:-noid} ${filepath:-nofilepath} $* >> $SUCCESS; fi
      }

#
# returns file name for pnfs id
#
pathfinder() {
    id=$1
    fname=`head -n 1 "/pnfs/fs/.(nameof)($id)"`
    sum=$fname
    while : ; do
	id=`head -n 1 "/pnfs/fs/.(parent)($id)" 2>/dev/null`
	if [ $? -ne 0 ] ; then break ; fi
	fname=`head -n 1 "/pnfs/fs/.(nameof)($id)" 2>/dev/null`
	if [ $? -eq 0 ] ; then
	    sum=${fname}/$sum
	fi
    done
    echo $sum
}

#
# TODO : be able to extract constants from somewhere (dcache configuration)
#

DCAP_DOOR=pnfs://fndca1.fnal.gov
DCAP_PORT=24125
DCAP_URL=${DCAP_DOOR}:${DCAP_PORT}
ADMIN_DOOR=fndca.fnal.gov
ADMIN_PORT=24223

#
# check if file is online
#
dc_check() {
    pnfs_id=$1
    dccp -P -t -1 ${DCAP_URL}/${pnfs_id} > /dev/null 2>&1
}

#
# pre-stage a file
#
dc_stage() {
    pnfs_id=$1
    dccp -P ${DCAP_URL}/${pnfs_id}
}

#
# admin interface
#
TMP=/tmp/$$.cmd

cmd="ssh -1 -x -o StrictHostKeyChecking=no -i $E_H/.ssh-dcache/identity -l enstore -c blowfish -p ${ADMIN_PORT} ${ADMIN_DOOR}"

admin_interface() {
    for i in "$@"
    do
      echo "$i" >> ${TMP}
    done
    echo ".."     >> ${TMP}
    echo "logoff" >> ${TMP}
    $cmd < $TMP 2>/dev/null | tr -d '\r'
    rm -f ${TMP}
}

rc_ls() {
    admin_interface "cd PoolManager" "rc ls ${1}.*" | grep -v "PoolManager" | grep "$1"
}


atrap1() { say real-encp trapped SIGHUP; }
atrap2() { say real-encp trapped SIGINT; }
atrap3() { say real-encp trapped SIGQUIT; exit 1; }
atrap9() { say real-encp trapped SIGKILL; exit 1; }

trap atrap1 1
trap atrap2 2
trap atrap3 3
trap atrap9 9

sP_bfid=0
sP_ls=0
P_nameof() { (cd $pnfs_root >/dev/null 2>&1;  cat ".(nameof)($1)" 2>/dev/null ); }
P_bfid()   { (cd $pnfs_root >/dev/null 2>&1;  cat ".(access)($1)(1)" ); sP_bfid=$?; }
P_size()   { (cd $pnfs_root >/dev/null 2>&1; stat ".(access)($1)" 2>/dev/null| grep Size: | awk '{print $2}' ); }
P_ls ()    { (cd $pnfs_root >/dev/null 2>&1;   ls ".(access)($1)" 2>/dev/null ); sP_ls=$?; }

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

#
# attempt to find RPM encp first
#
ENCP=`which encp 2>/dev/null`
#
# above should succeed already, but just in case
# try to source encp setup file if it has failed.
#
# execute setup anytime, cuz it might have been changed
# while dcache is running
#
ENCP_SETUP_FILE=/etc/profile.d/encp.sh
if [ -r ${ENCP_SETUP_FILE} ]; then
	. ${ENCP_SETUP_FILE}
	ENCP=`which encp 2>/dev/null`
fi


if [ -z "$ENCP" ]; then
    #
    # RPM apparently has not been found. Try ups/upd
    # this means that upsupdbootstrup should be available
    #
    possibleLocations="/fnal/ups/etc/setups.sh /local/ups/etc/setups.sh  /usr/local/etc/setups.sh"
    for i in $possibleLocations; do
	if [ -r $i ]; then
	    set +u; . $i; set -u
	    break
	fi
    done
    setup encp -q dcache >/dev/null 2>&1
    ENCP=`which encp 2>/dev/null`
fi

if [ -z "$ENCP" ]; then say $0 $* Can not find encp in our path; exit 1; fi

if [  -r /etc/dcache/encp.options ]; then
    . /etc/dcache/encp.options  # this sets variable options
elif  [ -r $E_H/dcache-deploy/scripts/encp.options ]; then
    . $E_H/dcache-deploy/scripts/encp.options # this sets variable options
else
    options="--verbose=4 --threaded --ecrc --bypass-filesystem-max-filesize-check --mmap-io --buffer-size 62914560"
fi

if [ $# -lt 3 ] ;then
    say Not enough arguments  $0 $args
    exit 4
else
    command=$1
    pnfsid=$2
    filepath=$3
    shift; shift; shift;
    say  $0 $*
fi

# parse the options passed in by the dcache
pnfs_root=""
while [ $# -gt 0 ] ;do
	if expr "$1" : "-pnfs=" >/dev/null 2>&1 ; then
	    pnfs_root=`echo $1 | sed -e "s/^-pnfs=//"`
	elif expr "$1" : "-command=" >/dev/null 2>&1 ; then
	    script_command=`echo $1 | sed -e "s/^-command=//"`
	elif expr "$1" : "-si=" >/dev/null 2>&1 ; then
	    #
	    # parse "-si" option
	    #
	    si=`echo $1 | sed -e 's/^-si=//'`
	    # split into list of key=value pairs
	    parts=`echo $si | tr ";" "\n"`
	    for p in $parts;
	    do
		F1=`echo $p | cut -d= -f1| sed -e 's/-/_/g'`
		F2=`echo $p | cut -d= -f2`
		if expr "${F1}" : "enstore://enstore" > /dev/null 2>&1 ; then
		    continue
		fi
		already="`eval echo \\$si_$F1 2>/dev/null`"
		if [ "$already" == "" -o "$already" == "<Unknown>" -o "$already" == "<unknown>" ]; then
		    eval si_$F1=\"$F2\"
		fi
	    done
	elif expr "$1" : "-uri="  >/dev/null 2>&1 ; then
	    #
	    # parse -uri option
	    #
	    uri=`echo $1 | sed -e 's/^-uri=//'`
	    parts=`echo $uri | tr "&" "\n"`
	    for p in $parts;
	      do
		if expr "${p}" : "enstore://enstore" > /dev/null 2>&1 ; then
		    continue
		fi
		eval uri_$p
	    done
	fi
	shift
done


# echo 'pnfs_root     = ' ${pnfs_root:-unset}
# echo 'script_command= ' ${script_command:-unset}


# si variables:

#echo 'si_size       = ' ${si_size:-unset}
#echo 'si_new        = ' ${si_new:-unset}
#echo 'si_stored     = ' ${si_stored:-unset}
#echo 'si_sClass     = ' ${si_sClass:-unset}
#echo 'si_cClass     = ' ${si_cClass:-unset}
#echo 'si_hsm        = ' ${si_hsm:-unset}
#echo 'si_accessLatency      = ' ${si_accessLatency:-unset}
#echo 'si_retentionPolicy      = ' ${si_retentionPolicy:-unset}
#echo 'si_StoreName  = ' ${si_StoreName:-unset}
#echo 'si_gid        = ' ${si_gid:-unset}
#echo 'si_uid        = ' ${si_uid:-unset}
#echo 'si_path       = ' ${si_path:-unset}
#echo 'si_group      = ' ${si_group:-unset}
#echo 'si_family     = ' ${si_family:-unset}
#echo 'si_bfid       = ' ${si_bfid:-unset}
#echo 'si_volume     = ' ${si_volume:-unset}
#echo 'si_location   = ' ${si_location:-unset}

# uri variables

#echo 'uri_location_cookie = ' ${uri_location_cookie:-unset}
#echo 'uri_size            = ' ${uri_size:-unset}
#echo 'uri_file_family     = ' ${uri_file_family:-unset}
#echo 'uri_original_name   = ' ${uri_original_name:-unset}
#echo 'uri_map_file        = ' ${uri_map_file:-unset}
#echo 'uri_pnfsid_file     = ' ${uri_pnfsid_file:-unset}
#echo 'uri_bfid            = ' ${uri_bfid:-unset}
#echo 'uri_origdrive       = ' ${uri_origdrive:-unset}
#echo 'uri_crc             = ' ${uri_crc:-unset}

fsize=${si_size}

#remove double slashes:

si_path=`echo $si_path | sed -e "s/\/\/*/\//g"`


pathtype1=`echo $si_path | grep -c "^/pnfs/fnal.gov/usr"`
pathtype2=`echo $si_path | grep -c "^/pnfs/fs/usr"`
let npathtypesok=${pathtype1}+${pathtype2}
if [ ${npathtypesok} -ne 0 ]; then
  filename="$si_path"
else
  filename=""
fi


if [ -z "$pnfs_root" ] ;then
   say PNFS root not found in $0 $args
   exit 1
fi

# Return codes
# Return Code         Meaning                                Pool Behaviour
#                                                Into HSM                     From HSM
# 30 <= rc < 40       User defined               Deactivates request          Reports Problem to PoolManager
# 41                  No Space Left on device    Pool Retries                 Disables Pool,  Reports Problem to PoolManager
# 42                  Disk Read I/O Error        Pool Retries                 Disables Pool,  Reports Problem to PoolManager
# 43                  Disk Write I/O Error       Pool Retries                 Disables Pool,  Reports Problem to PoolManager
# All other                                      Pool Retries                 Reports Problem to PoolManager

#------------------------------------------------------------------------------------------
if [ "$command" = "get" ] ; then

#
# Check if this is SFA file and we can just copy it
#
   py_file="/tmp/${si_bfid}_$$.py"
   enstore info --file ${si_bfid} 1> ${py_file} 2>/dev/null
   rc=$?
   if [ $rc -eq 0 ]; then
       package_id=`python -c '
import string
import sys
try:
  f=open("'${py_file}'","r")
  code="d=%s"%(string.join(f.readlines(),""))
  f.close()
  exec(code)
  print d.get("package_id")
except:
  sys.exit(1)
'`
       rc=$?
       if [ ${rc} -eq 0 -a "${package_id}" != "" -a "${package_id}" != "None" -a "${package_id}" != "${si_bfid}" ]; then
	   #
	   # get package pnfsid
	   #
	   package_pnfsid=`enstore info --file ${package_id} | grep pnfsid | sed -e "s/[[:punct:]]//g" | awk '{ print $NF}'`
	   #
	   # get list of children
	   #
	   n_children=`enstore info --file ${package_id}  | grep active_package_files_count |  sed -e "s/[[:punct:]]//g"  |awk '{ print $NF}'`
	   #
	   # getting list of children overloads info server, so only do it for smallish packages
	   #
	   if [ ${n_children} -lt 50 ]; then
	       children=`enstore info --children ${package_id} --field pnfs_id`
	       #
	       # loop over packaged files and pre-stage then with dccp -P
	       #
	       for child in ${children}; do
		   if [ "${child}" !=  "${package_pnfsid}"  -a  "${child}" !=  ${pnfsid} ]; then
		       #
		       # check that the file still exists
		       #
		       if [ ! -e ${pnfs_root}/".(nameof)(${child})" ]; then
			   continue
		       fi
		       #
		       # check if file is already online
		       #
		       dc_check ${child}
		       rc=$?
		       if [ ${rc} -eq 0 ]; then
			   say "File ${child} is online "
			   continue
		       fi
		       #
		       # file is offline, check if we already have it in PoolManager queue
		       #
		       rc_ls ${child}
		       rc=$?
		       if [ $rc -eq 0 ]; then
			   #
			   # File is in PoolManager queue, continue
			   #
			   say "File ${child} is staging. Skipping "
			   continue
		       fi
		       say "Pre-staging ${child}"
		       dc_stage ${child}
		       rc=$?
		       if [ ${rc} -ne 0 ]; then
			   say "Failed to pre-stage $child"
		       fi
		   fi
	       done
	   fi
	   #
	   # continue handling original file
	   #
	   package_path=`pathfinder ${package_pnfsid}`
	   #
	   # strip leading slash from location cookie
	   #
	   file_path=`echo ${uri_location_cookie} | sed -e 's/^\///g'`
	   #
	   # dcap preload library
	   #
	   export LD_PRELOAD=/usr/lib64/libpdcap.so.1
	   #
	   # extract file from tar
	   #
	   file_dir=`dirname ${filepath}`
	   #
	   # start timer to measure transfer time
	   #
	   t0=`date +"%s"`
	   (cd ${file_dir} && tar --seek --record-size=512 --strip-components 5 --force-local -xf ${package_path} ${file_path})
	   rc=$?
	   if [ $rc -eq 0 ]; then
	       chmod 0644 $filepath
	       touch $filepath
	       t1=`date +"%s"`
	       dt=$((t1-t0))
	       say SFA Completed untarring ${uri_size} bytes in ${dt} sec.
	       exit 0
	   else
	       rm -f ${filepath}
	       say Failed to untar file ${pnfsid}, Proceed to encp it
	   fi
       fi
       unset LD_PRELOAD
   fi
   #
   # try to get file by bfid
   #
   say g1 $ENCP $options --age-time 60 --delpri 10 --skip-pnfs --get-bfid ${si_bfid} $filepath
   nice -n -3 $ENCP $options --age-time 60 --delpri 10 --skip-pnfs --get-bfid ${si_bfid} $filepath  >>$LOGFILE 2>&1
   ENCP_EXIT=$?
   say encp --get-bfid ${si_bfid} $filepath, rc=$ENCP_EXIT
   if [ $ENCP_EXIT -eq 0 ]; then
       rm -f $out
       sayS g2s get, rc=$ENCP_EXIT
       exit $ENCP_EXIT
   else
       #
       # execute normal encp, if getting by bfid failed
       #
       say  g1   $ENCP $options --age-time 60 --delpri 10 --pnfs-mount $pnfs_root --shortcut --get-cache $pnfsid $filepath
       nice -n -3   $ENCP $options --age-time 60 --delpri 10 --pnfs-mount $pnfs_root --shortcut --get-cache $pnfsid $filepath >>$LOGFILE 2>&1
       ENCP_EXIT=$?
       say encp --get-cache $pnfsid $filepath, rc=$ENCP_EXIT
       if [ $ENCP_EXIT -eq 0 ]; then
	   rm -f $out
	   sayS g2s get, rc=$ENCP_EXIT
       else
	   sayE g2e get, rc=$ENCP_EXIT
       fi
       exit $ENCP_EXIT
   fi

#------------------------------------------------------------------------------------------
elif [ "$command" = "put" ] ; then

    encp --help | egrep "\-\-enable\-redirection" >/dev/null 2>&1 && options="${options:-} --enable-redirection"
    # files that bigger than 8 GB need to use the cern wrapper, not the default cpio
    big=`expr 8 \* 1024 \* 1024 \* 1024 - 10000`
    if [ $si_size -gt $big ]; then
      say p1 "si_size=$si_size bigger than $big... user cern wrapper"
      wrapper="--file-family-wrapper cern"
    else
      wrapper=""
    fi

# RDK: test against /pnfs/fnal.gov/usr, /pnfs/fs/usr to avoid local mount paths
    pathtype1=`echo $si_path | grep -c "^/pnfs/fnal.gov/usr"`
    pathtype2=`echo $si_path | grep -c "^/pnfs/fs/usr"`
    let npathtypesok=${pathtype1}+${pathtype2}
    CMD=""
    if [ ${npathtypesok} -ne 0 -a $OVERRIDE_PATH -eq 1 ]; then
        override=1; override_msg=" overriding path "
	CMD="$ENCP $options $wrapper --pnfs-mount $pnfs_root --shortcut --override-path $si_path --put-cache $pnfsid $filepath"
        say p9 $CMD
    else
        override=0; override_msg=" "
        say  p10  can not find acceptable path in SI.  si_path=\"$si_path\"  Using lookup mode
        sayE p10e can not find acceptable path in SI.  si_path=\"$si_path\"  Using lookup mode
	CMD="$ENCP $options $wrapper --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath"
        say p11 $CMD
    fi

    nice -n -3 $CMD >>$LOGFILE 2>&1

    ENCP_EXIT=$?

    #
    # make sure bfid, and layer4 are set
    #
    bfid=`enstore sfs --bfid ${pnfsid}`
    rc=$?
    if [ ${rc} -ne 0 ]; then bfid="notset"; fi

    say p12 $override_msg $CMD, bfid=$bfid, rc=$ENCP_EXIT
    if [ $ENCP_EXIT -ne 0 ];then
        sayE p13 $override_msg $CMD, bfid=$bfid, rc=$ENCP_EXIT
    fi

    if [ $ENCP_EXIT -ne 0 ]; then
	l4=`enstore sfs --xref ${pnfsid} | wc -l`
	rc=$?
	if [ ${rc} -ne 0 ]; then l4=0; fi
      # if file is already in enstore, then deactivate request
      if [ -n "$bfid" -a "$bfid" != "notset" -a ${l4} -eq 11 ]; then
        say p25 checking for already in enstore $bfid
        bfid_info="`enstore file --bfid $bfid 2>/dev/null`"
        location_cookie="`echo "$bfid_info" |grep location_cookie 2>/dev/null | cut -f2 -d: 2>/dev/null |sed -e "s/'//g" -e "s/,//g" -e "s/^ //" 2>/dev/null`"
        deleted="`echo "$bfid_info" |grep deleted 2>/dev/null | cut -f2 -d: 2>/dev/null |sed -e "s/'//g" -e "s/,//g"  -e "s/^ //" 2>/dev/null `"
        if [ -n "$location_cookie" -a "$deleted" = "no" ]; then
           say  p26 $pnfsid is already in enstore, bfid $bfid_info	This is ok and not an error
           sayE p26e $pnfsid is already in enstore, bfid $bfid_info	This is ok and not an error
           ENCP_EXIT=0  # already in enstore
        else
          say p27 location_cookie=${location_cookie:-notset} deleted=${deleted:-notset} = not in enstore
        fi
      else
        say p28 bfid empty or set to \"notset\"... skipping check for already in enstore
      fi
    fi

    if [ $ENCP_EXIT -eq 0 -o $ENCP_EXIT -eq 31 ]; then
      rm -f $out;
      sayS p29s put rc=$ENCP_EXIT
    fi
    say p30 put rc=$ENCP_EXIT  $pnfsid
    if [ $ENCP_EXIT -ne 0 ];then
      sayE p31e put rc=$ENCP_EXIT
    fi
    exit $ENCP_EXIT
#------------------------------------------------------------------------------------------
else
  say  $0 $args  Command not yet supported: $command
  sayE $0 $args  Command not yet supported: $command
  exit 5
fi
#------------------------------------------------------------------------------------------

say  ERROR $0 $args HOW DID WE GET HERE
exit 99
