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
if [ ! -d $dir ]; then mkdir $dir; fi
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

LOGFILE=$E_H/dcache-log/real-encp.log
ERROR=$E_H/dcache-log/real-encp-error.log
SUCCESS=$E_H/dcache-log/real-encp-success.log

args="$*"
say() { if [ -n "${LOGFILE-}" ]; then  echo $version `date` ${node:-nonode} ${command:-nocmd} ${pnfsid:-noid} ${filepath:-nofilepath} $* >> $LOGFILE; fi
                                       echo $version `date` ${node:-nonode} ${command:-nocmd} ${pnfsid:-noid} ${filepath:-nofilepath} $*
      }
sayE() { if [ -n "${ERROR}" ];   then  echo $version `date` ${node:-nonode} ${command:-nocmd} ${pnfsid:-noid} ${filepath:-nofilepath} $* >> $ERROR; fi
      }
sayS() { if [ -n "${ERROR}" ];   then  echo $version `date` ${node:-nonode} ${command:-nocmd} ${pnfsid:-noid} ${filepath:-nofilepath} $* >> $SUCCESS; fi
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
#. $E_H/dcache-deploy/scripts/encp.options # this sets variable options
options="--verbose=4 --threaded --ecrc --bypass-filesystem-max-filesize-check --resubmit-timeout 1800"

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
   t0=`date +"%s"`
   py_file="/tmp/${si_bfid}_$$.py"
   enstore info --file ${si_bfid} 1> ${py_file} 2>/dev/null
   rc=$?
   if [ $rc -eq 0 ]; then
       cache_location=`python -c '
import string
import sys
try:
  f=open("'${py_file}'","r")
  code="d=%s"%(string.join(f.readlines(),""))
  f.close()
  exec(code)
  if d["cache_status"] == "CACHED":
     print d["cache_location"]
  else:
     sys.exit(1)
except:
  sys.exit(1)
'`
       rc=$?
       rm -f ${py_file}
       if [ $rc -eq 0 -a "${cache_location}" != "" ]; then

	   krbdir="/usr/krb5/bin"
	   defaultDomain=".fnal.gov"
	   host=`uname -n`

	   if expr $host : '.*\.' >/dev/null;then
	       thisHost=$host;
	   else
	       thisHost=${host}${defaultDomain};
	   fi

	   OLDKRB5CCNAME=${KRB5CCNAME:-NONE}
	   KRB5CCNAME=/tmp/krb5cc_root_$$;export KRB5CCNAME
	   ${krbdir}/kinit -k host/${thisHost}

	   rc=0
	   #
	   # TODO: in the future need to get names from configuration
	   #
	   for node in pagg01 pagg02;
	     do
	     scp -o StrictHostKeyChecking=no -c blowfish root@${node}:${cache_location} $filepath
	     rc=$?
	     if [ $rc -eq 0 ]; then
		 break
	     fi
	   done
	   if [ $rc -eq 0 ]; then
	       crc=`ecrc $filepath -0 $filepath | awk '/CRC/ {print $2}' | sed -e 's/0x//'`
	       if [ "${crc}" != "${uri_crc}" ]; then
		   say "CRC do not match ${crc} ${uri_crc}"
		   rm -f $filepath
	       else
		   chown ${si_uid}.${si_gid} $filepath
		   chmod 0644 $filepath
		   t1=`date +"%s"`
		   dt=`echo "${t1}-${t0}"|bc`
		   say SFA Completed transferring ${uri_size} bytes in ${dt} sec.
		   exit 0
	       fi
	   else
	       rm -f $filepath
	   fi
       fi
   fi

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

    # a user may put a file into the dcache, execute an 'rm' on the pnfs file and then
    # later when the dcache tries to upload the file there isn't a pnfs id anymore!
    # check if the file is still in the pnfs database
    x="`P_ls $pnfsid`"
    if [ $sP_ls -eq 0 ]; then

# RDK: test against /pnfs/fnal.gov/usr, /pnfs/fs/usr to avoid local mount paths
        pathtype1=`echo $si_path | grep -c "^/pnfs/fnal.gov/usr"`
        pathtype2=`echo $si_path | grep -c "^/pnfs/fs/usr"`
        let npathtypesok=${pathtype1}+${pathtype2}
        if [ ${npathtypesok} -ne 0 -a $OVERRIDE_PATH -eq 1 ]; then
          override=1; override_msg=" overriding path "
          say p9 $ENCP $options $wrapper --pnfs-mount $pnfs_root --shortcut --override-path $si_path --put-cache $pnfsid $filepath
   nice -n -3    $ENCP $options $wrapper --pnfs-mount $pnfs_root --shortcut --override-path $si_path --put-cache $pnfsid $filepath >>$LOGFILE 2>&1
        else
          override=0; override_msg=" "
          say  p10  can not find acceptable path in SI.  si_path=\"$si_path\"  Using lookup mode
          sayE p10e can not find acceptable path in SI.  si_path=\"$si_path\"  Using lookup mode
          say p11 $ENCP $options $wrapper --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath
   nice -n -3     $ENCP $options $wrapper --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath >>$LOGFILE 2>&1
        fi

        ENCP_EXIT=$?

        bfid=`P_bfid $pnfsid 2>/dev/null`
        if [ "${bfid:-notset}" = "notset" ]; then bfid="notset"; fi
        say p12 $override_msg encp $options $wrapper --pnfs-mount $pnfs_root --shortcut --override-path $si_path --put-cache $pnfsid $filepath, bfid=$bfid, rc=$ENCP_EXIT
        if [ $ENCP_EXIT -ne 0 ];then
          sayE p13 $override_msg encp $options $wrapper --pnfs-mount $pnfs_root --shortcut --override-path $si_path --put-cache $pnfsid $filepath, bfid=$bfid, rc=$ENCP_EXIT
        fi

        if [ $override -eq 1 -a $ENCP_EXIT -ne 0 -a -z "$bfid" ];then
          say  p14  retrying $pnfsid without override of \"$si_path\"
          sayE p14e retrying $pnfsid without override of \"$si_path\"
          override=0; override_msg=" "
          say p15 $ENCP $options $wrapper --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath
   nice -n -3 $ENCP $options $wrapper --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath >>$LOGFILE 2>&1
          ENCP_EXIT=$?
          bfid=`P_bfid $pnfsid 2>/dev/null`
          if [ "${bfid:-notset}" = "notset" ]; then bfid="notset"; fi
          say p16 $override_msg $ENCP $options $wrapper --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath, bfid=$bfid, rc=$ENCP_EXIT
          if [ $ENCP_EXIT -ne 0 ]; then
            sayE p17e $override_msg $ENCP $options $wrapper --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath, bfid=$bfid, rc=$ENCP_EXIT
          fi
        else
          if [ $ENCP_EXIT -ne 0 ]; then
            sayE p17e encp failed, but not retrying with non override mode: override=$override ENCP_EXIT=$ENCP_EXIT bfid=$bfid, sP_bfidedu=$sP_bfid
          fi
          x=1
        fi

    else
        say p18 "The $pnfsid file seems to have been delete from pnfs space.  We're going to stash it away in case that comes back to haunt us"
        mountHost=`mount |grep /pnfs/fs |head -n1 | cut -f1 -d":"`  # first check what machine hosts the mount - use the 1st one found
        # get the filepath from the host
        # the filepath is only the filename, not the full path.  So we'll add the date to make it unique (man I hope this code is never the difference between success and failure!)
        destinationName=$pnfs_root/usr/dcache_trash_bin/$pnfsid.`date +%s`
        say  p19 $ENCP $options $wrapper $filepath $destinationName
  nice -n -3 $ENCP $options $wrapper $filepath $destinationName >>$LOGFILE 2>&1
        ENCP_EXIT=$?
        bfid=`P_bfid $pnfsid 2>/dev/null`
        if [ "${bfid:-notset}" = "notset" ]; then bfid="notset"; fi
        say p20 user removed file... saving encp $filepath $destinationName, bfid=$bfid, rc=$ENCP_EXIT
        if [ $ENCP_EXIT -ne 0 ]; then
          say p21 user removed file... saving encp $filepath $destinationName, bfid=$bfid, rc=$ENCP_EXIT
        fi
    fi

    if [ $ENCP_EXIT -eq 0 -o $ENCP_EXIT -eq 31 ]; then
        if [ ! -n "$bfid" -o $bfid = "notset" ]; then
	   say   p21.5  ERROR: NO BFID for last successful trasnsfer $pnfsid, set ENCP_EXIT to 27 # very strange, should be impossible
	   sayE  p21.5 ERROR: NO BFID for last successful trasnsfer $pnfsid, set ENCP_EXIT to 27 # very strange, should be impossible
           ENCP_EXIT=27
        fi
    fi

    # dcache creates the pnfs entry, then when encp copies it into enstore and updates the file size pnfs. check if right
    if [ $ENCP_EXIT -eq 0 ]; then
        if [ -n "$bfid" -a $bfid != "notset" ]; then
           size=`P_size $pnfsid`
  	   if [ -z "$size" -o $size -eq 0 ]; then
           if [ -n "$filename" -a 1 -eq 2 ]; then  #THIS DOES NOT WORK WHEN IT IS NEEDED, WORKS WHEN IT IS NOT NEEDED
	     say p22 touch ".(fset)(`basename $filename`)(size)($fsize)"
  	     (cd `dirname $filename` && touch ".(fset)(`basename $filename`)(size)($fsize)"  >> $LOGFILE 2>&1 )
             say p23 touch $filename $fsize done
           fi
           fi
        else
	   say   p24  ERROR: NO BFID for last successful trasnsfer $pnfsid # very strange, impossible
	   sayE  p24e ERROR: NO BFID for last successful trasnsfer $pnfsid # very strange, impossible
        fi

    # if file is already in enstore, then deactivate request
    else
      if [ -n "$bfid" -a "$bfid" != "notset" ]; then
        say p25 checking for already in enstore $bfid
        bfid_info="`enstore file --bfid $bfid 2>/dev/null`"
        location_cookie="`echo "$bfid_info" |grep location_cookie 2>/dev/null | cut -f2 -d: 2>/dev/null |sed -e "s/'//g" -e "s/,//g" -e "s/^ //" 2>/dev/null`"
        deleted="`echo "$bfid_info" |grep deleted 2>/dev/null | cut -f2 -d: 2>/dev/null |sed -e "s/'//g" -e "s/,//g"  -e "s/^ //" 2>/dev/null `"
        if [ -n "$location_cookie" -a "$deleted" = "no" ]; then
           say  p26 $pnfsid is already in enstore, bfid $bfid_info	This is ok and not an error
           sayE p26e $pnfsid is already in enstore, bfid $bfid_info	This is ok and not an error
           #ENCP_EXIT=31  # already in enstore
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

  fi

#------------------------------------------------------------------------------------------
else
  say  $0 $args  Command not yet supported: $command
  sayE $0 $args  Command not yet supported: $command
  exit 5

fi
#------------------------------------------------------------------------------------------

say  ERROR $0 $args HOW DID WE GET HERE
exit 99
