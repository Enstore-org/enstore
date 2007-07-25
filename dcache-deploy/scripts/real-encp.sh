#!/bin/sh 
#
# real-encp.sh
# ============
# Version L
# 2006 Sep 06  RDK  Re-sync'd to real-encp-cms.sh with the following changes:
#		1) removed one bit of CMS test related content
#		2) removed --priority switches from encp cmd line
#		3) added this commentary (thanks JonB for real-encp-cms.sh!)
# 2006 Sep 07  RDK  Remove possible pool copy as we cannot depend on there
#			being general volatile pools in all dcache systems.
#
# Version M
# 2006 Sep 12  RDK  Add E_H set check from Jon's real-encp-cms.sh
#
# Version N
 # 2006 Oct 16  RDK  Test si_path more carefully to avoid local mount point paths
#+

set -u
set +u; . /usr/local/etc/setups.sh; set -u
E_H=$ENSTORE_HOME
OVERRIDE_PATH=1  # 1 to enable, 0 to disable the --override-path
version=m

# get example
# ~enstore/dcache-deploy/scripts/real-encp.sh  get 000200000000000000007A80  /tmp/x1 '-si=size=312;new=true;stored=false;sClass=test.dcache;cClass=-;hsm=enstore;alloc-size=309155;onerror=default;timeout=-1;flag-c=1:34677ad2;uid=5744;;path=<Unknown>;group=test;family=dcache;bfid=<Unknown>;volume=<unknown>;location=<unknown>;' -pnfs=/pnfs/fs -command=/home/enstore/dcache-deploy/scripts/real-encp2.sh

# put example
# /usr/local/bin/real-encp.sh put 001400000000000000B177C0 /data/write-pool-1/data/001400000000000000B177C0 '-si=size=4274832;new=true;stored=false;sClass=cms.cms4;cClass=-;hsm=enstore;path=/pnfs/fnal.gov/usr/cms/WAX/4/pnfs/fnal.gov/cms/PCP04/Digi/eg03_jets_2g_pt50170/jon.test.103;;path=<Unknown>;group=cms;family=cms4;bfid=<Unknown>;volume=<unknown>;location=<unknown>;' -pnfs=/pnfs/fs -command=/usr/local/bin/real-encp.sh

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

set +xv
set +u; . $E_H/dcache-deploy/config/dCacheSetup; set -u # to get defaultPnfsServer and encp

ENCP=`which encp 2>/dev/null`
if [ -z "$ENCP" ]; then say $0 $* Can not find encp in our path; exit 1; fi
. $E_H/dcache-deploy/scripts/encp.options # this sets variable options

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
        #echo parsing \'$1\'
	if expr "$1" : "-pnfs=" >/dev/null 2>&1 ; then
	    pnfs_root=`echo $1 | sed -e "s/^-pnfs=//"`
	elif expr "$1" : "-command=" >/dev/null 2>&1 ; then
	    script_command=`echo $1 | sed -e "s/^-command=//"`
	elif expr "$1" : "-si=" >/dev/null 2>&1 ; then
	    si=`echo $1 | sed -e 's/^-si=//'`
            n=1 
            while [ $n -le 25 ]; do 
              F=`echo $si | cut -d\; -f$n`
	      if [ -z "$F" ]; then  n=`expr $n + 1`; continue; fi
              #DBG echo F=\"$F\"
              if [ -z "$F" ]; then break; fi
              F1=`echo $F | cut -d= -f1| sed -e 's/-/_/g'`
              F2=`echo $F | cut -d= -f2`
              already="`eval echo \\$si_$F1 2>/dev/null`"
              #DBG echo F1=$F1 F2=$F2 already=$already
              if [ "$already" == "" -o "$already" == "<Unknown>" -o "$already" == "<unknown>" ]; then
                eval si_$F1=\"$F2\"
              #DBG else 
              #DBG   echo si_$F1 already set to \"$already\", not resettting.
              fi
	      #1 x="echo \$si_$F1"
              #1 /bin/echo -n $x " = "
              #1 eval $x
              n=`expr $n + 1`
            done
	fi
	shift
done

if [ $si_size -gt 2147483647 ]; then 
  fsize=1
else
  fsize=$si_size
fi

# RDK: test against /pnfs/fnal.gov/usr, /pnfs/fs/usr to avoid local mount paths
pathtype1=`echo $si_path | grep -c "^/pnfs/fnal.gov/usr"`
pathtype2=`echo $si_path | grep -c "^/pnfs/fs/usr"`
let npathtypesok=${pathtype1}+${pathtype2}
if [ ${npathtypesok} -ne 0 ]; then
  filename="$si_path"
else
  filename=""
fi

# echo 'pnfs_root     = ' ${pnfs_root:-unset}
# echo 'script_command= ' ${script_command:-unset}
# echo 'si_size       = ' ${si_size:-unset}
# echo 'si_new        = ' ${si_new:-unset}
# echo 'si_stored     = ' ${si_stored:-unset}
# echo 'si_sClass     = ' ${si_sClass:-unset}
# echo 'si_cClass     = ' ${si_cClass:-unset}
# echo 'si_hsm        = ' ${si_hsm:-unset}
# echo 'si_alloc_size = ' ${si_alloc_size:-unset}
# echo 'si_onerror    = ' ${si_onerror:-unset}
# echo 'si_timeout    = ' ${si_timeout:-unset}
# echo 'si_flag_c     = ' ${si_flag_c:-unset}
# echo 'si_uid        = ' ${si_uid:-unset}
# echo 'si_path       = ' ${si_path:-unset}
# echo 'si_group      = ' ${si_group:-unset}
# echo 'si_family     = ' ${si_family:-unset}
# echo 'si_bfid       = ' ${si_bfid:-unset}
# echo 'si_volume     = ' ${si_volume:-unset}
# echo 'si_location   = ' ${si_location:-unset}

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

    # files that bigger than 8 GB need to use the cern wrapper, not the default cpio
    big=`expr 8 \* 1024 \* 1024 \* 1024 - 10000`
    if [ $si_size -gt $big ]; then 
      say p1 "si_size=$si_size bigger than $big... user cern wrapper"
      wrapper="--file-family-wrapper cern"
    else
      wrapper=""
    fi

    # pnfs file size may be wrong (timeouts)  Try setting to correct size
    size=`P_size $pnfsid`
    if [ -z "$size" ]; then
      dName=`P_nameof $pnfsid`  #very expensive
      if [ -z "$dName" ]; then 
        ENCP_EXIT=32
        say  p2  FILE has been deleted name=\"$dName\"
        sayE p2e FILE has been deleted name=\"$dName\"
        exit $ENCP_EXIT
      fi
    fi
    if [ -z "$size" -o $size -eq 0 ]; then
    if [ -n "$filename" -a 1 -eq 2 ]; then  #THIS DOES NOT WORK WHEN IT IS NEEDED, WORKS WHEN IT IS NOT NEEDED
      say p3 touch ".(fset)(`basename $filename`)(size)($fsize)" 
      (cd `dirname $filename` && touch ".(fset)(`basename $filename`)(size)($fsize)"  >> $LOGFILE 2>&1 )
      stat=$?
      say p4 touch $filename $fsize done
      if [ $stat -ne 0 ]; then
        say p5 touch ERROR
        ENCP_EXIT=52
        say  p6 touch rc=$ENCP_EXIT $pnfsid
        sayE p6e touch rc=$ENCP_EXIT $pnfsid
        exit $ENCP_EXIT
      fi
      size=`P_size $pnfsid`
    fi
    fi
    if [ $size -eq $fsize ]; then
      xxx=1
    else
      say p7 ERROR:  File size = $size,  si_size = $si_size
      ENCP_EXIT=52
      say  p8  size rc=$ENCP_EXIT $pnfsid File size = $size,  si_size = $si_size
      sayE p8e size rc=$ENCP_EXIT $pnfsid File size = $size,  si_size = $si_size
      exit $ENCP_EXIT
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
       
###     # Now unstick the file - bad, but bugs in dcache (1/6/06 - Bakken
###     echo `date` $E_H/dcache-deploy/scripts/unstick $pnfsid >> $LOGFILE 2>&1
###     $E_H/dcache-deploy/scripts/unstick $pnfsid >> $LOGFILE 2>&1

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

    # dcache creates the pnfs entry, then when encp copies it into enstore and updates the file size pnfs. check if right
    if [ $ENCP_EXIT -eq 0 ]; then
        if [ -n "$bfid" ]; then
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
           ENCP_EXIT=31  # already in enstore
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
