#! /bin/sh

mount_tape=''
set -- `getopt i:p:xn $*`
if [ $? != 0 ];then
    exit 1
fi
mount=1
position=0
deallocate=0
while [ $1 != -- ]; do
    case $1 in
    -x)
	set -xv;;
    -d)
	deallocate=1;;
    -n)
	mount=0;;
    -p) 
	position=$2
	shift;;
    -i)
	ocs_drive=$2
	shift;;
    esac
    shift
done
shift

if [ -z "${1-}" ]; then echo "usage $0 [-xnd] [-p position] [-i tape_drive] tape_name output_dir"; exit 1;
else
    tape=$1
fi
shift;
if [ -z "${1-}" ]; then echo "usage $0 [-xnd] [-p position] [-i tape_drive] tape_name output_dir"; exit 1;
else
    out_dir=$1
fi
ppid=`ps -p $$ -o ppid='' | sed -e "s/ //g"`
script_dir=${migration_scripts:-"/home/user1/bakken/tapemigr"}
log_dir=${migration_logs:-"/home/user1/bakken/tapemigr/logs"}

if [ $position != 0 ]; then mount_tape='-n';fi
. /usr/local/etc/setups.sh
setup ocs
if [ $mount != 0 ]; then
    ocs_drive=`$script_dir/allocate_drive.sh`
    if [ $? != 0 ]; then echo "drive allocation failed"
	exit 1
    fi
    echo $ocs_drive > ocs_drive.$ppid
fi
$script_dir/stage_tape.sh $mount_tape -p $position -i $ocs_drive $tape $out_dir
rc=$? >> $log_dir/$tape.log
if [ $rc != 0 ]; then 
    echo "tape staging failed"
else 
    echo "tape was staged successfully"
    ocs_dismount -t $ocs_drive -o unload
    rc=$?
    if [ $rc != 0 ];then
	echo "ocs_dismount returned $rc"
	echo "tape dismount failed"
	exit $rc
    else
	echo "tape is dismounted"
    fi
    
    if [ $deallocate != 0 ]; then
	ocs_deallocate -t $ocs_drive
	rc=$?
	if [ $rc != 0 ];then
	    echo "ocs_deallocate returned $rc"
	else
	    echo "tape drive is deallocated"
	fi
    fi
fi
exit $rc    



    
