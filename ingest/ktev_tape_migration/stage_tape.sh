#! /bin/sh
# tape drive must be allocated before running this script
# if no tape mount is required, the tape_drive must be specified
set -- `getopt i:p:xn $*`
if [ $? != 0 ];then
    exit 1
fi
mount=1
position=0
while [ $1 != -- ]; do
    case $1 in
    -x)
	set -xv;;
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

if [ -z "${1-}" ]; then echo "usage $0 [-xn] [-p position] [-i tape_drive] tape_name output_dir"; exit 1;
else
    tape=$1
fi
shift;
if [ -z "${1-}" ]; then echo "usage $0 [-xn] [-p position] [-i tape_drive] tape_name output_dir"; exit 1;
else
    out_dir=$1
fi

script_dir=${migration_scripts:-"/home/user1/bakken/tapemigr"}
log_dir=${migration_logs:-"/home/user1/bakken/tapemigr/logs"}
. /usr/local/etc/setups.sh
setup ocs
device=`ocs_devfile -t $ocs_drive`
if [ $mount != 0 ]; then
    ./mount_tape.sh $ocs_drive $tape
    if [ $? != 0 ]; then echo mount failed;exit 1; fi
fi
if [ ! -d $out_dir/$tape ]; then
    mkdir $out_dir/$tape
fi
$script_dir/copy_tape -i $device -p $position -o $out_dir/$tape -n 1000 >> $log_dir/$tape.log
rc=$?
if [ $rc -eq 2 ]; then 
    echo tape copy completed successfully
    rc=0
fi
exit $rc



    
