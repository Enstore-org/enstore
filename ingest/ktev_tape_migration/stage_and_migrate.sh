#! /bin/sh
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ -z "${1-}" ]; then echo "usage $0 tape_list input_dir output_dir"; exit 1;
else
    tape_list=$1
fi
shift
if [ -z "${1-}" ]; then echo "usage $0 tape_list input_dir output_dir"; exit 1;
else
    in_dir=$1
fi
shift
if [ -z "${1-}" ]; then echo "usage $0 tape_list input_dir output_dir"; exit 1;
else
    out_dir=$1
fi

script_dir=${migration_scripts:-"/home/user1/bakken/tapemigr"}
log_dir=${migration_logs:-"/home/user1/bakken/tapemigr/logs"}
mount_option=""
tape_drive=""
tapes=`cat $tape_list`
rc=0
for tape in $tapes
do
    $script_dir/stage.sh $mount_option $tape_drive $tape $in_dir >> $log_dir/$tape.log
    if [ $? != 0 ]; then
	echo stage of $tape failed
	exit 1
    fi
    echo $tape was staged successfully
    $script_dir/migrate.sh $tape $in_dir/$tape $out_dir >> $log_dir/$tape.log
    rc=$?
    if [ $rc != 0 ]; then
	echo migration of $tape failed
	break
    fi
    echo $tape was migrated successfully
    mount_option='-n'
    tape_drive="-i `cat ocs_drive.$$`"
done
ocs_deallocate -t `cat ocs_drive.$$`
rm ocs_drive.$$
exit $rc
   
    
    
