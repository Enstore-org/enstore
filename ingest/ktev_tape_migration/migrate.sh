#! /bin/sh
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ -z "${1-}" ]; then echo "usage $0 tape_name input_dir output_dir"; exit 1;
else
    tape=$1
fi
shift
if [ -z "${1-}" ]; then echo "usage $0 tape_name input_dir output_dir"; exit 1;
else
    in_dir=$1
fi
shift
if [ -z "${1-}" ]; then echo "usage $0 tape_name input_dir output_dir"; exit 1;
else
    out_dir=$1
fi

tname2dir() { echo $1 | awk '{printf("%s/%s/%s",substr($1,1,2),substr($1,1,4),$1)
			      }'
}

make_dir() {
    d_name=$1/"`tname2dir $2`"
    if [ ! -d $d_name ]; then
	mkdir -p $d_name
	if [ $? -ne 0 ] 
	then 
	    echo Could not create $d_name
 	    exit 1
	fi
    fi
    echo $d_name
}

. /usr/local/etc/setups.sh
ppid=`ps -p $$ -o ppid='' | sed -e "s/ //g"`
script_dir=${migration_scripts:-"/home/user1/bakken/tapemigr"}
log_dir=${migration_logs:-"/home/user1/bakken/tapemigr/logs"}
setup dcap
unset DCACHE_IO_TUNNEL
setup -q stken encp

dir_name="`make_dir $out_dir $tape`"


cd $in_dir
for f in `ls -1tr`;do
    if [ -s $dir_name/$f ]; then
	in_fsize=`ls -l $f | awk '{print $5}`
	out_fsize=`enstore pnfs --layer $dir_name/$f 2 | grep "l=" | awk '{FS="=";print $3}'|awk '{FS=";";print $1}'`
	if [ $in_fsize != $out_fsize ]; then
	    echo "$in_dir/$f $in_fsize $dir_name/$f $out_fsize"
	    rc=1
	    exit $rc
	else
	    rc=0
	    continue
	fi
    fi

    retry=0
    while /bin/true; do
	dccp $f $dir_name/$f >> $log_dir/$tape.log
	rc=$?
	if [ $rc != 0 ]; then
	    if [ $retry !=2 ]; then
		sleep 10
		retry=`expr $retry + 1`
		continue
	    else
		echo "failed to copy $f"
		break
	    fi
	else
	    echo "copied $f"
	    break
	fi
    done
done

if [ $rc != 0 ];then
    exit $rc
fi
$script_dir/check_files.sh $tape $in_dir $out_dir >> $log_dir/$tape.log
rc=$?
if [ $rc -eq 0 ];then rm $in_dir/*; echo "files compared, removed from the local directory"; fi
exit $rc