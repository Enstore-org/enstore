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
	mkdir -p $_name
    fi
    echo $d_name
}
dir_name="$out_dir/`tname2dir $tape`"

. /usr/local/etc/setups.sh

setup -q stken encp
cd $in_dir
rc=0
for f in `ls -1tr`;do
    in_fsize=`ls -l $f | awk '{print $5}`
    #out_fsize=`enstore pnfs --layer $dir_name/$f 2 | grep "l=" | awk '{FS="=";print $3}'|awk '{FS=";";print $1}'`
    out_fsize=`enstore pnfs --layer $dir_name/$f 2 | grep "l=" | awk '{FS=";"; for (i = 1; i <= NF; i++) if ($i ~ /l=+/) print $i}' | sed -e "s/[A-Za-z:=]//g"`
    if [ $in_fsize != $out_fsize ]; then
	echo "$in_dir/$f $in_fsize $dir_name/$f $out_fsize"
	rc=1
    fi
done
exit rc
