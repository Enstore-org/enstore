#! /bin/sh
#if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ -z "${1-}" ]; then echo "usage $0 <tape_drive> <tape_name>"; exit 1;
else
    ocs_drive=$1
    shift
fi
if [ -z "${1-}" ]; then echo "usage $0 <tape_drive> <tape_name>"; exit 1;
else
    tape=$1
fi
. /usr/local/etc/setups.sh
setup ocs
device=`ocs_devfile -t $ocs_drive`
if [ $? != 0 ]; then echo "faile to get device"; exit 1;fi
echo requesting operator to mount tape
ocs_request -t $ocs_drive -v $tape -r
if [ $? -ne 0 ]; then
    echo "tape $tape was not mounted"
    exit 1
fi
m_count=0
while /bin/true; do
    sleep 60
    mt -f $device rewind
    if [ $? -eq 0 ]; then break
    else
	m_count = `expr $m_count + 1`
	if [ $m_count -eq 5 ]; then
	    echo "tape is not online"
	    break
	fi
    fi
done
exit 0

