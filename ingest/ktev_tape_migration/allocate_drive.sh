#! /bin/sh
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
. /usr/local/etc/setups.sh
setup ocs
while /bin/true; do
    allocated=`ocs_allocate`
    if [ $? -ne 0 ]; then
	echo "can not allocate tape drive"
	exit 1
    fi
    ocs_drive=`echo $allocated | awk '{print $2}'`
    if [ -f "bad_drives" ]; then
	bad_drives=`cat bad_drives`
	for bad_drive in $bad_drives
	do
	    if [ $ocs_drive = $bad_drive ]; then
		echo "do not like this drive, will allocate another"
		ocs_deallocate -t $ocs_drive
		ocs_drive=
		break
	    fi
	done
    fi
    if [ -n $ocs_drive ]; then
	break
    fi
done
echo $ocs_drive
exit 0
