#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
set -u  # force better programming and ability to use check for not set

# check if file exist
check="${1:-x}" # check if file exist if argument is "c"
###
### Links in sbin directory
###
for file in `cat $ENSTORE_DIR/external_distr/sbin_links | fgrep -v "#"`
do
    if [ -n ${file} ]
    then
	do_update=1
	if [ $check = "-c" -a -r $ENSTORE_DIR/sbin/${file} ]
	then
	    # if we wanted to check for the existence of the file
	    # and it existed do not update it
	    do_update=0
	fi
	if [ $do_update -ne 0 ]
	then
	    rm -f $ENSTORE_DIR/sbin/${file}
	    ln -s $ENSTORE_DIR/src/${file}.py $ENSTORE_DIR/sbin/${file}
	fi
    fi
done

###
### Additional links in sbin directory for File Aggregation System
for path in `cat $ENSTORE_DIR/external_distr/file_aggregation_links | fgrep -v "#"`
do
    file=`basename $path`
    if [ -n ${file} ]
    then
	do_update=1
	if [ $check = "-c" -a -r $ENSTORE_DIR/sbin/${file} ]
	then
	    # if we wanted to check for the existance of the file
	    # and it existed do not update it
	    do_update=0
	fi
	if [ $do_update -ne 0 ]
	then
	    rm -f $ENSTORE_DIR/sbin/${file}
	    ln -s $ENSTORE_DIR/src/${path}.py $ENSTORE_DIR/sbin/${file}
	fi
    fi
done

###
### Additional links in sbin directory for wrappers
do_update=1
if [ $check = "-c" -a -r $ENSTORE_DIR/sbin/file_clerk ]
then
    # if we wanted to check for the existance of the file
    # and it existed do not update it
    do_update=0
fi
if [ $do_update -ne 0 ]
then
    rm -f $ENSTORE_DIR/sbin/file_clerk
    ln -s $ENSTORE_DIR/sbin/file_clerk_wrapper $ENSTORE_DIR/sbin/file_clerk
fi

do_update=1
if [ $check = "-c" -a -r $ENSTORE_DIR/sbin/migrator ]
then
    # if we wanted to check for the existance of the file
    # and it existed do not update it
    do_update=0
fi
if [ $do_update -ne 0 ]
then
    rm -f $ENSTORE_DIR/sbin/migrator
    ln -s $ENSTORE_DIR/sbin/migrator_wrapper $ENSTORE_DIR/sbin/migrator
fi

do_update=1
if [ $check = "-c" -a -r $ENSTORE_DIR/sbin/dispatcher ]
then
    # if we wanted to check for the existance of the file
    # and it existed do not update it
    do_update=0
fi
if [ $do_update -ne 0 ]
then
    rm -f $ENSTORE_DIR/sbin/dispatcher
    ln -s $ENSTORE_DIR/sbin/dispatcher_wrapper $ENSTORE_DIR/sbin/dispatcher
fi

###
### Additional links in bin directory
for file in `cat $ENSTORE_DIR/external_distr/bin_links | fgrep -v "#"`
do
    if [ -n ${file} ]
    then
	do_update=1
	if [ $check = "-c" -a -r $ENSTORE_DIR/bin/${file} ]
	then
	    # if we wanted to check for the existance of the file
	    # and it existed do not update it
	    do_update=0
	fi
	if [ $do_update -ne 0 ]
	then
	    rm -f $ENSTORE_DIR/bin/${file}
	    ln -s $ENSTORE_DIR/src/${file}.py $ENSTORE_DIR/bin/${file}
	fi
    fi
done
#######

do_update=1
if [ $check = "-c" -a  -r $ENSTORE_DIR/bin/enstore ]
then
    # if we wanted to check for the existence of the file
    # and it existed do not update it
    do_update=0
fi
if [ $do_update -ne 0 ]
then
    rm -f $ENSTORE_DIR/bin/enstore
    ln -s $ENSTORE_DIR/src/enstore_admin.py $ENSTORE_DIR/bin/enstore
    rm -f $ENSTORE_DIR/bin/migrate
    ln -s $ENSTORE_DIR/src/migrate_chimera.py $ENSTORE_DIR/bin/migrate
    rm -f $ENSTORE_DIR/bin/duplicate
    ln -s $ENSTORE_DIR/src/duplicate_chimera.py $ENSTORE_DIR/bin/duplicate
fi
######

do_update=1
if [ $check = "-c" -a -r $ENSTORE_DIR/sbin/enstoreCut ]
then
    # if we wanted to check for the existance of the file
    # and it existed do not update it
    do_update=0
fi
if [ $do_update -ne 0 ]
then
    rm -f $ENSTORE_DIR/sbin/enstoreCut;
    ln -s $ENSTORE_DIR/sbin/encpCut $ENSTORE_DIR/sbin/enstoreCut
fi
######

do_update=1
if [ $check = "-c" -a -r $ENSTORE_DIR/sbin/enmonitor ]
then
    # if we wanted to check for the existance of the file
    # and it existed do not update it
    do_update=0
fi
if [ $do_update -ne 0 ]
then
    rm -f $ENSTORE_DIR/sbin/enmonitor
    ln -s $ENSTORE_DIR/src/monitor_client.py  $ENSTORE_DIR/sbin/enmonitor

fi
######

do_update=1
if [ $check = "-c" -a -r $ENSTORE_DIR/bin/take_out ]
then
    # if we wanted to check for the existance of the file
    # and it existed do not update it
    do_update=0
fi
if [ $do_update -ne 0 ]
then
    rm -f $ENSTORE_DIR/bin/take_out
    ln -s $ENSTORE_DIR/bin/tape_aid_wrapper  $ENSTORE_DIR/bin/take_out

fi
######

do_update=1
if [ $check = "-c" -a -r $ENSTORE_DIR/bin/flip_tab ]
then
    # if we wanted to check for the existance of the file
    # and it existed do not update it
    do_update=0
fi
if [ $do_update -ne 0 ]
then
    rm -f $ENSTORE_DIR/bin/flip_tab
    ln -s $ENSTORE_DIR/bin/tape_aid_wrapper  $ENSTORE_DIR/bin/flip_tab

fi
######

