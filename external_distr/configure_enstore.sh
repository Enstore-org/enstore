#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

. /usr/local/etc/setups.sh
if [ $? -ne 0 ]
then 
    echo setup failed
    exit 1
fi

config_host=`echo $ENSTORE_CONFIG_HOST | cut -f1 -d\.`
this_host=`uname -n | cut -f1 -d\.`

if [ $this_host != $config_host ]
then
    echo You must run this script on ${config_host} only
    exit 1
fi

# check if config file exists
if [ ! -f $ENSTORE_CONFIG_FILE ]; then
    echo "enstore configuratrion file $ENSTORE_CONFIG_FILE does not exist"
    exit 1
fi
if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

rm -f ${FARMLETS_DIR}/*
if [ ! -d ${FARMLETS_DIR} ]; then
    mkdir ${FARMLETS_DIR}
fi

echo $config_host > ${FARMLETS_DIR}/${config_host}
echo $config_host > ${FARMLETS_DIR}/enstore
echo $config_host > ${FARMLETS_DIR}/servers



# get the rest of the servers

$ENSTORE_DIR/external_distr/extract_config_parameters.py server | while read server; do
    if [ $server != $config_host ]; then
	echo $server >> ${FARMLETS_DIR}/enstore
	echo $server >> ${FARMLETS_DIR}/servers
	echo $server > ${FARMLETS_DIR}/${server}
    fi
done

# get movers
$ENSTORE_DIR/external_distr/extract_config_parameters.py mover | while read mover; do
    grep $mover ${FARMLETS_DIR}/enstore
    if [ $? -ne 0 ]; then
	echo $mover >> ${FARMLETS_DIR}/enstore
    fi
    echo $mover >> ${FARMLETS_DIR}/movers
    echo $mover > ${FARMLETS_DIR}/${mover}

done

# reverse enstore to get enstore-down
tac ${FARMLETS_DIR}/enstore > ${FARMLETS_DIR}/enstore-down

