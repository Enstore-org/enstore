#!/bin/sh 
###############################################################################
#
# $Id
#
###############################################################################

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi
if [ "${1:-x}" = "fnal" ]; then export fnal=1; shift; else fnal=0;fi

echo "Creating setup-enstore file"
if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

this_host=`uname -n`
rpm -q enstore > /dev/null
if [ $? -eq 0 ]; 
then
    ENSTORE_DIR=`rpm -ql enstore | head -1`
else
    ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
fi
PYTHON_DIR=`rpm -ql Python-enstore | head -1`
FTT_DIR=`rpm -ql ftt | head -1`
PATH=/usr/sbin:$PATH
ENSTORE_HOME=`ls -d ~enstore`

if [ -s $ENSTORE_HOME/site_specific/config/setup-enstore ]
then 
    echo "$ENSTORE_HOME/site_specific/config/setup-enstore exists."
    echo "If you want to recreate it you need to delete existing file before running this script"
    exit 0
fi
ENSTORE_CONFIG_HOST=`uname -n`

rm -rf /tmp/enstore_header
echo "ENSTORE_DIR=$ENSTORE_DIR" > /tmp/enstore_header
echo "PYTHON_DIR=$PYTHON_DIR" >> /tmp/enstore_header
echo "FTT_DIR=$FTT_DIR" >> /tmp/enstore_header
    
echo "ENSTORE_HOME=$ENSTORE_HOME" >> /tmp/enstore_header

rm -rf $ENSTORE_HOME/site_specific/config/setup-enstore
cat /tmp/enstore_header $ENSTORE_DIR/external_distr/setup-enstore > $ENSTORE_HOME/site_specific/config/setup-enstore

echo "Finishing configuration of $ENSTORE_HOME/site_specific/config/setup-enstore"
echo "export ENSTORE_CONFIG_HOST=${ENSTORE_CONFIG_HOST}" >> $ENSTORE_HOME/site_specific/config/setup-enstore

port=7500

echo "export ENSTORE_CONFIG_PORT=${port}"
echo "export ENSTORE_CONFIG_PORT=${port}" >> $ENSTORE_HOME/site_specific/config/setup-enstore

config_file=${ENSTORE_HOME}/site_specific/config/enstore_system.conf
cp $ENSTORE_DIR/etc/minimal_enstore.conf ${config_file}

echo "export ENSTORE_CONFIG_FILE=${config_file}"
echo "export ENSTORE_CONFIG_FILE=${config_file}" >> $ENSTORE_HOME/site_specific/config/setup-enstore

mm="enstore@"${this_host}

echo "export ENSTORE_MAIL=${mm}"
echo "export ENSTORE_MAIL=${mm}" >> $ENSTORE_HOME/site_specific/config/setup-enstore


farmlets="/usr/local/etc/farmlets"

echo "export FARMLETS_DIR=${farmlets}"
echo "export FARMLETS_DIR=${farmlets}" >> $ENSTORE_HOME/site_specific/config/setup-enstore
if [ ! -d ${farmlets} ]
then
    echo "creating ${farmlets}"
    mkdir -p ${farmlets}
fi

echo ${this_host} > ${farmlets}/${this_host}
echo ${this_host} > ${farmlets}/enstore
echo ${this_host} > ${farmlets}/enstore-down


ENSSH=/usr/bin/ssh
ENSCP=/usr/bin/scp
echo "export ENSSH=${ENSSH}"
echo "export ENSSH=${ENSSH}" >> $ENSTORE_HOME/site_specific/config/setup-enstore
echo "export ENSCP=${ENSCP}"
echo "export ENSCP=${ENSCP}" >> $ENSTORE_HOME/site_specific/config/setup-enstore
    
chown -R enstore.enstore  $ENSTORE_HOME
 
