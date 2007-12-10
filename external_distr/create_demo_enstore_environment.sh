#!/bin/sh
###############################################################################
#
# $Id$
#
# Assuming that enstore account was created do the following
#
# 
###############################################################################

#set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi
if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

PATH=/usr/sbin:$PATH

PYTHON_DIR=`rpm -ql Python-enstore | head -1`

rpm -q enstore > /dev/null
if [ $? -eq 0 ]; 
then
    ENSTORE_DIR=`rpm -ql enstore | head -1`
else
    ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
fi
ENSTORE_HOME=`ls -d ~enstore`

if [ ! -d $ENSTORE_HOME/site_specific/config ];
then
    mkdir -p $ENSTORE_HOME/site_specific/config
fi

if [ ! -r $ENSTORE_HOME/site_specific/config/setup-enstore ];
then
    # this allows to not run this script on remote nodes.
    $ENSTORE_DIR/external_distr/create_setup_file.sh
fi
source $ENSTORE_HOME/site_specific/config/setup-enstore

echo "Creating .bashrc"
cp $ENSTORE_DIR/external_distr/.bashrc $ENSTORE_HOME
chown enstore.enstore $ENSTORE_HOME/.bashrc


echo "Copying $ENSTORE_DIR/external_distr/setups.sh to /usr/local/etc"
if [ ! -d "/usr/local/etc" ];
then
    mkdir -p /usr/local/etc
else
    d=`date +%F.%R`
    mv -f /usr/local/etc/setups.sh /usr/local/etc/setups.sh.$d
fi
if [ "${fnal:-x}" != "fnal" ];
then
    this_host=`uname -n`
    if [ ! -f $ENSTORE_CONFIG_FILE -a $this_host = $ENSTORE_CONFIG_HOST ];
    then
	echo "will install a minimal enstore configuration file: ${ENSTORE_DIR}/etc/minimal_enstore.conf"
	echo "it can be replased later"
	cp -p ${ENSTORE_DIR}/etc/minimal_enstore.conf $ENSTORE_CONFIG_FILE
    fi
fi    
sed -e "s?e_home=?e_home=$ENSTORE_HOME?" $ENSTORE_DIR/external_distr/setups.sh > /usr/local/etc/setups_rpm.sh
ln -s /usr/local/etc/setups_rpm.sh /usr/local/etc/setups.sh
host_name=`uname -n | cut -f1 -d\.`
sed -e "s?the_host=?the_host=\'$host_name\'?" $ENSTORE_CONFIG_FILE | sed -e "s?for_map=?for_map=\'$this_host\'?" > /tmp/enstore_config_file
rm $ENSTORE_CONFIG_FILE
mv /tmp/enstore_config_file $ENSTORE_CONFIG_FILE

echo ${host_name} > ${FARMLETS_DIR}/${host_name}
echo ${host_name} > ${FARMLETS_DIR}/enstore
echo ${host_name} > ${FARMLETS_DIR}/enstore-down
chown -R enstore.enstore $ENSTORE_HOME

exit 0
