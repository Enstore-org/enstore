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

rpm -q enstore > /dev/null
if [ $? -eq 0 ]; 
then
    ENSTORE_DIR=`rpm -ql enstore | head -1`
else
    echo "enstore rpm is not installed"
    exit 1
fi
PYTHON_DIR=$ENSTORE_DIR/Python
FTT_DIR=$ENSTORE_DIR/FTT
ENSTORE_HOME=`ls -d ~enstore`

if [ ! -d $ENSTORE_HOME/site_specific/config ];
then
    mkdir -p $ENSTORE_HOME/site_specific/config
fi
this_host=`uname -n`
ENSTORE_CONFIG_HOST=$this_host
export ENSTORE_CONFIG_HOST 

if [ ! -r $ENSTORE_HOME/site_specific/config/setup-enstore ];
then
    # this allows to not run this script on remote nodes.
    $ENSTORE_DIR/external_distr/create_setup_file.sh demo
fi
source $ENSTORE_HOME/site_specific/config/setup-enstore

echo "Creating .bashrc"
cp $ENSTORE_DIR/external_distr/.bashrc $ENSTORE_HOME
chown enstore.enstore $ENSTORE_HOME/.bashrc


echo "Copying $ENSTORE_DIR/external_distr/setups.sh to /usr/local/etc"
if [ ! -d "/usr/local/etc" ];
then
    mkdir -p /usr/local/etc
    install=1
else
install=0
    if [ -r "/usr/local/etc/setups.sh" ]; then
	grep "e_home=" /usr/local/etc/setups.sh
	##if [ $? -ne 0 ]; then
	##    # real ups setup file
	##    d=`date +%F.%R`
	##    mv -f /usr/local/etc/setups.sh /usr/local/etc/setups.sh.$d
	##    install=1
	##else
	##    # check if e_home is empty and if yes install correct value
	##    s=` grep "e_home=" /usr/local/etc/setups.sh | sed -e "s/^ *//" | sed -e "s/^[\t] *//" | cut -f2 -d"="` > /dev/null 2>&1
	##    if [ -z $s ]; then
	##	rm -rf /usr/local/etc/setups.sh
	##	install=1
	##    fi	
	##fi
    else
	install=1
    fi
fi

if [ ! -f $ENSTORE_CONFIG_FILE -a $this_host = $ENSTORE_CONFIG_HOST ];
then
    echo "will install a minimal enstore configuration file: ${ENSTORE_DIR}/etc/minimal_enstore.conf"
    echo "it can be replaced later"
    cp -p ${ENSTORE_DIR}/etc/minimal_enstore.conf $ENSTORE_CONFIG_FILE
    host_name=`uname -n | cut -f1 -d\.`
    sed -e "s?the_host=?the_host=\'$this_host\'?" $ENSTORE_CONFIG_FILE | sed -e "s?for_map=?for_map=\'$host_name\'?" > /tmp/enstore_config_file
    rm $ENSTORE_CONFIG_FILE
    mv /tmp/enstore_config_file $ENSTORE_CONFIG_FILE
fi
if [ ! -f /usr/local/etc/setups.sh -a $this_host = $ENSTORE_CONFIG_HOST ];
then
    rm -rf /usr/local/etc/setups_rpm.sh
    sed -e "s?e_home=?e_home=$ENSTORE_HOME?" $ENSTORE_DIR/external_distr/setups.sh > /usr/local/etc/setups_rpm.sh
    ln -s /usr/local/etc/setups_rpm.sh /usr/local/etc/setups.sh
fi

echo ${host_name} > ${FARMLETS_DIR}/${host_name}
echo ${host_name} > ${FARMLETS_DIR}/enstore
echo ${host_name} > ${FARMLETS_DIR}/enstore-down
chown -R enstore.enstore $ENSTORE_HOME

# Create necessary directories
if [ "${ENSTORE_OUT:-x}" != "x" ]; then
    if [ ! -d ${ENSTORE_OUT} ]; then
	mkdir -p ${ENSTORE_OUT}
	chmod 777 ${ENSTORE_OUT}
    fi
fi
html_dir=`$ENSTORE_DIR/external_distr/extract_config_parameters.py inquisitor | grep html_file | cut -f2 -d\:`
if [ -n $html_dir ]
then 
    echo "creating $html_dir"
    mkdir -p $html_dir
    chown enstore.enstore $html_dir
fi

backup_dir=`$ENSTORE_DIR/external_distr/extract_config_parameters.py backup | grep ^dir | cut -f2 -d\:`
if [ -n $backup_dir ]
then 
    echo "creating $backup_dir"
    mkdir -p $backup_dir
    chown enstore.enstore $backup_dir
fi
log_dir=`$ENSTORE_DIR/external_distr/extract_config_parameters.py log_server | grep log_file_path | cut -f2 -d\:`
if [ -n $log_dir ]
then 
    echo "creating $log_dir"
    mkdir -p $log_dir
    en_log_root_dir=`dirname ${log_dir}`
    chown -R enstore.enstore $en_log_root_dir
fi

exit 0
