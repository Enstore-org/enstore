#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set


if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

# Setup Enstore vars
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

ansible_dir=/etc/ansible

if [ ! -d ${ansible_dir} ]; then
    echo "No /etc/ansible dir: making inventory in current directory"
    ansible_dir=.
else
    ansible_dir=${ansible_dir}/enstore
    if [ ! -d ${ansible_dir} ]; then
        mkdir ${ansible_dir}
    fi
fi

ansible_file=${ansible_dir}/${ENSTORE_GANG}.yml
echo "Inventory file is ${ansible_file}"

if [ -f ${ansible_file} ]; then
    "Inventory file already exists; updating"
    update="update"
    sleep 2  # Give a chance to cancel
fi

inventory_script=$ENSTORE_DIR/external_distr/generate_ansible_inventory.py
$inventory_script -o $ansible_file -c $ENSTORE_CONFIG_FILE $update
