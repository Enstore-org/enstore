#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

#set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi
preun="${1:--1}"
if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

PATH=/usr/sbin:$PATH

# do as much as possible
if [ -f /usr/local/etc/setups.sh ]
then
    source /usr/local/etc/setups.sh
fi

if [ $preun -eq 0 ] # the last rpm. Remove enstore
then
    echo "Creating backup of $ENSTORE_HOME to /tmp/enstore_home.tgz"
    cd /
    tar czf /tmp/enstore_home.tgz $ENSTORE_HOME


    echo "creating backup of /usr/local/etc to /tmp/enstore_etc.tgz"
    cd /
    tar czf /tmp/enstore_etc.tgz /usr/local/etc 
    echo "Removing farmlets"
    rm -rf $FARMLETS_DIR
    echo "Restoring /etc/rc.d/rc.local"
    mv -f /etc/rc.d/rc.local.enstore_save /etc/rc.d/rc.local

    echo "Restoring /etc/sudoers"
    mv -f /etc/sudoers.enstore_save /etc/sudoers

    echo "Removing /etc/rc.d/init.d/enstore-boot"
    /etc/rc.d/init.d/enstore-boot stop
    rm -rf /etc/rc.d/init.d/enstore-boot

    echo "Cleaning /etc/rc.d/rc3.d and /etc/rc.d/rc6.d"
    find /etc/rc.d/rc3.d -name "*enstore-db" | xargs rm -rf
    find /etc/rc.d/rc6.d -name "*enstore-db" | xargs rm -rf
    find /etc/rc.d/rc3.d -name "*enstore-boot" | xargs rm -rf
    find /etc/rc.d/rc6.d -name "*enstore-boot" | xargs rm -rf


    echo "Removing /usr/local/etc/setups.sh"
    rm -rf /usr/local/etc/setups.sh 
    echo "removing $ENSTORE_HOME/site_specific/config"
    rm -rf $ENSTORE_HOME/site_specific/config

    echo "Deleting user 'enstore'"
    userdel -r enstore

    echo "Deleting group 'enstore'"
    groupdel enstore
fi
exit 0
