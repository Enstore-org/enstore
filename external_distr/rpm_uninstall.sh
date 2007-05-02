#!/bin/sh
###############################################################################
#
# $Id:
#
###############################################################################

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

PATH=/usr/sbin:$PATH

. /usr/local/etc/setups.sh
echo "Removing farmlets"
rm -rf $FARMLETS_DIR

echo "Restoring /etc/rc.d/rc.local"
cp -pf /etc/rc.d/rc.local.enstore_save /etc/rc.d/rc.local
rm -rf /etc/rc.d/rc.local.enstore_save

echo "Restoring /etc/sudoers"
cp -pf /etc/sudoers.enstore_save /etc/sudoers
rm -rf /etc/sudoers.enstore_save

echo "Removing /etc/rc.d/init.d/enstore-boot"
/etc/rc.d/init.d/enstore-boot stop
rm -rf /etc/rc.d/init.d/enstore-boot

echo "Removing /etc/rc.d/init.d/enstore-db"
/etc/rc.d/init.d/enstore-db stop
rm -rf /etc/rc.d/init.d/enstore-db

echo "Cleaning /etc/rc.d/rc3.d and /etc/rc.d/rc6.d"
find /etc/rc.d/rc3.d -name "*enstore-db" | xargs rm -rf
find /etc/rc.d/rc6.d -name "*enstore-db" | xargs rm -rf
find /etc/rc.d/rc3.d -name "*enstore-boot" | xargs rm -rf
find /etc/rc.d/rc6.d -name "*enstore-boot" | xargs rm -rf


echo "Removing /usr/local/etc/setups.sh"
rm -rf /usr/local/etc/setups.sh 
echo "removing $ENSTORE_DIR/config"
rm -rf $ENSTORE_DIR/config

echo "Deleting user 'enstore'"
userdel -r enstore

echo "Deleting group 'enstore'"
groupdel enstore

exit 0
