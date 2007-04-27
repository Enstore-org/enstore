#!/bin/sh
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

PATH=/usr/sbin:$PATH
# check if user "enstore" and group "enstore "exist"

echo 'Checking if group "enstore" exists' 
grep enstore /etc/group
if [ $? -ne 0 ]; then
    echo 'Creating group "enstore"'
    groupadd -g 6209 enstore
fi
echo 'Creating user "enstore"'
useradd -u 6209 -g enstore enstore

echo "Creating sudoers file"
echo "The original is saved into /etc/sudoers.enstore_save"
if [ ! -f /etc/sudoers.enstore_save ]; then
    cp /etc/sudoers /etc/sudoers.enstore_save
fi
cp /etc/sudoers.enstore_save /etc/sudoers.e
chmod 740 /etc/sudoers.e
PYTHON_DIR=`rpm -ql Python-enstore | head -1`
ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
echo "Cmnd_Alias      PYTHON  = ${PYTHON_DIR}/bin/python" >> /etc/sudoers.e
echo "Cmnd_Alias      PIDKILL = ${ENSTORE_DIR}/bin/pidkill, ${ENSTORE_DIR}/bin/pidkill_s, /bin/kill" >> /etc/sudoers.e
echo "enstore ALL=NOPASSWD:PYTHON, NOPASSWD:PIDKILL" >> /etc/sudoers.e
cp /etc/sudoers.e /etc/sudoers
chmod 440 /etc/sudoers

echo "Copying $ENSTORE_DIR/external_distr/setups.sh to /usr/local/etc"
if [ ! -d "/usr/local/etc" ]
then
    mkdir -p /usr/local/etc
fi
cp -rpf $ENSTORE_DIR/external_distr/setups.sh /usr/local/etc

echo "Copying $ENSTORE_DIR/external_distr/setup-enstore to $ENSTORE_DIR/config"
if [ ! -d $ENSTORE_DIR/config ]
then
    mkdir -p $ENSTORE_DIR/config
fi

cp -rpf $ENSTORE_DIR/external_distr/setup-enstore $ENSTORE_DIR/config
rm -f $ENSTORE_DIR/debugfiles.list
rm -f $ENSTORE_DIR/debugsources.list

echo "Copying $ENSTORE_DIR/bin/enstore-boot to /etc/rc.d/init.d"
cp -f $ENSTORE_DIR/bin/enstore-boot /etc/rc.d/init.d
echo "Configuring the system to start enstore on boot"
`/etc/rc.d/init.d/enstore-boot install`
echo "Saving /etc/rc.d/rc.local to /etc/rc.d/rc.local.enstore_save"
cp -pf /etc/rc.d/rc.local /etc/rc.d/rc.local.enstore_save
echo "Copying $ENSTORE_DIR/external_distr/rc.local to /etc/rc.d"
cp -f $ENSTORE_DIR/external_distr/rc.local /etc/rc.d


echo "To finish the installation you need to run $ENSTORE_DIR/external_distr/install.sh
as user root on the host where the enstore configuration server will be running and only on
this host" 

exit 0
