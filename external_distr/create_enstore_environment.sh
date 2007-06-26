#!/bin/sh
###############################################################################
#
# $Id:
#
# Assuming that enstore account was created do the following
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
ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
FTT_DIR=`rpm -ql ftt | head -1`
ENSTORE_HOME=`ls -d ~enstore`

if [ ! -r $ENSTORE_HOME/site_specific/config/setup-enstore ]
then
    # this allows to not run this script on remote nodes.
    $ENSTORE_DIR/external_distr/create_setup_file.sh
else
    source $ENSTORE_HOME/site_specific/config/setup-enstore
    
fi

echo "Creating .bashrc"
cp $ENSTORE_DIR/external_distr/.bashrc $ENSTORE_HOME
chown enstore.enstore $ENSTORE_HOME/.bashrc
 
echo "Creating sudoers file"
echo "The original is saved into /etc/sudoers.enstore_save"
if [ ! -f /etc/sudoers.enstore_save ]; then
    cp /etc/sudoers /etc/sudoers.enstore_save
fi
cp /etc/sudoers.enstore_save /etc/sudoers.e
chmod 740 /etc/sudoers.e


echo "Cmnd_Alias      PYTHON  = ${PYTHON_DIR}/bin/python" >> /etc/sudoers.e
echo "Cmnd_Alias      PIDKILL = ${ENSTORE_DIR}/bin/pidkill, ${ENSTORE_DIR}/bin/pidkill_s, /bin/kill" >> /etc/sudoers.e
echo "enstore ALL=NOPASSWD:PYTHON, NOPASSWD:PIDKILL" >> /etc/sudoers.e
cp /etc/sudoers.e /etc/sudoers
chmod 440 /etc/sudoers

echo "Copying $ENSTORE_DIR/external_distr/setups.sh to /usr/local/etc"
if [ ! -d "/usr/local/etc" ]
then
    mkdir -p /usr/local/etc
else
    cp -rf /usr/local/etc/setups.sh /usr/local/etc/setups.sh.sav
fi
    
sed -e "s?e_dir=?e_dir=$ENSTORE_HOME?" $ENSTORE_DIR/external_distr/setups.sh > /usr/local/etc/setups.sh

rm -f $ENSTORE_DIR/debugfiles.list
rm -f $ENSTORE_DIR/debugsources.list

echo "Copying $ENSTORE_DIR/bin/enstore-boot to /etc/rc.d/init.d"
cp -f $ENSTORE_DIR/bin/enstore-boot /etc/rc.d/init.d
echo "Configuring the system to start enstore on boot"
/etc/rc.d/init.d/enstore-boot install
echo "Saving /etc/rc.d/rc.local to /etc/rc.d/rc.local.enstore_save"
cp -pf /etc/rc.d/rc.local /etc/rc.d/rc.local.enstore_save
echo "Copying $ENSTORE_DIR/sbin/rc.local to /etc/rc.d"
cp -f $ENSTORE_DIR/sbin/rc.local /etc/rc.d


exit 0
