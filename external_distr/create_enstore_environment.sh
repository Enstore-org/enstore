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
if [ "${1:-x}" = "fnal" ]; then export fnal=$1; shift; else fnal="x";fi
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

if [ $fnal = "fnal" ]; then
    rm -rf $ENSTORE_HOME/enstore
    ln -s $ENSTORE_DIR $ENSTORE_HOME/enstore
fi
if [ ! -r $ENSTORE_HOME/site_specific/config/setup-enstore ];
then
    # this allows to not run this script on remote nodes.
    $ENSTORE_DIR/external_distr/create_setup_file.sh $fnal
fi
source $ENSTORE_HOME/site_specific/config/setup-enstore

if [ $fnal != "fnal" ];
then
    echo "Creating .bashrc"
    cp $ENSTORE_DIR/external_distr/.bashrc $ENSTORE_HOME
fi
chown enstore.enstore $ENSTORE_HOME/.bashrc

if [ $fnal = "fnal" ];
then
    ln -s $ENSTORE_DIR/sbin/gettkt $ENSTORE_HOME/gettkt
    chown enstore.enstore $ENSTORE_HOME/gettkt
    if [ -f $ENSTORE_DIR/etc/xinetd.conf ]
    then
	cp -f $ENSTORE_DIR/etc/xinetd.conf /etc/xinetd.conf
    fi
    if [ -f $ENSTORE_DIR/etc/ntp.conf ]
    then 
	cp -f $ENSTORE_DIR/etc/ntp.conf /etc/ntp.conf 
    fi

if [ ! -f $ENSTORE_HOME/.forward ]
then 
    echo "Creating $ENSTORE_HOME/.forward"
    echo $ENSTORE_MAIL > $ENSTORE_HOME/.forward
    chown enstore.enstore $ENSTORE_HOME/.forward
fi
if [ ! -f /root/.forward ]
then 
    echo "Creating /root/.forward"
    echo $ENSTORE_MAIL > /root/.forward
fi


if [ ! -d $ENSTORE_HOME/CRON ];
then
    echo "Creating $ENSTORE_HOME/CRON"
    mkdir $ENSTORE_HOME/CRON
    chown enstore.enstore $ENSTORE_HOME/CRON
fi
if [ ! -d /root/CRON ];
then
    echo "Creating /root/CRON"
    mkdir /root/CRON
fi


echo "Copying $ENSTORE_DIR/external_distr/setups.sh to /usr/local/etc"
if [ ! -d "/usr/local/etc" ];
then
    mkdir -p /usr/local/etc
else
    d=`date +%F.%R`
    mv -f /usr/local/etc/setups.sh /usr/local/etc/setups.sh.$d
fi
    
sed -e "s?e_dir=?e_dir=$ENSTORE_HOME?" $ENSTORE_DIR/external_distr/setups.sh > /usr/local/etc/setups_rpm.sh
ln -s /usr/local/etc/setups_rpm.sh /usr/local/etc/setups.sh

exit 0
