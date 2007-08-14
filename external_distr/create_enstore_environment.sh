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
if [ "${1:-x}" = "fnal" ]; then export fnal=$1; shift; else fnal="";fi
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

if [ -n $fnal ]; then
    rm -rf $ENSTORE_HOME/enstore
    ln -s $ENSTORE_DIR $ENSTORE_HOME/enstore
fi
if [ ! -r $ENSTORE_HOME/site_specific/config/setup-enstore ]
then
    # this allows to not run this script on remote nodes.
    $ENSTORE_DIR/external_distr/create_setup_file.sh $fnal
else
    source $ENSTORE_HOME/site_specific/config/setup-enstore
    
fi

echo "Creating .bashrc"
cp $ENSTORE_DIR/external_distr/.bashrc $ENSTORE_HOME
chown enstore.enstore $ENSTORE_HOME/.bashrc
 
echo "Copying $ENSTORE_DIR/external_distr/setups.sh to /usr/local/etc"
if [ ! -d "/usr/local/etc" ]
then
    mkdir -p /usr/local/etc
else
    d=`date +%F.%R`
    mv -f /usr/local/etc/setups.sh /usr/local/etc/setups.sh.$d
fi
    
sed -e "s?e_dir=?e_dir=$ENSTORE_HOME?" $ENSTORE_DIR/external_distr/setups.sh > /usr/local/etc/setups_rpm.sh
ln -s /usr/local/etc/setups_rpm.sh /usr/local/etc/setups.sh

exit 0
