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
if [ ! -r $ENSTORE_HOME/site_specific/config/setup-enstore ]
then
    # this allows to not run this script on remote nodes.
    $ENSTORE_DIR/external_distr/create_setup_file.sh $fnal
fi
source $ENSTORE_HOME/site_specific/config/setup-enstore

echo "Creating .bashrc"
if [ $fnal = "fnal" ];
then 
    ENSTORE_CONFIG_HOST=`$ENSTORE_DIR/ups/chooseConfig`
    kdestroy
    KRB5CCNAME=/tmp/krb5cc_enstore_$$;export KRB5CCNAME
    defaultDomain=".fnal.gov"

    # we need the full domain name, if no domain is there, add default one on

    if expr $this_host : '.*\.' >/dev/null;then 
       thisHost=$this_host;
    else 
       thisHost=${this_host}${defaultDomain};
    fi
    kinit -k -t /local/ups/kt/enstorekt enstore/cd/${thisHost}
    # change permissions for credentials file
    cred_f=`echo $KRB5CCNAME | cut -f2 -d\:`
    if [ $? -eq 0 ]; then
	chmod 666 $cred_f
    fi
    echo "trying to get .bashrc configuration host"
    scp -rp enstore\@$ENSTORE_CONFIG_HOST:$ENSTORE_HOME/.bashrc $ENSTORE_HOME
else
    echo "Creating .bashrc"
    cp $ENSTORE_DIR/external_distr/.bashrc $ENSTORE_HOME
fi
chown enstore.enstore $ENSTORE_HOME/.bashrc

if [ ! -d $ENSTORE_HOME/CRON ]
then
    echo "Creating $ENSTORE_HOME/CRON"
    mkdir $ENSTORE_HOME/CRON
    chown enstore.enstore $ENSTORE_HOME/CRON
fi

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
