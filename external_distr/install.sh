#!/bin/sh
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

echo " 
You need to run this script only on the enstore configuration server host."

read -p "Are you on this host?[y/N]: " REPLY
echo $REPLY
if [ "$REPLY" = "y" -o "$REPLY" = "Y" ] 
then
    ENSTORE_CONFIG_HOST=`uname -n`
else
    exit 1
fi
ENSTORE_DIR=`rpm -ql enstore_sa | head -1`

PATH=/usr/sbin:$PATH

echo "Copying $ENSTORE_DIR/external_distr/setup-enstore to $ENSTORE_DIR/config"
if [ ! -d $ENSTORE_DIR/config ]
then
    mkdir -p $ENSTORE_DIR/config
fi

cp -rpf $ENSTORE_DIR/external_distr/setup-enstore $ENSTORE_DIR/config

echo "Finishing configuration of $ENSTORE_DIR/config/setup-enstore"
echo "export ENSTORE_CONFIG_HOST=${ENSTORE_CONFIG_HOST}"
echo "export ENSTORE_CONFIG_HOST=${ENSTORE_CONFIG_HOST}" >> $ENSTORE_DIR/config/setup-enstore

read -p "Enter ENSTORE configuration server port [7500]: " REPLY
if [ -z "$REPLY" ]
then 
	REPLY=7500
fi
echo "export ENSTORE_CONFIG_PORT=${REPLY}"
echo "export ENSTORE_CONFIG_PORT=${REPLY}" >> $ENSTORE_DIR/config/setup-enstore

read -p "Enter ENSTORE configuration file location [${ENSTORE_DIR}/config/enstore.conf]: " REPLY
if [ -z "$REPLY" ]
then
    REPLY=${ENSTORE_DIR}/config/enstore.conf
fi
echo "export ENSTORE_CONFIG_FILE=${REPLY}"
echo "export ENSTORE_CONFIG_FILE=${REPLY}" >> $ENSTORE_DIR/config/setup-enstore

read -p "Enter ENSTORE mail address: " REPLY

echo "export ENSTORE_MAIL=${REPLY}"
echo "export ENSTORE_MAIL=${REPLY}" >> $ENSTORE_DIR/config/setup-enstore

#read -p "Enter ENSTORE web site directory: " REPLY
#echo "export ENSTORE_WWW_DIR=${REPLY}"
#echo "export ENSTORE_WWW_DIR=${REPLY}" >> $ENSTORE_DIR/config/setup-enstore
#if [ ! -d ${REPLY} ]
#then
#    echo "creating ${REPLY}"
#    mkdir -p ${REPLY}
#fi

read -p "Enter ENSTORE farmlets dir [/usr/local/etc/farmlets]: " REPLY
if [ -z "$REPLY" ]
then
        REPLY="/usr/local/etc/farmlets"
fi
echo "export FARMLETS_DIR=${REPLY}"
echo "export FARMLETS_DIR=${REPLY}" >> $ENSTORE_DIR/config/setup-enstore
if [ ! -d ${REPLY} ]
then
    echo "creating ${REPLY}"
    mkdir -p ${REPLY}
fi



echo "
Enstore install finished.
Please check $ENSTORE_DIR/config/setup-enstore.

Now you can proceed with enstore configuration.

For the system recommendations and layout please read ${ENSTORE_DIR}/etc/configuration_recommendations.
To create a system configuration file you can use ${ENSTORE_DIR}/etc/enstore_configuration_template
or refer to one of real enstore configuration files, such as ${ENSTORE_DIR}/etc/stk.conf"
 
