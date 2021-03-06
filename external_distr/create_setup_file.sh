#!/bin/sh 
###############################################################################
#
# $Id$
#
###############################################################################

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi
if [ "${1:-x}" = "fnal" ]; then export fnal=1; shift; else fnal=0;fi
if [ "${1:-x}" = "demo" ]; then export demo=1; shift; else demo=0;fi

if [ "${ENSTORE_VERBOSE:-x}" != "x" ]; then
    set -xv
fi 

echo "Creating setup-enstore file"
if [ "`whoami`" != "root" ]
then
    echo You need to run this script as user "root"
    exit 1
fi
this_host=`uname -n`
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
PATH=/usr/sbin:$PATH
ENSTORE_HOME=`ls -d ~enstore`

if [ -s $ENSTORE_HOME/site_specific/config/setup-enstore ]
then 
    echo "$ENSTORE_HOME/site_specific/config/setup-enstore exists."
    echo "If you want to recreate it you need to delete existing file before running this script"
    exit 0
fi

if [ $fnal -eq 0 ]; then
    if [ "${ENSTORE_CONFIG_HOST:-x}" = "x" ]; then 
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
    fi
else
    if [ "${ENSTORE_CONFIG_HOST:-x}" = "x" ]; then 
	ENSTORE_CONFIG_HOST=`$ENSTORE_DIR/ups/chooseConfig`
    fi

    #kdestroy
    KRB5CCNAME=/tmp/krb5cc_enstore_$$;export KRB5CCNAME
    defaultDomain=".fnal.gov"

    # we need the full domain name, if no domain is there, add default one on

    if expr $this_host : '.*\.' >/dev/null;then 
       thisHost=$this_host;
    else 
       thisHost=${this_host}${defaultDomain};
    fi
    if [ -d "/fnal" ];
    then
	prefix="/fnal"
    else
	prefix="/local"
    fi
    kinit -k -t ${prefix}/ups/kt/enstorekt enstore/cd/${thisHost}
    # change permissions for credentials file
    cred_f=`echo $KRB5CCNAME | cut -f2 -d\:`
    if [ $? -eq 0 ]; then
	chmod 666 $cred_f
    fi
fi


if [ $this_host != $ENSTORE_CONFIG_HOST ];
then
    echo "trying to get setup-enstore from enstore configuration host"
    scp -rp enstore\@$ENSTORE_CONFIG_HOST:$ENSTORE_HOME/site_specific/ $ENSTORE_HOME
    if [ $? -ne 0 ]; then
	echo "scp failed. Please enable its ssh access to ${ENSTORE_CONFIG_HOST} and rerun this script"
 	exit 1
    fi
    echo "trying to get .bashrc and .bash_profile from enstore configuration host"
    scp -p enstore\@$ENSTORE_CONFIG_HOST:$ENSTORE_HOME/.bashrc $ENSTORE_HOME
    scp -p enstore\@$ENSTORE_CONFIG_HOST:$ENSTORE_HOME/.bash_profile $ENSTORE_HOME
    if [ $fnal -ne 0 ]
    then
	if [ ! -f $ENSTORE_HOME/.k5login ]
	then
	    scp -p enstore\@$ENSTORE_CONFIG_HOST:$ENSTORE_HOME/.k5login $ENSTORE_HOME
	fi
    fi
    if [ -r $ENSTORE_HOME/site_specific/config/setup-enstore ];
    then
	exit 0
    fi
else
    if [ $fnal -ne 0 ]
    then
	if [ ! -f $ENSTORE_HOME/.bashrc ];
	then
	    echo "installing .bashrc from $ENSTORE_DIR/etc/enstore.bashrc"
	    cp -p $ENSTORE_DIR/etc/enstore.bashrc $ENSTORE_HOME/.bashrc
	fi
        if [ ! -f $ENSTORE_HOME/.bash_profile ];
 	then
            echo "installing .bash_profile from $ENSTORE_DIR/etc/enstore.bash_profile" 
	    cp -p $ENSTORE_DIR/etc/enstore.bash_profile $ENSTORE_HOME/.bash_profile
	fi
    fi

    
fi



echo "Copying $ENSTORE_DIR/external_distr/setup-enstore to $ENSTORE_HOME/site_specific/config"

if [ ! -d $ENSTORE_HOME/site_specific/config ]
then
    su enstore -c "cp -rp $ENSTORE_DIR/site_specific $ENSTORE_HOME"
    rm -rf $ENSTORE_HOME/site_specific/config/setup-enstore
fi

cp $ENSTORE_DIR/external_distr/setup-enstore $ENSTORE_HOME/site_specific/config/setup-enstore

echo "Finishing configuration of $ENSTORE_HOME/site_specific/config/setup-enstore"
echo "export ENSTORE_CONFIG_HOST=${ENSTORE_CONFIG_HOST}" >> $ENSTORE_HOME/site_specific/config/setup-enstore

if [ $fnal -eq 0 ]; then
    if [ "${ENSTORE_CONFIG_HOST:-x}" = "x" ]; then
	read -p "Enter ENSTORE configuration server port [7500]: " REPLY
	if [ -z "$REPLY" ]
	then 
	    REPLY=7500
	fi
    else
	REPLY=7500
    fi
else
    REPLY=7500
fi

copy_conf=""
config_file=""    
echo "export ENSTORE_CONFIG_PORT=${REPLY}"
echo "export ENSTORE_CONFIG_PORT=${REPLY}" >> $ENSTORE_HOME/site_specific/config/setup-enstore

if [ $fnal -eq 0 ]; then
    if [ $demo -ne 0 ];then
	REPLY=${ENSTORE_HOME}/site_specific/config/enstore_system.conf
    else
	read -p "Enter ENSTORE configuration file location [${ENSTORE_HOME}/site_specific/config/enstore_system.conf]: " REPLY
	if [ -z "$REPLY" ]
	then
	    REPLY=${ENSTORE_HOME}/site_specific/config/enstore_system.conf
	fi
	config_file=$REPLY
	read -p "Copy config file from another location [path or CR] :" copy_conf
    fi
    
else
    if [ "${ENSTORE_USER_DEFINED_CONFIG_FILE:-x}" = "x" ]
    then
	ff=`$ENSTORE_DIR/ups/chooseConfig file`
	R=${ENSTORE_HOME}/enstore/etc/${ff}
	su enstore -c "cd `dirname $R`; cvs update `basename $R`"
	REPLY="\$ENSTORE_HOME/enstore/etc/${ff}"
    else
	REPLY=${ENSTORE_USER_DEFINED_CONFIG_FILE}
    fi
fi

echo "export ENSTORE_CONFIG_FILE=${REPLY}"
echo "export ENSTORE_CONFIG_FILE=${REPLY}" >> $ENSTORE_HOME/site_specific/config/setup-enstore

if [ $fnal -eq 0 ]; then
    if [ $demo -ne 0 ];then
	REPLY=root@localhost
    else
	read -p "Enter ENSTORE mail address: " REPLY
    fi
else
    mm=`$ENSTORE_DIR/ups/chooseConfig mail`
    REPLY="$mm"
fi

echo "export ENSTORE_MAIL=${REPLY}"
echo "export ENSTORE_MAIL=${REPLY}" >> $ENSTORE_HOME/site_specific/config/setup-enstore

#read -p "Enter ENSTORE web site directory: " REPLY
#echo "export ENSTORE_WWW_DIR=${REPLY}"
#echo "export ENSTORE_WWW_DIR=${REPLY}" >> $ENSTORE_DIR/config/setup-enstore
#if [ ! -d ${REPLY} ]
#then
#    echo "creating ${REPLY}"
#    mkdir -p ${REPLY}
#fi

if [ $fnal -eq 0 ]; then
    if [ $demo -ne 0 ];then
	REPLY="/usr/local/etc/farmlets"
    else
	read -p "Enter ENSTORE farmlets dir [/usr/local/etc/farmlets]: " REPLY
	if [ -z "$REPLY" ]
	then
	    REPLY="/usr/local/etc/farmlets"
	fi
    fi
else
    REPLY="/usr/local/etc/farmlets"
fi

echo "export FARMLETS_DIR=${REPLY}"
echo "export FARMLETS_DIR=${REPLY}" >> $ENSTORE_HOME/site_specific/config/setup-enstore
if [ ! -d ${REPLY} ]
then
    echo "creating ${REPLY}"
    mkdir -p ${REPLY}
fi
if [ $fnal -eq 0 ]; then
    echo "Assuming that ssh and scp wil be used for remote commands"
   ENSSH=`which ssh`
   ENSCP=`which scp`
fi

if [ "${ENSSH:-x}" != "x" ]
then
  echo "export ENSSH=${ENSSH}"
  echo "export ENSSH=${ENSSH}" >> $ENSTORE_HOME/site_specific/config/setup-enstore
fi
if [ "${ENSCP:-x}" != "x" ]
then
  echo "export ENSCP=${ENSCP}"
  echo "export ENSCP=${ENSCP}" >> $ENSTORE_HOME/site_specific/config/setup-enstore
fi

if [ "${copy_conf:-x}" != "x" ]
then
    echo "Creating config file from $copy_conf"
    cp -f $copy_conf $config_file
fi
    
chown -R enstore.enstore  $ENSTORE_HOME
if [ $fnal -ne 0 ]
then
    kdestroy
fi
echo "
Please check $ENSTORE_HOME/site_specific/config/setup-enstore.
In case you are going to use ssh for product distribution, updates and maintenance you need to add the following entries
to $ENSTORE_HOME/site_specific/config/setup-enstore:
ENSSH=<path to ssh binary>
ENSCP=<path scp bynary>

Now you can proceed with enstore configuration.

For the system recommendations and layout please read ${ENSTORE_DIR}/etc/configuration_recommendations.
To create a system configuration file you can use ${ENSTORE_DIR}/etc/enstore_configuration_template
or refer to one of real enstore configuration files, such as ${ENSTORE_DIR}/etc/stk.conf
After the enstore configuration file has has been created you can proceed 
with ${ENSTORE_DIR}/external_distr/make_farmlets.sh.
After farmlets are created you can proceed with distributed configuration of enstore 
and complete its installation with ${ENSTORE_DIR}/external_distr/configure_enstore.sh"
 
