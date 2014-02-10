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

if [ "${ENSTORE_VERBOSE:-x}" != "x" ]; then
    set -xv
fi

if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

PATH=/usr/sbin:$PATH

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
    if [ -f $ENSTORE_DIR/etc/xinetd.conf ]
    then
	cp -f $ENSTORE_DIR/etc/xinetd.conf /etc/xinetd.conf
    fi
    if [ -f $ENSTORE_DIR/etc/ntp.conf ]
    then
	cp -f $ENSTORE_DIR/etc/ntp.conf /etc/ntp.conf
    fi
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
    install=1
else
install=0
    if [ -r "/usr/local/etc/setups.sh" ]; then
	grep "e_home=" /usr/local/etc/setups.sh
	if [ $? -ne 0 ]; then
	    # real ups setup file
	    d=`date +%F.%R`
	    mv -f /usr/local/etc/setups.sh /usr/local/etc/setups.sh.$d
	    install=1
	else
	    # check if e_home is empty and if yes install correct value
	    s=` grep "e_home=" /usr/local/etc/setups.sh | sed -e "s/^ *//" | sed -e "s/^[\t] *//" | cut -f2 -d"=" | awk '{print $2}'` > /dev/null 2>&1

	    if [ -z $s ]; then
		rm -rf /usr/local/etc/setups.sh
		install=1
	    fi
	fi
    else
	install=1
    fi
fi

if [ $install -eq 1 ]; then
    sed -e "s?e_home=?e_home=$ENSTORE_HOME?" $ENSTORE_DIR/external_distr/setups.sh > /usr/local/etc/setups_rpm.sh

    ln -s /usr/local/etc/setups_rpm.sh /usr/local/etc/setups.sh
fi
chown -R enstore.enstore $ENSTORE_HOME
if [ $fnal != "fnal" ];
then
    this_host=`uname -n`
    if [ ! -f $ENSTORE_CONFIG_FILE -a $this_host = $ENSTORE_CONFIG_HOST ];
    then
	echo "will install a minimal enstore configuration file: ${ENSTORE_DIR}/etc/minimal_enstore.conf"
	echo "it can be replaced later"
	cp -p ${ENSTORE_DIR}/etc/minimal_enstore.conf $ENSTORE_CONFIG_FILE
    fi
fi
if [ "${ENSTORE_OUT:-x}" != "x" ]; then
    if [ ! -d ${ENSTORE_OUT} ]; then
	mkdir -p ${ENSTORE_OUT}
	chmod 777 ${ENSTORE_OUT}
    fi

fi
exit 0
