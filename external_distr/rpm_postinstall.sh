#!/bin/sh
#!/bin/sh
###############################################################################
#
# $Id:
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
enstore_istalled=0

PATH=/usr/sbin:$PATH

if [ -f /usr/local/etc/setups.sh ]; then
    # enstore is installed
    source /usr/local/etc/setups.sh
    enstore_istalled=1
fi
if [ -z $PYTHON_DIR ]; then
    PYTHON_DIR=`rpm -ql Python-enstore | head -1`
fi

if [ -z $ENSTORE_DIR ]; then
    ENSTORE_DIR=`rpm -ql enstore_sa | head -1`
fi

if [ -z $FTT_DIR ]; then
    FTT_DIR=`rpm -ql ftt | head -1`
fi

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

if [ $enstore_installed -eq 0 ]; then
    echo "Copying $ENSTORE_DIR/external_distr/setups.sh to /usr/local/etc"
    if [ ! -d "/usr/local/etc" ]
    then
	mkdir -p /usr/local/etc
    fi
    
    sed -e "s?e_dir=?e_dir=$ENSTORE_DIR?" $ENSTORE_DIR/external_distr/setups.sh > /usr/local/etc/setups.sh
    #cp -rpf $ENSTORE_DIR/external_distr/setups.sh /usr/local/etc

    echo "Copying $ENSTORE_DIR/external_distr/setup-enstore to $ENSTORE_DIR/config"
    if [ ! -d $ENSTORE_DIR/config ]
    then
	mkdir -p $ENSTORE_DIR/config
    fi

    rm -rf /tmp/enstore_header
    echo "ENSTORE_DIR=$ENSTORE_DIR" > /tmp/enstore_header
    echo "PYTHON_DIR=$PYTHON_DIR" >> /tmp/enstore_header
    echo "FTT_DIR=$FTT_DIR" >> /tmp/enstore_header

    rm -rf $ENSTORE_DIR/config/setup-enstore
    cat /tmp/enstore_header $ENSTORE_DIR/external_distr/setup-enstore > $ENSTORE_DIR/config/setup-enstore

    rm -f $ENSTORE_DIR/debugfiles.list
    rm -f $ENSTORE_DIR/debugsources.list
fi

echo "Copying $ENSTORE_DIR/bin/enstore-boot to /etc/rc.d/init.d"
cp -f $ENSTORE_DIR/bin/enstore-boot /etc/rc.d/init.d
echo "Configuring the system to start enstore on boot"
/etc/rc.d/init.d/enstore-boot install
echo "Saving /etc/rc.d/rc.local to /etc/rc.d/rc.local.enstore_save"
cp -pf /etc/rc.d/rc.local /etc/rc.d/rc.local.enstore_save
echo "Copying $ENSTORE_DIR/sbin/rc.local to /etc/rc.d"
cp -f $ENSTORE_DIR/sbin/rc.local /etc/rc.d

if [ $enstore_installed -eq 0 ]; then
echo "To finish the installation you need to run $ENSTORE_DIR/external_distr/install.sh
as user root on the host where the enstore configuration server will be running and only on
this host" 
fi

exit 0
