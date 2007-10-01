#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
place="${1:-ftp://ssasrv1.fnal.gov/en/enstore_related}"
. /usr/local/etc/setups.sh
setup enstore

echo "installing enstore_html"
/sbin/service httpd stop
rpm -U --force --nodeps ${place}/enstore_html-1.0-0.i386.rpm

rpm -q postgesql > /dev/null
if [ $? -ne 0 ]; 
then
    echo "installing postgres"
rpm -U --force ${place}/postgresql-libs-8.2.4-1PGDG.i686.rpm ${place}/postgresql-8.2.4-1PGDG.i686.rpm ${place}/postgresql-server-8.2.4-1PGDG.i686.rpm ${place}/postgresql-devel-8.2.4-1PGDG.i686.rpm
fi

echo "installing pnfs"
if [ -r /etc/rc.d/init.d/pnfs ];
then
    chmod 755 /etc/rc.d/init.d/pnfs
    /etc/rc.d/init.d/pnfs stop
    if [ -x /etc/rc.d/init.d/postgres ]; then /etc/rc.d/init.d/postgres stop; fi
    #/etc/rc.d/init.d/postgresql stop
fi
rpm -U --force ${place}/pnfs-3.1.10-1f.i386.rpm
# complete after install pnfs configuration
# copy setup

$ENSTORE_DIR/external_distr/extract_config_parameters.py pnfs_server | cut -f1,2 -d\: --output-delimiter=" " > /tmp/pnfs_conf.tmp
while read f1 f2; do eval pnfs_${f1}=$f2; done < /tmp/pnfs_conf.tmp
rm -rf install_database.tmp
echo pnfs host: ${pnfs_host}
this_host=`uname -n`
if [ $this_host = $pnfs_host ];
then
    echo "Configuring this host to run postgres"
    /sbin/chkconfig postgresql on
    echo "Starting postges"
    /etc/init.d/postgresql start
    echo "Configuring this host to run pnfs server"
    /sbin/chkconfig pnfs add
    /sbin/chkconfig pnfs on
    #echo "Starting pnfs"   # do not start pnfs as it will crash if there is no database
    #/etc/init.d/pnfs start
fi
echo "Creating pnfsSetup"
case $this_host in
	stken*)
	    pnfsSetup_file=stken-pnfsSetup
	    ;;
	d0en*)
	    pnfsSetup_file=d0en-pnfsSetup
	    ;;
	cdfen*)
	    pnfsSetup_file=cdfen-pnfsSetup
	    ;;
esac
if [ ! -d /usr/etc ];then mkdir /usr/etc;fi
if [ ! -r /usr/etc/pnfsSetup ]; then cp ${ENSTORE_DIR}/etc/${pnfsSetup_file} /usr/etc/pnfsSetup; fi
if [ ! -r /usr/etc/pnfsSetup.sh ]; then ln -s /usr/etc/pnfsSetup /usr/etc/pnfsSetup.sh; fi

echo "Enabling Enstore log directory"
$ENSTORE_DIR/external_distr/extract_config_parameters.py log_server | cut -f1,2 -d\: --output-delimiter=" " > /tmp/pnfs_conf.tmp
while read f1 f2; do eval ${f1}=$f2; done < /tmp/log_conf.tmp
rm -rf /tmp/log_conf.tmp

chown -R enstore.enstore `dirname $logfile_path`
