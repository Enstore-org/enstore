#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
place="${1:-ftp://ssasrv1.fnal.gov/en/enstore_related}"
psql_place=/home/moibenko
. /usr/local/etc/setups.sh
setup enstore

echo "installing enstore_html"
/sbin/service httpd stop
#rpm -U --force --nodeps ${place}/enstore_html-1.0-0.i386.rpm
rpm -U --force --nodeps /usr/src/redhat/RPMS/i386/enstore_html-1.0-0.i386.rpm

rpm -q postgesql > /dev/null
if [ $? -ne 0 ]; 
then
    echo "installing postgres"
rpm -U --force ${psql_place}/postgresql-libs-8.1.10-1PGDG.i686.rpm ${psql_place}/postgresql-8.1.10-1PGDG.i686.rpm ${psql_place}/postgresql-server-8.1.10-1PGDG.i686.rpm ${psql_place}/postgresql-devel-8.1.10-1PGDG.i686.rpm
fi

echo "installing pnfs"
if [ -r /etc/rc.d/init.d/pnfs ];
then
    chmod 755 /etc/rc.d/init.d/pnfs
    /etc/rc.d/init.d/pnfs stop
    if [ -x /etc/rc.d/init.d/postgres ]; then /etc/rc.d/init.d/postgres stop; fi
    #/etc/rc.d/init.d/postgresql stop
fi
echo "Installing pnfs"
#rpm -U --force --nodeps http://www.dcache.org/downloads/releases/pnfs-postgresql-3.1.10-7.i386.rpm
rpm -U --force ftp://enconfig1/en/lts44/i386/pnfs-3.1.10-2f.i386.rpm
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
    /sbin/service postgresql initdb
    /etc/init.d/postgresql start
    echo "Configuring this host to run pnfs server"
    /sbin/chkconfig --add pnfs
    /sbin/chkconfig pnfs on
    #echo "Starting pnfs"   # do not start pnfs as it will crash if there is no database
    #/etc/init.d/pnfs start
fi
echo "Creating pnfsSetup"
pnfsSetup_file=stken-pnfsSetup
if [ ! -d /usr/etc ];then mkdir /usr/etc;fi
if [ ! -r /usr/etc/pnfsSetup ]; then cp ${ENSTORE_DIR}/etc/${pnfsSetup_file} /usr/etc/pnfsSetup; fi
if [ ! -r /usr/etc/pnfsSetup.sh ]; then ln -s /usr/etc/pnfsSetup /usr/etc/pnfsSetup.sh; fi

#create pnfs directory
if [ ! -d /pnfs ];
then
    mkdir /pnfs
    chmod 777 /pnfs
fi
# create pnfs db area
mkdir -p -m 777 `grep "database=" /usr/etc/pnfsSetup | cut -f2 -d"="`
mkdir -p -m 777 `grep "database_postgres" /usr/etc/pnfsSetup | cut -f2 -d"="`
mkdir -p -m 777 `grep "trash" /usr/etc/pnfsSetup | cut -f2 -d"="`
pnfsdLog=`grep dbserverLog /usr/etc/pnfsSetup | cut -f2 -d"="`
mkdir -p -m 777 `dirname ${pnfsdLog}`


echo "Enabling Enstore log directory"
$ENSTORE_DIR/external_distr/extract_config_parameters.py log_server | grep log_file_path | cut -f1,2 -d\: --output-delimiter=" " > /tmp/log_conf.tmp
while read f1 f2; do eval $f1=$f2; done < /tmp/log_conf.tmp
rm -rf /tmp/log_conf.tmp

log_dir=`dirname $log_file_path`
if [ -d $log_dir ];
then
    chown -R enstore.enstore `dirname $log_file_path`
fi

#create pnfs directory
