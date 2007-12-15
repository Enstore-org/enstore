#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
PATH=$PATH:/sbin

place="${1:-ftp://ssasrv1.fnal.gov/en/lts44/i386}"
#psql_place=/home/moibenko
. /usr/local/etc/setups.sh
setup enstore

echo "installing enstore_html"
/sbin/service httpd stop
#rpm -U --force --nodeps ${place}/enstore_html-1.0-0.i386.rpm
rpm -U --force --nodeps ${place}/enstore_html-1.0-1.noarch.rpm

rpm -q postgesql > /dev/null
if [ $? -ne 0 ]; 
then
    echo "installing postgres"
    echo "installing postgres"
    rpm -U --force ${place}/postgresql-libs-8.2.4-1PGDG.i686.rpm ${place}/postgresql-8.2.4-1PGDG.i686.rpm ${place}/postgresql-server-8.2.4-1PGDG.i686.rpm ${place}/postgresql-devel-8.2.4-1PGDG.i686.rpm
    rm -f /tmp/postgresql
    mv /etc/rc.d/init.d/postgresql /tmp/postgresql
    echo "Modifying dbuser name"
    sed -e 's/postgres:postgres/enstore:enstore/' -e 's/l postgres/l enstore/' /tmp/postgresql > /etc/init.d/postgresql 
    chmod a+x /etc/init.d/postgresql
    echo "Change shared memory settings"
    echo "kernel.shmmax=268435456" >> /etc/sysctl.conf
    echo "kernel.shmall=268435456" >> /etc/sysctl.conf
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
echo "pnfs host: $pnfs_host"
echo "Creating pnfsSetup"
pnfsSetup_file=stken-pnfsSetup
if [ ! -d /usr/etc ];then mkdir /usr/etc;fi
if [ ! -r /usr/etc/pnfsSetup ]; then cp ${ENSTORE_DIR}/etc/${pnfsSetup_file} /usr/etc/pnfsSetup; fi
if [ ! -r /usr/etc/pnfsSetup.sh ]; then ln -s /usr/etc/pnfsSetup /usr/etc/pnfsSetup.sh; fi

. /usr/etc/pnfsSetup.sh
echo "PGDATA=$database_postgres" > /etc/sysconfig/pgsql/postgresql
# create pnfs db area
mkdir -p -m 777 $database
mkdir -p -m 777 $database_postgres
mkdir -p -m 777 $trash
pnfsdLog=dbserverLog
mkdir -p -m 777 `dirname ${pnfsdLog}`
chown enstore $database_postgres
this_host=`uname -n`
if [ $this_host != $pnfs_host ];
then
this_host=`uname -n | cut -f1 -d\.`
fi
if [ $this_host = $pnfs_host ];
then
    echo "Configuring this host to run postgresQL"
    /sbin/chkconfig postgresql on
    echo "Initializing postgresQL"
    service postgresql initdb
    echo "Changing pg_hba.conf"
    cp $database_postgres/pg_hba.conf $database_postgres/pg_hba.conf.sav
    echo "local   all         all trust" > $database_postgres/pg_hba.conf
    echo "host    all         all         127.0.0.1/32 trust" >> $database_postgres/pg_hba.conf

    echo "Starting postgresQL"
    /etc/init.d/postgresql start
    echo "Configuring this host to run pnfs server"
    /sbin/chkconfig --add pnfs
    /sbin/chkconfig pnfs on
    #echo "Starting pnfs"   # do not start pnfs as it will crash if there is no database
    #/etc/init.d/pnfs start
fi

echo "Creating pnfs Databases"
$ENSTORE_DIR/external_distr/create_pnfs_db.sh -x 


##### !!!!!! The commented lines were moved to create_pnfs_db.sh

#echo "Starting pnfs"   # do not start pnfs as it will crash if there is no database
#/etc/init.d/pnfs start
#create pnfs directory
#if [ ! -d /pnfs/fs ];
#then
#    mkdir -p /pnfs/fs
#    chmod -R 777 /pnfs/fs
#    mount -o intr,hard,rw localhost:/fs   /pnfs/fs
#fi

echo "Enabling Enstore log directory"
$ENSTORE_DIR/external_distr/extract_config_parameters.py log_server | grep log_file_path | cut -f1,2 -d\: --output-delimiter=" " > /tmp/log_conf.tmp
while read f1 f2; do eval $f1=$f2; done < /tmp/log_conf.tmp
rm -rf /tmp/log_conf.tmp

log_dir=`dirname $log_file_path`
if [ -d $log_dir ];
then
    chown -R enstore.enstore `dirname $log_file_path`
fi

