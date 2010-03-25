#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
set -u  # force better programming and ability to use check for not set

usage() {
echo "$0 [force] [url]"
}

lforce=""

while [ $# -gt 0 ];
do
	case $1 in
		-x) set -xv; shift;	;;
		-h) usage; exit 0;	;;
		force)  force="--${1}"; shift;	;;
		*) place_from_cmd=$1; shift;	;;
	esac;
done
if [ "${force:-x}" != "x" ]
then lforce=$force
fi

processor=`uname -i`
if [ "${place:-x}" = "x" ]; then
    place=$place_from_cmd
fi

if [ "${ENSTORE_VERBOSE:-x}" != "x" ]; then
    set -xv
fi 

. /usr/local/etc/setups.sh
setup enstore

# enstore html requires postgresql
# install postgresql first
rpm -q postgresql > /dev/null
if [ $? -ne 0  -o -n "${lforce}" ]; 
then
    echo "installing postgres"
    # always use --force as we need to make sure that
    # the postgres rpms are installed from our repository
    rpm -U --force ${place}/${processor}/postgresql-*
    rpm -U --force ${place}/${processor}/compat-postgresql-libs-*
    if [ $? -ne 0 ]; 
    then
	echo "installation of postgresql failed"
	exit 1
    fi;
    #yum update postgres
    rm -f /tmp/postgresql
    mv /etc/rc.d/init.d/postgresql /tmp/postgresql
    echo "Modifying dbuser name"
    sed -e 's/postgres:postgres/enstore:enstore/' -e 's/l postgres/l enstore/' /tmp/postgresql > /etc/init.d/postgresql 
    chmod a+x /etc/init.d/postgresql
    echo "Change shared memory settings"
    echo "kernel.shmmax=1073741824" >> /etc/sysctl.conf
    echo "kernel.shmall=1073741824" >> /etc/sysctl.conf
fi

echo "installing enstore_html"
/sbin/chkconfig --list httpd >/dev/null 2&>1
if [ $? -eq 0 ]; then
    /sbin/service httpd stop
else
    # install httpd
    yum -y install httpd
fi

rpm -U ${lforce} --nodeps ${place}/noarch/enstore_html-2.0-0.noarch.rpm

rpm -q pnfs > /dev/null
if [ $? -ne 0  -o -n "${lforce}" ];
then
    echo "installing pnfs"
    if [ -r /etc/rc.d/init.d/pnfs ];
    then
	chmod 755 /etc/rc.d/init.d/pnfs
	/etc/rc.d/init.d/pnfs stop
	if [ -x /etc/rc.d/init.d/postgresql ]; then /etc/rc.d/init.d/postgresql stop; fi
	#/etc/rc.d/init.d/postgresql stop
    fi
    rpm -U ${lforce} ${place}/${processor}/pnfs-postgresql-3.1.18-1-SL5x.x86_64.rpm
    # complete after install pnfs configuration
    # copy setup

    $ENSTORE_DIR/external_distr/extract_config_parameters.py pnfs_server | cut -f1,2 -d\: --output-delimiter=" " > /tmp/pnfs_conf.tmp
    while read f1 f2; do eval pnfs_${f1}=$f2; done < /tmp/pnfs_conf.tmp
    rm -rf install_database.tmp
    echo pnfs host: ${pnfs_host}
    this_host=`uname -n`
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
		*)
		pnfsSetup_file=stken-pnfsSetup
    esac
    if [ ! -d /usr/etc ];then mkdir /usr/etc;fi
    if [ ! -r /usr/etc/pnfsSetup ]; then cp ${ENSTORE_DIR}/etc/${pnfsSetup_file} /usr/etc/pnfsSetup; fi
    if [ ! -r /usr/etc/pnfsSetup.sh ]; then ln -s /usr/etc/pnfsSetup /usr/etc/pnfsSetup.sh; fi

    . /usr/etc/pnfsSetup.sh
    echo "PGDATA=$database_postgres" > /etc/sysconfig/pgsql/postgresql
    if [ $this_host = $pnfs_host ];
    then
	PATH=$PATH:$pnfs/tools
	mkdir -p $database_postgres
	mkdir -p $database
	mkdir -p /pnfs/fs
	chmod 777 /pnfs/fs
	mkdir -p $trash/1
	mkdir -p $trash/2
	mkdir -p $trash/4
	chown enstore $database_postgres
	echo "Configuring this host to run postgres"
	/sbin/chkconfig postgresql on
	echo "Initializing postgres"
	su enstore -c "initdb -D $database_postgres"    
	echo "Starting postgres"
	/etc/init.d/postgresql start
	echo "Configuring this host to run pnfs server"
	/sbin/chkconfig --add pnfs
	/sbin/chkconfig pnfs on
	echo "creating admin DB"
	mdb create admin `dirname $database`/admin
	echo "creating data DB"
	mdb create data `dirname $database`/data
	#echo "Starting pnfs"   # do not start pnfs as it will crash if there is no database
	#/etc/init.d/pnfs start
    fi

    # install compress
    yum -y install ncompress
    #create pnfs directory
    if [ ! -d /pnfs ];
    then
	mkdir /pnfs
	chmod 777 /pnfs
    fi
fi
echo "Enabling Enstore log directory"
$ENSTORE_DIR/external_distr/extract_config_parameters.py log_server | grep "log_file_path" | cut -f1,2 -d\: --output-delimiter=" " > /tmp/log_conf.tmp
while read f1 f2; do eval $f1=$f2; done < /tmp/log_conf.tmp
#rm -rf /tmp/log_conf.tmp ---- REMOVE

#log_dir=`dirname $log_file_path`
if [ -d $log_file_path ];
then
    chown -R enstore.enstore $log_file_path
fi

