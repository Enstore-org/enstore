#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################

if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

source /usr/local/etc/setups.sh
if [ $? -ne 0 ]
then 
    echo setup failed
    exit 1
fi


PATH=/usr/sbin:$PATH
config_host=`echo $ENSTORE_CONFIG_HOST | cut -f1 -d\.`
this_host=`uname -n | cut -f1 -d\.`

echo "This script will create all enstore databases that will be served by one postmaster.
If you want a different configuration you can use this script as a guide."

# get enstore db parameters
$ENSTORE_DIR/external_distr/extract_config_parameters.py database  | cut -f1,2 -d\: --output-delimiter=" " > install_database.tmp
while read f1 f2; do eval $f1=$f2; done < install_database.tmp
rm -rf install_database.tmp
echo Database Host: ${db_host}
echo Database Port: ${db_port}
echo Database Directory: ${dbarea}
echo Database Name: ${dbname}
echo Database Base Server Owner: $dbserverowner
echo Data Base User: $dbuser

# get accounting db parameters
$ENSTORE_DIR/external_distr/extract_config_parameters.py accounting_server  | cut -f1,2 -d\: --output-delimiter=" " > install_database.tmp
while read f1 f2; do eval acc_$f1=$f2; done < install_database.tmp
rm -rf install_database.tmp
if [ -z "$acc_dbhost" -o  "$acc_dbhost" != "$db_host" ]; then
    echo This script does not allow DB servers on different hosts.
    echo Please set accounting_server db_host entry in $ENSTORE_CONFIG_FILE to ${db_host}
    exit 1
fi

if [ -z "$acc_dbport" -o  "$acc_dbport" != "$db_port" ]; then
    echo This script does not allow DB servers on different hosts.
    echo Please set accounting_server db_port entry in $ENSTORE_CONFIG_FILE to ${db_port}
    exit 1
fi

if [ -z "$acc_dbserverowner" -o  "$acc_dbserverowner" != "$dbserverowner" ]; then
    echo This script does not allow different DB server owners
    echo Please set accounting_server dbserverowner entry in $ENSTORE_CONFIG_FILE to ${dbserverowner}
    exit 1
fi

if [ -z "$acc_dbuser" -o  "$acc_dbuser" != "$dbuser" ]; then
    echo This script does not allow different DB users
    echo Please set accounting_server dbuser entry in $ENSTORE_CONFIG_FILE to ${dbuser}
    exit 1
fi

if [ -z "$acc_dbarea" -o  "$acc_dbarea" != "$dbarea" ]; then
    echo This script does not allow different DB areas
    echo Please set accounting_server dbarea entry in $ENSTORE_CONFIG_FILE to ${dbarea}
    exit 1
fi
echo Accounting Database Name ${acc_dbname}

# get dirvestat db parameters
$ENSTORE_DIR/external_distr/extract_config_parameters.py drivestat_server  | cut -f1,2 -d\: --output-delimiter=" " > install_database.tmp
while read f1 f2; do eval ds_$f1=$f2; done < install_database.tmp
rm -rf install_database.tmp
if [ -z "$ds_dbhost" -o  "$ds_dbhost" != "$db_host" ]; then
    echo This script does not allow DB servers on different hosts.
    echo Please set drivestat_server db_host entry in $ENSTORE_CONFIG_FILE to ${db_host}
    exit 1
fi

if [ -z "$ds_dbport" -o  "$ds_dbport" != "$db_port" ]; then
    echo This script does not allow DB servers on different hosts.
    echo Please set drivestat_server db_port entry in $ENSTORE_CONFIG_FILE to ${db_port}
    exit 1
fi

if [ -z "$ds_dbserverowner" -o  "$ds_dbserverowner" != "$dbserverowner" ]; then
    echo This script does not allow different DB server owners
    echo Please set drivestat_server dbserverowner entry in $ENSTORE_CONFIG_FILE to ${dbserverowner}
    exit 1
fi

if [ -z "$ds_dbuser" -o  "$ds_dbuser" != "$dbuser" ]; then
    echo This script does not allow different DB users
    echo Please set drivestat_server dbuser entry in $ENSTORE_CONFIG_FILE to ${dbuser}
    exit 1
fi

if [ -z "$ds_dbarea" -o  "$ds_dbarea" != "$dbarea" ]; then
    echo This script does not allow different DB areas
    echo Please set drivestat_server dbarea entry in $ENSTORE_CONFIG_FILE to ${dbarea}
    exit 1
fi
echo Drivestat Database Name ${ds_dbname}


if [ $this_host != $db_host ]
then
    echo You must run this script on ${db_host} only
    exit 1
fi

if [ -z $POSTGRES_DIR ]
then
    echo "Postgres is not installed on this host. Please istall postgres 
or modify $ENSTORE_DIR/site_specific/config/setup-enstore"
    exit 1
fi

# check postgres version
v=`psql -V | head -1 | cut -f3 -d" " | cut -f1 -d"."`
if [ $v -ne 8 ]; then
    echo The PosgreSQL version for this script must be 8
fi

enstore_dir=${ENSTORE_DIR}
database_area=$dbarea
database_scripts=$enstore_dir/databases/scripts
database_owner=$dbserverowner
db_owner=$dbuser

db_startup_script=$database_scripts/enstore-db
db_schema=$enstore_dir/databases/schemas/enstoredb.schema
databases="${dbname} ${acc_dbname} ${ds_dbname}"

# find the host
host=`uname -n | cut -d. -f1`
hostip=`host tundra | cut -f4 -d" "`

# add this line to hba_file
hba_file="host    all         enstore     $hostip     255.255.255.255   trust"


echo "The following variables will be used:"
echo "enstore_dir: $enstore_dir"
echo "database_area: $database_area"
echo "database_owner: $database_owner"
echo "db_owner: $db_owner"
echo "hba_file: $database_area/pg_hba.conf (you may want to modify it later)"
echo "databases: $databases"

# need to setup enstore first
if [ "$enstore_dir" == "" ]
then
	echo "need to set up enstore first"
	exit 1
fi

# check if $database_area exist. If so, rename it

if [ -d $database_area ]
then
	echo $database_area exists, rename it
	dest=$database_area.`date +%F.%T`
	echo mv $database_area $dest
	mv $database_area $dest
fi


# create database owner account

echo 'Checking if group ${database_owner} exists' 
grep ${database_owner} /etc/group
if [ $? -ne 0 ]; then
    echo 'Creating group ${database_owner}'
    groupadd -g 4525 ${database_owner}
fi
echo 'Creating user ${database_owner}'
useradd -u 1342 -g ${database_owner} ${database_owner}

# create database user account

echo 'Checking if group ${db_owner} exists' 
grep ${db_owner} /etc/group
if [ $? -ne 0 ]; then
    echo 'Creating group ${db_owner}'
    groupadd -g 6209 ${database_owner}
fi
echo 'Creating user ${database_owner}'
useradd -u 6209 -g ${database_owner} ${database_owner}


# create initial database area
echo create database area $database_area
echo mkdir -p $database_area
mkdir -p $database_area
echo chown $database_owner.$database_owner $database_area
chown $database_owner.$database_owner $database_area
echo chmod go-rwx $database_area
chmod go-rwx $database_area
echo su $database_owner -c \"initdb -D $database_area\"
su $database_owner -c "initdb -D $database_area"

#  modify create pg_hba.conf
echo su $database_owner cat \"$hba_file >> $database_area/pg_hba.conf\"
su $database_owner -c "cat $hba_file >> $database_area/pg_hba.conf"

# bring up database server
echo su $database_owner -c \"postmaster -D $database_area -p $db_port -i \&\"
su $database_owner -c "postmaster -D $database_area -p $db_port -i &"
# echo su $database_owner -c "$db_startup_script start"
# su $database_owner -c "$db_startup_script start"

# wait for database server startup
echo waiting for database server to start up
sleep 10

# create user enstore
#
# This is a little bit different in batch mode
echo su $database_owner createuser -p $db_port -d -S -R -P enstore 
su $database_owner -c "psql template1 -p $db_port -c \"create user enstore password 'enstore_user' createdb;\""

# create databases

for db_name in $databases
do
	echo su $db_owner createdb -p $db_port $db_name
	su $db_owner -c "createdb -p $db_port $db_name"
done

# # create languages for enstoredb
# echo su $database_owner -c \"createlang -p $db_port plpgsql $db_name\"
# su $database_owner -c "createlang -p $db_port plpgsql $db_name"
# echo su $database_owner -c \"createlang -p $db_port c $db_name\"
# su $database_owner -c "createlang -p $db_port c $db_name"

# create schemas
for db_name in $databases
do
	if [ "$db_name" == "enstore" ]
	then
		db_schema=$enstore_dir/databases/schemas/accounting.schema
	else
		db_schema=$enstore_dir/databases/schemas/$db_name.schema
	fi
	echo su $database_owner psql -p $db_port $db_name -f $db_schema
	su $database_owner -c "psql -p $db_port $db_name -f $db_schema"
done
