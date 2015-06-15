#!/usr/bin/env python
###############################################################################
#
# This script is to be used to restore and start pnfs from live backup
#
#
# $Id$
###############################################################################

import getopt
import os
import pwd
import re
import string
import subprocess
import sys
import time
import types

import configuration_client
import enstore_constants
import enstore_functions2
import e_errors
import file_utils

sys2host={'cms':  ('cmspnfs1', 'psql-data'),
          'd0en': ('d0ensrv1n', 'psql-data'),
          'stken': ('stkensrv1n', 'psql-data'),
          'cdfen': ('cdfensrv1n', 'psql-data'),
          'dmsen': ('dmsen01', 'psql-data'),
          }

PSQL_COMMAND = "psql -U enstore postgres -c 'select datname from pg_database' 2> /dev/null"

def copy(source,destination):
    try:
        s = open(source,"r")
        d = open(destination,"w")
        for l in s:
            d.write(l)
        return 0
    except Exception, msg:
        sys.stderr.write("Got exception %s copying %s to %s"%(str(msg),source,destination,))
        sys.stderr.flush()
        return 1


class PnfsSetup:
    # find uncommented lines that look like
    # " var = /value "
    # there could be arbitrary number of white spaces between words
    #
    MATCH=re.compile("^[\s]*[^#]+[\s]*[\w]+[\s]*=[\s]*[\w\-/~]+")
    # default pnfsSetup file name
    NAME="pnfsSetup"
    # default destination directory
    DEFAULT_LOCATION="/usr/etc"
    # mandatory keys:
    REMOTE_BACKUP="remotebackup"
    TRASH="trash"
    DATABASE_POSTGRES="database_postgres"
    DATABASE="database"
    PNFS="pnfs"
    def __init__(self,fname):
        f=open(fname,"r")
        self.contents={}
        self.pnfs_dir = "/opt/pnfs"
        for line in f:
            if not line : continue
            if PnfsSetup.MATCH.match(line):
                data = line.split("=")
                try:
                    ls=data[0].strip()
                    rs=string.join(data[1:],"=").strip()
                    if ls==PnfsSetup.REMOTE_BACKUP:
                        ls="#"+ls
                        self.remote_backup_host, self.remote_backup_dir = rs.split("#")[0].strip().split(":")
                    self.contents[ls]=rs
                except Exception, msg:
                    sys.stderr.write("Got exception %s parsing line %s"%(str(msg),line))
		    sys.stderr.flush()
		    pass
        f.close()
        pgdb = self.contents[PnfsSetup.DATABASE_POSTGRES].split("#")[0].strip().rstrip("/")
        self.backup_name = os.path.basename(pgdb.rstrip("/"))

    def __repr__(self):
        s=""
        for k, v in self.contents.iteritems():
            s+="%s=%s\n"%(k,v)
        return s

    def __str__(self):
        return self.__repr__()

    def write(self,directory=None):
        if not directory :
            directory = PnfsSetup.DEFAULT_LOCATION
        f=open(os.path.join(directory,PnfsSetup.NAME),"w")
        f.write(str(self))
        f.close()

    def __setitem__(self,key,value):
        if self.contents.has_key(key):
            self.contents[key]=value
        else:
            raise KeyError, "No such key %s"%(key,)

    def __getitem__(self,key):
        return self.contents[key]

def checkPostgres():
	psqlFile = os.popen(PSQL_COMMAND)
	psqlOutput = psqlFile.readlines()
	psqlFile.close()
	if not psqlOutput:
		#sys.stderr.write('Postgres server is not running properly yet\n')
		return False
	return True

def getPostgresqlVersion():
    ret = enstore_functions2.shell_command2("psql --version")
    if not ret:
        return None

    rc = ret[0]
    if rc:
        return None

    out = ret[1]
    out = string.join(re.sub("[^0-9.]+","",out).split('.')[:2],'.')
    return out

class PnfsDbRestore:

    def __init__(self):
        self.config   = configuration_client.get_config_dict()
        self.systems  = self.config.get('known_config_servers',{})
        self.pnfs_host = None

    def get_configuration_client(self):
        return self.config

    def get_enstore_systems(self):
        list = []
        for s in sys2host.keys() :
            list.append(s)
        for s in self.systems.keys():
            if s == "status" : continue
            list.append(s)
        return list

    def usage(self,cmd):
        print "Usage: %s -s [--system=] -t [backup_time=] "%(cmd,)
        print "\t allowed systems: ", self.get_enstore_systems()
        print "\t specify timestamp YYYY-MM-DD to get backup up to certain date"

    def recover(self,name,backup_time=None):
        postgresqlVersion = getPostgresqlVersion()
        if not postgresqlVersion:
             print "Failed to determine postgresql version"
             return 1
        pnfsSetupFile=os.path.join(os.getenv("ENSTORE_DIR"),"etc/%s-pnfsSetup"%(name))
        if sys2host.has_key(name):
            self.pnfs_host = sys2host[name][0]
        for s in self.systems:
            if s != name : continue
            server_name,server_port = self.systems.get(s)
            config_server_client   = configuration_client.ConfigurationClient((server_name, server_port))
            pnfs_server_dict=config_server_client.get('pnfs_server')
            if pnfs_server_dict["status"][0] == e_errors.KEYERROR:
                self.pnfs_host = sys2host[name][0]
            elif  pnfs_server_dict["status"][0] ==e_errors.OK:
                self.pnfs_host=config_server_client.get('pnfs_server').get('host')
            else:
                print "Failed to communicate with config server ",pnfs_server_dict["status"]
                return 1
	#
	# read configuration file
	#
	pnfsSetup = PnfsSetup(pnfsSetupFile)
	#
	# write modified configuration file in place (/usr/etc/pnfsSetup)
	#
	pnfsSetup.write()
        for cmd in ["umount -f /pnfs/fs", \
                    "/sbin/service postgresql-%s stop"%(postgresqlVersion,)]:
            print "Executing command ",cmd
            rc=os.system(cmd)
            if rc != 0 :
                sys.stderr.write("Failed to execute command '%s' ignoring\n"%(cmd,))
                sys.stderr.flush()
        backup_file = get_backup(pnfsSetup)
	pnfs_db = pnfsSetup[pnfsSetup.DATABASE]
        if not os.path.exists(pnfs_db):
            os.makedirs(pnfs_db)
        else:
            parent = pnfs_db
            for i in range(2):
                parent = os.path.dirname(parent)
            os.rename(parent,"%s-%s.%d"%(parent,
                                         time.strftime("%b-%d-%Y",time.localtime()),
                                         os.getpid()))
            os.makedirs(pnfs_db)

        os.chdir(os.path.dirname(pnfs_db))
        cwd=os.getcwd()

	for d in ['%s/log'%(os.path.dirname(cwd),),\
		  pnfsSetup[pnfsSetup.TRASH]]:
            if os.path.exists(d):
                file_utils.rmdir(d)
            print "Creating directory ",d
            os.makedirs(d)

        # copy backup file over

        cmd = "/usr/bin/rsync -e rsh  %s:%s . "%(pnfsSetup.remote_backup_host,
						 backup_file)
        print 'copying: %s'% (cmd,)
        err=os.system(cmd)
        if err:
            raise RuntimeError, '%s failed w/ exit code %d' % (cmd, err)

        cmd='tar xzvf %s --preserve-permissions --same-owner'%(os.path.basename(backup_file),)
        print 'Extracting %s with: %s'% (os.path.basename(backup_file), cmd)
        os.system(cmd)

        # fill in the database location in sysconfig file

        psql_data=pnfsSetup[PnfsSetup.DATABASE_POSTGRES]
        f=open("/etc/sysconfig/pgsql/postgresql-%s"%(postgresqlVersion,),"w")
        f.write("PGDATA=%s\n"%(psql_data))
        f.close()


        # create recovery.conf
        rdir = '%s:%s.xlogs'% (pnfsSetup.remote_backup_host,
			       os.path.dirname(backup_file))
        cmd = "restore_command = '%s/sbin/enrcp %s/"% (os.getenv("ENSTORE_DIR"),
                                                       rdir) + "%f.Z %p.Z" + " && uncompress %p.Z'"
	pgdb = pnfsSetup[PnfsSetup.DATABASE_POSTGRES]
        print 'Creating recovery.conf: %s'% (cmd, )
        f=open('%s/recovery.conf'%(pgdb,), 'w')
        f.write('%s\n'%(cmd,))
        if (backup_time != None) :
            f.write("recovery_target_time='%s'\n"%(backup_time,))
            f.write("recovery_target_inclusive='true'\n")
        f.close()
        os.system("cat %s/recovery.conf"%(pgdb))

	for cmd in ['sed -i "s/archive_command/#archive_command/g" %s/postgresql.conf '% (pgdb,), \
                    'sed -i "s/^[ \t\r]*archive_mode[ \t\r]*=[ \t\r]*on/archive_mode = off/g" %s/postgresql.conf '% (pgdb,),\
                    'sed -i "s/^[ \t\r]*logging_collector[ \t\r]*=[ \t\r]*on/logging_collector = off/g" %s/postgresql.conf '% (pgdb,),\
		    "mkdir -p %s/pg_xlog/archive_status"% (pgdb,),\
		    "chown -R enstore.enstore %s"% (pgdb,),\
                    "rm -f %s/postmaster.pid"%(pgdb),\
                    "/sbin/service postgresql-%s start"%(postgresqlVersion,)]:
            rc = os.system(cmd)
            if rc != 0 :
                sys.stderr.write("Command %s failed, bailing out "%(cmd,))
                sys.stderr.flush()
                return rc

        rc = checkPostgres()
        print "Starting DB server"

        while not rc:
            print "Waiting for DB server"
            time.sleep(60)
            rc = checkPostgres()

        print "DB server is ready"

        cmd='dcache start'
        print "Starting chimera: %s"% (cmd,)
        rc=os.system(cmd)
	if rc != 0 :
		sys.stderr.write("Command %s failed, bailing out "%(cmd,))
		sys.stderr.flush()
		return rc
        print "DONE"
        return 0


def get_command_output(command):
    child = os.popen(command)
    data  = child.read()
    err   = child.close()
    if err:
	raise RuntimeError, '%s failed w/ exit code %d' % (command, err)
    return data[:-1] # (skip '\n' at the end)
#
# get next to the last backup
#
def get_backup(pnfsSetup):
	cmd='rsh %s  "ls -t %s/%s.*|head -n 2|tail -1"'%(pnfsSetup.remote_backup_host,
							 pnfsSetup.remote_backup_dir,
							 pnfsSetup.backup_name)
	return get_command_output(cmd)


if __name__ == "__main__" :
    r = PnfsDbRestore();

    uname = pwd.getpwuid(os.getuid())[0]
    backup_time      = None
    sysname          = None
    emergency        = False
    ourhost          = string.split(os.uname()[1],'.')[0]

    if uname != 'root':
        print "Must be 'root' to run this program"
        sys.exit(1)
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:t:es:", ["help","system=","backup_time=","emergency"])
    except getopt.GetoptError:
        print "Failed to process arguments"
        r.usage(sys.argv[0])
        sys.exit(2)
    for o, a in opts:
        if o in ("-h", "--emergency") :
            emergency=True
        if o in ("-h", "--help"):
            r.usage(sys.argv[0])
            sys.exit(1)
        if o in ("-s", "--system"):
            sysname = a
        if o in ("-t", "--backup_time"):
            backup_time = a
    for value in sys2host.values():
        if ourhost == value[0] and emergency == False :
            print "You are running on production system - ",ourhost
            print "Re-run specifying --emergency switch to proceed"
            r.usage(sys.argv[0])
            sys.exit(1)
    if (sysname == None or sysname=="") :
        print "Error: Must specify enstore system name"
        r.usage(sys.argv[0])
        sys.exit(1)
    else :
        if sysname not in r.get_enstore_systems() and not sys2host.has_key(sysname):
            print "Error: Unknown system specified ", sysname
            print "Known systems : ", r.get_enstore_systems()
            r.usage(sys.argv[0])
            sys.exit(1)
    if (backup_time == None or backup_time=="" ) :
        backup_time=None

    rc = r.recover(sysname,backup_time)
    sys.exit(rc)
