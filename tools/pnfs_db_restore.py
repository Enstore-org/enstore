#!/usr/bin/env python
# $Id$
import os
import sys
import popen2
import string
import pwd
import getopt
import time
import types

import configuration_client
import enstore_constants

sys2host={'cms': ('cmspnfs1', 'psql-data'),
          'eag':('eagpnfs1','psql-data'),
          'sdss': ('?????', '?????'),
          }

version2version={'v8_0_7' : '8.0',
                 'v8_1_3' : '8.1',
                 'v8_2_6' : '8.2',
                 'v8_3_0' : '8.3',
                 }

postgres_version = 'v8_1_3'

class PnfsDbRestore:

    def __init__(self):
        self.config   = configuration_client.get_config_dict()
        self.systems = self.config.get('known_config_servers')
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
        print "Usage: %s -s [--system=] -t [backup_time=] -p [--postgres_version=]"%(cmd,)
        print "\t allowed systems: ", self.get_enstore_systems()
        print "\t specify timestamp YYYY-MM-DD to get backup up to certain date" 
        versions=""
        for p in version2version.keys():
            versions=versions+p+","
        print "\t allowed postgres versions: "+versions[:-1]

    def recover(self,name,backup_time=None):
        pnfsSetupFile=os.path.join(os.getenv("ENSTORE_DIR"),"etc/%s-pnfsSetup"%(name))
        if sys2host.has_key(name):
            self.pnfs_host = sys2host[name][0]
        for s in self.systems:
            if s != name : continue
            server_name,server_port = self.systems.get(s)
            config_server_client   = configuration_client.ConfigurationClient((server_name, server_port))
            if config_server_client.get('pnfs_server',None) != None:
                self.pnfs_host=config_server_client.get('pnfs_server').get('host')
        cmd = "cp %s /tmp/pnfsSetup.%s"%(pnfsSetupFile, self.pnfs_host)
        os.system(cmd)
        pnfs_db, pgdb, trash, backup_host, backup_dir, pnfs_dir, backup_name = get_config(self.pnfs_host)

        cmd='umount /pnfs/fs'
        print 'Unmounting pnfs: %s'% (cmd,)
        os.system(cmd)

        cmd='/etc/init.d/pnfs stop'
        print "Stopping pnfs: %s"% (cmd,)
        os.system(cmd)

        cmd='/etc/init.d/postgres-boot stop'
        print "Stopping DB server: %s"% (cmd,)
        os.system(cmd)

        backup_file = get_backup(backup_host, backup_dir,  backup_name)
        # go to a proper directory
        if not os.path.exists(pnfs_db):
            os.makedirs(pnfs_db)
        else:
           os.rename(pnfs_db,"%s-%s.%d"%(pnfs_db,
                                         time.strftime("%b-%d-%Y",time.localtime()),
                                         os.getpid()))
           os.makedirs(pnfs_db)

        os.chdir(os.path.dirname(pnfs_db))
        cwd=os.getcwd()

        # clean directories if exist

        print 'Cleaning %s'% (cwd,)
        os.system('rm -rf *')
        d='%s/log'%(os.path.dirname(cwd),)
        print 'Recreating %s'% (d,)
        os.system('rm -rf %s'% (d,))
        os.system('mkdir -p %s'% (d,))
        d='%s/trash'%(os.path.dirname(cwd),)
        print 'Recreating %s'% (d,)
        os.system('rm -rf %s'% (d,))
        os.system('mkdir -p %s' %(d,)) 

        #copy a backup file
        cmd = "/usr/bin/rsync -e rsh  %s:%s . "%(backup_host, backup_file)
        print 'copying: %s'% (cmd,)
        err=os.system(cmd)
        if err:
            raise RuntimeError, '%s failed w/ exit code %d' % (cmd, err)

        cmd='tar xzvf %s --preserve-permissions --same-owner'%(os.path.basename(backup_file),)
        print 'Extracting %s with: %s'% (os.path.basename(backup_file), cmd)
        os.system(cmd)

        # create recovery.conf
        rdir = '%s:%s.xlogs'% (backup_host, os.path.dirname(backup_file))
        cmd = "restore_command = '/home/enstore/enstore/sbin/enrcp %s/"% (rdir) + "%f.Z %p.Z" + " && uncompress %p.Z'"
        print 'Creating recovery.conf: %s'% (cmd, )
        f=open('%s/recovery.conf'%(pgdb,), 'w')
        f.write('%s\n'%(cmd,))
        if (backup_time != None) :
            f.write("recovery_target_time='%s'\n"%(backup_time,))
            f.write("recovery_target_inclusive='true'\n")        
        f.close()
        os.system("cat %s/recovery.conf"%(pgdb))
        cmd = 'sed -i "s/archive_command/#archive_command/g" %s/postgresql.conf '% (pgdb,)
        os.system(cmd)
#        cmd = 'sed -i "s# *shared_buffers*=*.*#shared_buffers = 126976 #" %s/postgresql.conf'% (pgdb,)
#        os.system(cmd)

        #create xlog dirs
        cmd = 'mkdir -p %s/pg_xlog/archive_status'% (pgdb,)
        print 'Creating xlog dirs: %s'% (cmd, )
        os.system(cmd)
        cmd='chown -R enstore.enstore %s'% (pgdb,)
        print 'Changing Ownership: %s'% (cmd,)
        os.system(cmd)

        ## print "the following steps need to be made manually to ensure that all is rigth" 
        ## print "Now start postgreSQL and pnfs. Mount /pnfs/fs"
        ## print "postgreSQL can be started by /etc/init.d/postgres-boot start"
        ## print "pnfs can be started by /etc/init.d/pnfs start"

        os.system("rm -f %s/postmaster.pid"%(pgdb))

        # start postgres
        cmd='/etc/init.d/postgres-boot start %s'%(postgres_version,)
        print "Starting DB server: %s"% (cmd,)
        time.sleep(360)
        os.system(cmd)
        # this is sloppy but how else can I check that postmasted is ready
        # recovery may take a very long time

        while 1:
            print "Waiting for DB server"
            rc = os.system('%s/tools/tstpostgres > /dev/null 2>&1'%(pnfs_dir,)) >> 8
            print 
            print "Waiting for DB server", rc
            if rc == 0:
                print "DB server is ready"
                break


        cmd='. /usr/local/etc/setups.sh; setup pnfs; export LD_LIBRARY_PATH=$PNFS_DIR/tools/linux/pgsql/lib; /etc/init.d/pnfs start'
        print "Starting pnfs: %s"% (cmd,)
        os.system(cmd)


        cmd='mount /pnfs/fs'
        print 'Mounting pnfs: %s'% (cmd,)
        os.system(cmd)
        return 0



def get_command_output(command):
    child = os.popen(command)
    data  = child.read()
    err   = child.close()
    if err:
	raise RuntimeError, '%s failed w/ exit code %d' % (command, err)
    return data[:-1] # (skip '\n' at the end)

    
    
def get_config(host):
    #    print "get_config PNFS HOST ",host
    #    cmd = "rcp %s:/usr/etc/pnfsSetup pnfsSetup.%s"%(host, host)
    #    pipeObj = popen2.Popen3(cmd, 0, 0)
    #    if pipeObj is None:
    #        print "%s failed"%(cmd, )
    #        sys.exit(1) 
    #    stat = pipeObj.wait()
    #    result = pipeObj.fromchild.readlines()  # result has returned string
    #
    f = open("/tmp/pnfsSetup.%s"%(host,), 'r')
    of = open("/tmp/pnfsSetup.%s.tmp"%(host,), 'w')
    while 1:
        l = f.readline()
        
        if not l:
            break
        if l[0] == '#':
            # skip comment line
            continue
        a=l[:-1].split('=')
        if a[0] == 'database':
            pnfs_db = a[1]
        if a[0] == 'database_postgres':
            pgdb = a[1]
            backup_name=pgdb.split('/')[-1]
        if a[0] == 'trash':
            trash = a[1]
        if a[0] == 'remotebackup':
            backup_host, backup_dir = a[1].split(':')
            l1 = string.join(('#', l))
            l = l1
        of.write(l)
    of.close()
    f.close()
    os.system("mv /tmp/pnfsSetup.%s.tmp /tmp/pnfsSetup.%s"%(host, host))
    os.system("cp -f /tmp/pnfsSetup.%s /usr/etc/pnfsSetup"%(host, ))
    # get local pnfs directory
    pnfs_dir=os.popen('. /usr/local/etc/setups.sh; setup pnfs; echo $PNFS_DIR').readlines()[0][:-1]
    #
    # kludge
    #
    # cmd = "rsh %s:/usr/etc/pnfsSetup pnfsSetup.%s"%(host, host)
    os.system('. /usr/local/etc/setups.sh; unsetup postgres')
    cmd = ". /usr/local/etc/setups.sh; setup postgres %s"%(postgres_version,)
    print "setting postgres ",postgres_version
    if ( os.system(cmd) ):
        print "failed to ",cmd
        sys.exit(1)
    return pnfs_db, pgdb, trash, backup_host, backup_dir,pnfs_dir, backup_name
#
# get next to the last backup
#
def get_backup(backup_host, backup_dir,  backup_name):
    cmd='rsh %s  "ls -t %s/%s.*|head -n 2|tail -1"'%(backup_host, backup_dir,  backup_name)
#    cmd='rsh %s  "ls -t %s/%s.*|head -n 1"'%(backup_host, backup_dir,  backup_name)
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
        opts, args = getopt.getopt(sys.argv[1:], "hs:p:t:es:", ["help","system=","postgres_version=","backup_time=","emergency"])
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
        if o in ("-p", "--postgres_version"):
            postgres_version = a
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

#    if sys2host.has_key(sysname):
#        pnfs_host   = sys2host[sysname][0]
#        backup_name = sys2host[sysname][1]
#    else:
#        print "Error: no such system ",sysname
#        r.usage(sys.argv[0])
#        sys.exit(1)

    if not version2version.has_key(postgres_version):
        print "Error: no such postgres version", postgres_version
        r.usage(sys.argv[0])
        sys.exit(1)
    rc = r.recover(sysname,backup_time)
    sys.exit(rc)



