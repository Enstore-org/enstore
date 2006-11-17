#!/usr/bin/env python
# $Id$
import os
import sys
import popen2
import string
import pwd
import getopt
import time

sys2host={'cdf': ('cdfensrv1','psql-data'),
          'cms': ('cmspnfs', 'psql-data'),
          'd0': ('d0ensrv1', 'data'),
          'stk':('stkensrv1','psql-data'),
          'sdss': ('?????', '?????'),
          }
version2version={'v8_0_7' : '8.0',
                 'v8_1_3' : '8.1'
                 }

def usage(cmd):
#    print "Usage: %s {cdf|cms|d0|stk|sdss} [timestamp]"%(cmd,)
    print "Usage: %s -s [--system=] -t [backup_time=] -p [--pnfs_version=]"%(cmd,)
    print "\t allowed systems: cms|cdf|d0|stk"
    print "\t specify timestamp YYYY-MM-DD to get backup up to certain date" 
    print "\t allowed pnfs versions v8_0_7, v8_1_3"
#    sys.exit(1)
    
def get_config(host):
    print "get_config PNFS HOST ",host
    cmd = "rcp %s:/usr/etc/pnfsSetup pnfsSetup.%s"%(host, host)
    pipeObj = popen2.Popen3(cmd, 0, 0)
    if pipeObj is None:
        print "%s failed"%(cmd, )
        sys.exit(1) 
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()  # result has returned string

    f = open("pnfsSetup.%s"%(host,), 'r')
    of = open("pnfsSetup.%s.tmp"%(host,), 'w')
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
        if a[0] == 'trash':
            trash = a[1]
        if a[0] == 'remotebackup':
            backup_host, backup_dir = a[1].split(':')
            l1 = string.join(('#', l))
            l = l1
        of.write(l)
    of.close()
    f.close()
    os.system("mv pnfsSetup.%s.tmp pnfsSetup.%s"%(host, host))
#    os.system("chown root.root pnfsSetup.%s"%(host, ))
    os.system("cp -f pnfsSetup.%s /usr/etc/pnfsSetup"%(host, ))
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
    return pnfs_db, pgdb, trash, backup_host, backup_dir,pnfs_dir

# get last backup
def get_backup(backup_host, backup_dir,  backup_name):
    cmd = 'rsh %s "ls -ltr %s/%s.*"'%(backup_host, backup_dir,  backup_name)
    pipeObj = popen2.Popen3(cmd, 0, 0)
    if pipeObj is None:
        print "%s failed"%(cmd, )
        sys.exit(1) 
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()  # result has returned string

    # take a second to last backup
    # as the last may not be completed yet
    a = result[len(result)-1].split(' ')
    return a[len(a)-1][:-1]

def recover(backup_time=None):
    pnfs_db, pgdb, trash, backup_host, backup_dir, pnfs_dir = get_config(pnfs_host)
    
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
    cmd = '/usr/bin/rsync -e rsh  %s:%s .'%(backup_host, backup_file)
    print 'copying: %s'% (cmd,)
    os.system(cmd)

    # untar the file
    cmd='tar xzvf %s --preserve-permissions --same-owner'%(os.path.basename(backup_file),)
    print 'Extracting %s with: %s'% (os.path.basename(backup_file), cmd)
    os.system(cmd)


    # create recovery.conf
    rdir = '%s:%s.xlogs'% (backup_host, os.path.dirname(backup_file))
    cmd = "restore_command = '/home/enstore/enstore/sbin/enrcp %s/"% (rdir) + "%f.Z %p.Z" + " && uncompress %p.Z'"
    #cmd = "restore_command = 'klist;source /home/enstore/gettkt;klist;rsync %s/"% (rdir) + "%f.Z %p.Z" + " && uncompress %p.Z'"
    print 'Creating recovery.conf: %s'% (cmd, )
    f=open('%s/recovery.conf'%(pgdb,), 'w')
    f.write('%s\n'%(cmd,))
    if (backup_time != None) :
        f.write("recovery_target_time='%s'\n"%(backup_time,))
        f.write("recovery_target_inclusive='true'\n")        
    f.close()
    os.system("cat %s/recovery.conf"%(pgdb))
    
    # disable archive_command
    print "CWD",cwd
    cmd = 'sed -e "s/archive_command/#archive_command/g w f.1" %s/postgresql.conf'% (pgdb,)
    os.system(cmd)
    cmd = "log_line_prefix = '<%t> '"
    os.system('echo "%s" >> f.1'%(cmd,))
    cmd = 'mv f.1 %s/postgresql.conf'%(pgdb,)
    os.system(cmd)


    #create xlog dirs
    cmd = 'mkdir -p %s/pg_xlog/archive_status'% (pgdb,)
    print 'Creating xlog dirs: %s'% (cmd, )
    os.system(cmd)
    cmd='chown -R enstore.enstore %s'% (pgdb,)
    print 'Changing Ownership: %s'% (cmd,)
    os.system(cmd)

    #
    ## print "the following steps need to be made manually to ensure that all is rigth" 
    ## print "Now start postgreSQL and pnfs. Mount /pnfs/fs"
    ## print "postgreSQL can be started by /etc/init.d/postgres-boot start"
    ## print "pnfs can be started by /etc/init.d/pnfs start"



    # start postgres
    cmd='/etc/init.d/postgres-boot start'
    #cmd='source /home/enstore/gettkt;. /usr/local/etc/setups.sh; setup pnfs; . /usr/etc/pnfsSetup; su enstore -c "$PNFS_DIR/tools/linux/pgsql/bin/postmaster -D $database_postgres >/diska/postgres-log/postgres.logfile 2>&1 &"'
    #cmd='su enstore; source /usr/local/etc/setups.sh; setup pnfs; . /usr/etc/pnfsSetup; $PNFS_DIR/tools/linux/pgsql/bin/postmaster -D $database_postgres >/diska/postgres-log/postgres.logfile 2>&1 &'
    print "Starting DB server: %s"% (cmd,)
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
    
if __name__ == "__main__" :
    uname = pwd.getpwuid(os.getuid())[0]
    backup_time      = None
    sysname          = None
    postgres_version = 'v8_1_3'
    if uname != 'root':
        print "Must be 'root' to run this program"
        sys.exit(1)
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:p:t:", ["help","system=","postgres_version=","backup_time="])
    except getopt.GetoptError:
        print "Failed to process arguments"
        usage(sys.argv[0])
        sys.exit(2)
    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(1)
        if o in ("-s", "--system"):
            sysname = a
        if o in ("-p", "--postgres_version"):
            postgres_version = a
        if o in ("-t", "--backup_time"):
            backup_time = a
    if (sysname == None or sysname=="") :
        print "Error: Must specify enstore system name"
        usage(sys.argv[0])
        sys.exit(1)
    if (backup_time == None or backup_time=="" ) :
        backup_time=None
    if sys2host.has_key(sysname):
        pnfs_host   = sys2host[sysname][0]
        backup_name = sys2host[sysname][1]
    else:
        print "Error: no such system ",sysname
        usage(sys.argv[0])
        sys.exit(1)
    if not version2version.has_key(postgres_version):
        print "Error: no such postgres version", postgres_version
        usage(sys.argv[0])
        sys.exit(1)
    print sysname, postgres_version, backup_time
    rc = recover(backup_time)
    sys.exit(rc)



