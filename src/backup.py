#! /usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import sys
import time
import string
import errno
import stat
import glob

# enstore imports
import Trace
import configuration_client	# to talk to configuration server
import option		        # to get default host and port
import e_errors                 # error information
import log_client               # for getting info into the log
import hostaddr
import enstore_functions2

journal_backup = 'JOURNALS'     # for journal file backup

def logthis(code, message):
    #Trace.log(code,message)
    log_client.logthis(code,message)
    print "Logging", code, message

# get_size(dbFile) -- get the number of records and size of a dbFile
def get_size(dbHome,dbFile):
    nkeys="Unknown"
    cmd = "db_stat -h " + dbHome + " -d " + dbFile
    lines=os.popen(cmd).readlines()
    for l in lines:
        if string.find(l,"Number of keys in the tree")>=0:
            w=string.split(l)
            nkeys=w[0]
            break

    size = os.stat(os.path.join(dbHome,dbFile))[stat.ST_SIZE]
    return (nkeys, size)

def pgdb_backup(host, port, dbHome):
    path = os.path.join(dbHome, 'enstoredb.dmp')
    cmd = "pg_dump -h %s -p %d -F c -f %s enstoredb"%(host, port, path)
    os.system(cmd)


def archive_backup(hst_bck,hst_local,dir_bck):

    if hst_bck == hst_local:
	try:
	   os.mkdir(dir_bck)
	except os.error, msg:
	   logthis(e_errors.INFO,"Error: %s %s"%(dir_bck,msg))
	   sys.exit(1)

        # try to compress the tarred file
        # try gzip first, if it does not exist, try compress
        # never mind if the compression programs are missing

	if os.system("gzip -f *.tar"):	# failed?
            os.system("compress *.tar")
        tarfiles=glob.glob("*")
        for file in tarfiles:
            cmd = "cp %s %s"%(file, os.path.join(dir_bck, file))
            os.system(cmd)
    else :
	cmd="enrsh "+hst_bck+" 'mkdir -p "+dir_bck+"'"
	logthis(e_errors.INFO,cmd)
	ret=os.system(cmd)
        # check it
        cmd2 = "enrsh "+hst_bck+" 'ls -d "+dir_bck+"'"
        res = os.popen(cmd2).readlines()
        ret = 1
        for i in res:
            if i.strip() == dir_bck:
                ret = 0
                break
	if ret !=0 :
	   logthis(e_errors.INFO, "Failed: %s"%(cmd,))
	   sys.exit(1)

        jou_dir = os.path.join(os.path.split(dir_bck)[0], journal_backup)

        # check and create the directory if it is needed
	cmd = "enrsh "+hst_bck+" 'ls -d "+jou_dir+"'"
        res = os.popen(cmd).readlines()
        ret = 1
        for i in res:
            if i.strip() == jou_dir: # found it
                ret = 0
                break
        if ret != 0:
            # create it
            cmd = "enrsh "+hst_bck+" 'mkdir -p "+jou_dir+"'"
            logthis(e_errors.INFO,cmd)
            ret = os.system(cmd)
            # check it
            cmd2= "enrsh "+hst_bck+" 'ls -d "+jou_dir+"'"
            res = os.popen(cmd2).readlines()
            ret = 1
            for i in res:
                if i.strip() == jou_dir: # found it
                    ret = 0
                    break
            if ret != 0:
                logthis(e_errors.INFO, "Failed: %s"%(cmd,))
                sys.exit(1)

        # try to compress the tarred file
        # try gzip first, if it does not exist, try compress
        # never mind if the compression programs are missing

        fjbk = 'file.tar.gz'
        vjbk = 'volume.tar.gz'

	if os.system("gzip -f *.tar"):	# failed?
            os.system("compress *.tar")
            fjbk = 'file.tar.Z'
            vjbk = 'volume.tar.Z'

        # do not gzip enstoredb.dmp any more, it is already compressed

	cmd="enrcp *.tar* " + " %s "%("enstoredb.dmp")+ hst_bck+":"+dir_bck
	logthis(e_errors.INFO, cmd)
	ret=os.system(cmd)
	if ret !=0 :
           logthis(e_errors.INFO,"Failed: %s"%(cmd,))
	   sys.exit(1)
        # duplicate the journal backup in another directory
        time_stamp = '.'+str(time.time())
        p = string.split(fjbk, '.')
        p[0] = p[0]+time_stamp
        fp = string.join(p, '.')
        cmd = "enrcp "+fjbk+' '+hst_bck+":"+os.path.join(jou_dir, fp)
        if os.system(cmd):
            Trace.log(e_errors.ERROR, "Failed: "+cmd)
            sys.exit(1)

        p = string.split(vjbk, '.')
        p[0] = p[0]+time_stamp
        fp = string.join(p, '.')
        cmd = "enrcp "+vjbk+' '+hst_bck+":"+os.path.join(jou_dir, fp)
        if os.system(cmd):
            Trace.log(e_errors.ERROR, "Failed: "+cmd)
            sys.exit(1)

        now=time.gmtime(time.time())
        day=now[7]
        hour=now[3]
        node=os.uname()[1]
        gang=node[0:3]
        if gang == 'd0e':
            gang = 'd0'

#        tarfiles=glob.glob("*.tar*")
#        for file in tarfiles:
#            # copy the files over to a "paranoid" backup copy. Continue if error
#            cmd = "enrcp %s cachen2a:/diska/enstore_backup/%sen/database/%s.%s.%s"%(file,gang,day,hour,file)
#            logthis(e_errors.INFO,cmd)
#            if os.system(cmd):
#                Trace.log(e_errors.ERROR, "Failed,ignored: "+cmd)
#            os.unlink(file)

def archive_clean(ago,hst_local,hst_bck,bckHome):
    today=time.time()
    day=ago*24*60*60
    lastday=today-day
    if hst_bck==hst_local :
       logthis(e_errors.INFO, repr(bckHome))
       for name in os.listdir(bckHome):
	statinfo=os.stat(bckHome+"/"+name)
	if statinfo[stat.ST_MTIME] < lastday :
	   cmd="rm -rf "+bckHome+"/"+name
	   logthis(e_errors.INFO, repr(cmd))
	   ret=os.system(cmd)
	   if ret !=0 :
		 logthis(e_errors.INFO, "Failed: %s"%(cmd,))
    else :
        remcmd="find %s -type d -mtime %s"%(bckHome,ago)
	cmd="enrsh %s '%s'"%(hst_bck,remcmd)
	logthis(e_errors.INFO, repr(cmd))
        names= map(string.strip,os.popen(cmd).readlines())
        for name in names:
		logthis(e_errors.INFO, name)
		if name and name != bckHome:
                    cmd="enrsh "+hst_bck+" 'rm -rf "+name+"'"
                    logthis(e_errors.INFO, cmd)
                    ret=os.system(cmd)
                    if ret != 0 :
                        logthis(e_errors.INFO, "Command %s failed"%(cmd,))


class BackupInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=1):
        option.Interface.__init__(self, args=args, user_mode=user_mode)

def do_work(intf):
    Trace.init("BACKUP")

    try:
	dbInfo = configuration_client.ConfigurationClient(
			(enstore_functions2.default_host(),
			 enstore_functions2.default_port())).get('database')
        dbHome = dbInfo.get('db_backup_dir',dbInfo.get('db_dir'))
	jouHome = dbInfo['jou_dir']
    except:
	dbHome=os.environ['ENSTORE_DIR']
        jouHome=dbHome

    print "dbInfo =", `dbInfo`

    try:
    	os.chdir(dbHome)
    except os.error, msg:
	logthis(e_errors.INFO,
                "Backup Error: os.chdir(%s): %s"%(dbHome,msg))
	sys.exit(1)

    backup_config = configuration_client.ConfigurationClient(
                        (enstore_functions2.default_host(),
			 enstore_functions2.default_port())).get('backup')

    print "backup_config =", `backup_config`

    try:
        bckHome = backup_config['dir']
    except:
	bckHome="/tmp/backup"
        try:
	    os.mkdir(bckHome)
	except  os.error, msg :
	    if msg.errno == errno.EEXIST :
		pass
	    else :
                logthis(e_errors.INFO, "backup Error - os.mkdir(%s): %s" %
                        (bckHome, msg))
		sys.exit(1)
    dir_bck=bckHome+"/dbase."+repr(time.time())
    hst_local,junk,junk=hostaddr.gethostinfo()
    try:
        hst_bck = backup_config['host']
    except:
	hst_bck = hst_local
    logthis(e_errors.INFO, "Start postgresql database backup")
    pgdb_backup(dbInfo['db_host'], dbInfo['db_port'], dbHome)
    logthis(e_errors.INFO, "End postgresql database backup")
    logthis(e_errors.INFO, "Start volume backup")
    os.system("enstore volume --backup")
    logthis(e_errors.INFO, "End  volume backup")
    logthis(e_errors.INFO, "Start file backup")
    os.system("enstore file --backup")
    logthis(e_errors.INFO, "End file backup")
    os.system("mv %s/*.tar %s"%(jouHome, dbHome))
    logthis(e_errors.INFO, "Start moving to archive")
    archive_backup(hst_bck,hst_local,dir_bck)
    logthis(e_errors.INFO, "Stop moving to archive")
    Trace.trace(6,"backup exit ok")
    return 0



if __name__=="__main__":
    intf = BackupInterface(user_mode=0)

    sys.exit(do_work(intf))
