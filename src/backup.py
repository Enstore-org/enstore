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
import interface		# to get default host and port
import e_errors                 # error information
import log_client               # for getting info into the log


# get_size(dbFile) -- get the number of records and size of a dbFile

def logthis(code, message):
    #Trace.log(code,message)
    log_client.logthis(code,message)
    print "Logging", code, message

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

def backup_dbase(dbHome):

    dbFiles=""
    filelist = map(string.strip,os.popen("db_archive -s  -h"+dbHome).readlines())
    
    for name in filelist:
        (nkeys, size) = get_size(dbHome,name)
        stmsg = "%s :  Number of keys = %s  Database size = %s"%(name,nkeys,size)
        logthis(e_errors.INFO, stmsg)
        fp = open(name+".stat", "w")
        fp.write(stmsg)
	fp.close()
    	dbFiles=dbFiles+" "+name
    logfiles = string.join(glob.glob('log.*'),' ')
    statfiles = string.join(glob.glob('*.stat'),' ')
    BFIDS = string.join(glob.glob('BFIDS'),' ')
    cmd="tar cvf dbase.tar %s %s %s %s"%(dbFiles,logfiles,statfiles,BFIDS)
    logthis(e_errors.INFO,cmd)
    
    ret=os.system(cmd)
    if ret !=0 :
	logthis(e_errors.INFO, "Failed: %s"%(cmd,))
	sys.exit(1)
    filelist = map(string.strip,os.popen("db_archive  -h"+dbHome).readlines())
    for name in filelist:
	os.unlink(name)
    filelist=os.listdir('.')
    for name in filelist:
        if len(name)>5 and name[-5:]=='.stat':
            os.unlink(name)


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

	if os.system("gzip *.tar"):	# failed?
            os.system("compress *.tar")
        tarfiles=glob.glob("*.tar*")
        for file in tarfiles:
            os.rename(file, os.path.join(dir_bck, file))
    else :
	cmd="rsh "+hst_bck+" 'mkdir -p "+dir_bck+"'"
	logthis(e_errors.INFO,cmd)
	ret=os.system(cmd)
	if ret !=0 :
	   logthis(e_errors.INFO, "Failed: %s"%(cmd,))
	   sys.exit(1)

        # try to compress the tarred file
        # try gzip first, if it does not exist, try compress
        # never mind if the compression programs are missing

	if os.system("gzip *.tar"):	# failed?
            os.system("compress *.tar")

	cmd="rcp *.tar* " + hst_bck+":"+dir_bck
	logthis(e_errors.INFO, cmd)
	ret=os.system(cmd)
	if ret !=0 :
           logthis(e_errors.INFO,"Failed: %s"%(cmd,))
	   sys.exit(1)
        tarfiles=glob.glob("*.tar*")
        for file in tarfiles:
            os.unlink(file)

 
    
def archive_clean(ago,hst_local,hst_bck,bckHome):
    import stat
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
	cmd="rsh %s '%s'"%(hst_bck,remcmd)
	logthis(e_errors.INFO, repr(cmd))
        names= map(string.strip,os.popen(cmd).readlines())
        for name in names:
		logthis(e_errors.INFO, name)
		if name and name != bckHome:
                    cmd="rsh "+hst_bck+" 'rm -rf "+name+"'"
                    logthis(e_errors.INFO, cmd)
                    ret=os.system(cmd)
                    if ret != 0 :
                        logthis(e_errors.INFO, "Command %s failed"%(cmd,))
		
if __name__=="__main__":
    import string
    import hostaddr
    Trace.init("BACKUP")
    Trace.trace(6,"backup called with args %s"%(sys.argv,))

    if len(sys.argv) > 1 :
	ago=string.atoi(sys.argv[1])
    else :
	ago=100

    try:
	dbInfo = configuration_client.ConfigurationClient(
			(interface.default_host(),
			interface.default_port())).get('database')
        dbHome = dbInfo['db_dir']
	jouHome = dbInfo['jou_dir']

    except:
	dbHome=os.environ['ENSTORE_DIR']
        jouHome=dbHome
    try:
    	os.chdir(dbHome)
    except os.error, msg:
	logthis(e_errors.INFO,
                "Backup Error: os.chdir(%s): %s"%(dbHome,msg))
	sys.exit(1)

    backup_config = configuration_client.ConfigurationClient(
                        (interface.default_host(),
                        interface.default_port())).get('backup')

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
    logthis(e_errors.INFO, "Start database backup")
    backup_dbase(dbHome)
    logthis(e_errors.INFO, "End database backup")
    logthis(e_errors.INFO, "Start volume backup")
    os.system("enstore volume --backup")
    logthis(e_errors.INFO, "End  volume backup")
    logthis(e_errors.INFO, "Start file backup")
    os.system("enstore file --backup")
    logthis(e_errors.INFO, "End file backup")
    logthis(e_errors.INFO, "Start moving to archive")
    archive_backup(hst_bck,hst_local,dir_bck)
    logthis(e_errors.INFO, "Stop moving to archive")
    # logthis(e_errors.INFO, "Start cleanup archive")
    # archive_clean(ago,hst_local,hst_back,bckHome)
    # logthis(e_errors.INFO, "End  cleanup archive")
    Trace.trace(6,"backup exit ok")
    sys.exit(0)
