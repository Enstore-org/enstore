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

def get_size(dbFile):

    cmd = "db_stat -h " + dbHome + " -d " + dbFile + " | grep \"Number of keys in the tree\" | awk '{print $1}'"
    nkeys = os.popen(cmd).readline()[:-1]
    size = os.stat(dbFile)[stat.ST_SIZE]

    return (nkeys, size)

def backup_dbase():

    dbFile=""
    for name in os.popen("db_archive -s  -h"+dbHome).readlines():
        (nkeys, size) = get_size(name[:-1])
        stmsg = name[:-1] + " : Number of keys = "+nkeys+"  Database size = " + repr(size)
        logthis(e_errors.INFO, stmsg)
        fp = open(name[:-1]+".stat", "w")
        fp.write(stmsg)
	fp.close()
    	dbFile=dbFile+" "+name[:-1]
    cmd="tar cvf dbase.tar "+dbFile+" log.* *.stat"
    logthis(e_errors.INFO,repr(cmd))
    
    ret=os.system(cmd)
    if ret !=0 :
	logthis(e_errors.INFO, "Failed: %s"%repr(cmd))
	sys.exit(1)	
    for name in os.popen("db_archive  -h"+dbHome).readlines():
	os.system("rm "+name[:-1])
    os.system("rm *.stat")

def archive_backup():

    if hst_bck == hst_local:
	try:
	   os.mkdir(dir_bck)
	except os.error:
	   logthis(e_errors.INFO,"Error: "+dir_bck+" "+str(sys.exc_info()[1][1]))
	   sys.exit(1)

        # try to compress the tared file
        # try gzip first, if it does not exist, try compress
        # never mind if the compression programs are missing

	if os.system("gzip *.tar"):	# failed?
            os.system("compress *.tar")

	cmd="mv *.tar* "+dir_bck
	logthis(e_errors.INFO,cmd)
	ret=os.system(cmd)	
	if ret !=0 :
	   logthis(e_errors.INFO, "Failed: %s"%cmd)
	   sys.exit(1)
    else :
	cmd="rsh "+hst_bck+" 'mkdir -p "+dir_bck+"'"
	logthis(e_errors.INFO,cmd)
	ret=os.system(cmd)
	if ret !=0 :
	   logthis(e_errors.INFO, "Failed: %s"%cmd)
	   sys.exit(1)

        # try to compress the tared file
        # try gzip first, if it does not exist, try compress
        # never mind if the compression programs are missing

	if os.system("gzip *.tar"):	# failed?
            os.system("compress *.tar")

	cmd="rcp *.tar* " + hst_bck+":"+dir_bck
	logthis(e_errors.INFO, cmd)
	ret=os.system(cmd)
	if ret !=0 :
           logthis(e_errors.INFO,"Failed: %s"%cmd)
	   sys.exit(1)
	ret=os.system("rm *.tar*")
	if ret !=0 :
	   logthis(e_errors.INFO, "Failed: %s"%cmd)
	   sys.exit(1)		   
 
    
def archive_clean(ago):
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
		 logthis(e_errors.INFO, "Failed: %s"%cmd)
    else :
	remcmd="find "+bckHome+" -type d -mtime +"+repr(ago)
	cmd="rsh "+hst_bck+" "+"'"+remcmd+"'"
	logthis(e_errors.INFO, repr(cmd))
	for name in os.popen(cmd).readlines():
		name=name[:-1]
		logthis(e_errors.INFO, repr(name))
		if name :
		   if name != bckHome:
		      cmd="rsh "+hst_bck+" 'rm -rf "+name+"'"
		      logthis(e_errors.INFO, repr(cmd))
		      ret=os.system(cmd)
		      if ret != 0 :
			logthis(e_errors.INFO, "Command %s failed"%cmd)
		
if __name__=="__main__":
    import string
    import hostaddr
    Trace.init("BACKUP")
    Trace.trace(6,"backup called with args "+repr(sys.argv))

    if len(sys.argv) > 1 :
	ago=string.atoi(sys.argv[1])
    else :
	ago=100

    try:
	#dbHome = configuration_client.ConfigurationClient(\
	#		(interface.default_host(),\
	#		interface.default_port()), 3).get('database')['db_dir']
	dbInfo = configuration_client.ConfigurationClient(\
			(interface.default_host(),\
			interface.default_port())).get('database')
        dbHome = dbInfo['db_dir']
	jouHome = dbInfo['jou_dir']

    except:
	dbHome=os.environ['ENSTORE_DIR']
        jouHome=dbHome
    try:
    	os.chdir(dbHome)
    except os.error:
	logthis(e_errors.INFO,
                  "Backup Error: os.chdir "+dbHome+" "+\
                  str(sys.exc_info()[0])+(sys.exc_info()[1]))
	sys.exit(1)

    #backup_config = configuration_client.ConfigurationClient(\
    #                    (interface.default_host(),\
    #                    interface.default_port()), 3).get('backup')
    backup_config = configuration_client.ConfigurationClient(\
                        (interface.default_host(),\
                        interface.default_port())).get('backup')

    try:
        bckHome = backup_config['dir']
    except:
	bckHome="/tmp/backup"
        try:
	    os.mkdir(bckHome)
	except  os.error :
	    if sys.exc_info()[1][0] == errno.EEXIST :
		pass
	    else :
                logthis(e_errors.INFO, "backup Error - mkdir "+bckHome+\
                            str(sys.exc_info()[0])+str(sys.exc_info()[1]))
		sys.exit(1)
    dir_bck=bckHome+"/dbase."+repr(time.time())
    hst_local,junk,junk=hostaddr.gethostinfo()
    try:
        hst_bck = backup_config['host']
    except:
	hst_bck=hst_local
    logthis(e_errors.INFO, "Start database backup")
    backup_dbase()
    logthis(e_errors.INFO, "End database backup")
    logthis(e_errors.INFO, "Start volume backup")
    os.system("enstore volume --backup")
    logthis(e_errors.INFO, "End  volume backup")
    logthis(e_errors.INFO, "Start file backup")
    os.system("enstore file --backup")
    logthis(e_errors.INFO, "End file backup")
    logthis(e_errors.INFO, "Start moving to archive")
    archive_backup()
    logthis(e_errors.INFO, "Stop moving to archive")
    # logthis(e_errors.INFO, "Start cleanup archive")
    # archive_clean(ago)
    # logthis(e_errors.INFO, "End  cleanup archive")
    Trace.trace(6,"backup exit ok")
    sys.exit(0)
