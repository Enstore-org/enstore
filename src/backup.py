###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import sys
import time

# enstore imports
import Trace

def backup_dbase():

    dbFile=""
    for name in os.popen("db_archive -s  -h"+dbHome).readlines():
	dbFile=dbFile+" "+name[:-1]
    cmd="tar cvf dbase.tar "+dbFile+" log.*"
    print cmd
    ret=os.system(cmd)
    if ret !=0 :
	print "Failed:",cmd
	sys.exit(1)	
    for name in os.popen("db_archive  -h"+dbHome).readlines():
	os.system("rm "+name[:-1])
def archive_backup():

    if hst_bck == hst_local:
	try:
	   os.mkdir(dir_bck)
	except os.error:
	   print "Error:",dir_bck,sys.exc_info()[1][1]
	   sys.exit(1)
	cmd="mv *.tar "+dir_bck
	print cmd
	ret=os.system(cmd)	
	if ret !=0 :
	   print "Failed:",cmd
	   sys.exit(1)
    else :
	cmd="rsh "+hst_bck+" 'mkdir -p "+dir_bck+"'"
	print cmd
	ret=os.system(cmd)
	if ret !=0 :
	   print "Failed:",cmd
	   sys.exit(1)
	cmd="rcp *.tar " + hst_bck+":"+dir_bck
	print cmd
	ret=os.system(cmd)
	if ret !=0 :
	   print "Failed:",cmd
	   sys.exit(1)
	ret=os.system("rm *.tar")
	if ret !=0 :
	   print "Failed:",cmd
	   sys.exit(1)		   
 
    
def archive_clean(ago):
    import stat
    today=time.time()
    day=ago*24*60*60
    lastday=today-day
    if hst_bck==hst_local :
       print bckHome
       for name in os.listdir(bckHome):
	statinfo=os.stat(bckHome+"/"+name)
	if statinfo[stat.ST_MTIME] < lastday :
	   cmd="rm -rf "+bckHome+"/"+name
	   print cmd
	   ret=os.system(cmd)
	   if ret !=0 :
		print "Failed:",cmd
    else :
	remcmd="find "+bckHome+" -type d -mtime +"+repr(ago)
	cmd="rsh "+hst_bck+" "+"'"+remcmd+"'"
	print cmd
	for name in os.popen(cmd).readlines():
		name=name[:-1]
		print name
		if name :
		   if name != bckHome:
		      cmd="rsh "+hst_bck+" 'rm -rf "+name+"'"
		      print cmd
		      ret=os.system(cmd)
		      if ret != 0 :
			print "Command",cmd, "failed"
		
if __name__=="__main__":
    import string
    Trace.init("backup")
    Trace.trace(1,"backup called with args "+repr(sys.argv))

    try:
	import SOCKS; socket = SOCKS
    except ImportError:
	import socket
    if len(sys.argv) > 1 :
	ago=string.atoi(sys.argv[1])
    else :
	ago=10

    try:
	dbHome=os.environ['ENSTORE_DB']
    except:
	dbHome=os.environ['ENSTORE_DIR']
    try:
    	os.chdir(os.environ['ENSTORE_DB'])
    except os.error:
	print "Error:",cmd,sys.exc_info()[1][1]
        Trace.trace(0,"backup Error - "+repr(cmd)+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	sys.exit(1)
    try:
	bckHome=os.environ['ENSTORE_DB_BACKUP']
    except:
	bckHome="/tmp/backup"
        try:
	    os.mkdir(bckHome)
	except  os.error :
	    if sys.exc_info()[1][0] == errno.EEXIST :
		pass
	    else :
		print "Error: ",sys.exc_info()[1][1]
                Trace.trace(0,"backup Error - "+repr(cmd)+\
                            str(sys.exc_info()[0])+str(sys.exc_info()[1]))
		sys.exit(1)
    dir_bck=bckHome+"/dbase."+repr(time.time())
    hst_local=socket.gethostname()
    try:
	hst_bck=os.environ['ENSTORE_BCKP_HST']
    except:
	hst_bck=hst_local
    print "Start database backup"
    backup_dbase()
    print "End database backup"
    print "Start volume backup"
    os.system("ecmd vcc --backup")
    print "End  volume backup"
    print "Start file backup"
    os.system("ecmd fcc --backup")
    print "End file backup"
    print "Start moving to archive"
    archive_backup()
    print "Stop moving to archive"
    print "Start cleanup archive"
    archive_clean(ago)
    print "End  cleanup archive"
    Trace.trace(1,"backup exit ok")
    sys.exit(0)
