###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import sys
import time
import string
import errno

# enstore imports
import Trace
import configuration_client	# to talk to configuration server
import interface		# to get default host and port
import e_errors                 # error information

dbHome=""    # quiet lint
dir_bck=""   # quiet lint
bckHome=""   # quiet lint
hst_bck=""   # quiet lint
hst_local="" # quiet lint

def backup_dbase():

    dbFile=""
    for name in os.popen("db_archive -s  -h"+dbHome).readlines():
	dbFile=dbFile+" "+name[:-1]
    cmd="tar cvf dbase.tar "+dbFile+" log.*"
    Trace.log(e_errors.INFO,repr(cmd))
    ret=os.system(cmd)
    if ret !=0 :
	Trace.log(e_errors.INFO, "Failed: %s"%repr(cmd))
	sys.exit(1)	
    for name in os.popen("db_archive  -h"+dbHome).readlines():
	os.system("rm "+name[:-1])
def archive_backup():

    if hst_bck == hst_local:
	try:
	   os.mkdir(dir_bck)
	except os.error:
	   Trace.log(e_errors.INFO,
                     "Error: "+dir_bck+" "+str(sys.exc_info()[1][1]))
	   sys.exit(1)
	cmd="mv *.tar "+dir_bck
	Trace.log(e_errors.INFO,cmd)
	ret=os.system(cmd)	
	if ret !=0 :
	   Trace.log(e_errors.INFO, "Failed: %s"%cmd)
	   sys.exit(1)
    else :
	cmd="rsh "+hst_bck+" 'mkdir -p "+dir_bck+"'"
	Trace.log(e_errors.INFO,cmd)
	ret=os.system(cmd)
	if ret !=0 :
	   Trace.log(e_errors.INFO, "Failed: %s"%cmd)
	   sys.exit(1)
	cmd="rcp *.tar " + hst_bck+":"+dir_bck
	Trace.log(e_errors.INFO, cmd)
	ret=os.system(cmd)
	if ret !=0 :
           Trace.log(e_errors.INFO,"Failed: %s"%cmd)
	   sys.exit(1)
	ret=os.system("rm *.tar")
	if ret !=0 :
	   Trace.log(e_errors.INFO, "Failed: %s"%cmd)
	   sys.exit(1)		   
 
    
def archive_clean(ago):
    import stat
    today=time.time()
    day=ago*24*60*60
    lastday=today-day
    if hst_bck==hst_local :
       Trace.log(e_errors.INFO, repr(bckHome))
       for name in os.listdir(bckHome):
	statinfo=os.stat(bckHome+"/"+name)
	if statinfo[stat.ST_MTIME] < lastday :
	   cmd="rm -rf "+bckHome+"/"+name
	   Trace.log(e_errors.INFO, repr(cmd))
	   ret=os.system(cmd)
	   if ret !=0 :
		 Trace.log(e_errors.INFO, "Failed: %s"%cmd)
    else :
	remcmd="find "+bckHome+" -type d -mtime +"+repr(ago)
	cmd="rsh "+hst_bck+" "+"'"+remcmd+"'"
	Trace.log(e_errors.INFO, repr(cmd))
	for name in os.popen(cmd).readlines():
		name=name[:-1]
		Trace.log(e_errors.INFO, repr(name))
		if name :
		   if name != bckHome:
		      cmd="rsh "+hst_bck+" 'rm -rf "+name+"'"
		      Trace.log(e_errors.INFO, repr(cmd))
		      ret=os.system(cmd)
		      if ret != 0 :
			Trace.log(e_errors.INFO, "Command %s failed"%cmd)
		
if __name__=="__main__":
    import string
    Trace.init("BACKUP")
    Trace.trace(6,"backup called with args "+repr(sys.argv))

    try:
	import SOCKS
        socket = SOCKS
    except ImportError:
	import socket
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
	Trace.log(e_errors.INFO,
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
                Trace.log(e_errors.INFO, "backup Error - mkdir "+bckHome+\
                            str(sys.exc_info()[0])+str(sys.exc_info()[1]))
		sys.exit(1)
    dir_bck=bckHome+"/dbase."+repr(time.time())
    hst_local=socket.gethostname()
    try:
        hst_bck = backup_config['host']
    except:
	hst_bck=hst_local
    Trace.log(e_errors.INFO, "Start database backup")
    backup_dbase()
    Trace.log(e_errors.INFO, "End database backup")
    Trace.log(e_errors.INFO, "Start volume backup")
    os.system("ecmd vcc --backup")
    Trace.log(e_errors.INFO, "End  volume backup")
    Trace.log(e_errors.INFO, "Start file backup")
    os.system("ecmd fcc --backup")
    Trace.log(e_errors.INFO, "End file backup")
    Trace.log(e_errors.INFO, "Start moving to archive")
    archive_backup()
    Trace.log(e_errors.INFO, "Stop moving to archive")
    Trace.log(e_errors.INFO, "Start cleanup archive")
    archive_clean(ago)
    Trace.log(e_errors.INFO, "End  cleanup archive")
    Trace.trace(6,"backup exit ok")
    sys.exit(0)
