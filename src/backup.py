###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import sys
import time
import string

# enstore imports
import Trace
import configuration_client	# to talk to configuration server
import interface		# to get default host and port
import generic_cs		# to get enprint

def backup_dbase():

    dbFile=""
    for name in os.popen("db_archive -s  -h"+dbHome).readlines():
	dbFile=dbFile+" "+name[:-1]
    cmd="tar cvf dbase.tar "+dbFile+" log.*"
    generic_cs.enprint(cmd)
    ret=os.system(cmd)
    if ret !=0 :
	generic_cs.enprint("Failed: "+cmd)
	sys.exit(1)	
    for name in os.popen("db_archive  -h"+dbHome).readlines():
	os.system("rm "+name[:-1])
def archive_backup():

    if hst_bck == hst_local:
	try:
	   os.mkdir(dir_bck)
	except os.error:
	   generic_cs.enprint("Error: "+dir_bck+" "+str(sys.exc_info()[1][1]))
	   sys.exit(1)
	cmd="mv *.tar "+dir_bck
	generic_cs.enprint(cmd)
	ret=os.system(cmd)	
	if ret !=0 :
	   generic_cs.enprint("Failed: "+cmd)
	   sys.exit(1)
    else :
	cmd="rsh "+hst_bck+" 'mkdir -p "+dir_bck+"'"
	generic_cs.enprint(cmd)
	ret=os.system(cmd)
	if ret !=0 :
	   generic_cs.enprint("Failed: "+cmd)
	   sys.exit(1)
	cmd="rcp *.tar " + hst_bck+":"+dir_bck
	generic_cs.enprint(cmd)
	ret=os.system(cmd)
	if ret !=0 :
	   generic_cs.enprint("Failed: "+cmd)
	   sys.exit(1)
	ret=os.system("rm *.tar")
	if ret !=0 :
	   generic_cs.enprint("Failed: "+cmd)
	   sys.exit(1)		   
 
    
def archive_clean(ago):
    import stat
    today=time.time()
    day=ago*24*60*60
    lastday=today-day
    if hst_bck==hst_local :
       generic_cs.enprint(bckHome)
       for name in os.listdir(bckHome):
	statinfo=os.stat(bckHome+"/"+name)
	if statinfo[stat.ST_MTIME] < lastday :
	   cmd="rm -rf "+bckHome+"/"+name
	   generic_cs.enprint(cmd)
	   ret=os.system(cmd)
	   if ret !=0 :
		 generic_cs.enprint("Failed: "+cmd)
    else :
	remcmd="find "+bckHome+" -type d -mtime +"+repr(ago)
	cmd="rsh "+hst_bck+" "+"'"+remcmd+"'"
	generic_cs.enprint(cmd)
	for name in os.popen(cmd).readlines():
		name=name[:-1]
		generic_cs.enprint(name)
		if name :
		   if name != bckHome:
		      cmd="rsh "+hst_bck+" 'rm -rf "+name+"'"
		      generic_cs.enprint(cmd)
		      ret=os.system(cmd)
		      if ret != 0 :
			generic_cs.enprint("Command "+cmd+" failed")
		
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
	dbHome = configuration_client.ConfigurationClient(\
			interface.default_host(),\
			string.atoi(interface.default_port()), 3).get('database')['db_dir']
    except:
	dbHome=os.environ['ENSTORE_DIR']
    try:
    	os.chdir(dbHome)
    except os.error:
	generic_cs.enprint("Error: "+cmd+" "+str(sys.exc_info()[1][1]))
        Trace.trace(0,"backup Error - "+repr(cmd)+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	sys.exit(1)

    backup_config = configuration_client.ConfigurationClient(\
                        interface.default_host(),\
                        string.atoi(interface.default_port()), 3).get('backup')

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
		generic_cs.enprint("Error: "+str(sys.exc_info()[1][1]))
                Trace.trace(0,"backup Error - "+repr(cmd)+\
                            str(sys.exc_info()[0])+str(sys.exc_info()[1]))
		sys.exit(1)
    dir_bck=bckHome+"/dbase."+repr(time.time())
    hst_local=socket.gethostname()
    try:
        hst_bck = backup_config['host']
    except:
	hst_bck=hst_local
    generic_cs.enprint("Start database backup")
    backup_dbase()
    generic_cs.enprint("End database backup")
    generic_cs.enprint("Start volume backup")
    os.system("ecmd vcc --backup")
    generic_cs.enprint("End  volume backup")
    generic_cs.enprint("Start file backup")
    os.system("ecmd fcc --backup")
    generic_cs.enprint("End file backup")
    generic_cs.enprint("Start moving to archive")
    archive_backup()
    generic_cs.enprint("Stop moving to archive")
    generic_cs.enprint("Start cleanup archive")
    archive_clean(ago)
    generic_cs.enprint("End  cleanup archive")
    Trace.trace(1,"backup exit ok")
    sys.exit(0)
