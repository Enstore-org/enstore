import os
import string
import sys
import re
import time
import stat

OLD_IPMI_NODES = ["d0ensrv3.fnal.gov",
		  "stkensrv0.fnal.gov",   "stkensrv2.fnal.gov",
		  "stkensrv3.fnal.gov",   "stkensrv4.fnal.gov",
		  "cdfensrv0.fnal.gov",   "cdfensrv2.fnal.gov",
		  "cdfensrv3.fnal.gov",   "cdfensrv4.fnal.gov",
		  "d0enmvr4a.fnal.gov",   "d0enmvr5a.fnal.gov",   "d0enmvr6a.fnal.gov",
		  "d0enmvr7a.fnal.gov",   "d0enmvr9a.fnal.gov",   "d0enmvr10a.fnal.gov",
		  "d0enmvr11a.fnal.gov",  "d0enmvr12a.fnal.gov",  "d0enmvr13a.fnal.gov",
		  "d0enmvr14a.fnal.gov",  "d0enmvr15a.fnal.gov",  "d0enmvr16a.fnal.gov",
		  "d0enmvr17a.fnal.gov",  "d0enmvr18a.fnal.gov",  "d0enmvr19a.fnal.gov",
		  "d0enmvr20a.fnal.gov",  "d0enmvr21a.fnal.gov",  "d0enmvr22a.fnal.gov",
		  "d0enmvr23a.fnal.gov",  "d0enmvr24a.fnal.gov",  "d0enmvr25a.fnal.gov",
		  "stkenmvr1a.fnal.gov",  "stkenmvr2a.fnal.gov",  "stkenmvr3a.fnal.gov",
		  "stkenmvr4a.fnal.gov",  "stkenmvr5a.fnal.gov",  "stkenmvr6a.fnal.gov",
		  "stkenmvr7a.fnal.gov",  "stkenmvr8a.fnal.gov",  "stkenmvr9a.fnal.gov",
		  "cdfenmvr1a.fnal.gov",  "cdfenmvr2a.fnal.gov",  "cdfenmvr3a.fnal.gov",
		  "cdfenmvr4a.fnal.gov",  "cdfenmvr5a.fnal.gov",  "cdfenmvr6a.fnal.gov",
		  "cdfenmvr7a.fnal.gov",  "cdfenmvr8a.fnal.gov",  "cdfenmvr9a.fnal.gov",
		  "cdfenmvr10a.fnal.gov",
		  ]

# ignore console servers and null movers
NO_IPMI_NODES = ["d0ensrv5.fnal.gov",    "d0ensrv7.fnal.gov",    "d0enmvr8a.fnal.gov",
		 "stkensrv5.fnal.gov",   "stkensrv7.fnal.gov",
		 "cdfensrv5.fnal.gov",   "cdfensrv7.fnal.gov",
		 "d0enmvr1a.fnal.gov",   "d0enmvr2a.fnal.gov",   "d0enmvr3a.fnal.gov",
		 "d0enmvr8a.fnal.gov",
		 "stkenmvr12a.fnal.gov", "stkenmvr13a.fnal.gov", "stkenmvr14a.fnal.gov",
		 ]

def isTime(nm,dur):
	try:
		crtTime=os.stat(nm)[stat.ST_CTIME]
		if time.time()-crtTime>=dur:
			return 0 #not sync for dur hours
		else:
			return -1 #not sync less than dur
	except:
		f=open(nm,"w")
		f.close()
		return -1 #not sync less than dur

def checkFirmware():
   if os.uname()[1] in OLD_IPMI_NODES:
	   senDict={'Processor 1 Temp':0,\
		    'Processor 2 Temp':0,\
		    'Processor 1 Fan':0,\
		    'Processor 2 Fan':0,\
		    }
   else:
	   senDict={'Processor 1 Temp':0,\
		    'Processor 2 Temp':0,\
		    'Processor Fan 1':0,\
		    'Processor Fan 2':0,\
		    }
   p=os.popen("%s/sdrread"%(os.environ['IPMI_DIR'],),'r')
   retList=p.readlines()
   for line in retList:
	if string.find(line,"GETINFO failed")>=0:
		return 2,line[:-1]
	if string.find(line,"Error sending command to IPMB")>=0:
		return 1,line[:-1]
	for str in senDict.keys():
		if string.find(line,'%s:' % (str,))>=0:
			senDict[str]=1
   for str,val in senDict.items():
	if not val:
		if not isTime("/var/ngop/include/.sensor",600):
			return 2,"Missing information at least about %s" % (str,)
		else:
			return 0,"Missing information at least about %s for less then 10 min" % (str,
)
   try:
	os.unlink("/var/ngop/include/.sensor")
   except:
	pass
   return 0,"Ok"

def check(cmd):
    p=os.popen(cmd,'r')
    retVal=p.readlines()
    p.close()
    return retVal

if __name__=="__main__":
    if len(sys.argv)!=2:
	print -1
	print -1
	sys.exit(0)

    if os.uname()[1] in NO_IPMI_NODES:
	    if sys.argv[1]=="fan":
		    print 3000
		    print 3000
	    else:
		    print 0
		    print 0
	    sys.exit(0)

    if sys.argv[1]=="temp":
	cmd="%s/sdrread|grep 'Processor.*Temp:'|awk -F':' '{print $3}'|awk '{print $2}'|awk -F'C' '{print $2}'"%(os.environ['IPMI_DIR'],)
	ret=check(cmd)
	if len(ret)!=2:
	    print -1
	    print -1
	    sys.exit(0)
	try:
	    for r in ret:
		print string.atoi(string.strip(r[:-1]))
	except:
	    print -1
	    print -1
	    sys.exit(0)
    elif sys.argv[1]=="fan":
	cmd="%s/sdrread|grep 'Processor.*Fan:'|awk -F':' '{print $3}'|awk '{print $1}'"%(os.environ['IPMI_DIR'],)
	ret=check(cmd)
	if len(ret)!=2:
	    print -1
	    print -1
	    sys.exit(0)
	try:
	    for r in ret:
		print string.atof(string.strip(r[:-1]))
	except:
	    print -1
	    print -1
    elif sys.argv[1]=="firmware":
	retVal,reason=checkFirmware()
	print retVal
	print re.sub(" ",'_',reason)
    else:
	print -1
	print -1
