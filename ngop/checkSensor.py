import os
import string
import sys
import re
import time
import stat

NEW_IPMI_NODES = ["d0ensrv0.fnal.gov", "d0ensrv1.fnal.gov",
		  "d0ensrv2.fnal.gov", "d0ensrv4.fnal.gov"]

NO_IPMI_NODES = ["d0ensrv5.fnal.gov", "d0ensrv7.fnal.gov"]

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
   if os.uname()[1] in NEW_IPMI_NODES:
	   ipmi = "ipmi_new"
	   senDict={'Processor 1 Temp':0,\
		    'Processor 2 Temp':0,\
		    'Processor Fan 1':0,\
		    'Processor Fan 2':0,\
		    }
   else:
	   ipmi = "ipmi"
	   senDict={'Processor 1 Temp':0,\
		    'Processor 2 Temp':0,\
		    'Processor 1 Fan':0,\
		    'Processor 2 Fan':0,\
		    }
   p=os.popen("/home/enstore/%s/sdrread"%(ipmi,),'r')
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
	    print 0
	    print 0
	    sys.exit(0)
	
    if os.uname()[1] in NEW_IPMI_NODES:
	    ipmi = "ipmi_new"
    else:
	    ipmi = "ipmi"
    if sys.argv[1]=="temp":
        cmd="/home/enstore/%s/sdrread|grep 'Processor.*Temp:'|awk -F':' '{print $3}'|awk '{print $2}'|awk -F'C' '{print $2}'"%(ipmi,)
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
        cmd="/home/enstore/%s/sdrread|grep 'Processor.*Fan:'|awk -F':' '{print $3}'|awk '{print $1}'"%(ipmi,)
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

