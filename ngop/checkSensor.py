import os
import string
import sys
import re
import time
import stat
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
   p=os.popen('/home/enstore/ipmi/sdrread','r')
   retList=p.readlines()
   senDict={'Processor 1 Temp':0,\
            'Processor 2 Temp':0,\
            'Processor 1 Fan':0,\
            'Processor 2 Fan':0,\
   }
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
    if sys.argv[1]=="temp":
        cmd="/home/enstore/ipmi/sdrread|grep 'Processor.*Temp:'|awk -F':' '{print $3}'|awk '{print $2}'|awk -F'C' '{print $2}'"
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
        cmd="/home/enstore/ipmi/sdrread|grep 'Processor.*Fan:'|awk -F':' '{print $3}'|awk '{print $1}'"
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

