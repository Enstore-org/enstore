import os
import string
import sys
import stat
import time

def isTime(nm,dur):
        try:
                crtTime=os.stat(nm)[stat.ST_CTIME]
                if time.time()-crtTime>=dur:
                        os.unlink(nm)
                        return 0 #not sync for dur hours
                else:
                        return -1 #not sync less than dur
        except:
                f=open(nm,"w")
                f.close()
                return -1 #not sync less than dur
def check(cmd):
    p=os.popen(cmd,'r')
    retVal=p.readlines()
    p.close()
    if len(retVal)!=1: #always should return just one line
        print -3
        sys.exit(0)
    return string.strip(retVal[0][:-1])

if __name__=="__main__":
	try:
		dur=string.atoi(sys.argv[1])*60*60 # hours out of sync
		disp=string.atof(sys.argv[2]) # time off
	except:
		dur=4*60*60
		disp=3.0
	fileName="/var/ngop/include/.timeSync"
	#check if xntpd is running
        retVal=string.atoi(check("cat /proc/[0-9]*/stat|grep \(*xntpd\)|wc -l"))
	if retVal==0:
	    # try ntpd only
	    retVal=string.atoi(check("cat /proc/[0-9]*/stat|grep \(*ntpd\)|wc -l"))
	    executable = "ntpdc"
	else:
	    executable = "xntpdc"
        if retVal==0:
            try:
                os.unlink(fileName)
            except:
                pass
            print -2 #-2  - not alive
            sys.exit(0)
	#check if peer exists
	retVal=string.atoi(check("/usr/sbin/%s -p | grep '*'|wc -l"%(executable,)))
        if  retVal==0: #no peer to syncronize
                r=isTime(fileName,dur)
                if r==0:
                        print  0 #not sync for dur time
                else:
                        print  -1
                sys.exit(0)
	#find disp
        try:
            retVal=string.atof(check("echo `/usr/sbin/%s -p | grep '*'|awk '{print $8}'`"%(executable,)))
            if retVal>disp:
                r=isTime(fileName,dur)
                if r==0:
                        print 0 #not sync for dur time
                else:
                        print  -1
            else:# time is synchronize
                try:
                        os.unlink(fileName)
                except:
                        pass
                print  1
        except: #disp is not int
            print -3 # garbage in the line

