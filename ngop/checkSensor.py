import os
import string
import sys
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
	cmd="/home/enstore/ipmi/sdrread|grep 'Error sending command to IPMB'"
        ret=check(cmd)
        if len(ret)!=1:
            print 1
        else:
            print 0		
    else:
        print -1
        print -1



