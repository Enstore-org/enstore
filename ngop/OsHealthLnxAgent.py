# @(#) $Id$
# $Author$
# $Log$
# Revision 1.13  2001/03/22 22:23:52  tlevshin
# b0_6, bug fixes, doAction Api,added name to ma name
#
# Revision 1.12  2001/01/12 19:51:37  tlevshin
# *** empty log message ***
#
# Revision 1.11  2001/01/12 17:37:39  tlevshin
# *** empty log message ***
#
# Revision 1.10  2000/12/19 17:56:15  tlevshin
# added temperature
#
# Revision 1.9  2000/12/06 16:19:13  tlevshin
# *** empty log message ***
#
# Revision 1.8  2000/12/04 16:03:53  tlevshin
# *** empty log message ***
#
# Revision 1.7  2000/11/30 21:29:10  tlevshin
# *** empty log message ***
#
# Revision 1.6  2000/11/30 20:51:11  tlevshin
# *** empty log message ***
#
# Revision 1.5  2000/09/29 21:49:15  tlevshin
# *** empty log message ***
#
# Revision 1.4  2000/09/27 22:07:24  tlevshin
# *** empty log message ***
#
# Revision 1.3  2000/08/31 14:54:02  tlevshin
# *** empty log message ***
#
# Revision 1.2  2000/08/30 22:26:07  tlevshin
# *** empty log message ***
#
# Revision 1.1  2000/07/26 21:28:40  tlevshin
# initial version for standart monitoring agent
#


import os
import string
from Worker import Worker
import MA_API   
import ngop_global

class OSFunc(Worker):
    def __init__(self):
	Worker.__init__(self)
	self.runAway={}

    def chkProc(self,name):
	file,str = string.splitfields(name,',')
	print "file is %s, str is %s" % (file, str)
	cnt=0
	try:
	    lines = os.popen('cat %s' % (file,),'r').readlines()

	    if not len(lines):
		return -1
	    for line in lines:
		if string.find(line,str)>=0:
		    cnt=cnt+1
	except:
	    cnt=-1
        
        print "The number of processor is %s"%(cnt,)
	return cnt

    def tempMsr(self,name):
	try:
	    lines=os.popen('/usr/bin/sysstat','r').readlines()
	except:
	    return -1

        if not len(lines):
            return -1

	tmpStr=""
	for line in lines:
	    if string.find(line,name)>=0:
		tmpStr=line[:-1]
		break
	if not len(tmpStr):
	    return -1
	temp=float(string.splitfields(string.splitfields(tmpStr,':')[1])[0])
	return temp


    def checkSensor(self, name):
        try:
            lines=os.popen("/usr/local/bin/sdrread" , "r").readlines()
            print "checkSensor and name is %s"%(name,)
        except:
            return -1

        if not len(lines):   
            return -1
       
        name1, name2 = string.splitfields(name,":") 
        found=0
        for line in lines:
            list1 = string.splitfields(line,":")
            if len(list1) > 1:
               if  list1[1] == name1:
                 found = 1
                 break
        for line in lines:
            list2 = string.splitfields(line,":")
            if len(list2) > 1:
               if  list2[1] == name2:
                 break

        if not found:
            return -1

        if found == 1:      
            val1=float(string.splitfields(list1[2])[0])
            val2=float(string.splitfields(list2[2])[0])
            print "value of %s is %s"%(list1[1], val1)
            print "value of %s is %s"%(list2[1], val2)
            if string.find(line, "Temp") >= 0:
               print "return value is %s"%(max(val1, val2),)
               return max(val1, val2)
            else:
               print "return value is %s"%(min(val1, val2),)
               return min(val1, val2)


    def timeSync(self,name):
        try:
            lines=os.popen("/usr/sbin/xntpdc -p | grep '*'", "r").readlines()
	except:
	    return -1

        if not len(lines):
            return 0 

        try:
            val = float(string.splitfields(lines[0])[-1])
            print "Time is syncronized"
            print "the value of disp is %s"%(val,)
            val = 1000000*val
            print "return value is %s"%(val,)
            return val
        except:
            return 0

    def fileSystem(self,name):
	try:
	    lines=os.popen('df %s' % name,'r').readlines()[1:]
            if not len(lines):
                return -2

	    line=string.join(lines)
            print line
	    return string.atoi(string.splitfields(line)[4][:-1])
	except:
	    return -1

    def cpuLoad(self,name=None):
	line=os.popen('uptime','r').readlines()[0]
        if not len(line):
            return -1

	ind=string.find(line,'load average:')
	return string.atof(string.splitfields(line[ind+len('load average:'):],',')[1])

    def memLoad(self,name):
	lines=os.popen('free -o ','r').readlines()
        if not len(lines):
            return -1

	if name=="memory":
	    line=string.strip(lines[1])
	    line=string.strip(line[len('Mem:'):])
	    arg=string.splitfields(line)
	    total=string.atof(arg[0])
	    used=string.atof(arg[1])-string.atof(arg[4])-string.atof(arg[5])
	if name=="swap":
	    line=string.strip(lines[2])
	    line=string.strip(line[len('Swap:'):])
	    arg=string.splitfields(line)
	    total=string.atof(arg[0])
	    used=string.atof(arg[1])
	return int((used/total)*100)

    def usrUsage(self,str):
	list=string.splitfields(str,',')
	name=list[0]
	if name=='usrNum':
	    return  int(string.strip(os.popen('who|wc -l','r').\
				     readline()[:-1]))
	elif name=='procNum':
	    return int(string.strip(os.popen('ps hax|wc -l','r').readline()[:-1]))
	elif name=="runAway":
	    tm=list[1]
	    threshold=list[2]
	    l=os.popen("ps auxgww|awk '{print $1,$2,$3}'",'r').readlines()
	    max=0
	    for line in l[1:]:
		user,proc,cpu=string.splitfields(line[:-1])
		if self.runAway.has_key(proc):
		    cpuList=self.runAway[proc][1]
		    cpuList.append(float(cpu))
		else:
		    if float(cpu)>threshold:
			self.runAway[proc]=(user,[])
			self.runAway[proc][1].append(float(cpu))

		max=0
		
		dict=self.runAway.keys()
		lenDict=len(dict)
		for i in range(lenDict):
		    key=dict[i]
		    cpuList=self.runAway[key][1] 
		    if len(cpuList)==tm:
			average=0
			for cpu in cpuList:
			    average=average+cpu
			average=average/len(cpuList)
			if average>max:
			    max=average
			    user=self.runAway[key][0]
			    proc=key
			else:
			    del self.runAway[key]
	    if max>threshold:
		return max
	    else:
		return max

    def daemonAlive(self,name):
	import glob
        dirs = glob.glob('/proc/[0-9]*')
        if not len(dirs):
            return -1

        for dir in dirs:
	    file = dir + '/stat'
	    try:    f = open(file, 'r')
	    except: continue
	    if not f:       continue
	    lines = f.readlines()
	    f.close()
	    for line in lines:
		if not line:    continue
		list=string.splitfields(line)
		if len(list)>2:
		    if string.find(list[1],name)>=0:
			return 1
	return 0

def usage():
    print "Usage: ngop linux_agent [-h] [-d debug_level] config.xml"
    print "Where: -h - to see this message"
    print "       -d debug_level - integer from 1 to 3"
    print "        config.xml - name of configuration file"
    print "Defaults: no debug messages" 


if __name__ == '__main__':
       import sys
       import getopt
       try:
	   optlist,args = getopt.getopt(sys.argv[1:],"d:h",['help'])
       except getopt.error,error_msg:
	   print "Error:",error_msg
	   usage()
	   sys.exit(1)
       for opt in optlist:
	if opt[0]=='-d':
	    try:
		ngop_global.G_Debug=int(opt[1])
	    except:
		usage()
		sys.exit(1)
      
       if len(args)!=1:
	   print "You have to provides the name of xml file"
	   sys.exit(1)
       try:
	   cfg=args[0]
	   open(cfg,'r')
       except:
	   print "Failed to open cfg xml file: %s,%s" % (sys.exc_type,sys.exc_value)
	   sys.exit(1)

       try:
	   cl=MA_API.MAClient(cfg,OSFunc())
       except ngop_global.MAError,reason:
	   print reason
	   sys.exit(1)
       cl.register()
       if ngop_global.G_Debug:
	   cl.display()
       cl.run()

