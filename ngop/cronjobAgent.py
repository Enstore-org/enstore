import os
import string
import time
import stat
##from Worker import Worker
##import MA_API   
##import ngop_global

flag = 0
CRON_DIR = "/var/spool/cron"

def checkprint(str):
    if flag == 1:
       print str

def calcuTime(time, num):
    if string.find(time, '/') > 0:
          dis0 = int(string.splitfields(time, '/')[-1])
          if string.find(time, '-') < 0:
             return dis0 
          else:
             ts = string.splitfields(time, '/')[0]
             t1 = string.splitfields(ts,'-')[0]
             t2 = string.splitfields(ts,'-')[-1]
             dis = num - int(t2) + int(t1)
             return max(dis, dis0)
    elif string.find(time, ',') > 0:
          ts = string.splitfields(time, ',')
          if string.find(time, '-') < 0:
             dis0 = 0
             t0 = 0
             for t in ts:
                 t = int(t)
                 dis = t - t0   
                 if dis > dis0:
                    dis0 = dis
                 t0 = t
             dis1 = num - t + int(ts[0])
             return max(dis1, dis0)
          else:
             dis0 = 0
             t0 = 0
             t1 = 0 
             t2 = 0
             for t in ts:
                 lst = string.splitfields(t, '-')
                 t1 = int(lst[0])
                 t2  = int(lst[-1])
                 ds = t1 - t0
                 if ds > 1:
                     dis = ds
                 else:
                     dis = 1
                 t0 = t2
                 if dis > dis0: 
                     dis0 = dis

             t = int(string.splitfields(ts[0],'-')[0])
             dis1 = num - t2 + t
             return max(dis1, dis0)
    elif string.find(time, '-') > 0:
          t1 = string.splitfields(time, '-')[0]
          t2 = string.splitfields(time, '-')[-1]
          dis = num - int(t2) + int(t1)
          return max(dis, 1)
    else:
          return num

def setTime(time, num):
    if string.find(time, '/') > 0:
          b0 = int(string.splitfields(time,'/')[-1])
          if string.find(time,'-') < 0:
             return b0
          else:
             ts = string.splitfields(time, '/')[0]
             t1 = string.splitfields(ts,'-')[0]
             t2 = string.splitfields(ts,'-')[-1]
             b = num - int(t2) + int(t1)
             return max(b0, b)
    elif string.find(time, ',') > 0:
          t1 = string.splitfields(time,',')[0]
          t2 = string.splitfields(time,',')[-1]
          if string.find(time,'-') > 0:
             t1 = string.splitfields(t1,'-')[0]
             t2 = string.splitfields(t2,'-')[-1]
          b = num - int(t2) + int(t1)
          return b
    elif string.find(time, '-') > 0:
          t1 = string.splitfields(time,'-')[0]
          t2 = string.splitfields(time,'-')[-1]
          b = num - int(t2) + int(t1)
          return max(b, 1)
    else:    
          return num

def calcuFreq(line):
    m, h, dm, my, dw = string.splitfields(line)[:5]
    if dw == "*" and my == "*" and dm == "*" and h == "*":
       return calcuTime(m, 60)
    elif dw == "*" and my == "*" and dm == "*":
       mins = setTime(m, 60)
       return 60*(calcuTime(h, 24)-1) + mins   
    elif dw == "*" and my == "*":
       hrs = setTime(h, 24)
       mins = setTime(m, 60)
       return 60*24*(calcuTime(dm, 31)-1) + 60*(hrs-1) + mins
    elif dw == '*' and dm == '*':
       hrs = setTime(h, 24)
       mins = setTime(m, 60)
       return 60*24*31*(calcuTime(my,12)-1) + 60*24*(-1) + 60*(hrs-1) + mins
    elif my == "*" and dm == "*":
       hrs = setTime(h, 24)
       mins = setTime(m, 60)
       return 60*24*(calcuTime(dw, 7)-1) + 60*(hrs-1) + mins
    elif dw == "*":
       dayMs = setTime(dm, 31)
       hrs = setTime(h, 24)
       mins = setTime(m, 60)
       return 60*24*31*(calcuTime(my, 12)-1) + 60*24*(dayMs-1)\
              + 60*(hrs-1) + mins
    elif my == "*":       	 # this case needs to be improved
       hrs = setTime(h, 24)
       mins = setTime(m, 60)
       return max((60*24*(calcuTime(dm,31)-1) + 60*(hrs-1) + mins),\
                  (60*24*(calcuTime(dw,7) -1) + 60*(hrs-1) + mins))
    elif dm == '*':       	 # this case needs to be improved
       hrs = setTime(h, 24)
       mins = setTime(m, 60)
       dayWks = setTime(dw, 7)
       return 60*24*31*(calcuTime(my,12)-1) + 60*24*(dayWks - 1)\
                                           + 60*(hrs-1) + mins
    else:                        # this case needs to improved
       hrs = setTime(h, 24)
       mins = setTime(m, 60)
       dayWks = setTime(dw, 7)
       dayMs = setTime(dm, 31)
       days = min(dayMs, dayWks)
       return 60*24*31*(calcuTime(my,12)-1)+60*24*(days-1)+60*(hrs-1)+mins


##class CJFunc(Worker):
class CJFunc:
    def __init__(self):
	##Worker.__init__(self)
	self.runAway={}

    # this method returns the following -
    #           0 : cronjob hasn't been running too long
    #          -1 : cronjob state is unknown
    #          -2 : cronjob has been running too long
    def checkcron(self, ngop_name):
        try:                       # test file in my dir
            dirNames = os.popen("ls %s"%(CRON_DIR,),'r').readlines()
            checkprint(dirNames)
	except:
	    return -1

        if not len(dirNames):
            return -1 
        
        stateFlag = 0
        disc = ""
        names = ['enstore', 'root']
        for it in dirNames:
          if it in names:
            name = names[names.index(it)]
            filePat1 = "/home/%s/CRON/*ACTIVE"%(name,)
            try:
              	actFiles = os.popen("ls %s"%(filePat1,), 'r').readlines()
            except:
                return -1

            filePat2 = "%s/%s"%(CRON_DIR, name) #test file in my dir
            checkprint(filePat2)
            try:
                lines = os.popen("less %s"%(filePat2,), 'r').readlines()
            except:
                return -1
    
            for fl in actFiles:  # need more checks in this loop 
                checkprint(fl)
                for line in lines:
                   if string.find(string.splitfields(line)[0],'#') == 0:
                      continue
                   fName = string.splitfields(line)[7]
                   if string.find(fl, fName) >=0 :
                     freq = calcuFreq(line)                      
                     checkprint("file, frequency are %s, %s"%(fName,
                                                                    freq))
                     file_mtime = os.stat(fl)[stat.ST_MTIME]
                     now = time.time()
                     interval = now - file_mtime
                     checkprint("interval is %s"%(interval,))
                     if interval > 3*freq:
                        disc = disc + "The cron job %s is running too long\n"%(fName)
                        stateFlag = -2

        checkprint("cron jobs state is %s"%(stateFlag,))
        checkprint(disc)
        return stateFlag


def usage():
    print "Usage: ngop linux_agent [-h] [-d debug_level] config.xml"
    print "Where: -h - to see this message"
    print "       -d debug_level - integer from 1 to 3"
    print "        config.xml - name of configuration file"
    print "Defaults: no debug messages" 


if __name__ == '__main__':
       flag = 1
       cl=CJFunc()
       cl.checkcron("test")

       """
       import sys
       import getopt
       cfg=None
       try:
	   optlist,args = getopt.getopt(sys.argv[1:],"c:d:h",['help'])
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
	elif opt[0]=='-c':
	    cfg=opt[1]

      
       if not cfg :
	   print "You have to provide the name of the xml file"
	   sys.exit(1)

       if len(args) > 1:
          if args[1] == "--print":
             flag = 1

       try:
	   open(cfg,'r')
       except:
	   print "Failed to open cfg xml file: %s,%s" %\
                 (sys.exc_type,sys.exc_value)
	   sys.exit(1)

       try:
	   cl=MA_API.MAClient(cfg,CJFunc())
       except ngop_global.MAError,reason:
	   print reason
	   sys.exit(1)
       cl.register()
       if ngop_global.G_Debug:
	   cl.display()
       cl.run()
       """
