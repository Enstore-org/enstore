import os
import string
import calendar
from Worker import Worker
import MA_API   
import ngop_global

flag = 0
CRON_DIR = "/var/spool/cron"

class CJFunc(Worker):
    def __init__(self):
	Worker.__init__(self)
	self.runAway={}

    def checkprint(self, flg, str):
        if flg == 1:
           print str

    def calcuTime(self, time, num):
        if string.find(time, '/') > 0:
              dis0 = string.atoi(string.splitfields(time, '/')[-1])
              if string.find(time, '-') < 0:
                 return dis0 
              else:
                 ts = string.splitfields(time, '/')[0]
                 t1 = string.splitfields(ts,'-')[0]
                 t2 = string.splitfields(ts,'-')[-1]
                 dis = num - string.atoi(t2) + string.atoi(t1)
                 return max(dis, dis0)
        elif string.find(time, ',') > 0:
              ts = string.splitfields(time, ',')
              if string.find(time, '-') < 0:
                 dis0 = 0
                 t0 = 0
                 for t in ts:
                     t = string.atoi(t)
                     dis = t - t0   
                     if dis > dis0:
                        dis0 = dis
                     t0 = t
                 dis1 = num - t + string.atoi(ts[0])
                 return max(dis1, dis0)
              else:
                 dis0 = 0
                 t0 = 0
                 t1 = 0 
                 t2 = 0
                 for t in ts:
                     lst = string.splitfields(t, '-')
                     t1 = string.atoi(lst[0])
                     t2  = string.atoi(lst[-1])
                     ds = t1 - t0
                     if ds > 1:
                         dis = ds
                     else:
                         dis = 1
                     t0 = t2
                     if dis > dis0: 
                         dis0 = dis

                 t = string.atoi(string.splitfields(ts[0],'-')[0])
                 dis1 = num - t2 + t
                 return max(dis1, dis0)
        elif string.find(time, '-') > 0:
              t1 = string.splitfields(time, '-')[0]
              t2 = string.splitfields(time, '-')[-1]
              dis = num - string.atoi(t2) + string.atoi(t1)
              return max(dis, 1)
        else:
              return num

    def setTime(self, time, num):
        if string.find(time, '/') > 0:
              b0 = string.atoi(string.splitfields(time,'/')[-1])
              if string.find(time,'-') < 0:
                 return b0
              else:
                 ts = string.splitfields(time, '/')[0]
                 t1 = string.splitfields(ts,'-')[0]
                 t2 = string.splitfields(ts,'-')[-1]
                 b = num - string.atoi(t2) + string.atoi(t1)
                 return max(b0, b)
        elif string.find(time, ',') > 0:
              t1 = string.splitfields(time,',')[0]
              t2 = string.splitfields(time,',')[-1]
              if string.find(time,'-') > 0:
                 t1 = string.splitfields(t1,'-')[0]
                 t2 = string.splitfields(t2,'-')[-1]
              b = num - string.atoi(t2) + string.atoi(t1)
              return b
        elif string.find(time, '-') > 0:
              t1 = string.splitfields(time,'-')[0]
              t2 = string.splitfields(time,'-')[-1]
              b = num - string.atoi(t2) + string.atoi(t1)
              return max(b, 1)
        else:    
              return num

    def calcuFreq(self, line):
        m, h, dm, my, dw = string.splitfields(line)[:5]
        if dw == "*" and my == "*" and dm == "*" and h == "*":
           return self.calcuTime(m, 60)
        elif dw == "*" and my == "*" and dm == "*":
           mins = self.setTime(m, 60)
           return 60*(self.calcuTime(h, 24)-1) + mins   
        elif dw == "*" and my == "*":
           hrs = self.setTime(h, 24)
           mins = self.setTime(m, 60)
           return 60*24*(self.calcuTime(dm, 31)-1) + 60*(hrs-1) + mins
        elif dw == '*' and dm == '*':
           hrs = setTime(h, 24)
           mins = setTime(m, 60)
           return 60*24*31*(calcuTime(my,12)-1) + 60*24*(-1) + 60*(hrs-1) + mins
        elif my == "*" and dm == "*":
           hrs = self.setTime(h, 24)
           mins = self.setTime(m, 60)
           return 60*24*(self.calcuTime(dw, 7)-1) + 60*(hrs-1) + mins
        elif dw == "*":
           dayMs = self.setTime(dm, 31)
           hrs = self.setTime(h, 24)
           mins = self.setTime(m, 60)
           return 60*24*31*(self.calcuTime(my, 12)-1) + 60*24*(dayMs-1)\
                  + 60*(hrs-1) + mins
        elif my == "*":       			# this case needs to be improved
           hrs = setTime(h, 24)
           mins = setTime(m, 60)
           return max((60*24*(calcuTime(dm,31)-1) + 60*(hrs-1) + mins),\
                      (60*24*(calcuTime(dw,7) -1) + 60*(hrs-1) + mins))
        elif dm == '*':       			# this case needs to be improved
           hrs = setTime(h, 24)
           mins = setTime(m, 60)
           dayWks = setTime(dw, 7)
           return 60*24*31*(calcuTime(my,12)-1) + 60*24*(dayWks - 1)\
                                               + 60*(hrs-1) + mins
        else:                              	# this case needs to improved
           hrs = setTime(h, 24)
           mins = setTime(m, 60)
           dayWks = setTime(dw, 7)
           dayMs = setTime(dm, 31)
           days = min(dayMs, dayWks)
           return 60*24*31*(calcuTime(my,12)-1)+60*24*(days-1)+60*(hrs-1)+mins

    def convertMonth(self, mon):
        if mon in calendar.month_abbr:
            return calendar.month_abbr.index(mon)
        else:
            return 0     
  
    def checkcron(self,name):
        try:                       # test file in my dir
            dirNames = os.popen("ls %s"%(CRON_DIR,),'r').readlines()
            self.checkprint(flag, dirNames)
	except:
	    return -1

        if not len(dirNames):
            return -1 
        
        stateFlag = 0
        disc = ""
        for it in dirNames:
          name = 'enstore'            #now only set up this one name for the test        
          if string.find(it, name) >= 0:
            filePat1 = "/home/%s/CRON/*ACTIVE"%(name,)
            try:
              	actFiles = os.popen("ls %s"%(filePat1,), 'r').readlines()
            except:
                return -1

            filePat2 = "%s/%s"%(CRON_DIR, name) #test file in my dir
            self.checkprint(flag, filePat2)
            try:
                lines = os.popen("less %s"%(filePat2,), 'r').readlines()
            except:
                return -1
    
            for fl in actFiles:  # need more checks in this loop 
                self.checkprint(flag, fl)
                for line in lines:
                   if string.find(string.splitfields(line)[0],'#') == 0:
                      continue
                   fName = string.splitfields(line)[7]
                   if string.find(fl, fName) >=0 :
                     freq = self.calcuFreq(line)                      
                     self.checkprint(flag, fName)
                     self.checkprint(flag, freq)
                     tm = os.popen('date','r').readlines()
                     tm = tm[0]
                     fi = os.popen("ls -l %s"%(fl,),'r').readlines()
                     fi = fi[0]
                     fiL = string.splitfields(fi)[5:8]
                     tmL = string.splitfields(tm)[1:4]
                     self.checkprint(flag, string.splitfields(tm)[1:4])
                     self.checkprint(flag, string.splitfields(fi)[5:8])
                     monC = self.convertMonth(tmL[0])
                     monS = self.convertMonth(fiL[0])
                     dayC = tmL[1]
                     dayS = fiL[1]
                     hourC,minuC = string.splitfields(tmL[2],':')[0:2]
                     hourS,minuS = string.splitfields(fiL[2],':')[0:2]
                     diffMon = monC - monS
                     diffD = string.atoi(dayC) - string.atoi(dayS)
                     diffH = string.atoi(hourC) - string.atoi(hourS) 
                     diffM = string.atoi(minuC) - string.atoi(minuS)
                     if diffMon < 0:
                        diffMon = 12 + diffMon
                     if diffD < 0:
                        diffD = 31 + diffD
                        diffMon = diffMon -1
                     if diffH < 0:
                        diffH = 24 + diffH
                        diffD = diffD -1
                     if diffM < 0:
                        diffM = 60 + diffM
                        diffH = diffH -1
                     self.checkprint(flag,"%s %s %s %s"%(diffMon,diffD,diffH,diffM))
                     interval= 31*24*60*diffMon+24*60*diffD+60*diffH+diffM
                     self.checkprint(flag, interval)
                     if interval > 3*freq:
                        disc = disc + "The cron job %s is running too long\n"%(fName)
                        stateFlag = -2

        self.checkprint(flag, "cron jobs state is %s"%(stateFlag,))
        self.checkprint(flag, disc)
        return stateFlag



def usage():
    print "Usage: ngop linux_agent [-h] [-d debug_level] config.xml"
    print "Where: -h - to see this message"
    print "       -d debug_level - integer from 1 to 3"
    print "        config.xml - name of configuration file"
    print "Defaults: no debug messages" 


if __name__ == '__main__':
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

