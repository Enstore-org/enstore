import MA_API
import time
import sys
import os
import string
import random
DOWN=0
UP=1
UNKNOWN=-1
#***************************************************************
# YOU HAVE TO REPLACE THIS!!!!!
def dummy(name):
	val=random.random()
	if val>=0.5:
		state=DOWN
		dsc='oops,%s_more_then_0.5' %(name,) #!no blanks here
	else:
		state=UP
		dsc='working!'
        return  val,state,dsc
#**************************************************************
if __name__=="__main__":
    checkTime=60 			# monitoring interval
    maName="BigAswitchAgent" 	        #name of the monitoring agent
    sysName="big"       		#system name
    clusterName="D0en" 			#cluster name
    nodeName="d0ensrv2" 		#node name
    meNames=["tcp","icmp","snmp","ip","udp","if"] 
					#names of monitored element
    meType="Network" 			#type of monitored element
    heartbeat="300" 			# heartbeat rate in sec
    serverHost='fnisd1'
    #!!!!!!!!!!!!!!!!!!!!Replace with the name of your host!!!!!!!!!!!!!!!!!!!!!!!
    serverPort="19997"
    cl=MA_API.MAClient() 		# creates MAClient object
    cl.setMAAttrib(maName,heartbeat,serverHost,serverPort) #sets attributes
    cl.addSystem(sysName,clusterName) 	# configures the system
    oldStateList=[]
    for elmName in meNames:
        cl.addME(sysName,clusterName,elmName,meType,nodeName)
        #configures system monitored elements list
        oldStateList.append(1)
    cl.register() #registers monitoring agent with NGOP Central Server
      
    while 1:
	i=-1
        for elmName in meNames:
            i = i+1
            print "\ni is %s"%(i,)
            print oldStateList
            val,state,desc=dummy(elmName)
	    print elmName,state,desc, val
            if oldStateList[i]==state: continue #nothing has changed
            eventDict={'EventType':meType, 'EventName':elmName,'EventValue':val, \
              'State':state,'SevLevel':5}
            eventDict['Description']=desc
            status,reason=cl.sendEvent(sysName,clusterName,elmName,nodeName,eventDict)
	    oldStateList[i]=state
            print "status is %s"%(status,)
            if not status: print "Error:",reason
#	    i=i+1
        time.sleep(checkTime)

