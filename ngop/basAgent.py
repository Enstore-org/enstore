#########################################################################
#									#
# This configuration agent meets the requirements:			#
# 1. updating MIB by snmpwalk(). 					#
# 2. updating the node dictionaries.		                        #
# 3. updating the html files which can be posted on the enstore web page#
# 4. checking the MIB's tcp, udp, ip, icmp, snmp and ifeXtension group  #
#    objects and sending the events and corresponding statistics to 	#
#    NGOP Central Server if there is any object value reaches the	#
#    corresponding threshold.						#
#									#
#########################################################################  
#
import MA_API
import time
import sys
import os
import string
#import HTMLgen
import regsub

import re
import node_init


DOWN      = 0
UP        = 1
UNDEFINED = -1


BLACK = '#000000'
WHITE = '#FFFFFF'
TEAL = '#008080'
BLUE  = '#0000FF'
GREEN = '#00FF00'
MAGENTA = '#FF00FF'
CYAN  = '#00FFFF'
YELLOW = '#FFFF00'
SILVER = '#C0C0C0'
NAVY = '#000080'
BGCOLOR = "#DFFOFF"
TEXTCOL = "#000066"

FLOW_L = ['duplex', 'adminrxflow', 'operrxflow',  'admintxflow', 'opertxflow']


## the lists of the groups of MIB variables to be monitored
 
SYS_L  = ['Descr', 'ObjectID', 'UpTime', 'Contact', 'Name', 'Location',\
          'Services']

TCP_L  = ['RtoAlgorithm', 'RtoMin', 'RtoMax', 'MaxConn', 'ActiveOpens',\
         'PassiveOpens', 'AttemptFails', 'EstabResets', 'CurrEstab',\
         'InSegs', 'OutSegs', 'RetransSegs', 'InErrs', 'OutRsts']

UDP_L  = ['InDatagrams', 'NoPorts', 'InErrors', 'OutDatagrams']

IP_L   = ['Forwarding', 'DefaultTTL', 'InReceives', 'InHdrErrors',\
          'InAddrErrors', 'ForwDatagrams', 'InUnknownProtos',\
          'InDiscards', 'InDelivers', 'OutRequests', 'OutDiscards',\
          'OutNoRoutes', 'ReasmTimeout', 'ReasmReqds', 'ReasmOKs', \
          'ReasmFails', 'FragOKs', 'FragFails', 'FragCreates', \
          'RoutingDiscards']

ICMP_L = ['InMsgs', 'InErrors', 'InDestUnreachs', 'InTimeExcds',\
          'InParmProbs', 'InSrcQuenchs', 'InRedirects', 'InEchos',\
          'InEchoReps', 'InTimestamps', 'InTimestampReps','InAddrMasks',\
          'InAddrMaskReps', 'OutMsgs', 'OutErrors', 'OutDestUnreachs',\
          'OutTimeExcds', 'OutParmProbs', 'OutSrcQuenchs',\
          'OutRedirects', 'OutEchos', 'OutEchoReps',  'OutTimestamps',\
          'OutTimestampReps', 'OutAddrMasks', 'OutAddrMaskReps']

SNMP_L = ['InPkts', 'OutPkts', 'InBadVersions', 'InBadCommunityNames',\
          'InBadCommunityUses', 'InASNParseErrs', 'InTooBigs',\
          'InNoSuchNames', 'InBadValues', 'InReadOnlys', 'InGenErrs',\
          'InTotalReqVars', 'InTotalSetVars', 'InGetRequests',\
          'InGetNexts', 'InSetRequests', 'InGetResponses', 'InTraps',\
          'OutTooBigs', 'OutNoSuchNames', 'OutBadValues', 'OutGenErrs',\
          'OutGetRequests', 'OutGetNexts', 'OutSetRequests', \
          'OutGetResponses', 'OutTraps', 'EnableAuthenTraps']

## the list of the variables for each node to monitor for ifeXtension group
          
LIST1  = ['Index', 'Descr', 'Type', 'Mtu', 'Speed','PhysAddress', \
          'AdminStatus', 'OperStatus', 'LastChange', 'InOctets', \
          'InUcastPkts', 'InNUcastPkts', 'InDiscards', 'InErrors', \
          'InUnknownProtos', 'OutOctets', 'OutUcastPkts', 'OutNUcastPkts',\
          'OutDiscards', 'OutErrors', 'OutQLen', 'Specific']

LIST2  = ['Name', 'InMulticastPkts', 'InBroadcastPkts',\
          'OutMulticastPkts', 'OutBroadcastPkts', 'LinkUpDownTrapEnable',\
          'HighSpeed', 'PromiscuousMode', 'ConnectorPresent']
   
LIST3  = ['DropEvents', 'Octets', 'Pkts', 'BroadcastPkts', \
          'MulticastPkts', 'CRCAlignErrors', 'UndersizePkts', \
          'OversizePkts', 'Fragments', 'Jabbers', 'Collisions', \
          'Pkts64Octets', 'Pkts65to127Octets', 'Pkts128to255Octets',\
          'Pkts256to511Octets', 'Pkts512to1023Octets',\
          'Pkts1024to1518Octets', 'Owner', 'Status']

LIST4 = ['Type', 'Mtu','Speed', 'ConnectorPresent', 'DropEvents', \
         'CRCAlignErrors', 'UndersizePkts', 'OversizePkts', 'Fragments', \
         'Jabbers']

## the list of the column headers for node table (html file)

LIST = ['Index', 'Descr', 'Type', 'Mtu', 'Speed','Phys\nAddress', \
        'Admin\nStatus', 'Oper\nStatus', 'Last\nChange', 'In\nOctets', \
        'In\nUcast\nPkts', 'In\nNUcast\nPkts', 'In\nDiscards', 'In\nErrors', \
        'In\nUnknown\nProtos', 'Out\nOctets', 'Out\nUcast\nPkts',\
        'Out\nNUcast\nPkts', 'Out\nDiscards', 'Out\nErrors', 'Out\nQLen',\
        'Specific', 'Name', 'In\nMulticast\nPkts', 'In\nBroadcast\nPkts',\
        'Out\nMulticast\nPkts', 'Out\nBroadcast\nPkts',\
        'Link\nUpDown\nTrapEnable', 'High\nSpeed','Promiscuous\nMode',\
        'Connector\nPresent', 'Drop\nEvents', 'Octets', 'Pkts',\
        'Broadcast\nPkts', 'Multicast\nPkts', 'CRCAlign\nErrors', \
        'Under\nsize\nPkts', 'Over\nsize\nPkts', 'Fragments', 'Jabbers',\
        'Collisions', 'Pkts64O\nctets', 'Pkts65\nto127O\nctets',\
        'Pkts128\nto255O\nctets', 'Pkts256\nto511O\nctets',\
        'Pkts512\nto1023O\nctets', 'Pkts1024\nto1518O\nctets',\
        'Owner', 'Status']

SUBLIST = ['Type', 'Mtu', 'Speed', 'Admin\nStatus', 'In\nDiscards', 'In\nErrors',\
           'In\nUnknown\nProtos', 'Out\nDiscards', 'Out\nErrors', \
           'Connector\nPresent', 'Drop\nEvents', 'CRCAlign\nErrors',
           'Under\nsize\nPkts', 'Over\nsize\nPkts', 'Collisions']

L = [20, 10, 20, 10, 10, 6, 10, 6, 1, 2, 1, 2]

PATH = 'testfile/www'

node_d = node_init.node_d
nodes = node_d.keys()      
nodeValue = node_d.values()

miblist = {}   
mibflow = {}
system_d = {} 
tcp_d = {}
udp_d = {}
ip_d = {}
icmp_d = {}
snmp_d = {}

system0 = {}
tcp0 = {}
udp0 = {}
ip0 = {}
icmp0 = {}
snmp0 = {}
node0 = {}

f = 0			# print requirement flag


#########################################################################
## This function prints the output if printing is required as the argu  # 
#########################################################################

def checkprint(flg, str):
    if flg == 1:
       print str 



#########################################################################
## This function searches a string from the input file and returns an   #
## abstract string to the caller                                        #
#########################################################################
def getit(searchStr, fileName):
    #check if input file exists
    if os.access(fileName, 0) == 0:
         return ""
    #input file is there
    else:
        file = open(fileName, 'r')
          
        flag = 0
        for line in file.readlines():
                #check match string in file
                if string.find(line, searchStr) >= 0:
                        words = string.split(line,"=")
                        flag = 1
                        break
          
        #if the string match
        if flag == 1:
                #send field 2 as output
                str = words[1]
        #else the string not match
        else:
                str = ""

        pat1 = ' '
        #pat1 = ' "?'
        #pat2 = '"'
        pat2 = '\n'
        #check pattern at begin of str
        if (string.find(str, pat1, 0) == 0):
                #replace the pattern
                str= string.replace(str,pat1, "")
        #check pattern at end of str
        if (string.rfind(str, pat2) == len(str)-1):
                #replace the pattern
                str = string.replace(str, pat2, "")  
                        
        return str
  
########################################################################
## This function searches a pattern from the input file and returns a  #
## string that indicates the duplex state of the node to the caller    #
######################################################################## 
def getdup(pathName, dupfile):
    card, port = string.split(pathName,'/')[0:2]
    pat = ".%s.%s ="% (card, port)
                
    #check if input file exists   
    if os.access(dupfile, 0) == 0:
         return ""
    #input file is there
    else:
        file = open(dupfile, "r")
        flag = 0   
        for line in file.readlines():
                #check match string in file
                if string.find(line, pat) >= 0:
                        words = string.split(line)
                        flag = 1
                        break
        #if string match
        if flag == 1:
                dup = words[-1]
                testVal = string.atoi(dup)
                if  testVal == 1:
                        return "Half"
                elif testVal == 2:
                        return "Full"
                elif testVal == 3:
                        return "DISAGREE"
                elif testVal == 4:
                        return "Auto"
                else:
                        return dup
        #else string not match
        else:
                return ""


########################################################################
## This function searches a pattern from the input files and returns   #
## a string that indicates the states of rx flow and tx flow           #
########################################################################
def getflow(pathName, fileStr):  
    card, port = string.split(pathName, '/')[0:2]
    pat = ".%s.%s ="%(card, port) 
          
    fileName1 = "%s/admin%sflow"%(PATH, fileStr,)
    fileName2 = "%s/oper%sflow"%(PATH, fileStr,)
        
    #check if input file exists
    if os.access(fileName1, 0) == 0:
         return ""
    #input file is there
    else:
        file = open(fileName1, "r")  
        flag = 0  
        for line in file.readlines():
                #check match string in file
                if string.find(line, pat) >= 0:
                        words = string.split(line)
                        flag = 1
                        break
        if flag == 1:
        #if the string is in the file
                testVal1 = string.atoi(words[-1])
                if  testVal1 == 1:
                        aa = "on"
                elif  testVal1 == 2:
                        aa = "off"
                elif testVal1 == 3:
                        aa = "desired"
                else:
                        aa = words[-1]
        #else if the string is not in the file
        else:
                aa = ""
    
    #check if input file exists
    if os.access(fileName2, 0) == 0:
         return ""
    #input file is there  
    else:
        file = open(fileName2, "r")   
        flag = 0
        for line in file.readlines():
                #check match string in file
                if string.find(line, pat) >= 0:
                        words = string.split(line)
                        flag = 1
                        break
        #if the string is in the file
        if flag == 1:
                testVal2 = string.atoi(words[-1])
                if   testVal2 == 1:
                        oo = "on"
                elif  testVal2 == 2:
                        oo = "off"
                elif testVal2 == 3:
                        oo = "disagree"
                else:  
                        oo = words[-1]
        #else if the string is not in the file
        else:
                oo = ""
    return "%s+%s"%(oo,aa)
  
  
##########################################################################
## This function calls a function getit() to get the abstract string from#
## the input file, filles the dictionary with the string in a loop, and  #
## finally, returns the dictionary to the caller.                        #
##########################################################################
def get_mib_info(list, path, filenum):
    dat_d = {}  
    for i in list:
        str = "%s%s.0"%(path, i)
        dat_d[i] = getit(str, miblist[filenum])
    return dat_d

#########################################################################
## This function prints the summarized information of the monitored MIB #
## group or the monitored node, decises the state for the group or the  #
## node according to the statistics of the values and returns the       #
## corresponding state, value, description, event name and sever level. #
#########################################################################
def sumThem(r, o, y, u, val, obj, dscc):
        checkprint(f,"\nThere are totally %s values reaching red threshold"%(r,))
        checkprint(f,"There are total %s values reaching orange threshold"%(o,))
        checkprint(f,"There are total %s values reaching yellow threshold"%(y,))
        checkprint(f,"There are total %s values undefined"%(u,))

        if r != 0:
                state = DOWN
                sev = 0
        elif o != 0:
                state = UP
                sev = 4
        elif y != 0:
                state = UP
                sev = 1
        else:
                state = UNDEFINED
                sev = 2
        val = val
        evt = obj
        dsc="There_are_totally_%s_red_thresholds__%s_orange_thresholds__\
%s_yellow_thresholds__%s_undefined__They_are%s"%(r, o, y, u, dscc)
        
        return  state, val, dsc, evt, sev

########################################################################
## This function sets the flag for the monitored object, formats the   #
## description, counts the state for the monitored objects and returns #
## updated flag, descrition, state, value, event name, sever level and #
## the statistics of the monitored object state to the caller.         #
########################################################################
def setThem(dscc, state1, val1, dsc1, evt1, sev1, r, o, y, u, name):
    flag = 1
    dscc = "%s_%s_%s"%(dscc, name, dsc1)
    state, val, dsc, evt, sev = state1, val1, dsc1, evt1, sev1
    checkprint(f,"%s %s %s %s %s %s"%(name, state, val, dsc, evt, sev))
    if state == DOWN and sev == 0:
        r = r + 1
    elif sev == 4:
        o = o + 1
    elif sev == 1:
        y = y + 1
    else:
        u = u + 1
    return flag, dscc, state, val, dsc, evt, sev, r, o, y, u

#########################################################################
## This function subtracts the value from the dictionary, calculates the#
## dispression, compares the dispression with the given threshold and   #
## returns the conrresponding state, value, event name to the calller.  #
## There is one level threshold to be compared.                         #   
#########################################################################
def checkVal(d, str, targetVal, name, d0):
    evt = "%s%s"%(name, str)
    if not len(d):
        return UNDEFINED, -1, "No_information_in_node", "Undefined event", 2
    else:
        val = string.atoi(d[str]) - string.atoi(d0[str])
        if val < 0:
                val = - val
        
        if val > targetVal:
                return DOWN, val, "%s_great_%s"%(str,targetVal), evt, 0
        else:
                return UP, val, "OK", evt, 0


##########################################################################
## This function subtracts the value from a given dictionary, calculates #
## the dispression, compares the dispression with the given threshold and#
## returns the conrresponding state, value, event name to the calller.   #
## There are two levels threshold to be compared.                        #
##########################################################################
def check2Val(d, str, redVal, orgVal, name, d0):
    evt = "%s%s"%(name, str)
    if not len(d):
        return UNDEFINED, -1, "No_information_in_node", "Undefined event", 2
    else:
        val = string.atoi(d[str]) - string.atoi(d0[str])
        if val < 0:
                val = - val
        
        if val > redVal:
                return DOWN, val, "%s_great_%s"%(str,redVal), evt, 0
        elif val > orgVal and val <= redVal:
                return UP, val, "%s_great_%s"%(str,orgVal),evt,4
        else:
                return UP, val, "OK", evt, 0

##########################################################################
## This function subtracts the value from a given dictionary, calculates #
## the dispression, compares the dispression with the given threshold and#
## returns the conrresponding state, value, event name to the calller.   #
## There are three levels threshold to be compared.                      #
##########################################################################
def check3Val(d, str, redVal, orgVal, ylwVal, name, d0):
    evt = "%s%s"%(name, str)
    if not len(d):
        return UNDEFINED, -1, "No_information_in_node", "Undefined event", 2
    else:
        val = string.atoi(d[str]) - string.atoi(d0[str])
        if val < 0:
                val = - val
        
        if val > redVal:   
                return DOWN, val, "%s_great_%s"%(str,redVal), evt, 0
        elif val > orgVal:
                return UP, val, "%s_great_%s"%(str,orgVal), evt, 4  
        elif val > ylwVal:
                return UP, val, "%s_great_%s"%(str,ylwVal), evt, 1
        else:
                return UP, val, "OK", evt, 0
                
##########################################################################
## This function subtracts a string that indicates the state of the      #
## monitored object from a given dictionary, compares the string with the#
## given threshold string and returns the conrresponding state, value,   #
## event name to the calller.                                            #
## There are two levels threshold to be compared.                        #
##########################################################################
def check2Stat(d, str, redVal, orgVal, name):
    evt = "%s%s"%(name, str)
    if not len(d):
        return UNDEFINED, -1, "No_information_in_node", "Undefined event", 2
    else:
        val = d[str]
        if val == redVal:  
                return DOWN, d[str], "%s_is_%s"%(str, redVal), evt, 0
        elif val == orgVal:
                return UP, d[str], "%s_is_%s"%(str, orgVal), evt, 4 
        else:
                return UP, d[str], "OK", evt, 0

##########################################################################
## This function subtracts a string from a given dictionary, compares    #
## the string with the given threshold string and returns the            #
## conrresponding state, value, event name to the calller.               #
## There are one level threshold to be compared.                         #
##########################################################################
def checkStr(d, str, goodStr, name):
    evt = "%s%s"%(name, str)
    if not len(d):
        return UNDEFINED, -1, "No_information_in_node", "Undefined event", 2
    else:
        val = d[str]
        if val != goodStr:  
                return DOWN, d[str], "%s_not_good"%(str,), evt, 0
        else:
                return UP, d[str], "OK", evt, 0
        
        
##########################################################################
## This function subtracts a string from a given dictionary, compares    #
## the string with the given threshold strings and returns the           #
## conrresponding state, value, event name to the calller.               #
## There are two level threshold to be compared.                         #
##########################################################################
def check2Str(d, str, goodStr, warnStr, name):
    evt = "%s%s"%(name, str)
    if not len(d):
        return UNDEFINED, -1, "No_information_in_node", "Undefined event", 2
    else:
        val = d[str]
        if val == warnStr:
                return UP, d[str], "%s_not_good"%(str,), evt, 4
        elif val == goodStr:
                return UP, d[str], "OK", evt, 0
        else:
                return UNDEFINED, val, "Undefined_string", str, 2


#####################################################################
## This  function  checkes  the  monitored  object  variables.      #
## If the value reaches the threshold, returns the description      #
## that  contains  statistics  of the  object  information          #
#####################################################################
def checkNode(elmName):
    if elmName == "tcp":
   	d = tcp_d
        flag = 0
        dscc = ""
        r = 0
        o = 0
        y = 0
        u = 0
        for j in [TCP_L[6], TCP_L[7], TCP_L[11], TCP_L[12], TCP_L[13]]:
       	    state1, val1, dsc1, evt1, sev1 = \
                        check2Val(d, j, 5, 0,elmName, tcp0)
            if state1 != UP or sev1 != 0:
                 flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                 setThem(dscc, state1, val1, dsc1, evt1, sev1,\
                         r, o, y, u, "")

        if flag == 1:
            return sumThem(r, o, y, u, val, evt, dscc)  
        else:
            return UP, 0, "OK", "tcp", 0

    elif elmName == "udp":
        d = udp_d
        return  check3Val(d, "InErrors", 50, 25, 5, elmName, udp0)

    elif elmName == "ip":
        d = ip_d
        flag = 0
        state1, val1, dsc1, evt1, sev1 = check3Val(d, "InHdrErrors", 100,50,25,\
                                                   elmName,ip0)
        if state1 != UP or sev1 != 0:
	   flag = 1
           state, val, dsc, evt, sev = state1, val1, dsc1, evt1, sev1
           checkprint(f,"%s %s %s %s %s %s"%("\n", state, val, dsc, evt, sev))
        else:
           state1, val1, dsc1, evt1, sev1 = check2Val(d, "InAddrErrors", 10,0,\
                                                      elmName,ip0)
           if state1 != UP  or sev1 != 0:
                flag = 1
                state, val, dsc, evt, sev = state1, val1, dsc1, evt1, sev1
                checkprint(f,"%s %s %s %s %s %s"%("\n", state, val, dsc, evt, sev))

        if flag == 1:
		return state, val, dsc, evt, sev       
        else: 
		return UP, 0, "OK", "ip", 0

    elif elmName == "icmp":
        d = icmp_d
        l1 = ICMP_L[1:4] + ICMP_L[5:7] + ICMP_L[14:17] + ICMP_L[18:20]
        l2 = [1, 5, 5, 1, 1, 1, 5, 5, 1, 1]
        i = 0
        flag = 0
        dscc = "" 
        r = 0
        o = 0
        y = 0
        u = 0
        for j in l1:
           state1, val1, dsc1, evt1, sev1 = \
                        check2Val(d, j, 10, l2[i], elmName, icmp0)
           if state1 != UP or sev1 != 0:
           	flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                setThem(dscc, state1, val1, dsc1, evt1, sev1, \
                        r, o, y, u, "")
           i = i + 1

        if flag == 1:
           return sumThem(r, o, y, u, val, evt, dscc)
        else:
           return UP, 0, "OK", "icmp", 0

    elif elmName == "snmp":
        d = snmp_d
        flag = 0
        state1, val1, dsc1, evt1, sev1 = check2Val(d, "InBadCommunityNames", 5,0,\
                                                   elmName, snmp0)
        if state1 != UP or sev1 != 0:
           flag = 1
	   state, val, desc, evt, sev = state1, val1, dsc1, evt1, sev1
           checkprint(f,"%s %s %s %s %s %s"%("\n", state, val, dsc, evt, sev))
        else:
           state1, val1, dsc1, evt1, sev1 = check2Val(d, "InASNParseErrs", 5,0,\
                                                      elmName, snmp0)
           if state1 != UP or sev1 != 0:
                flag = 1
                state, val, dsc, evt, sev = state1, val1, dsc1, evt1, sev1
                checkprint(f,"%s %s %s %s %s %s"%("\n", state, val, dsc, evt, sev))
        if flag == 1:
                return state, val, dsc, evt, sev
        else:
                return UP, 0, "OK", "snmp", 0

    elif elmName =="if":
      flag = 0
      print ""
      dscc = "" 
      r = 0
      o = 0
      y = 0
      u = 0
      for nodeNam in nodes:
        d = node_d[nodeNam]
        d0 = node0[nodeNam]
        state1, val1, dsc1, evt1, sev1 = check2Stat(d, "AdminStatus", "down(2)",\
                                                    "testing(3)", elmName)
        if state1 != UP or sev1 != 0:
                flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                setThem(dscc, state1, val1, dsc1, evt1, sev1,\
                        r, o, y, u, nodeNam)
        else:
             	state1,val1,dsc1,evt1,sev1 = check2Stat(d,"OperStatus", "down(2)",\
                                                        "testing(3)", elmName)
                if state1 != UP or sev1 != 0:
                      flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                      setThem(dscc, state1, val1, dsc1, evt1, sev1,\
                              r,o,y,u, nodeNam)

        if (string.find(nodeNam,'pwr') < 0): 	#check if it's power node
        	state1, val1, dsc1, evt1, sev1 = check2Str(d, "duplex",\
                                                  "Full", "Half", elmName)    
                if state1 != UP or sev1 != 0:
	   		flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                	setThem(dscc, state1, val1, dsc1, evt1, sev1,\
                                r, o, y, u,  nodeNam)
	else:
		evt = "%sduplex"%(nodeNam)
        	val = d['duplex']
        	if val == 'Half' or val == 'Auto' or val == 'Full':
                 	state =  UP
                        dsc =  "Duplex_is_good"
                        sev = 0
                else:
                        state = UNDEFINED
                        dsc =  "Undefined"
                        sev = 2


	for i in ['RxFlow', 'TxFlow']:
                state1, val1, dsc1,evt1,sev1 = checkStr(d, i, "off+off", elmName)
                if state1 != UP or sev1 != 0:
			flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                        setThem(dscc, state1, val1, dsc1, evt1, sev1,\
                                r,o,y,u, nodeNam)
          
        i = 0
        for j in [LIST1[12], LIST1[13], LIST1[18], LIST1[19]]:
               	state1, val1, dsc1, evt1, sev1 = check3Val(d,j,\
                            L[i],L[i+4],L[i+8], elmName,d0)
             	if state1 != UP or sev1 != 0:
			flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                        setThem(dscc, state1, val1, dsc1, evt1, sev1, \
                                r, o, y, u, nodeNam)
		i = i + 1

        state1, val1, dsc1, evt1, sev1 = check2Val(d, "InUnknownProtos",5,0,\
                                                 elmName, d0)
        if state1 != UP or sev1 != 0:
		flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                setThem(dscc, state1, val1, dsc1, evt1, sev1, \
                        r, o, y, u, nodeNam)

	state1, val1, dsc1,evt1,sev1 = checkVal(d,"Collisions",0,elmName,d0)
        if state1 != UP or sev1 != 0: 
		flag, dscc, state, val, dsc, evt, sev, r, o, y, u = \
                setThem(dscc, state1, val1, dsc1, evt1, sev1, \
                        r, o, y, u, nodeNam)

      if flag == 1:
       	return sumThem(r, o, y, u, val, elmName, dscc)  
      else:
      	return UP, val, "OK", "if", 0

    else:
	return UNDEFINED, -1, "Unable_to_execute", "Undefined event", 2


##########################################################################
## This function set up html tables for the MIB groups by HTMLgen.       #
##########################################################################
def setTable(node, list, tabName, htmName, headName):
    d = node 
    doc = HTMLgen.SimpleDocument(title=tabName,
                                 background="enstore.gif",
                                 textcolor="#000066")
    table = HTMLgen.Table(
        tabletitle = tabName, 
        cell_spacing = 5, cell_padding = 2,
        border = 2, width = 100, cell_align = "right",   
        heading=[headName, "Value"])
    table.body = []
    doc.append(table)
    for i in list:
        table.body.append([HTMLgen.Text(i), d[i] ])
    doc.write("%s.html"%(htmName,))


#########################################################################
## This function set up html tables for the nodes of IfeXtension        #
## group by HTMLgen                                                     #
#########################################################################
def setNodeTab(tabName, head, htmlName, list):
    doc = HTMLgen.SimpleDocument(title=tabName,
                                 background="enstore.gif",
                                 textcolor="#000066")
    table = HTMLgen.Table(
        tabletitle = tabName, 
        cell_spacing = 5, cell_padding = 2,
        border=2, width=100, cell_align="right",
        heading=head)
    table.body = []
    doc.append(table)
    nd = nodes
    nd.sort()
    for i in nd:
        line = [node_d[i]['name']]
        for j in list:
           line.append(node_d[i][j])
        table.body.append(line)
    doc.write("%s.html"%(htmlName,))

    
#########################################################################
# The following is the main routine.                                    #
#########################################################################

if __name__=="__main__":
    import sys
    
    if len(sys.argv) > 1:   	# check print requirement
       args = sys.argv
       if args[1] == "--print": # if print is required
          f = 1			# set flag on

    mib = ".1.3.6.1.2.1"   #.iso.org.dod.internet.mgmt.mib-2
    
    flownum = ".1.3.6.1.4.1.9.5.1.4.1.1."
    
    mibdir = "testfile/www"
           
    for i in 1, 2, 3, 4, 5, 6, 7, 10, 11, 16, 17, 31, 47:
        miblist[i] = "%s/%d"%(mibdir, i)
                
    mibflow["duplex"] = ["%s/%s"%(mibdir,"duplex"), "%s%s"%(flownum, "10")]

    k = 12
    for j in FLOW_L[1:]:
        k = k + 1
        mibflow[j] = ["%s/%s"%(mibdir,j), "%s%d"%(flownum, k)]


#### Run snmpwalk to get MIB 

    for i in [1, 2, 4, 5, 6, 7, 11, 16, 31]:
      	id = "%s.%s"%(mib,i)
    
        #check if input file exists, remove it
        if os.access(miblist[i], 0) == 1:
          	os.remove(miblist[i])

        os.system("snmpwalk s-d0-fcc2w.fnal.gov public %s > \
                           %s"%(id,miblist[i]))

                  
    for j in FLOW_L:
        #check if input file exists, remove it
        if os.access(mibflow[j][0], 0) == 1:
           	os.remove(mibflow[j][0])
	
        os.system("snmpwalk s-d0-fcc2w.fnal.gov public %s > %s"\
                           %(mibflow[j][1], mibflow[j][0]))
   
#### setup the lists that will be the keys to update the node dictionary
 
    cableVal = []
    for dic in nodeValue:
        cableVal.append(dic['cable'])
       
    ind2 = []
    for i in cableVal:
        pat = "interfaces.ifTable.ifEntry.ifIndex.%s\n"%(i,)
        file = open(miblist[16], "r")
        for line in file.readlines():
              #check match string in file
              if string.find(line, pat) >= 0:  
                  str = string.split(line, ".")[5]
                  ind2.append(string.split(str," ")[0])

#### Update the node dictionary

    count = 0
    for i in nodes:
        if node_d[i][node_init.PROD] == node_init.YES:
            node_d[i]['color'] = BLUE
        else:
            node_d[i]['color'] = TEAL
    
        i1 = node_d[i][node_init.CABLE]
        i2 = ind2[count]
        count = count + 1
        str = "ifMIB.ifMIBObjects.ifXTable.ifXEntry.ifName.%s"%(i1,)
	
        i3 = getit(str, miblist[31])
        str = "interfaces.ifTable.ifEntry.ifOperStatus.%s"%(i1,)
        node_d[i]['status'] = getit(str, miblist[2])

        str = "ifMIB.ifMIBObjects.ifXTable.ifXEntry.ifHighSpeed.%s"%(i1,)
        speed = getit(str, miblist[31])
    
        pattern = 'Gauge:'
        #check pattern in the output of speed
        if (string.find(speed, pattern, 0) >= 0):
                #replace the pattern
                output = string.replace(speed, pattern, "")
                node_d[i]['speed'] = output
        else:
                node_d[i]['speed'] = speed
        
        
        node_d[i]['duplex'] = getdup(i3, mibflow['duplex'][0])
        
        str = "16.1.1.1.13.%s"%(i2,)
        node_d[i]['collection'] = getit(str, miblist[16])
        
        node_d[i]['RxFlow'] = getflow(i3, 'rx')
        node_d[i]['TxFlow'] = getflow(i3, 'tx')
              
        str = "interfaces.ifTable.ifEntry.ifInOctets.%s"%(i1,)
        node_d[i]['InBytes'] = getit(str, miblist[2])
   
        str = "interfaces.ifTable.ifEntry.ifOutOctets.%s"%(i1,)
        node_d[i]['OutBytes'] = getit(str, miblist[2])
        
        str = "interfaces.ifTable.ifEntry.ifInErrors.%s"%(i1,)
        node_d[i]['InError'] = getit(str, miblist[2])
                
        str = "interfaces.ifTable.ifEntry.ifOutErrors.%s"%(i1,)
        node_d[i]['OutError'] = getit(str, miblist[2])

      

#### Update the group dictionaries

    system_d = get_mib_info(SYS_L, 'system.sys', 1)
    tcp_d = get_mib_info(TCP_L, 'tcp.tcp', 6)  
    udp_d = get_mib_info(UDP_L, 'udp.udp', 7)
    ip_d = get_mib_info(IP_L, 'ip.ip', 4)
    icmp_d = get_mib_info(ICMP_L, 'icmp.icmp', 5)
    snmp_d = get_mib_info(SNMP_L, 'snmp.snmp', 11)

    
#### Update the node dictionary

    for i in LIST1:
        for j in nodes:
                str = "interfaces.ifTable.ifEntry.if%s.%s"% \
                       (i, node_d[j]['cable'])
                node_d[j][i] = getit(str, miblist[2])
        
    for i in LIST2:
        for j in nodes:
                str = "ifMIB.ifMIBObjects.ifXTable.ifXEntry.if%s.%s"% \
                       (i,node_d[j]['cable'])
                node_d[j][i] = getit(str, miblist[31])
        
    count = 2
    for i in LIST3:
        count = count + 1
        n = 3
        for j in nodes:
                str = "16.1.1.1.%s.%s"%(count, ind2[n-3])
                node_d[j][i] = getit(str, miblist[16])
                n = n + 1

#### Set up the list for the further to set up html tables   

    list = LIST1 + LIST2 + LIST3
    head = ['Node Name'] + LIST

    sublist =  LIST1[2:5] + [LIST1[6]] + LIST1[12:15] + LIST1[18:20] + \
              [LIST2[-1]] + [LIST3[0]] + LIST3[5:8] + [LIST3[10]]
    head2 = ['Node Name'] + SUBLIST

#### Save the initiation of nodes as the offset for monitoring MIB  

    tcp0 = tcp_d
    udp0 = udp_d
    ip0 = ip_d
    icmp0 = icmp_d
    snmp0 = snmp_d
    node0 = node_d

#### check node and send event to NCS

    checkTime=60                	# monitoring interval
    maName="SiscoSwitchAgent"           	# name of the monitoring agent
    sysName="SiscoSwitch" 		# system name
    clusterName="END0" 			# cluster name
    nodeName="d0ensrv1"     		# node name, it is hostname exactly.
                                        # To register monitoring agent with
                                        # NGOP Central Server, this name 
                                        # should be one of the node names in
                                        # ifeXtension group
    meNames=['tcp', 'udp', 'icmp', 'snmp', 'ip', 'if'] 		
                                        # names of monitored element
    meType="Network" 			# type of monitored element
    heartbeat="300" 			# heartbeat rate in sec
    serverHost='ngop'   		# points  to my machine
    serverPort="19997"			# my port number
    cl=MA_API.MAClient() 		# creates MAClient object
    cl.setDebug(6)
    cl.setMAAttrib(maName,heartbeat,serverHost,serverPort) 
					#sets attributes
    cl.addSystem(sysName,clusterName) 	# configures the system

    oldStateList = []
    oldSevLvList = []
    for elmName in meNames:
           cl.addME(sysName,clusterName,elmName,meType,nodeName)
           				#configures system monitored elements list
           oldStateList.append(1)
           oldSevLvList.append(0)
    cl.register() 			#registers monitoring agent with 
					#NGOP Central Server

    checkprint(f, oldStateList)
    checkprint(f, oldSevLvList)

    while(1):
        i = -1
        for elmName in meNames:
            i = i + 1
            state, val, desc, evtName, sevLevel = checkNode(elmName)
            checkprint(f, "\ni is %s"%(i,))
            checkprint(f, oldStateList)
            checkprint(f, oldSevLvList)
            checkprint(f,"%s %s %s %s %s"%(elmName, state, desc, evtName, sevLevel))
            if oldStateList[i] == state and oldSevLvList[i] == sevLevel: 
		continue 		#nothing has changed
            eventDict={'EventType':meType, 'EventName':evtName,'EventValue':val, \
                                            'State':state, 'SevLevel':sevLevel}
            eventDict['Description'] = desc
	    status, reason = cl.sendEvent(eventDict,sysName, clusterName, elmName,\
  					  nodeName)
            oldStateList[i] = state
            oldSevLvList[i] = sevLevel
	    checkprint(f,"%s %s %s %s"%(status, elmName, evtName, sevLevel))
		
            if not status: 
               	checkprint(f,"%s %s"%( "Error:", reason))
            else:
                checkprint(f, "!this is the status and reason %s,%s !"%(status, reason))

        time.sleep(checkTime)


### Run snmpwalk to update MIB

        for i in [1, 2, 4, 5, 6, 7, 11, 16, 31]:
            id = "%s.%s"%(mib,i)
                
            #check if input file exists, remove it
            if os.access(miblist[i], 0) == 1:
                os.remove(miblist[i])
   
            os.system("snmpwalk s-d0-fcc2w.fnal.gov public %s > \
                       %s"%(id,miblist[i]))
    
   	for j in FLOW_L:
        	#check if input file exists, remove it
            if os.access(mibflow[j][0], 0) == 1:
                os.remove(mibflow[j][0])
    
            os.system("snmpwalk s-d0-fcc2w.fnal.gov public %s > %s"\
                           %(mibflow[j][1], mibflow[j][0]))

	ind2 = []
    	for i in cableVal:
            pat = "interfaces.ifTable.ifEntry.ifIndex.%s\n"%(i,)
            file = open(miblist[16], "r")
            for line in file.readlines():
              	#check match string in file
              	if string.find(line, pat) >= 0:
                  	str = string.split(line, ".")[5]
                  	ind2.append(string.split(str," ")[0])

### Update the node dictionary

        count = 0
    	for i in nodes:
            i1 = node_d[i][node_init.CABLE]
            i2 = ind2[count]
            count = count + 1
            str = "ifMIB.ifMIBObjects.ifXTable.ifXEntry.ifName.%s"%(i1,)
            i3 = getit(str, miblist[31])

            str = "interfaces.ifTable.ifEntry.ifOperStatus.%s"%(i1,)
            node_d[i]['status'] = getit(str, miblist[2])
    
            str = "ifMIB.ifMIBObjects.ifXTable.ifXEntry.ifHighSpeed.%s"%(i1,)
            speed = getit(str, miblist[31])

            pattern = 'Gauge:'
            #check pattern in the output of speed
            if (string.find(speed, pattern, 0) >= 0):
                #replace the pattern
                output = string.replace(speed, pattern, "")
                node_d[i]['speed'] = output
            else:
                node_d[i]['speed'] = speed   

    
            node_d[i]['duplex'] = getdup(i3, mibflow['duplex'][0])
            
            str = "16.1.1.1.13.%s"%(i2,)
            node_d[i]['collection'] = getit(str, miblist[16])
        
            node_d[i]['RxFlow'] = getflow(i3, 'rx')
            node_d[i]['TxFlow'] = getflow(i3, 'tx')
        
	    str = "interfaces.ifTable.ifEntry.ifInOctets.%s"%(i1,)
            node_d[i]['InBytes'] = getit(str, miblist[2])
        
            str = "interfaces.ifTable.ifEntry.ifOutOctets.%s"%(i1,)
            node_d[i]['OutBytes'] = getit(str, miblist[2])
        
            str = "interfaces.ifTable.ifEntry.ifInErrors.%s"%(i1,)
            node_d[i]['InError'] = getit(str, miblist[2])
        
            str = "interfaces.ifTable.ifEntry.ifOutErrors.%s"%(i1,)
            node_d[i]['OutError'] = getit(str, miblist[2])

### Update the group dictionaries
                
    	system_d = get_mib_info(SYS_L, 'system.sys', 1)
    	tcp_d = get_mib_info(TCP_L, 'tcp.tcp', 6)
    	udp_d = get_mib_info(UDP_L, 'udp.udp', 7)
    	ip_d = get_mib_info(IP_L, 'ip.ip', 4)
        icmp_d = get_mib_info(ICMP_L, 'icmp.icmp', 5)
        snmp_d = get_mib_info(SNMP_L, 'snmp.snmp', 11)
            
### Update the node dictionary

        for i in LIST1:
            for j in nodes:
                str = "interfaces.ifTable.ifEntry.if%s.%s"% \
                       		(i, node_d[j]['cable']) 
                node_d[j][i] = getit(str, miblist[2])
        
    	for i in LIST2:
            for j in nodes:
                str = "ifMIB.ifMIBObjects.ifXTable.ifXEntry.if%s.%s"% \
                       		(i,node_d[j]['cable'])
                node_d[j][i] = getit(str, miblist[31])
        
    	count = 2
    	for i in LIST3:
            count = count + 1
            n = 3
            for j in nodes:
                str = "16.1.1.1.%s.%s"%(count, ind2[n-3])
                node_d[j][i] = getit(str, miblist[16])
		n = n + 1

        print ""

### Setup HTML files to send the updated information to the web page

        setTable(system_d, SYS_L, "System Table", "systemtable", "System")
        setTable(tcp_d, TCP_L, "Tcp Table", "tcptable", "Tcp")
    	setTable(udp_d, UDP_L, "Udp Table", "udptable", "Ucp")
    	setTable(ip_d, IP_L, "Ip Table", "iptable", "Ip")
    	setTable(icmp_d, ICMP_L, "Icmp Table", "icmptable", "Icmp")
    	setTable(snmp_d, SNMP_L, "Snmp Table", "snmptable", "Snmp")
        setNodeTab('Node Table', head, 'nodeTable', list)
        setNodeTab("Sub Node Table", head2, 'subnodeTable', sublist)


