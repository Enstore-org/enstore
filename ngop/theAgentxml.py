import MA_API
import time
import sys
import os
import string
import HTMLgen
import regsub

import re
import node_init

from Worker import Worker

DOWN      = 0
UP        = 1
UNDEFINED = -1


BLACK = '#000000'
WHITE = '#FFFFFF'
TEAL  = '#008080'
RED   = '#FF0000'
BLUE  = '#0000FF'
GREEN = '#00FF00'
MAGENTA = '#FF00FF'
CYAN  = '#00FFFF'
YELLOW = '#FFFF00'
SILVER = '#C0C0C0'
CORAL = '#FF7F50'
TURQUOISE = '#40E0D0'
NAVY = '#000080'


FLOW_L = ['duplex', 'adminrxflow', 'operrxflow',  'admintxflow', 'opertxflow']

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

CHECK_L = [1, 2, 0, 1, 2, 0]

LIST4 = ['Type', 'Mtu','Speed', 'ConnectorPresent', 'DropEvents', \
         'CRCAlignErrors', 'UndersizePkts', 'OversizePkts', 'Fragments', \
         'Jabbers']

PATH = 'testfile/www'



#define the modules
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
    
def get_mib_info(list, path, filenum):
    dat_d = {}  
    for i in list:
        str = "%s%s.0"%(path, i)
        dat_d[i] = getit(str, miblist[filenum])
    return dat_d

########

lass NodeFunc(Worker):
        def __init__(self):
                Worker.__init__(self)

        def checkNode(self, elmNamee):
                if elmName == "tcp":
                        d = tcp_d
                        if not len(d):
                                return UNDEFINED
                        val = d['InErrs']
                        return string.atoi(val)

                elif elmName == "udp":
                        d = udp_d
                        if not len(d):
                                return UNDEFINED
                        val = d['InErrors']
                        return string.atoi(val)

                elif elmName == "ip":
                        d = ip_d
                        if not len(d):
                                return UNDEFINED
                        val = d['InHdrErrors']  
                        return  string.atoi(val)
                else:
                        return UNDEFINED


        
def printThem(d,list):
    print ""
    for i in list:
   	print "%s is %s"%(i, d[i])

def setTable(node, list, tabName, htmName, headName):
    d = node 
    doc = HTMLgen.SimpleDocument(title=tabName)
    table = HTMLgen.Table(
        tabletitle = tabName,
        border=2, width=100,cell_align="right",   
        heading=[headName, "Value"])
    table.body = []
    doc.append(table)
    for i in list:
        table.body.append([HTMLgen.Text(i), d[i] ])
    doc.write("%s.html"%(htmName,))


################################

if __name__=="__main__":

    mib = ".1.3.6.1.2.1"   #.iso.org.dod.internet.mgmt.mib-2
    
    flownum = ".1.3.6.1.4.1.9.5.1.4.1.1."
    
   # mibdir = "/var/www"
   # mibdir = sys.argv[1]
    mibdir = "testfile/www"
           
    miblist = {}
    for i in 1, 2, 3, 4, 5, 6, 7, 10, 11, 16, 17, 31, 47:
        miblist[i] = "%s/%d"%(mibdir, i)
    #print miblist
                
    mibflow = {}
    mibflow["duplex"] = ["%s/%s"%(mibdir,"duplex"), "%s%s"%(flownum, "10")]

    k = 12
    for j in FLOW_L[1:]:
        k = k + 1
        mibflow[j] = ["%s/%s"%(mibdir,j), "%s%d"%(flownum, k)]

###


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
   
###
                  
    node_d = node_init.node_d
    nodes = node_d.keys() 
    nodeValue = node_d.values()
  
    cableVal = []
    for dic in nodeValue:
        cableVal.append(dic['cable'])
    #print cableVal
       
    ind2 = []
    for i in cableVal:
        pat = "interfaces.ifTable.ifEntry.ifIndex.%s\n"%(i,)
        file = open(miblist[16], "r")
        for line in file.readlines():
              #check match string in file
              if string.find(line, pat) >= 0:  
                  str = string.split(line, ".")[5]
                  ind2.append(string.split(str," ")[0])
    #print ind2

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

    system_d = get_mib_info(SYS_L, 'system.sys', 1)
    tcp_d = get_mib_info(TCP_L, 'tcp.tcp', 6)  
    udp_d = get_mib_info(UDP_L, 'udp.udp', 7)
    ip_d = get_mib_info(IP_L, 'ip.ip', 4)
    icmp_d = get_mib_info(ICMP_L, 'icmp.icmp', 5)
    snmp_d = get_mib_info(SNMP_L, 'snmp.snmp', 11)
    
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
        for j in nodes:
                str = "16.1.1.1.%s.%s"%(count, ind2[count-3])
                node_d[j][i] = getit(str, miblist[16])
    #print node_d['srv1']

#### Setup HTML files to send the data to the web page
   
    setTable(tcp_d, TCP_L, "Tcptable", "tcptable", "Tcp") 
    print "html table setup!!!!"
    setTable(udp_d, UDP_L, "Udptable", "udtable", "Ucp")
    setTable(ip_d, IP_L, "Iptable", "iptable", "Ip")
    setTable(icmp_d, ICMP_L, "Icmptable", "icmptable", "Icmp")
    setTable(snmp_d, SNMP_L, "Snmptable", "snmptable", "Snmp")
    

####check node and send event to NCS

    cl = MA_API.MAClient(sys.argv[1], NodeFunc())
    cl.register()
    cl.run()


