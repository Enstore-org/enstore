#!/usr/bin/env python
######################################################################################################
# NAME         : FERMI LABS - RICHARD KENNA
# DATE         : JULY 29, 1999
# DESCRIPTION  : THIS PROGRAM DOES A MULTI-TASKING CALL TO A NETWORK PERFORMANCE TESTER
#              : THE PROGRAM DOES ONE WAY TESTING - WHERE A NODE WILL EITHER SEND OR RECIEVE
#              : DATA BUT NOT BOTH. IT DOES TWO WAY TESTING - WHERE A NODE WILL SEND AND
#              : RECIEVE DATA AT THE SAME TIME. IT DOES SELF TEST - WHERE A NODE WILL TEST ITSELF.
#              : YOU CAN ALSO GIVE IT A LIST FROM A FILE, TELLING THE PROGRAM WHAT NODES TO TEST OR
#              : YOU CAN MANUALLY INPUT A LIST FROM THE COMMAND LINE. THE LAST OPTION IS HOW
#              : LONG YOU WANT THE TEST TO LAST.
# PRECONDITION : VALID NODES TO TEST, VALID FILE CONTAINING TEST LIST(IF OPTION IS USED).
# POSTCONDITION: THE PROGRAM WILL TELL IF THERE IS ANY OPTION ERRORS AND IF EVERYTHING
#              : CHECKS OUT(OPTION WISE), THE PROGRAM WILL PRINT THE TEST RESULTS TO SCREEN.
######################################################################################################

import time
import os
import re
import string
import posixpath
import getopt
import sys

paramdict = {}      # HOLDS THE FLAG PARAMETERS FOR THE PROGRAM
resultdict = {}     # HOLDS THE FINAL RESULT TIME FOR THE TESTS PERFORMED
testdict = {}       # HOLDS THE CHILD PID IN CASE THE TEST 'HANGS'
tstseqdict = {}     # HOLDS THE SEQUENCE IN WHICH THE NODES ARE TO BE TESTED IN
tempresltdict = {}  # HOLDS THE STRING FROM THE TEST. IT IS NEEDED IN CASE THE CHILD TEST HANGS - THE PROGRAM WON'T HANG
testList = []       # HOLDS THE LIST OF NODES TO BE TESTED
cuTime = "5"        # CLEAN UP TIME. A DELAY THAT ALLOWS THE SYSTEM TO FINISH UP TESTING. IF YOU SET THIS VALUE TO 3 OR
                    # LESS, THE SYSTEM WILL GET MORE SLOW OR HUNG (-1) INDICATIONS THAN IT NORMALLY SHOULD
defTstTime = "5"    # DEFAULT TIME FOR TEST TO RUN IF NO  OPTION GIVEN
defNetPerfPath = "/opt/netperf/netperf"           # LOCATION OF NETWORK PERFORMANCE TEST
defListPath = "/usr/local/etc/farmlets/d0en"      # LOCATION OF FILE CONTAINING LIST OF NODES TO BE TESTED
ROUTE="/sbin/route"
route_add=""
route_del=""
TRUE = 1
FALSE = 0

####################################################################
# DESCRIPTION  : ACTUALLY CALLS THE TEST PROGRAM.
# PRECONDITION : FROMNODE, TONODE AND TESTTIME EXISTS AND ARE VALID
# POSTCONDITION: CALLS THE TEST PROGRAM FROM A 'RSH' AND STORES THE
#              : RESULT IN 'TMPRESLTDICT'
####################################################################
def testSeq(fromNode, toNode):
    global route_add,route_del
    ppid = os.getpid()
    testdict["%s %s" % (fromNode, toNode)] = ""
    if fromNode[0:4] == "d0en":
	if fromNode[-1:] == "a":
	    route_add = "%s add -host %s eth0" % (ROUTE,toNode,)
	    route_del = "%s del %s" % (ROUTE,toNode,)
	elif fromNode[-1:] == "b":
	    route_add = "%s add -host %s eth1" % (ROUTE,toNode,)
	    route_del = "%s del %s" % (ROUTE,toNode,)
    else:
        route_add = "pwd"
        route_del = "pwd"
    # print 'rsh %s "%s;%s -l %s -H %s;%s" ' % (fromNode, route_add, defNetPerfPath, paramdict['testtime'], toNode,route_del)
    tempresltdict["%s %s" % (fromNode, toNode)] = os.popen('rsh %s "%s;%s -l %s -H %s;%s" 2>/dev/null' % (fromNode, route_add, defNetPerfPath, paramdict['testtime'], toNode,route_del))
    if testdict["%s %s" % (fromNode, toNode)] == "":
        testdict["%s %s" % (fromNode, toNode)] = findChild(ppid, fromNode, toNode)
    
###########################################################################
# DESCRIPTION  : FINDS THE CHILD PROCESS OF THE SYSTEM BEING TESTED.
#              : THIS IS IN CASE THE SYSTEM HANGS. THE PROGRAM HAS
#              : THE CHILD ID SO IT CAN KILL THE PROCESS.
# PRECONDITION : YOU HAVE THE PID FROM THE CALLING FUNCTION AND YOU KNOW
#              : WHAT NODE SYSTEM THE PROGRAM IS CALLING FROM AND TO.
#              :   ******* NOTE *******
#              : YOU MAY HAVE TO ADD AN ADDITIONAL W TO THE 'PS WHJ'
#              : COMMNAND IF THE "/OPT/NETPERF/NETPERF" PATH GETS LONGER.
# POSTCONDITION: RETURNS THE CHILD PID THAT WAS JUST CREATED.
###########################################################################
def findChild(ppid, fromNode, toNode):
    global route_add,route_del
    #print "in findChild",ppid, 'rsh %s "%s;%s' % (fromNode,route_add,defNetPerfPath,)
    cpid = ""
    a = os.popen("ps hjw")  # IF /OPT/NETPERF/NETPERF PATH CHANGES, YOU MAY HAVE TO ADD
    a = a.read()            # AN ADDITIONAL W IF THE PATH NAME GETS TO LONG.
    #print "in findChild",a
    a = string.split(a, "\n")
    num = 0
    while num < len(a) - 1:
        b = string.split(a[num])
        if b[0] == str(ppid) and string.find(a[num], 'rsh %s "%s;%s' % (fromNode,route_add,defNetPerfPath,)) >= 0:
            cpid = str(b[1])
            break
        else:
            cpid = -1
        num = num + 1
    return cpid

###############################################################################
# DESCRIPTION  : CHECKS TO SEE IF A PROCESS IS HUNG OR NOT RESPONDING
#              : IF IT IS HUNG, IT WILL KILL THAT PROCESS. IF THE PROCESS
#              : ISN'T HUNG, IT WILL PUT THE TEST RESULTS INTO THE RESULTDICT
# PRECONDITION : THE 'TESTDICT' HAS THE CORRECT PID SO THE FUNCTION WILL KNOW
#              : WHICH PID'S NEED TO BE KILLED
# POSTCONDITION: ANY HUNG PROCESSESS WILL BE KILLED AND ANY TEST RESULTS THAT
#              : ARE GOOD ARE PUT INTO THE 'RESULTDICT'.
###############################################################################
def tstHung():
    num = 0
    #print "In tstHung. num=",num
    while num < len(testdict.keys()):
        name = testdict.keys()[num]
        #print "In tstHung. name=",name,testdict[name]
        fromNode = name[0]
        toNode = name[1]
        
        if (paramdict['fFlg'] == FALSE) and (fromNode == toNode):
            resultdict[name] = ""         # DON'T LOOK TO SEE IF HUNG IF THERE WAS NO SELF TEST
        else:
            if testdict[name] < 0:        # IF LESS THAN 0 THAN AN ERROR OCCURRED AND NO SELF TEST WAS EVER DONE.
                resultdict[name] = "-2"
            else:
                isAlive = os.popen("ps hj %s 2>/dev/null" % testdict[name])
                isAlive = isAlive.read()
                if string.find(string.lower(isAlive), "zombie") < 0:                 # SEE IF CHILD PID COMPLETED OK.
                    os.system("kill -9 %s >/dev/null 2>/dev/null" % testdict[name])  # IF NOT - KILL THE CHILD
                    resultdict[name] = "-1"
                else:
                    tempresltdict[name] = tempresltdict[name].read()
                    #print  tempresltdict[name]
                    tempresltdict[name] = string.split(tempresltdict[name], "\n")
                    if string.find(tempresltdict[name][0], "TCP STREAM TEST") < 0:   # SEE IF TEST WAS EVER DONE
                        resultdict[name] = "-2"
                    else:
                        tempresltdict[name] = tempresltdict[name][6]                 # IF IT DID PUT THE RESULTS INTO THE TEMPRESULTDICT
                        tempresltdict[name] = string.split(tempresltdict[name])
                        resultdict[name] = tempresltdict[name][4]
        num = num + 1

##########################################################################
# DESCRIPTION  : THIS FUNCTION GENERATES THE SEQUENCE IN WHICH
#              : THE NODES WILL BE TESTED.
# PRECONDITION : 'TESTLIST' EXISTS AND HAS VALID NODES IN IT
# POSTCONDITION: A TEST SEQUENCE THAT DOESN'T CAUSE A NODE TO TRY
#              : TO DO MANY TESTS AT ONCE IS GENERATED.
##########################################################################
def genTstSeq():
    MAX_ROW = MAX_COL = len(testList)   # IT WOULD TAKE TO LONG TO EXPLAIN HOW THIS WORKS.
    col_ptr = 0                         # BUT IF YOU TRACE IT THROUGH YOU'LL SEE HOW IT WORKS.
    while col_ptr < MAX_COL:
        tstseqdict[col_ptr] = []
        t1_col = col_ptr
        num_ent = t1_col + 1
        if num_ent % 2 == 1:
            rd_num = num_ent / 2 + 1
        else:
            rd_num = num_ent / 2
        if col_ptr == MAX_COL - 1:
            rd_num = MAX_COL
        t1_row = 0
        while rd_num > 0:
            tstseqdict[col_ptr].append("%s %s" % (testList[t1_row], testList[t1_col]))
            t1_row = t1_row + 1
            t1_col = t1_col - 1
            rd_num = rd_num - 1
        t2_row = col_ptr + 1
        t2_col = MAX_COL - 1
        while t2_row < MAX_ROW:
            tstseqdict[col_ptr].append("%s %s" % (testList[t2_row], testList[t2_col]))
            t2_row = t2_row + 1
            t2_col = t2_col - 1
        while t1_col >= 0:
            tstseqdict[col_ptr].append("%s %s" % (testList[t1_row], testList[t1_col]))
            t1_col = t1_col - 1
            t1_row = t1_row + 1
        col_ptr = col_ptr + 1

################################################################
# DESCRIPTION  : KEEPS TRACK OF THE TESTING SEQUENCE.
# PRECONDITION : THE TEST SEQUENCE HAS ALREADY BEEN GENERATED
#              : AND THE NODES ARE VALID
# POSTCONDITION: GOES THROUGH THE TEST SEQUENCE DICTIONARY.
################################################################
def testNodes():
    numTstGrps = len(tstseqdict.keys())
    grpSeqNum = 0
    testNum = 1
    
    seqLen = len(tstseqdict[grpSeqNum])
    if paramdict['fFlg']:
        numTests = numTstGrps * numTstGrps
    else:
        numTests = numTstGrps * numTstGrps - numTstGrps
        
    if paramdict['oFlg']:
        if seqLen % 2 == 1:
            toTest = seqLen / 2 + 1
        else:
            toTest = seqLen / 2
    else:
        toTest = seqLen
        
    if numTests < 3:
        if numTstGrps == 2 and paramdict['tFlg'] and not paramdict['fFlg']:
            testTime = string.atoi(paramdict['testtime']) + string.atoi(cuTime)
        else:
            testTime = numTests * string.atoi(paramdict['testtime']) + string.atoi(cuTime)
    else:
        if paramdict['tFlg']:
            testTime = numTstGrps * string.atoi(paramdict['testtime']) + string.atoi(cuTime)
        else:
            testTime = 2 * numTstGrps * string.atoi(paramdict['testtime']) + string.atoi(cuTime)
    sys.stdout.write("\nThere are %s tests:\n" % str(numTests))
    sys.stdout.write("This program will take app. %s seconds to run.\n" % str(testTime))
    sys.stdout.write("It could take much longer depending on how busy the systems are.\n")
    prevTstNum = testNum
    while grpSeqNum < numTstGrps:
        sys.stdout.write("\n")
        
        tstLoopCntr = 0
        while tstLoopCntr < toTest:
            name = tstseqdict[grpSeqNum][tstLoopCntr]
            tmpName = string.split(name)
            fromNode = tmpName[0]
            toNode = tmpName[1]
            if (paramdict['fFlg'] == FALSE) and (fromNode == toNode):
                resultdict[name] = ""
            else:
                if prevTstNum != testNum:
                    sys.stdout.write(", ")
                sys.stdout.write("%3s" % str(testNum))
                sys.stdout.flush()
                testSeq(fromNode, toNode)
                testNum = testNum + 1
            tstLoopCntr = tstLoopCntr + 1
        if prevTstNum != testNum:                          # DON'T PAUSE FOR TEST IF TEST WASN'T DONE
            prevTstNum = testNum
            time.sleep(string.atoi(paramdict['testtime'])) # MAY HAVE TO ADD 1 SECOND IF NEXT GROUP OF TESTS START BEFORE
                                                           # THE LAST GROUP WAS FINISHED
        while tstLoopCntr < seqLen:
            name = tstseqdict[grpSeqNum][tstLoopCntr]
            tmpName = string.split(name)
            fromNode = tmpName[0]
            toNode = tmpName[1]
            if (paramdict['fFlg'] == FALSE) and (fromNode == toNode):
                resultdict[name] = ""
            else:
                sys.stdout.write(", ")
                sys.stdout.write("%3s" % str(testNum))
                sys.stdout.flush()
                testSeq(fromNode, toNode)
                testNum = testNum + 1
            tstLoopCntr = tstLoopCntr + 1
        if prevTstNum != testNum:                          # DON'T PAUSE FOR TEST IF TEST WASN'T DONE
            prevTstNum = testNum
            time.sleep(string.atoi(paramdict['testtime'])) # MAY HAVE TO ADD 1 SECOND IF NEXT GROUP OF TESTS START BEFORE
        grpSeqNum = grpSeqNum + 1                          # THE LAST GROUP WAS FINISHED

##########################################################################
# DESCRIPTION  : PRINTS THE TEST RESULTS IN A VIEWABLE MANNER.
# PRECONDITION : THE RESULTS DICTIONARY EXISTS.
# POSTCONDITION: ALL THE OUTPUT IS SELF ADJUSTING TO WHAT THE INPUT WAS.
##########################################################################
def printResults():
    strtColWidth = 0
    hdrLen = 0
    lpCntr = 0

    while lpCntr < len(testList):
        if len(testList[lpCntr]) > strtColWidth:
            strtColWidth = len(testList[lpCntr])
        if len(testList[lpCntr]) < 7:
            colWidth = 7
        else:
            colWidth = len(testList[lpCntr]) + 1
        hdrLen = hdrLen + colWidth
        lpCntr = lpCntr + 1
    if strtColWidth < 7:
        strtColWidth = 7
    hdrLen = hdrLen + strtColWidth
    sys.stdout.write(string.center("TEST RESULTS\n", hdrLen))
    
    sys.stdout.write("\nfrom")
    lpCntr = 0
    while lpCntr < strtColWidth - 7:     # PUTS 'FROM' ON LEFT SIDE OF FIRST COLUME
        sys.stdout.write(" ")            # AND PUTS '\TO' ON THE RIGHT SIDE OF THE
        lpCntr = lpCntr + 1              # FIRST COLUME - NO MATTER HOW BIG THE COLUME GETS.
    sys.stdout.write("\\to")
    lpCntr = 0
    while lpCntr < len(testList):        # PRINTS THE TOP LINE LISTING THE 'TO' NODES
        if len(testList[lpCntr]) < 7:
            colWidth = 7
        else:
            colWidth = len(testList[lpCntr]) + 1
        sys.stdout.write(string.rjust(testList[lpCntr], colWidth))
        lpCntr = lpCntr + 1
    sys.stdout.write("\n")
    lpCntr = 0
    while lpCntr < hdrLen:
        sys.stdout.write("-")
        lpCntr = lpCntr + 1
    sys.stdout.write("\n")
    fromPtr = 0
    while fromPtr < len(testList):   # PRINTS THE 'FROM'NODES IN LEFT COLUME - ONE AT A TIME
        sys.stdout.write(string.ljust(testList[fromPtr], strtColWidth))
        toPtr = 0
        while toPtr < len(testList): # PRINTS THE TEST RESULTS
            if len(testList[toPtr]) < 7:
                colWidth = 7
            else:
                colWidth = len(testList[toPtr]) + 1
            sys.stdout.write(string.rjust(" %s" % resultdict["%s %s" % (testList[fromPtr], testList[toPtr])], colWidth))
            toPtr = toPtr + 1
        sys.stdout.write("\n")
        fromPtr = fromPtr + 1
    sys.stdout.write("\n\n    -2 : AN 'UNKNOWN ERROR' OCCURRED. RUN THE TEST BY HAND\n")
    sys.stdout.write("       : TO SEE WHAT THE ERROR WAS.\n")
    sys.stdout.write("""       : rsh fromNode "netPerfPath -l testtime -H toNode"\n""")
    sys.stdout.write("    -1 : SYSTEM HUNG OR HAS VERY SLOW RESPONSE\n")
    sys.stdout.write(" BLANK : SELF TEST. DEVICE DID NOT TEST ITSELF.\n")
    sys.stdout.write("# >= 0 : THROUGHPUT: NUM * 10^6 BITS/SEC\n\n")

####################################################################################
# DESCRIPTION  : PRINTS THE HELP FILE WHEN PROGRAM IS CALLED WITH THE '-H' OPTION.
# PRECONDITION : -H FLAG WAS USED WHEN PROGRAM WAS CALLED.
# POSTCONDITION: HELP FILE IS PRINTED TO THE SCREEN.
####################################################################################
def printHelp():
    sys.stdout.write("Here are the options to run this program:\n\n")
    sys.stdout.write("-h: HELP - Prints this help message.\n")
    sys.stdout.write("-o: ONE_WAY DATA TRANSFER (default) - Ex. node1 will transmit data but not recieve data,\n")
    sys.stdout.write("    or node1 will recieve the data but not transmit it at the same time.\n")
    sys.stdout.write("    ONE WAY TEST WILL TAKE 'NUM_NODES * 2 * TESTTIME' SECONDS TO COMPLETE.\n")
    sys.stdout.write("    *** NOTE *** '-o' and '-t' flags can't be set at the same time.\n")
    sys.stdout.write("-t: TWO_WAY DATA TRANSFER - Ex. node1 will transmit and recieve data\n")
    sys.stdout.write("    at the same time.\n")
    sys.stdout.write("    TWO WAY TEST WILL TAKE 'NUM_NODES * TESTTIME' SECONDS TO COMPLETE.\n")
    sys.stdout.write("    *** NOTE *** '-o' and '-t' flags can't be set at the same time.\n")
    sys.stdout.write("-l: '-l pathname/filename' - Read the test list from this file.\n")
    sys.stdout.write("    -l /usr/local/etc/farmlets/d0en is the default\n")
    sys.stdout.write("    *** NOTE *** '-l' and '-m' flags can't be set at the same time.\n")
    sys.stdout.write("""-m: '-m "dev1 dev2"' - Create the test list from the following devices.\n""")
    sys.stdout.write("    NOTE: the list of devices to be tested must be in double quotes and can be\n    of any length.\n")
    sys.stdout.write("    *** NOTE *** '-l' and '-m' flags can't be set at the same time.\n")
    sys.stdout.write("-s: SECONDS: '-s seconds' - THIS IS 'TESTTIME' - default is 5 seconds\n")
    sys.stdout.write("    How long (in seconds) you want\n")
    sys.stdout.write("    the device to be tested.\n")
    sys.stdout.write("-f: SELF-TEST: '-f y' or -f n' - '-f y' is default. Allows a system to test\n    itself. Ex: node1 to node1\n")
    
##############################################################
# DESCRIPTION  : GETS THE TEST LIST FROM A FILE.
# PRECONDITION : THE FILE MUST EXIST.
# POSTCONDITION: READS THE TEST DEVICES IN FROM THE FILE AND
#              : PUTS THEM IN THE 'TESTLIST'.
##############################################################
def getList():
    if  not posixpath.isfile(paramdict['testNodes']):
        sys.stderr.write("ERROR: Test list file doesn't exist.\n")
        sys.exit(1)
    input = open(paramdict['testNodes'], 'r')
    tmp = input.readlines()
    tmp = string.joinfields(tmp, "\n")
    tmp = string.split(tmp)
    num = 0
    while num < len(tmp):
        testList.append(tmp[num])
        num = num + 1
    
#################################################################
# DESCRIPTION  : GETS ALL THE COMMAND LINE ARGUEMENTS.
# PRECONDITION : NONE - THERE ARE DEFAULTS IF NO COMMAND LINE
#              : ARGUEMENTS ARE GIVEN.
# POSTCONDITION: SETS THE PARAMDICT VARIABLES SO THE PROGRAM
#              : KNOWS WHAT AND WHAT NOT TO DO.
#################################################################
def getArgs(args):
    paramdict['oFlg'] = FALSE
    paramdict['fFlg'] = TRUE
    paramdict['tFlg'] = FALSE
    paramdict['lFlg'] = FALSE
    paramdict['mFlg'] = FALSE
    paramdict['sFlg'] = FALSE

    paramdict['testNodes'] = defListPath
    paramdict['testtime'] = defTstTime
    
    optlist, args = getopt.getopt(sys.argv[1:], 'hots:l:m:f:')
    if len(optlist) > 4:
        sys.stderr.write("ERROR: To many parameters entered.\n%s -h for help\n" % os.path.basename(sys.argv[0]))
        sys.exit(1)
        
    num = 0
    while num < len(optlist):
        if optlist[num][0] == '-h':
            printHelp()
            sys.exit(0)
        elif optlist[num][0] == "-f":
            if string.lower(optlist[num][1]) == "n":
                paramdict['fFlg'] = FALSE
            elif not string.lower(optlist[num][1]) == "y":
                sys.stderr.write("ERROR: Illegal -f flag option.\nUSE '-h' option for details.\n")
                sys.exit(1)
        elif optlist[num][0] == "-o":
            paramdict['oFlg'] = TRUE
        elif optlist[num][0] == "-t":
            paramdict['tFlg'] = TRUE
        elif optlist[num][0] == "-s":
            paramdict['sFlg'] = TRUE
            paramdict['testtime'] = optlist[num][1]
        elif optlist[num][0] == "-l":
            paramdict['lFlg'] = TRUE
            paramdict['testNodes'] = optlist[num][1]
            getList()
        elif optlist[num][0] == '-m':
            paramdict['mFlg'] = TRUE
            tempList = string.split(optlist[num][1])
            tmpnum = 0
            while tmpnum < len(tempList):
                testList.append(tempList[tmpnum])
                tmpnum = tmpnum + 1
        num = num + 1
    if (paramdict['oFlg'] == TRUE and paramdict['tFlg'] == TRUE):
        sys.stderr.write("ERROR: Can't have -o and -t at the same time.\n%s -h for help\n" % os.path.basename(sys.argv[0]))
        sys.exit(1)
    if (paramdict['lFlg'] == TRUE and paramdict['mFlg'] == TRUE):
        sys.stderr.write("ERROR: Can't have -l and -m at the same time.\n%s -h for help\n" % os.path.basename(sys.argv[0]))
        sys.exit(1)
    if (paramdict['oFlg'] == FALSE and paramdict['tFlg'] == FALSE):
        paramdict['oFkg'] = TRUE
    if (paramdict['lFlg'] == FALSE and paramdict['mFlg'] == FALSE):
        paramdict['lFlg'] = TRUE
        getList()
        
if __name__ == "__main__":

    args = sys.argv
    getArgs(args)
    sys.stdout.write("%s\n" % testList)
    genTstSeq()
    testNodes()
    sys.stdout.write("\nSystem cleanup and looking for hung devices. . .")
    sys.stdout.flush()
    time.sleep(string.atoi(cuTime))
    tstHung()
    sys.stdout.write("\n\n")
    printResults()
