import os
import re
import string
import sys

global errDict     # These globals are to change dictionaries or to 
global errLine     # add to lists from various functions. You have one
global errList     # copy instead of working with multiple copies
global miscDict
global miscLine
global miscList
global msgDict
global msgList
global MAXMSG
errDict = {}
errLine = {}
errList = []
miscDict = {}
miscLine = {}
miscList = []
msgDict = {}
msgList = []
MAXMSG = 5         # YOU ONLY NEED TO CHANGE THIS NUMBER TO CHANGE HOW MANY LINES
                   # ARE TO BE PRINTED OUT I.E. UP TO 10; CHANGE 5 TO 10

def newLog(inFile):
    global errDict
    global errLine
    global errList
    global miscDict
    global miscLine
    global miscList
    global msgDict
    global msgList
    global MAXMSG
    
    t1lines = []
    TRUE = 1
    FALSE = 0
    input = open(inFile, 'r')
    linesIn = input.readlines()
    input.close()
    tmpLines = string.join(linesIn, "")
    
    num = 0   # FILTERS CONTROL CHARACTERS AND GRAPHIC CHARACTERS OUT OF THE TEXT 
    while num < len(tmpLines):
        if not int(ord(tmpLines[num])) & 128 and int(ord(tmpLines[num])) > 31:
            t1lines.append(tmpLines[num])
        num = num + 1
    
    tmpLines = string.joinfields(t1lines, "")
    year = tmpLines[0:4]
    t1lines = string.split(tmpLines, year)
    tmpLines = string.joinfields(t1lines, "\n%s" % year)
    tmpLines = string.split(tmpLines, "\n")
    del tmpLines[0]

    count = 0          # BREAKS THE LINE INTO STATISTICS. THE FIRST PART IS FOR ERRORS
    while count < len(tmpLines):
        errFlg = FALSE
        lowLine = string.lower(tmpLines[count])
        if string.find(lowLine, "negative") >= 0:
            errFlg = TRUE
        elif string.find(lowLine, "error") >= 0:
            errFlg = TRUE
        elif string.find(lowLine, "fail") >= 0:
            errFlg = TRUE
        elif string.find(lowLine, "no clean") >= 0:
            errFlg = TRUE
        elif string.find(lowLine, "no longer") >= 0:
            errFlg = TRUE
        elif string.find(lowLine, "warning") >= 0:
            errFlg = TRUE
        elif string.find(lowLine, " not ") >= 0:
            errFlg = TRUE

        if errFlg == TRUE:
            if string.find(lowLine, "mont") >= 0 or string.find(lowLine, "mount") >= 0:
                updList("MSG", "bad_mount", tmpLines[count])
            else:
                errFlg = FALSE
                if string.find(lowLine, "negative keep") >= 0:
                    updList("MSG", "negative_keep", tmpLines[count])
                elif string.find(lowLine, "negative home") >= 0:
                    updList("MSG", "negative_home", tmpLines[count])
                elif string.find(lowLine, "negative stat") >= 0:
                    updList("MSG", "negative_stat", tmpLines[count])
                elif string.find(lowLine, "keep request") >= 0 and string.find(lowLine, "with failure") >= 0:
                    updList("MSG", "keep_req_failure", tmpLines[count])
                else:
                    tempLine = lowLine[38:]
                    tempLine = string.lstrip(tempLine)
                    tempLine = string.rstrip(tempLine)
                    updList("ERR", tempLine, tmpLines[count])
            updList("MSG", "err_messages", tmpLines[count])
        else:                                                     # BREAKS LINE TO COUNTS OF VARIOUS COMMANDS
            if string.find(lowLine, "mont") >= 0:
                updList("MSG", "good_mount", tmpLines[count])
            elif string.find(lowLine, "mount req") >= 0:
                updList("MSG", "mount_req", tmpLines[count])
            elif string.find(lowLine, "mount") >= 0:
                updList("MSG", "good_mount", tmpLines[count])
            elif string.find(lowLine, "keep") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "keep", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "keep_req", tmpLines[count])
                else:
                    updList("MSG", "keep", tmpLines[count])
            elif string.find(lowLine, "view") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "view", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "view_req", tmpLines[count])
                else:
                    updList("MSG", "view", tmpLines[count])
            elif string.find(lowLine, "robhome") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "robot_home", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "robot_home_req", tmpLines[count])
                else:
                    updList("MSG", "robot_home", tmpLines[count])
            elif string.find(lowLine, "robot start") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "robot_start", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "robot_start_req", tmpLines[count])
                else:
                    updList("MSG", "robot_start", tmpLines[count])
            elif string.find(lowLine, "robot status") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "robot_status", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "robot_status_req", tmpLines[count])
                else:
                    updList("MSG", "robot_status", tmpLines[count])
            elif string.find(lowLine, "drive status") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "drive_status", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "drive_status_req", tmpLines[count])
                else:
                    updList("MSG", "drive_status", tmpLines[count])
            elif string.find(lowLine, "move") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "move", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "move_req", tmpLines[count])
                else:
                    updList("MSG", "move", tmpLines[count])
            elif string.find(lowLine, "insert") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "insert", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "insert_req", tmpLines[count])
                else:
                    updList("MSG", "insert", tmpLines[count])
            elif string.find(lowLine, "eject") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "eject", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "eject_req", tmpLines[count])
                else:
                    updList("MSG", "eject", tmpLines[count])
            elif string.find(lowLine, "list") >= 0:
                if string.find(lowLine, "completed") >= 0 or string.find(lowLine, "success") >= 0:
                    updList("MSG", "list", tmpLines[count])
                elif string.find(lowLine, "req") >= 0:
                    updList("MSG", "list_req", tmpLines[count])
                else:
                    updList("MSG", "list", tmpLines[count])
            elif string.find(lowLine, "home") >= 0:
                if string.find(lowLine, "positive") >= 0:
                    updList("MSG", "positive_home", tmpLines[count])
                else:
                    updList("MSG", "home", tmpLines[count])
            elif string.find(lowLine, "robot") >= 0:
                if string.find(lowLine, "status:") >= 0:
                    updList("MSG", "robot_status", tmpLines[count])
                else:
                    updList("MSG", "robot_unknown", tmpLines[count])
            elif string.find(lowLine, "invt") >= 0:
                if string.find(lowLine, "positive") >= 0:
                    updList("MSG", "invt_positive", tmpLines[count])
                else:
                    updList("MSG", "invt", tmpLines[count])
            elif string.find(lowLine, " stat ") >= 0 or string.find(lowLine, "status:") >= 0:
                updList("MSG", "stat", tmpLines[count])
            elif string.find(lowLine, "information") >= 0:
                updList("MSG", "information", tmpLines[count])
            else:                                                # USES REG EXP SEARCHES SINCE THERE CAN BE MANY
                miscFlg = TRUE                                   # CHARACTERS BETWEEN SEARCH STRINGS. THIS IS FOR MISC
                a = re.match("rqm.*cary", lowLine[38:])          # MESSAGES ALSO - HENCE: MISCFLG VARIABLE
                if type(a) != type(None):
                    tstStr = a.group(0)
                    miscFlg = FALSE
                    updList("MSG", "rqm_cary", tmpLines[count])
                a = re.match("rqm.*s0000", lowLine[38:])
                if type(a) != type(None):
                    tstStr = a.group(0)
                    miscFlg = FALSE
                    updList("MSG", "rqm_s0000", tmpLines[count])
                a = re.match("krn.*stat", lowLine[38:])
                if type(a) != type(None):
                    tstStr = a.group(0)
                    miscFlg = FALSE
                    updList("MSG", "krn_stat", tmpLines[count])
                a = re.match("krn.*look", lowLine[38:])
                if type(a) != type(None):
                    tstStr = a.group(0)
                    miscFlg = FALSE
                    updList("MSG", "krn_look", tmpLines[count])
                a = re.match("the .", lowLine[38:])
                if type(a) != type(None):
                    tstStr = a.group(0)
                    miscFlg = FALSE
                    if len(lowLine[38:]) > 8:
                        miscFlg = TRUE
                    else:
                        updList("MSG", tstStr, tmpLines[count])
                a = re.match("negative keep", lowLine[38:])
                if type(a) != type(None):
                    tstStr = a.group(0)
                    miscFlg = FALSE
                    updList("MSG", tstStr, tmpLines[count])
                a = re.match("rqm.*qhome", lowLine[38:])
                if type(a) != type(None):
                    tstSTr = a.group(0)
                    miscFlg = FALSE
                    updList("MSG", "rqm_qhome", tmpLines[count])
                a = re.match("rqm.*qswit", lowLine[38:])
                if type(a) != type(None):
                    tstStr = a.group(0)
                    miscFlg = FALSE
                    updList("MSG", "rqm_qswit", tmpLines[count])
                if miscFlg == TRUE:
                    tempLine = lowLine[38:]
                    tempLine = string.lstrip(tempLine)
                    tempLine = string.rstrip(tempLine)
                    updList("MISC", tempLine, tmpLines[count])
                    updList("MSG", "misc", tmpLines[count])
                    
        count = count + 1

# THIS FUNCTION BUILDS ALLTHE DICTIONARIES AND LISTS. IT ALSO KEEPS TRACK
# OF HOW MANY TIMES EACH LINE OCCURRED
def updList(listType, key, line):
    global errDict
    global errLine
    global errList
    global miscDict
    global miscLine
    global miscList
    global msgDict
    global msgList
    global MAXMSG
    
    if listType == "ERR":
        if errDict.has_key(key):
            errDict[key] = errDict[key] + 1
            if errDict[key] <= MAXMSG:
                errLine[key].append(line)
        else:
            errDict[key] = 1
            errList.append(key)
            errLine[key] = []
            errLine[key].append(line)
    elif listType == "MSG":
        if msgDict.has_key(key):
            msgDict[key] = msgDict[key] + 1
        else:
            msgDict[key] = 1
            msgList.append(key)
    elif listType == "MISC":
        if miscDict.has_key(key):
            miscDict[key] = miscDict[key] + 1
            if miscDict[key] <= MAXMSG:
                miscLine[key].append(line)
        else:
            miscDict[key] = 1
            miscList.append(key)
            miscLine[key] = []
            miscLine[key].append(line)

# THIS FUNCTION PRINTS OUT THE INFORMATION IN THE DICTIONARIES AND LISTS TO
# A REPORT FILE NAME 'FILE_NAME.RPT'. THIS IS WHERE FILE_NAME IS THE NAME OF THE
# LOG FILE THAT WAS ENTERED IN USING ARGV FUNCTION.
def printMsgs(outFile):
    global errDict
    global errLine
    global errList
    global miscDict
    global miscLine
    global miscList
    global msgDict
    global msgList
    global MAXMSG
    
    miscList.sort()
    msgList.sort()
    errList.sort()
    output = open(outFile, 'w')
    
    output.write("ERROR MESSAGES: \n\n")
    listCounter = 0
    while listCounter < len(errList):
        output.write("There are %4.0d error messages of this type: " % errDict[errList[listCounter]])
        output.write("%s\n" % errList[listCounter])
        if errDict[errList[listCounter]] <= MAXMSG:
            lineCounter = 0
            while lineCounter < errDict[errList[listCounter]]:
                output.write("%s\n" % errLine[errList[listCounter]][lineCounter])
                lineCounter = lineCounter + 1
        output.write("\n")
        listCounter = listCounter + 1

    output.write("\nMISC. MESSAGES:\n")
    listCounter = 0
    while listCounter < len(miscList):
        output.write("There are %4.0d types of this message: " % miscDict[miscList[listCounter]])
        output.write("%s\n" % miscList[listCounter])
        if miscDict[miscList[listCounter]] <= MAXMSG:
            lineCounter = 0
            while lineCounter < miscDict[miscList[listCounter]]:
                output.write("%s\n" % miscLine[miscList[listCounter]][lineCounter])
                lineCounter = lineCounter + 1
        output.write("\n")
        listCounter = listCounter + 1

    output.write("\nHere are the stats for the most common messages:\n\n")
    listCounter = 0
    while listCounter < len(msgList):
        output.write("%25s = %4.0d\n" % (msgList[listCounter], msgDict[msgList[listCounter]]))
        listCounter = listCounter + 1
    output.close()
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("ERROR: Usage - %s logName\n" % sys.argv[0])
        sys.exit(1)
    inFile = sys.argv[1]
    outFile = "%s.rpt" % inFile
    newLog(inFile)
    printMsgs(outFile)


