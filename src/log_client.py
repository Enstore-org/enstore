###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# Log client.                                                           #
# This is a simple log client. It sends log messages to the log server  #
# via port specified in the Log Server dictionary entry in the enstore  #
# configuration file ( can be specified separately)                     #
#########################################################################

# system imports
import sys
import os
import pwd
import string
import base64
import cPickle
import cStringIO		# to make freeze happy
import copy_reg			# to make freeze happy

#enstore imports
import generic_client
import udp_client
import Trace
import e_errors

MY_NAME = "LOG_CLIENT"
MY_SERVER = "log_server"
VALID_PERIODS = {"today":1, "week":7, "month":30, "all":-1}



class LoggerLock:
    def __init__(self):
	self.locked = 0
    def unlock(self):
	self.locked = 0
    def test_and_set(self):
	s = self.locked
	self.locked=1
	return s



#############################################################################################
# AUTHOR        : FERMI-LABS
# DATE          : JUNE 8, 1999
# DESCRIPTION   : THIS FUNCTION TAKES A LINE THAT IS PASSED TO IT BY THE CALLER, AND
#               : USES DICTIONARIES TO TRY SENSIBLE ERROR MESSAGES
# PRECONDITION  : A VALID LINE IN STRING FORMAT
# POSTCONDITION : AN ACCURATE (HOPEFULLY) ERROR MESSAGE
#############################################################################################
def genMsgType(msg, ln, severity):
    TRUE = 1
    FALSE = 0

    clientFlg = FALSE # DETERMINES IF A VALID CLIENT DEFINITION WAS FOUND
    functFlg  = FALSE # FOR FUNCTION DEFINITIONS
    sevFlg    = FALSE # FOR SEVERITY DEFINITIONS
    clientMsg = ''    # CONTAINS THE ACTUAL CLIENT PORTION OF ERROR MESSAGE
    functMsg  = ''    # FUNCTION PORTION OF ERROR MESSAGE
    sevMsg    = ''    # SEVERITY PORTION OF ERROR MESSAGE
    listNum  = 0      # MESSAGES START ON THIS PORTION OF LINE INPUT
    msgStrt   = 0     # ANCHOR FOR WHERE MESSAGE STARTS

    tmpLine = string.split(msg)      # 2 LINES CAUSE A GROUP OF CHARACTERS TO BE SPLIT APART AND THEN
    msg = string.joinfields(tmpLine) # RE-ASSEMBLED LEAVING ONLY 1 SPACE IN BETWEEN EACH GROUP
    lowLine = string.lower(msg)      # CONVERTS LINE TO ALL LOWER CASE FOR STRING CHECKS

    if string.find(lowLine, "file clerk") >= 0:
        cKey = "fc"
    elif string.find(lowLine, "file_clerk ") >= 0:
        cKey = "fc"
    elif string.find(lowLine, "alarm server") >= 0:
        cKey = "alarm_srv"
    elif string.find(lowLine, "alarm_server") >= 0:
        cKey = "alarm_srv"
    elif string.find(lowLine, "volume clerk") >= 0:
        cKey = "vc"
    elif string.find(lowLine, "volume_clerk ") >= 0:
        cKey = "vc"
    elif string.find(lowLine, "media changer") >= 0:
        cKey = "mc"
    elif string.find(lowLine, "media_changer ") >= 0:
        cKey = "mc"
    elif string.find(lowLine, "library manager") >= 0:
        cKey = "lm"
    elif string.find(lowLine, "library_manager ") >= 0:
        cKey = "lm"
    elif string.find(lowLine, "config server") >= 0:
        cKey = "cs"
    elif string.find(lowLine, "configuration server") >= 0:
        cKey = "cs"
    elif string.find(lowLine, "root error") >= 0:
        cKey = "re"
    elif string.find(lowLine, "root_error ") >= 0:
        cKey = "re"
    elif string.find(lowLine, "backup") >= 0:
        cKey = "backup"
    elif string.find(lowLine, " mover ") >= 0:
        cKey = "mvr"
    elif string.find(lowLine, "encp") >= 0:
        cKey = "encp"
    else:
        cKey = string.lower(tmpLine[msgStrt])
         
    if string.find(lowLine, "unmount") >= 0:
        fKey = "unmount"
    elif string.find(lowLine, "write_to_hsm") >= 0:
        fKey = "write_aml2"
    elif string.find(lowLine, "dismount") >= 0:
        fKey = "dismount"
    elif string.find(lowLine, "unload") >= 0:
        fKey = "dismount"
    elif string.find(lowLine, "find_mover") >= 0:
        fKey = "mvr_find"
    elif string.find(lowLine, "exception") >= 0:
        fKey = "exception"
    elif string.find(lowLine, "badmount") >= 0:
        fKey = "mount"
    elif string.find(lowLine, "getmoverlist") >= 0:
        fKey = "get_mv"
    elif string.find(lowLine, "getwork") >= 0:
        fKey = "get_wrk"
    elif string.find(lowLine, "get_work") >= 0:
        fKey = "get_wrk"
    elif string.find(lowLine, "get_suspect_vol") >= 0:
        fKey = "gsv"
    elif string.find(lowLine, "get_user_socket") >= 0:
        fKey = "gus"
    elif string.find(lowLine, "busy_vols") >= 0:
        fKey = "busy_vols"
    elif string.find(lowLine, "open_file_write") >= 0:
        fKey = "write_file"
    elif string.find(lowLine, "wrapper.write") >= 0:
        fKey = "write_wrapper"
    elif string.find(lowLine, "read ") >= 0:
        fKey = "read"
    elif string.find(lowLine, "reading") >= 0:
        fKey = "read"
    elif string.find(lowLine, "write ") >= 0:
        fKey = "write"
    elif string.find(lowLine, "writing") >= 0:
        fKey = "write"
    elif string.find(lowLine, "file database") >= 0:
        fKey = "filedb"
    elif string.find(lowLine, "volume database") >= 0:
        fKey = "voldb"
    elif string.find(lowLine, "added to mover list") >= 0:
        fKey = "add_list"
    elif string.find(lowLine, "update_mover_list") >= 0:
        fKey = "update_mover_list"
    elif string.find(lowLine, "get_work") >= 0:
        fKey = "get_work"
    elif string.find(lowLine, "next_work") >= 0:
        fKey = "next_work"
    elif string.find(lowLine, "insertvol") >= 0:
        fKey = "insert_vol"
    elif string.find(lowLine, "insert") >= 0:
        fKey = "insert"
    elif string.find(lowLine, "serverdied") >= 0:
        fKey = "server_died"
    elif string.find(lowLine, "cantrestart") >= 0:
        fKey = "cant_restart"
    elif string.find(lowLine, "no such vol") >= 0:
        fKey = "vol_err"
    elif string.find(lowLine, "unbind vol") >= 0:
        fKey = "unbind_vol"
    elif string.find(lowLine, "unbind") >= 0:
        fKey = "unbind"
    elif string.find(lowLine, " vol") >= 0:
        fKey = "vol"
    elif string.find(lowLine, "load") >= 0:
        fKey = "mount"
    elif string.find(lowLine, "load") >= 0:
        fKey = "mount"
    elif string.find(lowLine, "quit") >= 0:
        fKey = "quit"
    elif string.find(lowLine, "file") >= 0:
        fKey = "file "
    else:
        fKey = string.lower(tmpLine[msgStrt])
    
    if string.find(lowLine, "tape stall") >= 0:
        sKey = "ts"
    elif string.find(lowLine, "tape_stall") >= 0:
        sKey = "ts"
    elif string.find(lowLine, "getmoverlist") >= 0:
        sKey = "get_mv"
    elif string.find(lowLine, "getwork") >= 0:
        sKey = "get_wrk"
    elif string.find(lowLine, "get_work") >= 0:
        sKey = "get_wrk"
    elif string.find(lowLine, "get_suspect_vol") >= 0:
        sKey = "gsv"
    elif string.find(lowLine, "get_user_socket") >= 0:
        sKey = "gus"
    elif string.find(lowLine, "busy_vols") >= 0:
        sKey = "busy_vols"
    elif string.find(lowLine, "find_mover") >= 0:
        sKey = "mvr_find"
    elif string.find(lowLine, "open_file_write") >= 0:
        sKey = "write_file"
    elif string.find(lowLine, "wrapper.write") >= 0:
        sKey = "write_wrapper"
    elif string.find(lowLine, "completed precautionary") >= 0:
        sKey = "check_suc"
    elif string.find(lowLine, "performing precautionary") >= 0:
        sKey = "check"
    elif string.find(lowLine, "update_mover_list") >= 0:
        sKey = "update_mover_list"
    elif string.find(lowLine, "get_work") >= 0:
        sKey = "get_work"
    elif string.find(lowLine, "next_work") >= 0:
        sKey = "next_work"
    elif string.find(lowLine, "bad") >= 0:
        sKey = "bad"
    elif string.find(lowLine, "done") >= 0:
        sKey = "done"
    elif string.find(lowLine, "hurrah") >= 0:
        sKey = "hurrah"
    elif string.find(lowLine, "start{") >= 0:
        sKey = "start"
    elif string.find(lowLine, "(re)") >= 0:
        sKey = "restart"
    elif string.find(lowLine, "restart") >= 0:
        sKey = "restart"
    elif string.find(lowLine, "start") >= 0:
        sKey = "start"
    elif string.find(lowLine, "stop") >= 0:
        sKey = "stop"
    elif string.find(lowLine, "full") >= 0:
        sKey = "full "
    else:
        sKey = string.lower(tmpLine[msgStrt])

    while listNum < len(tmpLine):
        if clientFlg == TRUE and functFlg == TRUE and sevFlg == TRUE:
            break
        while 1:
            if listNum > msgStrt: # ONLY DO ELSE THE FIRST TIME THROUGH
                key = string.lower(tmpLine[listNum])
                cKey = key
                fKey = key
                sKey = key
            else:
                if e_errors.ctypedict.has_key(cKey):
                    clientMsg = e_errors.ctypedict[cKey]
                    clientFlg = TRUE
                if e_errors.ftypedict.has_key(fKey):
                    if e_errors.stypedict.has_key(fKey):
                        functMsg = e_errors.ftypedict[fKey]
                        functFlg = TRUE
                        sevMsg = e_errors.stypedict[fKey]
                        sevFlg = TRUE
                    else:
                        functMsg = e_errors.ftypedict[fKey]
                        functFlg = TRUE
                elif e_errors.stypedict.has_key(sKey):
                    sevMsg = e_errors.stypedict[sKey]
                    sevFlg = TRUE
                
            if clientFlg == FALSE:
                if e_errors.ctypedict.has_key(cKey):
                    clientMsg = e_errors.ctypedict[cKey]
                    clientFlg = TRUE
                    listNum = listNum + 1
                    break
                
            if functFlg == FALSE:
                if e_errors.ftypedict.has_key(fKey):
                    functMsg = e_errors.ftypedict[fKey]
                    functFlg = TRUE
                    listNum = listNum + 1
                    break
                
            if sevFlg == FALSE:
                if e_errors.stypedict.has_key(sKey):
                    sevMsg = e_errors.stypedict[sKey]
                    sevFlg = TRUE
                    listNum = listNum + 1
                    break
            listNum = listNum + 1
            break

    # THESE SERIES OF CHECKS ARE IF ANY OF THE PORTIONS OF THE ERROR MESSAGE
    # WEREN'T FOUND. IT TRIES TO COME UP WITH A SANE DEFAULT.
    if sevMsg == functMsg:
        functFlg = FALSE
        functMsg = ""
    if clientFlg == FALSE:
        clientMsg = string.upper(ln)
    clientMsg = "_" + clientMsg
    if string.lower(sevMsg) == "suc" and  string.lower(severity) != "i":
        sevFlg = FALSE
    if sevFlg == FALSE:
        sKey = string.lower(severity)
        sevMsg = e_errors.stypedict[sKey]
    if functFlg == TRUE:
        sevMsg = "_" + sevMsg
        
    return  "%s%s%s%s" % (Trace.MSG_TYPE, functMsg, sevMsg, clientMsg)
        
class LoggerClient(generic_client.GenericClient):

    def __init__(self,
                 csc = 0,                    # get our own configuration client
                 i_am_a = MY_NAME,           # Abbreviated client instance name
                                             # try to make it capital letters
                                             # not more than 8 characters long
                 servername = MY_SERVER):    # log server name
        # need the following definition so the generic client init does not
        # get another logger client
        self.is_logger = 1
        generic_client.GenericClient.__init__(self, csc, i_am_a)
        self.log_name = i_am_a
        try:
            self.uname = pwd.getpwuid(os.getuid())[0]
        except:
            self.uname = 'unknown'
        self.log_priority = 7
	lticket = self.csc.get( servername )
	self.logger_address = (lticket['hostip'], lticket['port'])
        self.log_dir = lticket.get("log_file_path", "")
        self.u = udp_client.UDPClient()
	Trace.set_log_func( self.log_func )
	self.lock = LoggerLock() 

    def log_func( self, time, pid, name, args ):
	#prevent log func from calling itself recursively
	if self.lock.test_and_set():
            return

	severity = args[0]
	msg      = args[1]
        if self.log_name:
            ln = self.log_name
        else:
            ln = name
	if severity > e_errors.MISC: severity = e_errors.MISC

        if string.find(msg, Trace.MSG_TYPE) < 0:
	    try:
		msg_type = genMsgType(msg, ln, e_errors.sevdict[severity])
	    except NameError:
		msg_type = "%sNAME_ERROR"%(Trace.MSG_TYPE,)
            msg = "%s %s" % (msg, msg_type)

	msg = '%.6d %.8s %s %s  %s' % (pid, self.uname,
				       e_errors.sevdict[severity],name,msg)
	ticket = {'work':'log_message', 'message':msg}
	self.u.send_no_wait( ticket, self.logger_address )
	return 	self.lock.unlock()

    def send( self, severity, priority, format, *args ):
	if args != (): format = format%args
	Trace.log( severity, format )
	return {"status" : (e_errors.OK, None)}

#
# priorty allows turning logging on and off in a server.
#  Coventions - setting log_priority to 0 should turn off all logging.
#             - default priority on send is 1 so the default is to log a message
#             - the default log_priority to test against is 10 so a priority
#                     send with priorty < 10 will normally be logged
#             - a brief trace message (1 per file per server should be priority 10
#             - file/server trace messages should 10> <20
#             - debugging should be > 20
    def set_logpriority(self, priority):
        self.log_priority = priority

    def get_logpriority(self):
        return self.log_priority

    # get the current log file name
    def get_logfile_name(self, rcv_timeout=0, tries=0):
        x = self.u.send( {'work':'get_logfile_name'}, self.logger_address,
			 rcv_timeout, tries )
        return x

    # get the last n log file names
    def get_logfiles(self, period, rcv_timeout=0, tries=0):
	x = self.u.send( {'work':'get_logfiles', 'period':period}, 
			 self.logger_address, rcv_timeout, tries )
        return x

    # get the last log file name
    def get_last_logfile_name(self, rcv_timeout=0, tries=0):
        x = self.u.send( {'work':'get_last_logfile_name'}, self.logger_address,
	                 rcv_timeout, tries )
        return x


# stand alone function to send a log message
def logthis(sev_level=e_errors.INFO, message="HELLO", logname="LOGIT"):
    import configuration_client
    # get config port and host
    port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
    host = os.environ.get('ENSTORE_CONFIG_HOST', '')
    # convert port to integer
    if port: port = string.atoi(port)
    if port and host:
        # if port and host defined create config client
        csc = configuration_client.ConfigurationClient((host,port))
        # create log client
        logc = LoggerClient(csc, logname, MY_SERVER)
    Trace.log(sev_level, message)
    
# send a message to the logger
def logit(logc, message="HELLO", logname="LOGIT"):
    # reset our log name
    logc.log_name = logname

    # send the message
    Trace.log(e_errors.INFO, message)

    return {"status" : (e_errors.OK, None)}

#################################################################################
# NAME        : FERMI LABS - RICHARD KENNA
# DATE        : JUNE 24, 1999
# DESCRIPTION : THIS FUNCTION TAKES A LINE INPUT AND RETURNS A USABLE DICTIONARY
#             : WITH THE FOLLOWING VALUES. THE COMMANDS ARE:
#             : TIME, SYS_NAME, PID, USR_NAME, SEVERITY, DEV_NAME,
#             : MSG, MSG_DICT AND MSG_TYPE
#             : TO USE: a = log.parse(lineIn)  - IT WILL RETURN THE DICTIONARY
#             : THEN TO SEE DIFFERENT VALUES, TYPE: a['time']
#             : IT WILL RESPOND WITH: '12:02:12' - OR THE TIME IN THE MESSAGE
#################################################################################
def parse(lineIn):

    tmpLine = string.split(lineIn)
    time = tmpLine[0]
    host = tmpLine[1]
    pid = tmpLine[2]
    user = tmpLine[3]
    severity = tmpLine[4]
    server = tmpLine[5]

    lineDict = { 'time' : time, 'host' : host, 'pid' : pid,
                 'user' : user, 'severity' : severity,
                 'server' : server }

    mNum = string.find(lineIn, server) + len(server) + 1
    dNum = string.find(lineIn, "MSG_DICT:")
    tNum = string.find(lineIn, Trace.MSG_TYPE)

    if tNum < 0:
        tNum = len(lineIn)
    else:
        msg_type = []
        num = tNum
        while num < len(lineIn):
            msg_type.append(lineIn[num])
            num = num + 1
        msg_type = string.joinfields(msg_type, "")
        msg_type = string.split(msg_type, "=")
        msg_type = msg_type[1]
        lineDict['msg_type'] = msg_type
    if dNum < 0:
        dNum = tNum;
    else:
        msg_dict = []
        num = dNum
        while num < tNum:
            msg_dict.append(lineIn[num])
            num = num + 1
        msg_dict = string.joinfields(msg_dict, "")
        msg_dict = string.split(msg_dict, ":")
        msg_dict = msg_dict[1]
        msg_dict = cPickle.loads(base64.decodestring(msg_dict))
        lineDict['msg_dict'] = msg_dict
    if mNum < dNum:
        msg = []
        num = mNum
        while num < dNum:
            msg.append(lineIn[num])
            num = num + 1
        msg = string.joinfields(msg, "")
        lineDict['msg'] = msg

    return lineDict

class LoggerClientInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
        self.do_parse = flag
        self.restricted_opts = opts
        self.message = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.get_logfile_name = 0
	self.get_logfiles = ""
	self.get_last_logfile_name = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options()+\
                   ["message=", "get_logfile_name", "get_last_logfile_name",
		    "get_logfiles="]


    """ 
    This function takes two arguments:
       severity - see severity codes above
       msg      - any string
    Example:
        Trace.log( ERROR, 'Error: errno=%d, and its interpretation is: %s'%\
	(err,os.strerror(err)) )
    """


def do_work(intf):
    # get a log client
    logc = LoggerClient((intf.config_host, intf.config_port), MY_NAME,
                        MY_SERVER)

    if intf.alive:
        ticket = logc.alive(MY_SERVER, intf.alive_rcv_timeout,
                            intf.alive_retries)

    elif intf.get_last_logfile_name:
        ticket = logc.get_last_logfile_name(intf.alive_rcv_timeout,\
	                                    intf.alive_retries)
	print(ticket['last_logfile_name'])

    elif intf.get_logfile_name:
        ticket = logc.get_logfile_name(intf.alive_rcv_timeout,\
	                               intf.alive_retries)
	print(ticket['logfile_name'])

    elif intf.get_logfiles:
        ticket = logc.get_logfiles(intf.get_logfiles, intf.alive_rcv_timeout,\
				   intf.alive_retries)
	print(ticket['logfiles'])

    elif intf.message:
        ticket = logit(logc, intf.message)

    else:
	intf.print_help()
        sys.exit(0)

    del logc.csc.u
    del logc.u		# del now, otherwise get name exception (just for python v1.5???)

    logc.check_ticket(ticket)

if __name__ == "__main__" :
    import sys
    Trace.init(MY_NAME)
    Trace.trace(6,"logc called with args "+repr(sys.argv))

    # fill in interface
    intf = LoggerClientInterface()

    do_work(intf)
