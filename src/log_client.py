#!/usr/bin/env python

"""
This is a simple log client. It sends log messages to the log server  
via port specified in the Log Server dictionary entry in the enstore  
configuration file ( can be specified separately)                     
"""

# system imports
import Queue
import base64
import cPickle
import errno
import fcntl
import os
import pwd
import select
import socket
import string
import sys
import threading
import time

# enstore imports
import Trace
import callback
import e_errors
import enstore_constants
import enstore_functions
import generic_client
import option

# Required for freezing encp:
has_multiprocessing = True
try:
    import multiprocessing
except ImportError:
    has_multiprocessing = False

MY_NAME = enstore_constants.LOG_CLIENT  # "LOG_CLIENT"
MY_SERVER = enstore_constants.LOG_SERVER  # "log_server"
YESTERDAY = "yesterday"
VALID_PERIODS = {"today": 1, YESTERDAY: 2, "week": 7, "month": 30, "all": -1}
RCV_TIMEOUT = 3
RCV_TRIES = 1
RECONNECT_TIMEOUT = 20
MAX_QUEUE_SIZE = 200000

##############################################################################
# AUTHOR        : FERMI-LABS
# DATE          : JUNE 8, 1999
# DESCRIPTION   : THIS FUNCTION TAKES A LINE THAT IS PASSED TO IT BY THE
#               : CALLER, AND USES DICTIONARIES TO TRY SENSIBLE ERROR MESSAGES
# PRECONDITION  : A VALID LINE IN STRING FORMAT
# POSTCONDITION : AN ACCURATE (HOPEFULLY) ERROR MESSAGE
##############################################################################


def genMsgType(msg, ln, severity):
    """generate formatted message for log server
       NB - does not seem to be used anywhere
    args:
        msg(str): message
        ln(int): line number
        severity(int): severity
        returns:
            (str): formatted message 
    """

    #TRUE = 1
    #FALSE = 0

    clientFlg = False  # DETERMINES IF A VALID CLIENT DEFINITION WAS FOUND
    functFlg = False  # FOR FUNCTION DEFINITIONS
    sevFlg = False  # FOR SEVERITY DEFINITIONS
    clientMsg = ''    # CONTAINS THE ACTUAL CLIENT PORTION OF ERROR MESSAGE
    functMsg = ''    # FUNCTION PORTION OF ERROR MESSAGE
    sevMsg = ''    # SEVERITY PORTION OF ERROR MESSAGE
    listNum = 0      # MESSAGES START ON THIS PORTION OF LINE INPUT
    msgStrt = 0     # ANCHOR FOR WHERE MESSAGE STARTS

    tmpLine = string.split(msg)      # 2 LINES CAUSE A GROUP OF CHARACTERS TO
    # BE SPLIT APART AND THEN
    msg = string.joinfields(tmpLine)  # RE-ASSEMBLED LEAVING ONLY 1 SPACE IN
    # BETWEEN EACH GROUP.
    lowLine = string.lower(msg)      # CONVERTS LINE TO ALL LOWER CASE FOR
    # STRING CHECKS.

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
        if clientFlg and functFlg and sevFlg:
            break
        while 1:
            if listNum > msgStrt:  # ONLY DO ELSE THE FIRST TIME THROUGH
                key = string.lower(tmpLine[listNum])
                cKey = key
                fKey = key
                sKey = key
            else:
                if e_errors.ctypedict.has_key(cKey):
                    clientMsg = e_errors.ctypedict[cKey]
                    clientFlg = True
                if e_errors.ftypedict.has_key(fKey):
                    if e_errors.stypedict.has_key(fKey):
                        functMsg = e_errors.ftypedict[fKey]
                        functFlg = True
                        sevMsg = e_errors.stypedict[fKey]
                        sevFlg = True
                    else:
                        functMsg = e_errors.ftypedict[fKey]
                        functFlg = True
                elif e_errors.stypedict.has_key(sKey):
                    sevMsg = e_errors.stypedict[sKey]
                    sevFlg = True

            if not clientFlg:  # clientFlg == False
                if e_errors.ctypedict.has_key(cKey):
                    clientMsg = e_errors.ctypedict[cKey]
                    clientFlg = True
                    listNum = listNum + 1
                    break

            if not clientFlg:  # functFlg == False
                if e_errors.ftypedict.has_key(fKey):
                    functMsg = e_errors.ftypedict[fKey]
                    functFlg = True
                    listNum = listNum + 1
                    break

            if not sevFlg:  # sevFlg == False
                if e_errors.stypedict.has_key(sKey):
                    sevMsg = e_errors.stypedict[sKey]
                    sevFlg = True
                    listNum = listNum + 1
                    break
            listNum = listNum + 1
            break

    # THESE SERIES OF CHECKS ARE IF ANY OF THE PORTIONS OF THE ERROR MESSAGE
    # WEREN'T FOUND. IT TRIES TO COME UP WITH A SANE DEFAULT.
    if sevMsg == functMsg:
        functFlg = False
        functMsg = ""
    if not clientFlg:  # clientFlg == False
        clientMsg = string.upper(ln)
    clientMsg = "_" + clientMsg
    if string.lower(sevMsg) == "suc" and string.lower(severity) != "i":
        sevFlg = False
    if not sevFlg:  # sevFlg == False
        sKey = string.lower(severity)
        sevMsg = e_errors.stypedict[sKey]
    if functFlg:  # functFlg == True
        sevMsg = "_" + sevMsg

    return "%s%s%s%s" % (Trace.MSG_TYPE, functMsg, sevMsg, clientMsg)


class LoggerClient(generic_client.GenericClient):
    """The LoggerClient class is a generic client that is used to send
    messages to the logger server. 
    """

    def __init__(self, csc, name=MY_NAME, server_name=MY_SERVER,
                 server_address=None, flags=0, alarmc=None,
                 rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES):

        # need the following definition so the generic client init does not
        # get another logger client
        flags = flags | enstore_constants.NO_LOG
        generic_client.GenericClient.__init__(self, csc, name, server_address,
                                              flags=flags, alarmc=alarmc,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              server_name=server_name)

        self.log_name = name
        try:
            self.uname = pwd.getpwuid(os.getuid())[0]
        except:
            self.uname = 'unknown'
        self.log_priority = 7
        #self.logger_address = self.get_server_address(servername, rcv_timeout, rcv_tries)
        self.logger_address = self.server_address
        lticket = self.csc.get(server_name, rcv_timeout, rcv_tries)
        self.log_dir = lticket.get("log_file_path", "")
        Trace.set_log_func(self.log_func)

    def log_func(self, time, pid, name, args):
        """method that is called to send message to log_server.
          Intended to be over_ridden using Trace.set_log_func()
          or log_client.LoggerClient.log_func = <function>
          NB - only reset in encp as far as I can tell

          Args:
            time: time of message
            pid: process id of message
            name: name of sender
            args: tuple of arguments

        """

        #Even though this implimentation of log_func() does not use the time
        # parameter, others will.

        __pychecker__ = "unusednames=time"

        severity = args[0]
        msg = args[1]
        #if self.log_name:
        #    ln = self.log_name
        #else:
        #    ln = name
        if severity > e_errors.MISC:
            msg = '%s %s' % (severity, msg)
            severity = e_errors.MISC

        msg = '%.6d %.8s %s %s  %s' % (pid, self.uname,
                                       e_errors.sevdict[severity], name, msg)
        ticket = {'work': 'log_message', 'message': msg}
        Trace.trace(300, "UDP %s" % (ticket,))
        self.u.send_no_wait(ticket, self.logger_address, unique_id=True)


    def set_logpriority(self, priority):
        """Set the log priority for this client.
        Args:
            priority (int):
              - setting log_priority to 0 should turn off all logging.
              - default priority on send is 1 so the default is to log a
                      message
              - the default log_priority to test against is 10 so a priority
                      send with priorty < 10 will normally be logged
              - a brief trace message (1 per file per server should be
                      priority 10
              - file/server trace messages should 10> <20
              - debugging should be > 20
        """
        self.log_priority = priority

    def get_logpriority(self):
        return self.log_priority

    def get_logfile_name(self, rcv_timeout=0, tries=0):
        """ get the current log file name
        """
        x = self.u.send({'work': 'get_logfile_name'}, self.logger_address,
                        rcv_timeout, tries)
        return x

    # get the last n log file names
    def get_logfiles(self, period, rcv_timeout=0, tries=0):
        x = self.u.send({'work': 'get_logfiles', 'period': period},
                        self.logger_address, rcv_timeout, tries)
        return x

    # get the last log file name
    def get_last_logfile_name(self, rcv_timeout=0, tries=0):
        x = self.u.send({'work': 'get_last_logfile_name'}, self.logger_address,
                        rcv_timeout, tries)
        return x


class TCPLoggerClient(LoggerClient):
    """
    Optional TCP/IP communications for log server.

    Under heavy network traffic a lot of UDP log messages may 
    get lost. TCP/IP communications guarantee message delivery. 
    This change implements TCP/IP log server, running in a separate 
    thread and accepting connections on the same as UDP server port. 
    TCP/IP log client takes messages into intermediate queue 
    and sends the out of it. IF log client looses connection 
    is starts dumping messages into a local file and tries to 
    re-establish connection. 
    The TCP/Client is used in mover.py to guaranty that all messages 
    from movers are logged and in migrator.py to log messages which 
    can be very big in size.

    To use, 'use_tcp_log_client': True in the config file for
    particular mover or migrator.
    """

    def __init__(self, csc, name=MY_NAME, server_name=MY_SERVER,
                 server_address=None, flags=0, alarmc=None,
                 rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES, reconnect_timeout=RECONNECT_TIMEOUT):

        LoggerClient.__init__(self, csc, name=MY_NAME, server_name=MY_SERVER,
                              server_address=None, flags=0, alarmc=None,
                              rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES)

        Trace.set_log_func(self.log_func)
        if has_multiprocessing:
            self.message_buffer = multiprocessing.Queue(MAX_QUEUE_SIZE)  # intermediate message storage
        else:
            self.message_buffer = Queue.Queue(MAX_QUEUE_SIZE)
        self.rcv_timeout = rcv_timeout
        self.connected = False
        self.reconnect_timeout = reconnect_timeout
        self.hostname = socket.gethostname()
        try:
            user_name = pwd.getpwuid(os.geteuid())[0]
        except KeyError:
            user_name = '%s' % (os.geteuid(),)
        # if there are problems with connection to log server dump messages locally
        dirpath = os.path.join(enstore_functions.get_enstore_tmp_dir(), user_name)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        self.dump_file = os.path.join(dirpath, name)
        self.dump_here = open(self.dump_file, 'a')

        self.lock = threading.Lock()
        self.message_arrived = threading.Event()
        # Start sender thread
        sender_thread = threading.Thread(target=self.sender)
        try:
            sender_thread.start()
            self.run = True
        except:
            exc, detail, tb = sys.exc_info()
            print "TCPLoggerClient problem", detail
            raise exc, detail

    def log_func(self, time, pid, name, args):
        #Even though this implimentation of log_func() does not use the time
        # parameter, others will.
        __pychecker__ = "unusednames=time"

        severity = args[0]
        msg = args[1]
        #if self.log_name:
        #    ln = self.log_name
        #else:
        #    ln = name
        if severity > e_errors.MISC:
            msg = '%s %s' % (severity, msg)
            severity = e_errors.MISC

        msg = '%.6d %.8s %s %s  %s' % (pid, self.uname,
                                       e_errors.sevdict[severity], name, msg)
        ticket = {'work': 'log_message', 'sender': self.hostname, 'message': msg}
        Trace.trace(301, "TCP %s" % (ticket,))
        try:
            self.message_buffer.put_nowait(ticket)
        except Queue.Full:
            print "Message Queue is full"

    def stop(self):
        self.run = False

    def connect(self):
        server_host_info = socket.getaddrinfo(self.logger_address[0], None)
        server_address_family = server_host_info[0][0]
        self.socket = socket.socket(server_address_family, socket.SOCK_STREAM)
        flags = fcntl.fcntl(self.socket.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(self.socket.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)
        self.socket.bind((socket.gethostname(), 0))
        try:
            self.socket.connect(self.logger_address)
        except socket.error, detail:
            print detail
            if hasattr(errno, 'EISCONN') and detail[0] == errno.EISCONN:
                pass
            #The TCP handshake is in progress.
            elif detail[0] == errno.EINPROGRESS:
                pass
            else:
                print "error connecting to %s (%s)" %\
                    (self.logger_address, detail)
                raise socket.error, detail

        #Check if the socket is open for reading and/or writing.
        r, w, ex = select.select([self.socket], [self.socket], [], self.rcv_timeout)

        if r or w:
            #Get the socket error condition...
            try:
                rtn = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            except:
                exc, detail, tb = sys.exc_info()
                print "RTN< EXC", exc, detail
                return False

        else:
            print "error connecting to %s (%s)" %\
                (self.logger_address, os.strerror(errno.ETIMEDOUT))
            raise errno.ETIMEDOUT

        #...if it is zero then success, otherwise it failed.
        if rtn != 0:
            raise socket.error, (rtn, os.strerror(rtn))
            #raise RuntimeError('error connecting to %s (%s)'%\
            #    (self.logger_address, os.strerror(rtn)))
        # we have a connection
        fcntl.fcntl(self.socket.fileno(), fcntl.F_SETFL, flags)
        self.connected = True
        return True

    def pull_message(self):
        message = None
        try:
            message = self.message_buffer.get(True, 1)
        except Queue.Empty:
            pass
        return message

    def write_to_local_log(self, message):
        tm = time.localtime(time.time())
        msg_to_write = '%.4d-%.2d-%.2d  %.2d:%.2d:%.2d %s %s\n' % (tm[0], tm[1], tm[2], tm[3], tm[4], tm[5], message.get('sender', 'unknown'), message.get('message'))
        self.dump_here.write(msg_to_write)
        self.dump_here.flush()

    def sender(self):
        try:
            self.conn_start = time.time()
            self.connect()
        except socket.error, detail:
            print "will try to reconnect"
        while True:
            if not self.run:
                self.socket.close()
                break
            message = self.pull_message()
            if message:
                if self.connected:
                    try:
                        callback.write_tcp_obj_new(self.socket, message)
                        #self.socket.send(cPickle.dumps(message))
                    except:
                        exc, detail, tb = sys.exc_info()
                        print "error sending: %s %s %s " % (exc, detail, message)
                        self.connected = False
                if not self.connected:
                    self.write_to_local_log(message)
                    dt = time.time() - self.conn_start
                    if dt >= self.reconnect_timeout:
                        # try to reconnect
                        try:
                            self.conn_start = time.time()
                            self.connect()
                        except socket.error, detail:
                            print "will try to reconnect"



def logthis(sev_level=e_errors.INFO, message="HELLO", logname="LOGIT"):
    """stand alone function to send a log message
    Args:
        sev_level: severity level
        message: message to send
        logname: name of the log

    """
    import configuration_client
    # get config port and host
    port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
    host = os.environ.get('ENSTORE_CONFIG_HOST', '')
    # convert port to integer
    if port:
        port = int(port)
    if port and host:
        # if port and host defined create config client
        csc = configuration_client.ConfigurationClient((host, port))
        # create log client  (Sets Trace global so Trace.log() works.)
        LoggerClient(csc, logname, MY_SERVER)
    Trace.init(logname)
    Trace.log(sev_level, message)

# send a message to the logger


def logit(logc, message="HELLO", logname="LOGIT"):
    """Test function to send a log message"""
    # reset our log name
    logc.log_name = logname

    # send the message
    Trace.log(e_errors.INFO, message)

    return {"status": (e_errors.OK, None)}

###############################################################################
# NAME        : FERMI LABS - RICHARD KENNA
# DATE        : JUNE 24, 1999
# DESCRIPTION : THIS FUNCTION TAKES A LINE INPUT AND RETURNS A USABLE
#             : DICTIONARY WITH THE FOLLOWING VALUES. THE COMMANDS ARE:
#             : TIME, SYS_NAME, PID, USR_NAME, SEVERITY, DEV_NAME,
#             : MSG, MSG_DICT AND MSG_TYPE
#             : TO USE: a = log.parse(lineIn)  - IT WILL RETURN THE DICTIONARY
#             : THEN TO SEE DIFFERENT VALUES, TYPE: a['time']
#             : IT WILL RESPOND WITH: '12:02:12' - OR THE TIME IN THE MESSAGE
###############################################################################


def parse(lineIn):
    """parse a log line and return a dictionary
    Args:
        lineIn(str): log line to parse
    Returns:
        msg_dict: dictionary with the following values:
            time(str): time
            host(str): host 
            pid(str): pid
            user(str): user
            severity(str): severity
            server(str): server
            msg(str): message

        """

    tmpLine = string.split(lineIn)
    time = tmpLine[0]
    host = tmpLine[1]
    pid = tmpLine[2]
    user = tmpLine[3]
    severity = tmpLine[4]
    server = tmpLine[5]

    lineDict = {'time': time, 'host': host, 'pid': pid,
                'user': user, 'severity': severity,
                'server': server}

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

    def __init__(self, args=sys.argv, user_mode=1):
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.message = ""
        self.alive_rcv_timeout = RCV_TIMEOUT
        self.alive_retries = RCV_TRIES
        self.get_logfile_name = 0
        self.get_logfiles = ""
        self.get_last_logfile_name = 0
        self.client_name = ""
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.log_options)

    log_options = {
        option.GET_LAST_LOGFILE_NAME: {option.HELP_STRING:
                                       "return the fname of yesturdays log file",
                                       option.DEFAULT_TYPE: option.INTEGER,
                                       option.DEFAULT_VALUE: option.DEFAULT,
                                       option.VALUE_USAGE: option.IGNORED,
                                       option.USER_LEVEL: option.ADMIN,
                                       },
        option.GET_LOGFILE_NAME: {option.HELP_STRING:
                                  "return the name of the current log file",
                                  option.DEFAULT_TYPE: option.INTEGER,
                                  option.DEFAULT_VALUE: option.DEFAULT,
                                  option.VALUE_USAGE: option.IGNORED,
                                  option.USER_LEVEL: option.ADMIN,
                                  },
        option.GET_LOGFILES: {option.HELP_STRING: "return the last 'n' log file "
                              "names (today, week, month, all)",
                              option.VALUE_TYPE: option.STRING,
                              option.VALUE_USAGE: option.REQUIRED,
                              option.VALUE_LABEL: "period",
                              option.USER_LEVEL: option.ADMIN,
                              },
        option.CLIENT_NAME: {option.HELP_STRING: "set log client name",
                             option.VALUE_TYPE: option.STRING,
                             option.VALUE_USAGE: option.REQUIRED,
                             option.VALUE_LABEL: "client_name",
                             option.USER_LEVEL: option.ADMIN,
                             },
        option.MESSAGE: {option.HELP_STRING: "log a message",
                         option.VALUE_TYPE: option.STRING,
                         option.VALUE_USAGE: option.REQUIRED,
                         option.VALUE_LABEL: "message",
                         option.USER_LEVEL: option.ADMIN,
                         },
    }


def do_work(intf):
    # get a log client
    if intf.client_name:
        name = intf.client_name
    else:
        name = MY_NAME
    Trace.init(name)
    logc = LoggerClient((intf.config_host, intf.config_port), name,
                        MY_SERVER)

    ticket = logc.handle_generic_commands(MY_SERVER, intf)
    if ticket:
        pass

    elif intf.get_last_logfile_name:
        ticket = logc.get_last_logfile_name(intf.alive_rcv_timeout,
                                            intf.alive_retries)
        print(ticket['last_logfile_name'])

    elif intf.get_logfile_name:
        ticket = logc.get_logfile_name(intf.alive_rcv_timeout,
                                       intf.alive_retries)
        print(ticket['logfile_name'])

    elif intf.get_logfiles:
        ticket = logc.get_logfiles(intf.get_logfiles, intf.alive_rcv_timeout,
                                   intf.alive_retries)
        print(ticket['logfiles'])

    elif intf.message:
        ticket = logit(logc, intf.message)

    else:
        intf.print_help()
        sys.exit(0)

    del logc.csc.u
    del logc.u  # del now, otherwise get name exception (just for python v1.5??)

    logc.check_ticket(ticket)



if __name__ == "__main__":   # pragma: no cover
    # fill in interface
    intf = LoggerClientInterface(user_mode=0)
    do_work(intf)
