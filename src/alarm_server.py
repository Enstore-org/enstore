#
# system import
import sys
import socket
import os

# enstore imports
import log_client
import configuration_client
import dispatching_worker
import interface
import enstore_status
import generic_server
import Trace
import alarm
import e_errors

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

DEFAULT_FILE_NAME = "/enstore_alarms.txt"
DEFAULT_PATROL_FILE_NAME = "/enstore_patrol.txt"
DEFAULT_SUSP_VOLS_THRESH = 3

SERVER = "server"
SUS_VOLS = "suspect_volumes"
LM = "library_manager"
ALARM = "alarm"

SEVERITY = "severity"
ROOT_ERROR = "root_error"
PID = "pid"
UID = "uid"
SOURCE = "source"

class AlarmServerMethods(dispatching_worker.DispatchingWorker):

    # pull out alarm info from the ticket and raise the alarm
    def post_alarm(self, ticket):
        if ticket.has_key(SEVERITY):
            severity = ticket[SEVERITY]
            # remove this entry from the dictionary, so if won't be included
            # as part of alarm_info
            del ticket[SEVERITY]
        else:
            severity = e_errors.DEFAULT_SEVERITY
        if ticket.has_key(ROOT_ERROR):
            root_error = ticket[ROOT_ERROR]
            # remove this entry from the dictionary, so if won't be included
            # as part of alarm_info
            del ticket[ROOT_ERROR]
        else:
            root_error = e_errors.DEFAULT_ROOT_ERROR
        if ticket.has_key(PID):
            pid = ticket[PID]
            # remove this entry from the dictionary
            del ticket[PID]
        else:
            pid = alarm.DEFAULT_PID
        if ticket.has_key(UID):
            uid = ticket[UID]
            # remove this entry from the dictionary
            del ticket[UID]
        else:
            uid = alarm.DEFAULT_UID
        if ticket.has_key(SOURCE):
            source = ticket[SOURCE]
            # remove this entry from the dictionary
            del ticket[SOURCE]
        else:
            source = alarm.DEFAULT_SOURCE
        # remove this entry from the dictionary, so it won't be included
        # as part of alarm_info
        if ticket.has_key("work"):
            del ticket["work"]

        theAlarm = self.alarm(severity, root_error, pid, uid, source, ticket)

        # send the reply to the client
        ret_ticket = { 'status' : (e_errors.OK, None),
                       ALARM    : repr(theAlarm.get_id()) }
        self.send_reply(ret_ticket)

    # raise the alarm
    def alarm(self, severity=e_errors.DEFAULT_SEVERITY,
              root_error=e_errors.DEFAULT_ROOT_ERROR,
              pid=alarm.DEFAULT_PID, uid=alarm.DEFAULT_UID,
              source=alarm.DEFAULT_SOURCE, alarm_info={}):
        # find out where the alarm came from
        try:
            host = socket.gethostbyaddr(self.reply_address[0])[0]
        except:
            host = str(sys.exc_info()[1])
        # we should only get a new alarm if this is not the same alarm as
        # one we already have
        theAlarm = self.find_alarm(host, severity, root_error, source,
                                   alarm_info)
        if not theAlarm:
            # get a new alarm
            theAlarm = alarm.Alarm(host, severity, root_error, pid, uid,
                                   source, alarm_info)
            # save it in memory for us now
            self.alarms[theAlarm.get_id()] = theAlarm
            # write it to the persistent file
            self.write_alarm_file(theAlarm)
            # write it out to the patrol file
            self.write_patrol_file()
        return theAlarm

    def find_alarm(self, host, severity, root_error, source, alarm_info):
        # find the alarm that matches the above information
        ids = self.alarms.keys()
        for id in ids:
            if self.alarms[id].compare(host, severity, root_error, source, \
                                       alarm_info) == alarm.MATCH:
                break
        else:
            return
        return self.alarms[id]

    def resolve(self, id):
        # an alarm is being resolved, we must do the following -
        #      remove it from our alarm dictionary
        #      rewrite the entire enstore_alarm file
        #      rewrite the enstore patrol file
        #      log this fact
        if self.alarms.has_key(id):
            del self.alarms[id]
            self.write_alarm_file()
            self.write_patrol_file()
            #Trace.log(e_errors.INFO,
            #          "Alarm with id = "+repr(id)+" has been resolved")
            return (e_errors.OK, None)
        else:
            # don't know anything about this alarm
            return (e_errors.NOALARM, None)

    def resolve_alarm(self, ticket):
        # get the unique identifier for this alarm
        id = ticket.get(ALARM, 0)
        status = self.resolve(id)

        # send the reply to the client
        ticket['status'] = status
        self.send_reply(ticket)
        
    def write_alarm_file(self, alarm=None):
        if alarm:
            self.alarm_file.open()
            self.alarm_file.write(alarm)
        else:
            self.alarm_file.open('w')
            self.write_file(self.alarm_file)
            
        self.alarm_file.close()

    def write_patrol_file(self):
        if not self.alarms == {}:
            self.patrol_file.open()
            self.write_file(self.patrol_file)
            self.patrol_file.close()
        else:
            # there are no alarms raised.  if the patrol file exists, we
            # need to delete it
            self.patrol_file.remove()

    def write_file(self, file):
        # write all of the alarms to the specified file
        if self.alarms:
            keys = self.alarms.keys()
            keys.sort()
            for key in keys:
                file.write(self.alarms[key])

    def get_log_path(self):
        log = self.csc.get("logserver")
        if log.has_key("log_file_path"):
            return log["log_file_path"]
        else:
            return "."
        
    # read the persistent alarm file if it exists.  this reads in all the
    # alarms that have not been resolved.
    def get_alarm_file(self):
        # the alarm file lives in the same directory as the log file
        self.alarm_file = enstore_status.EnAlarmFile(self.get_log_path()+ \
                                                     DEFAULT_FILE_NAME)
        self.alarm_file.open('r')
        self.alarms = self.alarm_file.read()
        self.alarm_file.close()

    # get the location of the file we will create for patrol and write any
    # alarms to it
    def get_patrol_file(self):
        # the patrol file lives in the same directory as the log file
        self.patrol_file = enstore_status.EnPatrolFile(self.get_log_path()+ \
                                                      DEFAULT_PATROL_FILE_NAME)
        self.write_patrol_file()

    # return the name of the patrol file
    def get_patrol_filename(self, ticket):
        ticket['patrol_file'] = self.patrol_file.real_file_name
        ticket['status'] = (e_errors.OK, None)
        self.send_reply(ticket)

class AlarmServer(AlarmServerMethods, generic_server.GenericServer):

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
        self.alarms = {}
        self.info_alarms = {}
        self.print_id = "ALRMS"
        self.verbose = verbose
        self.uid = os.getuid()
        self.pid = os.getpid()

	# get the config server
        configuration_client.set_csc(self, csc, host, port, verbose)
        keys = self.csc.get("alarm_server")
        self.hostip = keys['hostip']
        Trace.init(keys["logname"])
        self.sus_vol_thresh = keys.get("susp_vol_thresh",
                                       DEFAULT_SUSP_VOLS_THRESH)
        self.print_id = keys.get("logname", self.print_id)
        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))
        # get a logger
        self.logc = log_client.LoggerClient(self.csc, keys["logname"], \
                                            'logserver', 0)

        # see if an alarm file exists. if it does, open it and read it in.
        # these are the alarms that have not been dealt with.
        self.get_alarm_file()

        # get the patrol file name and location and write any current alarms
        # out to it.
        self.get_patrol_file()

class AlarmServerInterface(interface.Interface):

    def __init__(self):
        # fill in the defaults for possible options
        self.verbose = 0
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options()+\
	       ["verbose="] +\
	       self.help_options()

if __name__ == "__main__":
    Trace.init("alarm_server")
    Trace.trace( 6, "alarm server called with args "+repr(sys.argv) )

    # get interface
    intf = AlarmServerInterface()

    # get the alarm server
    als = AlarmServer(0, intf.verbose, intf.config_host, intf.config_port)

    while 1:
        try:
            Trace.trace(1,'Alarm Server (re)starting')
            als.logc.send(e_errors.INFO, 1, "Alarm Server (re)starting")
            als.serve_forever()
        except:
	    als.serve_forever_error("alarm_server", als.logc)
            continue
    Trace.trace(1,"Alarm Server finished (impossible)")
