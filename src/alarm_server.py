#
# system import
import sys
import os
import string

# enstore imports
import dispatching_worker
import interface
import enstore_files
import generic_server
import Trace
import alarm
import e_errors
import hostaddr

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

MY_NAME = "alarm_server"

DEFAULT_FILE_NAME = "/enstore_alarms.txt"
DEFAULT_PATROL_FILE_NAME = "/enstore_patrol.txt"
DEFAULT_SUSP_VOLS_THRESH = 3
DEFAULT_HTML_ALARM_FILE = "/enstore_alarms.html"

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
              source=alarm.DEFAULT_SOURCE, alarm_info=None):
        if alarm_info is None:
            alarm_info = {}
        # find out where the alarm came from
        host = hostaddr.address_to_name(self.reply_address[0])
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
            # write it to the persistent alarm file
            self.write_alarm_file(theAlarm)
            # write it out to the patrol file
            self.write_patrol_file()
	    # write it to the web page
	    self.write_html_file()
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
        #      rewrite the entire enstore_alarm file (txt and html)
        #      rewrite the enstore patrol file
        #      log this fact
        if self.alarms.has_key(id):
            del self.alarms[id]
            self.write_alarm_file()
            self.write_patrol_file()
	    self.write_html_file()
            Trace.log(e_errors.INFO,
                      "Alarm with id = "+repr(id)+" has been resolved")
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
        
    def dump(self, ticket):
	# dump our brains
	for key in self.alarms.keys():
	    print self.alarms[key]
        # send the reply to the client
        ticket['status'] = (e_errors.OK, None)
        self.send_reply(ticket)
        
    def write_alarm_file(self, alarm=None):
        if alarm:
            self.alarm_file.open()
            self.alarm_file.write(alarm)
        else:
            self.alarm_file.open('w')
            if self.alarms:
                keys = self.alarms.keys()
                keys.sort()
                for key in keys:
                    self.alarm_file.write(self.alarms[key])
        self.alarm_file.close()

    def write_patrol_file(self):
        if not self.alarms == {}:
            self.patrol_file.open()
            if self.alarms:
                keys = self.alarms.keys()
                keys.sort()
                for key in keys:
                    if self.alarms[key].severity == \
                       e_errors.sevdict[e_errors.ERROR]:
                        self.patrol_file.write(self.alarms[key])
            self.patrol_file.close()
        else:
            # there are no alarms raised.  if the patrol file exists, we
            # need to delete it
            self.patrol_file.remove()

    # create the web page that contains the list of raised alarms
    def write_html_file(self):
	self.alarmhtmlfile.open()
	self.alarmhtmlfile.write(self.alarms)
	self.alarmhtmlfile.close()

    def get_log_path(self):
        log = self.csc.get("log_server")
        return log.get("log_file_path", ".")
        
    def get_www_path(self):
        inq = self.csc.get("inquisitor")
        return inq.get("html_file", ".")
        
    # read the persistent alarm file if it exists.  this reads in all the
    # alarms that have not been resolved.
    def get_alarm_file(self):
        # the alarm file lives in the same directory as the log file
        self.alarm_file = enstore_files.EnAlarmFile(self.get_log_path()+ \
                                                     DEFAULT_FILE_NAME)
        self.alarm_file.open('r')
        self.alarms = self.alarm_file.read()
        self.alarm_file.close()

    # get the location of the file we will create for patrol and write any
    # alarms to it
    def get_patrol_file(self):
        # the patrol file lives in the same directory as the log file
        self.patrol_file = enstore_files.EnPatrolFile(self.get_log_path()+ \
                                                      DEFAULT_PATROL_FILE_NAME)
        self.write_patrol_file()

    # return the name of the patrol file
    def get_patrol_filename(self, ticket):
        ticket['patrol_file'] = self.patrol_file.real_file_name
        ticket['status'] = (e_errors.OK, None)
        self.send_reply(ticket)

class AlarmServer(AlarmServerMethods, generic_server.GenericServer):

    def __init__(self, csc):
        # need the following definition so the generic client init does not
        # get an alarm client
        self.is_alarm = 1
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        Trace.init(self.log_name)

        self.alarms = {}
        self.info_alarms = {}
        self.uid = os.getuid()
        self.pid = os.getpid()
        keys = self.csc.get(MY_NAME)
        self.hostip = keys['hostip']
        self.sus_vol_thresh = keys.get("susp_vol_thresh",
                                       DEFAULT_SUSP_VOLS_THRESH)
        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))

        # see if an alarm file exists. if it does, open it and read it in.
        # these are the alarms that have not been dealt with.
        self.get_alarm_file()

        # get the patrol file name and location and write any current alarms
        # out to it.
        self.get_patrol_file()

	# initialize the html alarm file
	self.alarmhtmlfile = enstore_files.HtmlAlarmFile("%s%s"%( \
	    self.get_www_path(),DEFAULT_HTML_ALARM_FILE))

	# write the current alarms to it
	self.write_html_file()

class AlarmServerInterface(interface.Interface):

    def __init__(self):
        # fill in the defaults for possible options
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options()+\
	       self.help_options()

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))
    Trace.trace( 6, "alarm server called with args "+repr(sys.argv) )

    # get interface
    intf = AlarmServerInterface()

    # get the alarm server
    als = AlarmServer((intf.config_host, intf.config_port))

    while 1:
        try:
            Trace.log(e_errors.INFO, "Alarm Server (re)starting")
            als.serve_forever()
	except SystemExit, exit_code:
	    sys.exit(exit_code)
        except:
	    als.serve_forever_error(als.log_name)
            continue
    Trace.trace(6,"Alarm Server finished (impossible)")
