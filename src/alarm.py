#
# system import
import time
import string

# enstore imports
import enstore_status
import e_errors
import Trace

# key to get at a server supplied short text string
SHORT_TEXT = "short_text"

DEFAULT_PID = -1
DEFAULT_UID = ""
DEFAULT_SOURCE = "None"

ROOT_ERROR = "root_error"
SEVERITY = "severity"

MATCH = 1
NO_MATCH = 0

PATROL_SEVERITY = { e_errors.sevdict[e_errors.ALARM] : '4',
                    e_errors.sevdict[e_errors.ERROR] : '4',
                    e_errors.sevdict[e_errors.USER_ERROR] : '3',
                    e_errors.sevdict[e_errors.WARNING] : '2',
                    e_errors.sevdict[e_errors.INFO] : '1',
                    e_errors.sevdict[e_errors.MISC] : '1'
                    }

class GenericAlarm:

    def __init__(self):
        self.timedate = time.time()
        self.id = self.timedate
        self.host = ""
        self.pid = DEFAULT_PID
        self.uid = DEFAULT_UID
        self.source = DEFAULT_SOURCE
        self.severity = e_errors.DEFAULT_SEVERITY
        self.root_error = e_errors.DEFAULT_ROOT_ERROR
        self.alarm_info = {}
        self.patrol = 0

    # output the alarm for patrol
    def prepr(self):
        self.patrol = 1
        alarm = repr(self)
        self.patrol = 0
        return alarm

    # return the a list of the alarm pieces we need to output
    def list_alarm(self):
	return [self.timedate, self.host, self.pid, self.uid, self.severity, 
		self.source, self.root_error, self.alarm_info]

    # output the alarm
    def __repr__(self):
        # format ourselves to be a straight ascii line of the same format as
        # mentioned above
        if self.patrol:
            host = string.split(self.host,".")
            # enstore's severities must be mapped to patrols'
            sev = PATROL_SEVERITY[self.severity]
            return string.join((host[0], "Enstore" , sev, self.short_text(),
                                "\n"))
        else:
            return repr(self.list_alarm())

    # format the alarm as a simple, short text string to use to signal
    # that there is something wrong that needs further looking in to
    def short_text(self):
        # ths simple string has the following format -
        #         servername on node - text string
        # where servername and node are replaced with the appropriate values
        str = "%s on %s at %s - "%(self.source, self.host,
                                   enstore_status.format_time(self.timedate))

        # look in the info dict.  if there is a key "short_text", use it to get
        # the text, else use default text just signaling a problem
        return str+self.alarm_info.get(SHORT_TEXT, "%s %s"%(self.root_error,
                                                            self.alarm_info))

    # compare the passed in info to see if it the same as that of the alarm
    def compare(self, host, severity, root_error, source, alarm_info):
        if (self.host == host and 
            self.root_error == root_error and
            self.severity == severity and
            self.source == source):
	    # now that all that is done we can compare the dictionary to see
	    # if it is the same
	    if len(alarm_info) == len(self.alarm_info):
		keys = self.alarm_info.keys()
		for key in keys:
		    if alarm_info.has_key(key):
			if not self.alarm_info[key] == alarm_info[key]:
			    # we found something that does not match
			    break
		    else:
			# there is no corresponding key
			break
		else:
		    # all keys matched between the two dicts
		    return MATCH

		return NO_MATCH
	    return NO_MATCH
        return NO_MATCH

    # return the alarms unique id
    def get_id(self):
        return self.id

class Alarm(GenericAlarm):

    def __init__(self, host, severity, root_error, pid, uid, source,
                 alarm_info=None):
        GenericAlarm.__init__(self)

        if alarm_info is None:
            alarm_info = {}
        self.host = host
        self.severity = severity
        self.root_error = root_error
        self.pid = pid
        self.uid = uid
        self.source = source
        self.alarm_info = alarm_info

class AsciiAlarm(GenericAlarm):

    def __init__(self, text):
        GenericAlarm.__init__(self)

        [self.timedate, self.host, self.pid, self.uid, self.severity,
         self.source, self.root_error, self.alarm_info] = eval(text)

class LogFileAlarm(GenericAlarm):

    def __init__(self, text, date):
	GenericAlarm.__init__(self)

	# get rid of the MSG_TYPE part of the alarm
	[text1, msg_type] = string.split(text, Trace.MSG_TYPE)
	[t, self.host, self.pid, self.uid, dummy, self.source,
	 text_dict] = string.split(text1, " ", 6)

	# assemble the real timedate
	self.timedate = time.strptime("%s %s"%(date, t), "%Y-%m-%d %H:%M:%S")
	self.id = self.timedate

	# split up the dictionary into components
	dict = eval(string.strip(text_dict))
	if dict.has_key(ROOT_ERROR):
	    self.root_error = dict[ROOT_ERROR]
	    del dict[ROOT_ERROR]
	else:
	    self.root_error = "UNKNOWN"
	if dict.has_key(SEVERITY):
	    self.severity = dict[SEVERITY]
	    del dict[SEVERITY]
	else:
	    self.root_error = "UNK"
	self.alarm_info = dict
