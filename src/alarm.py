#
# system import
import time
import string

# enstore imports
import e_errors

# key to get at a server supplied short text string
SHORT_TEXT = "short_text"

DEFAULT_PID = -1
DEFAULT_UID = ""
DEFAULT_SOURCE = "None"

MATCH = 1
NO_MATCH = 0

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

    # output the alarm
    def __repr__(self):
        # format ourselves to be a straight ascii line of the same format as
        # mentioned above
        if self.patrol:
            host = string.split(self.host,".")
            return string.join((host[0], "Enstore", repr(self.severity),
                                self.short_text(), "\n"))
        else:
            return repr([self.timedate, self.host, self.pid, self.uid,
                         self.severity, self.source, self.root_error,
                         self.alarm_info])

    # format the alarm as a simple, short text string to use to signal
    # that there is something wrong that needs further looking in to
    def short_text(self):
        # ths simple string has the following format -
        #         servername on node - text string
        # where servername and node are replaced with the appropriate values
        str = self.source+" on "+self.host+" - "

        # look in the info dict.  if there is a key "short_text", use it to get
        # the text, else use default text just signaling a problem
        return str+self.alarm_info.get(SHORT_TEXT, self.root_error)

    # compare the passed in info to see if it the same as that of the alarm
    def compare(self, host, severity, root_error, source, alarm_info):
        if (self.host == host and 
            self.severity == e_errors.sevdict[severity] and 
            self.root_error == root_error and
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
        # do not let the severity (which is actually a number), point outside
        # of the dictionary
        if severity >= len(e_errors.sevdict):
            severity = len(e_errors.sevdict)-1
        elif severity < 0:
            severity = 0
        self.severity = e_errors.sevdict[severity]
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
