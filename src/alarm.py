#
# system import
import time
import string

# enstore imports
import enstore_status
import e_errors

# key to get at a server supplied short text string
SHORT_TEXT = "short_text"

# use this if no SHORT_TEXT was part of the alarm
DEFAULT_TEXT = "has a problem"

class GenericAlarm:

    def __init__(self):
        self.timedate = time.time()
        self.id = self.timedate
        self.host = ""
        self.pid = -1
        self.uid = ""
        self.source = "None"
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
        # do not let the severity (which is actually a number), point outside
        # of the dictionary
        if self.severity >= len(e_errors.sevdict):
            self.severity = len(e_errors.sevdict)-1
        elif self.severity < 0:
            self.severity = 0

        # format ourselves to be a straight ascii line of the same format as
        # mentioned above
        if self.patrol:
            host = string.split(self.host,".")
            return string.join((host[0], "Enstore", repr(self.severity), \
                                self.short_text(), "\n"))
        else:
            return repr([self.timedate, self.host, self.pid, self.uid,
                         e_errors.sevdict[self.severity], self.source,
                         self.root_error, self.alarm_info])

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

    # return the alarms unique id
    def get_id(self):
        return self.id

class Alarm(GenericAlarm):

    def __init__(self, host, severity, root_error, alarm_info={}):
        GenericAlarm.__init__(self)
        if alarm_info.has_key("pid"):
            self.pid = alarm_info["pid"]
            # remove this entry from the dictionary
            del alarm_info["pid"]
        if alarm_info.has_key("uid"):
            self.uid = alarm_info["uid"]
            # remove this entry from the dictionary
            del alarm_info["uid"]
        if alarm_info.has_key("source"):
            self.source = alarm_info["source"]
            # remove this entry from the dictionary
            del alarm_info["source"]
        
        self.host = host
        self.severity = severity
        self.root_error = root_error
        self.alarm_info = alarm_info

class AsciiAlarm(GenericAlarm):

    def __init__(self, text):
        GenericAlarm.__init__(self)

        [self.timedate, self.host, self.pid, self.uid,
         e_errors.sevdict[self.severity], self.source, self.root_error,
         self.alarm_info] = eval(text)
