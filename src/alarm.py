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

# keyword to specify textual alarm information as read from the alarm file
TEXTKEY = "AscIITeXt"

class GenericAlarm:

    def __init__(self):
        self.timedate = time.time()
        self.host = ""
        self.pid = -1
        self.uid = -1
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
        # first format the dictionary containing the extra stuff
        rest = ""
        if not self.alarm_info == {}:
            keys = self.alarm_info.keys()
            keys.sort()
            for key in keys:
                if key == TEXTKEY:
                    rest = rest+" "+self.alarm_info[key]
                else:
                    rest = rest+" "+key+" = "+repr(self.alarm_info[key])

        # do not let the severity (which is actually a number), point outside
        # of the dictionary
        if self.severity >= len(e_errors.sevdict):
            self.severity = len(e_errors.sevdict)-1

        # format ourselves to be a straight ascii line of the same format as
        # mentioned above
        if self.patrol:
            host = string.split(self.host,".")
            return string.join((host[0], "Enstore", "2", \
                                enstore_status.format_time(self.timedate), \
                                repr(self.pid), repr(self.uid), \
                                e_errors.sevdict[self.severity], self.source, \
                                rest, "\n"))
        else:    
            return string.join((repr(self.timedate), \
                                self.host, repr(self.pid), repr(self.uid), \
                                e_errors.sevdict[self.severity], self.source, \
                                rest, "\n"))

    # format the alarm as a simple, short text string to use to signal
    # that there is something wrong that needs further looking in to
    def short_text(self):
        # ths simple string has the following format -
        #         servername on node - text string
        # where servername and node are replaced with the appropriate values
        str = self.source+" on "+self.host+" - "

        # look in the info dict.  if there is a key "short_text", use it to get
        # the text, else use default text just signaling a problem
        if self.alarm_info.has_key(SHORT_TEXT):
            str = str+self.alarm_info[SHORT_TEXT]
        else:
            str = str+DEFAULT_TEXT
        return str+"\n"

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
        # this alarm is formatted as a simple ascii text line.  we must
        # parse this line to pull out the alarm info.  the line should be
        # formatted as follows -
        #     date_time node pid uid severity server text
        ntext = string.strip(text)
        fields = string.split(ntext, " ", 6)
        self.timedate = string.atof(fields[0])
        self.host = fields[1]
        self.pid = string.atoi(fields[2])
        self.uid = string.atoi(fields[3])
        keys = e_errors.sevdict.keys()
        for key in keys:
            if e_errors.sevdict[key] == fields[4]:
                self.severity = key
                break
        self.source = fields[5]

        if len(fields) == 7:
            self.alarm_info[TEXTKEY] = fields[6]
        else:
            self.alarm_info = {}
