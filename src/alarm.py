from __future__ import print_function
#
# system import
import time
import string
import errno
import os

# enstore imports
import enstore_functions2
import enstore_constants
import e_errors
import Trace
from en_eval import en_eval

# key to get at a server supplied short text string
SHORT_TEXT = "short_text"

DEFAULT_PID = -1
DEFAULT_UID = ""
DEFAULT_SOURCE = None
DEFAULT_CONDITION = None
DEFAULT_TYPE = None

SEVERITY = "severity"

MATCH = 1
NO_MATCH = 0

PATROL_SEVERITY = {e_errors.sevdict[e_errors.ALARM]: '4',
                   e_errors.sevdict[e_errors.ERROR]: '4',
                   e_errors.sevdict[e_errors.USER_ERROR]: '3',
                   e_errors.sevdict[e_errors.WARNING]: '2',
                   e_errors.sevdict[e_errors.INFO]: '1',
                   e_errors.sevdict[e_errors.MISC]: '1'
                   }


class GenericAlarm:

    def __init__(self):
        self.timedate = time.time()
        self.timedate_last = self.timedate
        self.id = str(self.timedate)
        self.host = ""
        self.pid = DEFAULT_PID
        self.uid = DEFAULT_UID
        self.source = DEFAULT_SOURCE
        self.condition = DEFAULT_CONDITION
        self.type = DEFAULT_TYPE
        self.severity = e_errors.DEFAULT_SEVERITY
        self.root_error = e_errors.DEFAULT_ROOT_ERROR
        self.alarm_info = {}
        self.patrol = 0
        self.num_times_raised = 1
        self.ticket_generated = None

    def set_ticket(self, condition, type):
        self.condition = condition
        self.type = type

    def split_severity(self, sev):
        l = string.split(sev)
        sev = l[0]
        if len(l) == 2:
            tmp = string.replace(l[1], '(', '')
            tmp = string.replace(tmp, ')', '')
            num_times_raised = long(tmp)
        else:
            num_times_raised = 1
        return sev, num_times_raised

    # output the alarm for patrol
    def prepr(self):
        self.patrol = 1
        alarm = repr(self)
        self.patrol = 0
        return alarm

    # generate a ticket in remedy for this alarm
    def ticket(self):
        if not self.ticket_generated and self.condition:
            system_name = self.host
            # we need to remove the ".fnal.gov" extension
            system_name = enstore_functions2.strip_node(system_name)
            condition = self.condition
            short_message = self.root_error
            long_message = self.list_alarm()
            submitter = "MSS"
            user = "MSS"
            password = "2p9u6c"
            category = "MSS"
            aType = self.type
            item = "ALARM"
            # make sure long_message does not have embedded double
            # quotes
            l_message = "%s" % (long_message,)
            l_message = string.replace(l_message, '"', '')

            print(
                '$ENSTORE_DIR/sbin/generate_ticket %s "%s" "%s" "%s" %s %s %s %s "%s" %s' %
                (system_name,
                 condition,
                 short_message,
                 l_message,
                 submitter,
                 user,
                 password,
                 category,
                 aType,
                 item))
            os.system(
                '. /usr/local/etc/setups.sh;setup enstore; $ENSTORE_DIR/sbin/generate_ticket %s "%s" "%s" "%s" %s %s %s %s "%s" %s' %
                (system_name, condition, short_message, l_message, submitter, user, password, category, aType, item))
            self.ticket_generated = "YES"

    def seen_again(self):
        self.timedate_last = time.time()
        try:
            self.num_times_raised = self.num_times_raised + 1
        except OverflowError:
            self.num_times_raised = -1

    # return the a list of the alarm pieces we need to output
    def list_alarm(self):
        return [self.id, self.timedate_last, self.host, self.pid, self.uid,
                "%s (%s)" % (self.severity, self.num_times_raised),
                self.source, self.root_error,
                self.condition, self.type,
                self.ticket_generated, self.alarm_info]

    # output the alarm
    def __repr__(self):
        return repr(self.list_alarm())

    # format the alarm as a simple, short text string to use to signal
    # that there is something wrong that needs further looking in to
    def short_text(self):
        # ths simple string has the following format -
        #         servername on node - text string
        # where servername and node are replaced with the appropriate values
        aStr = "%s on %s at %s (%s) - " % (self.source, self.host,
                                           enstore_functions2.format_time(
                                               self.timedate),
                                           enstore_functions2.format_time(self.timedate_last))

        # look in the info dict.  if there is a key "short_text", use it to get
        # the text, else use default text just signaling a problem
        return aStr + self.alarm_info.get(SHORT_TEXT, "%s %s" % (self.root_error,
                                                                 self.alarm_info))

    # compare the passed in info to see if it the same as that of the alarm
    def compare(self, host, severity, root_error, source, alarm_info,
                condition, remedy_type):
        if (self.host == host and
            self.root_error == root_error and
            self.severity == severity and
            self.source == source and
            self.condition == condition and
                self.type == remedy_type):
            # now that all that is done we can compare the dictionary to see
            # if it is the same.  we need to ignore any r_a keywords first.
            alarm_info_t = alarm_info
            if enstore_constants.RA in alarm_info_t:
                del alarm_info_t[enstore_constants.RA]
            if enstore_constants.RA in self.alarm_info:
                del self.alarm_info[enstore_constants.RA]
            if len(alarm_info) == len(self.alarm_info):
                keys = self.alarm_info.keys()
                for key in keys:
                    if key in alarm_info:
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
                 condition, remedy_type, alarm_info=None):
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
        self.condition = condition
        self.type = remedy_type


class AsciiAlarm(GenericAlarm):

    def __init__(self, text):
        GenericAlarm.__init__(self)

        # if the alarm file has junk in it. protect against that
        try:
            [self.id, self.host, self.pid, self.uid, sev,
             self.source, self.root_error, self.alarm_info] = en_eval(text)
            self.severity, self.num_times_raised = self.split_severity(sev)
            self.timedate_last = float(self.id)
        except ValueError:
            # try the new format with the remedy ticket information in it
            try:
                [self.id, self.host, self.pid, self.uid, sev,
                 self.source, self.root_error,
                 self.condition, self.type,
                 self.ticket_generated, self.alarm_info] = en_eval(text)
                self.severity, self.num_times_raised = self.split_severity(sev)
                self.timedate_last = float(self.id)
            except ValueError:
                try:
                    # try the format with 2 timedates in it
                    [self.id, self.timedate_last, self.host, self.pid, self.uid, sev,
                     self.source, self.root_error,
                     self.condition, self.type,
                     self.ticket_generated, self.alarm_info] = en_eval(text)
                    self.severity, self.num_times_raised = self.split_severity(
                        sev)
                    self.timedate_last = float(self.timedate_last)
                except (TypeError, ValueError):
                    self.id = 0    # not a valid alarm


class LogFileAlarm(GenericAlarm):

    def unpack_dict(self, dict):
        if enstore_constants.ROOT_ERROR in dict:
            self.root_error = dict[enstore_constants.ROOT_ERROR]
            del dict[enstore_constants.ROOT_ERROR]
        else:
            self.root_error = "UNKNOWN"
        if SEVERITY in dict:
            self.severity = dict[SEVERITY]
            del dict[SEVERITY]
        else:
            self.severity = e_errors.ALARM
        self.alarm_info = dict

    def __init__(self, text, date):
        GenericAlarm.__init__(self)

        self.num_times_raised = 1

        # get rid of the MSG_TYPE part of the alarm
        [t, self.host, self.pid, self.uid, dummy, self.source,
         text] = string.split(text, " ", 6)

        # we need to get rid of the MSG_TYPE text.  it may be at the
        # beginning or the end.
        text = string.replace(text, Trace.MSG_ALARM, "")

        # assemble the real timedate
        self.timedate = time.strptime("%s %s" % (date, t), "%Y-%m-%d %H:%M:%S")
        # i am doing this explicitly because it seems that time.strptime will
        # return 0 for the DST flag even if it is DST
        self.timedate = (self.timedate[0], self.timedate[1],
                         self.timedate[2], self.timedate[3],
                         self.timedate[4], self.timedate[5],
                         self.timedate[6], self.timedate[7],
                         -1)
        self.id = str(self.timedate)
        self.timedate_last = self.timedate

        text = string.strip(text)
        # text may be only a dictionary or it may be of the following format -
        #       root-error, {...} (severity : n)
        if text[0] == "{":
            aDict = en_eval(text)
            # split up the dictionary into components
            self.unpack_dict(aDict)
        else:
            index = string.find(text, ", {")
            if index == -1:
                # we could not find the substring, punt
                self.root_error = text
                self.severity = e_errors.ALARM
                self.alarm_info = {}
            else:
                self.root_error = text[0:index]
                # now pull out any dictionary, skip the ", "
                index = index + 2
                end_index = string.find(text, "} (")
                if end_index == -1:
                    # couldn't find it, punt again
                    self.severity = e_errors.ALARM
                    self.alarm_info = text[index:]
                else:
                    aDict = en_eval(text[index:end_index + 1])
                    self.alarm_info = aDict
                    # now get the severity
                    index = string.rfind(text, ")")
                    if index == -1:
                        # could not find it
                        self.severity = e_errors.ALARM
                    else:
                        sev = text[index - 1]
                        for k, v in e_errors.sevdict.items():
                            if v == sev:
                                self.severity = k
                                break
                        else:
                            # there was no match
                            self.severity = e_errors.ALARM
