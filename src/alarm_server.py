#! /usr/bin/env python
# system import
import sys
import os
import string

# enstore imports
import setpath
import dispatching_worker
import enstore_files
import generic_server
import event_relay_client
import monitored_server
import Trace
import alarm
import e_errors
import hostaddr
import enstore_constants
import enstore_functions2
import www_server
import enstore_html

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

MY_NAME = "alarm_server"

DEFAULT_FILE_NAME = "enstore_alarms.txt"
DEFAULT_SUSP_VOLS_THRESH = 3
DEFAULT_HTML_ALARM_FILE = "enstore_alarms.html"


SEVERITY = alarm.SEVERITY


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
        if ticket.has_key(enstore_constants.ROOT_ERROR):
            root_error = ticket[enstore_constants.ROOT_ERROR]
            # remove this entry from the dictionary, so if won't be included
            # as part of alarm_info
            del ticket[enstore_constants.ROOT_ERROR]
        else:
            root_error = e_errors.DEFAULT_ROOT_ERROR
        if ticket.has_key(enstore_constants.PID):
            pid = ticket[enstore_constants.PID]
            # remove this entry from the dictionary
            del ticket[enstore_constants.PID]
        else:
            pid = alarm.DEFAULT_PID
        if ticket.has_key(enstore_constants.UID):
            uid = ticket[enstore_constants.UID]
            # remove this entry from the dictionary
            del ticket[enstore_constants.UID]
        else:
            uid = alarm.DEFAULT_UID
        if ticket.has_key(enstore_constants.SOURCE):
            source = ticket[enstore_constants.SOURCE]
            # remove this entry from the dictionary
            del ticket[enstore_constants.SOURCE]
        else:
            source = alarm.DEFAULT_SOURCE
        # remove this entry from the dictionary, so it won't be included
        # as part of alarm_info
        if ticket.has_key("work"):
            del ticket["work"]

        theAlarm = self.alarm(severity, root_error, pid, uid, source, ticket)

        # send the reply to the client
        ret_ticket = { 'status' : (e_errors.OK, None),
                       enstore_constants.ALARM    : repr(theAlarm.get_id()) }
        self.send_reply(ret_ticket)

    def default_action(self, theAlarm, isNew, params=[]):
        if isNew:
            # save it in memory for us now
            self.alarms[theAlarm.get_id()] = theAlarm
        else:
            # this new alarm is the same as an old one.  bump the counter
            # in the old one and rewrite the web pages
	    theAlarm.seen_again()

        # write it to the persistent alarm file
        self.write_alarm_file(theAlarm)
        # write it to the web page
        self.write_html_file()

    # all action methods need to call this function
    def action_defaults(self, theAlarm, isNew, params):
        if isNew:
            self.info_alarms[theAlarm.get_id()] = theAlarm

    # params = [action_name, 1|*, optional_params...]
    # if 1, only send mail the first time the alarm is seen
    #    *, always send mail when get this alarm
    def send_mail_action(self, theAlarm, isNew, params):
        params_len = len(params)
        if isNew or (params_len > 1 and params[1] == "*"):
            enstore_functions2.send_mail(MY_NAME, theAlarm, "Alarm raised", params[2])
        self.action_defaults(theAlarm, isNew, params)

    # in order to create a new alarm action, do -
    #  1. add an element to self.ALARM_ACTIONS
    #  2. add a method with parameters like in send_mail_action
    #  3. this method should call action_defaults method

    # handle the alarm
    def process_alarm(self, theAlarm, isNew):
        actions = self.severity_actions.get(theAlarm.severity, [])
        if actions:
            # there is a specified action(s) for this alarm
            for action in actions:
                do_this = self.ALARM_ACTIONS.get(action[0], None)
                if do_this:
                    do_this(theAlarm, isNew, action)
                else:
                    # this was an unsupported action
                    Trace.log(e_errors.USER_ERROR,
                              "Unsupported action (%s) specified in alarm (%s). Ignoring."%(action[0],
                                                                                            theAlarm))
            pass
        else:
            # no specified action, do the default
            self.default_action(theAlarm, isNew)

    def find_alarm(self, host, severity, root_error, source, alarm_info):
        # find the alarm that matches the above information
        ids = self.alarms.keys()
        for id in ids:
            if self.alarms[id].compare(host, severity, root_error, source, \
                                       alarm_info) == alarm.MATCH:
                return self.alarms[id]
        ids = self.info_alarms.keys()
        for id in ids:
            if self.info_alarms[id].compare(host, severity, root_error, source, \
                                            alarm_info) == alarm.MATCH:
                return self.info_alarms[id]
        return
        

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
            # process the alarm depending on the action
            self.process_alarm(theAlarm, 1)
	else:
            # process the alarm depending on the action
            self.process_alarm(theAlarm, 0)

        Trace.trace(20, repr(theAlarm.list_alarm()))
        return theAlarm

    def resolve(self, id):
        # an alarm is being resolved, we must do the following -
        #      remove it from our alarm dictionary
        #      rewrite the entire enstore_alarm file (txt and html)
        #      log this fact
        if self.alarms.has_key(id):
            del self.alarms[id]
            self.write_alarm_file()
	    self.write_html_file()
            t = "Alarm with id = "+repr(id)+" has been resolved"
            Trace.log(e_errors.INFO, t)
            Trace.trace(20, t)
            return (e_errors.OK, None)
        else:
            # don't know anything about this alarm
            return (e_errors.NOALARM, None)

    def resolve_alarm(self, ticket):
        # get the unique identifier for this alarm
        id = ticket.get(enstore_constants.ALARM, 0)
	ticket = {}
	if id == enstore_html.RESOLVEALL:
	    for id in self.alarms.keys():
		status = self.resolve(id)
		ticket[id] = status
	else:
	    status = self.resolve(id)
	    ticket[id] = status

        # send the reply to the client
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

    # create the web page that contains the list of raised alarms
    def write_html_file(self):
	self.alarmhtmlfile.open()
	self.alarmhtmlfile.write(self.alarms, self.get_www_host())
	self.alarmhtmlfile.close()

    def get_log_path(self):
        log = self.csc.get("log_server")
        return log.get("log_file_path", ".")
        
    def get_www_host(self):
        inq = self.csc.get("inquisitor")
        return inq.get("www_host", ".")
        
    def get_www_path(self):
        inq = self.csc.get("inquisitor")
        return inq.get("html_file", ".")
        
    # read the persistent alarm file if it exists.  this reads in all the
    # alarms that have not been resolved.
    def get_alarm_file(self):
        # the alarm file lives in the same directory as the log file
        self.alarm_file = enstore_files.EnAlarmFile("%s/%s"%(self.get_log_path(),
							     DEFAULT_FILE_NAME))
        self.alarm_file.open('r')
        self.alarms = self.alarm_file.read()
        self.alarm_file.close()


class AlarmServer(AlarmServerMethods, generic_server.GenericServer):

    def __init__(self, csc):
        # actions supported by the alarm server.  these actions can be
        # specified in the config file for specific severities.  if no
        # action is specified, the default action is to display the alarm
        # on the alarm page
        self.ALARM_ACTIONS = {'html_page' : self.default_action,
                              'send_mail' : self.send_mail_action,
                              }

        generic_server.GenericServer.__init__(self, csc, MY_NAME,
					      flags=enstore_constants.NO_ALARM)
        Trace.init(self.log_name)

        self.alarms = {}
        # keep a record of the alarms not written to the html file
        self.info_alarms = {}
        self.uid = os.getuid()
        self.pid = os.getpid()
        keys = self.csc.get(MY_NAME)
        self.hostip = keys['hostip']
	self.alive_interval = monitored_server.get_alive_interval(self.csc,
								  MY_NAME,
								  keys)
        self.sus_vol_thresh = keys.get("susp_vol_thresh",
                                       DEFAULT_SUSP_VOLS_THRESH)
        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))

	self.system_tag = www_server.get_system_tag(self.csc)

        # figure out which severities will have non-default actions
        self.severity_actions = keys.get('alarm_actions', {})

        # see if an alarm file exists. if it does, open it and read it in.
        # these are the alarms that have not been dealt with.
        self.get_alarm_file()

	# initialize the html alarm file
	self.alarmhtmlfile = enstore_files.HtmlAlarmFile("%s/%s"%( \
	    self.get_www_path(), DEFAULT_HTML_ALARM_FILE), self.system_tag)

	# write the current alarms to it if 
	self.write_html_file()

	# start our heartbeat to the event relay process
	self.erc.start_heartbeat(enstore_constants.ALARM_SERVER, 
				 self.alive_interval)


class AlarmServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)

        # now parse the options
        #self.parse_options()

    def valid_dictionaries(self):
        return (self.help_options,)


if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))
    Trace.trace( 6, "alarm server called with args "+repr(sys.argv) )

    intf = AlarmServerInterface()
    csc = intf.config_host, intf.config_port
    als = AlarmServer(csc)
    als.handle_generic_commands(intf)
    
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
