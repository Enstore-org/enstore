#! /usr/bin/env python
"""
Enstore alarm server
"""

# system import
import sys
import os
import string
import re

# enstore imports
import dispatching_worker
import enstore_files
import generic_server
import monitored_server
import Trace
import alarm
import e_errors
import hostaddr
import enstore_constants
import enstore_mail
import www_server
import enstore_html
import event_relay_messages
import types


def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

MY_NAME = enstore_constants.ALARM_SERVER   #"alarm_server"

DEFAULT_FILE_NAME = "enstore_alarms.txt"
DEFAULT_SUSP_VOLS_THRESH = 3
DEFAULT_HTML_ALARM_FILE = "enstore_alarms.html"


SEVERITY = alarm.SEVERITY


class AlarmServerMethods(dispatching_worker.DispatchingWorker):
    """
    Implements alarm server methods
    """

    def get_from_ticket(self, ticket, key, default):
        if ticket.has_key(key):
            rtn = ticket[key]
            # remove this entry from the dictionary, so if won't be included
            # as part of alarm_info
            del ticket[key]
        else:
            rtn = default
        return rtn;

    def post_alarm(self, ticket):
        """
        Pull out alarm info from the ticket and raise the alarm.

        :type ticket: :obj:`dict`
        :arg ticket: alarm ticket
        """
        severity = self.get_from_ticket(ticket, SEVERITY, e_errors.DEFAULT_SEVERITY)
        root_error = self.get_from_ticket(ticket, enstore_constants.ROOT_ERROR,
                                          e_errors.DEFAULT_ROOT_ERROR)
        pid = self.get_from_ticket(ticket, enstore_constants.PID, alarm.DEFAULT_PID)
        uid = self.get_from_ticket(ticket, enstore_constants.UID, alarm.DEFAULT_UID)
        source = self.get_from_ticket(ticket, enstore_constants.SOURCE, alarm.DEFAULT_SOURCE)
        remedy_type = self.get_from_ticket(ticket, enstore_constants.REMEDY_TYPE, alarm.DEFAULT_TYPE)
        condition = self.get_from_ticket(ticket, enstore_constants.CONDITION, alarm.DEFAULT_CONDITION)
        # remove this entry from the dictionary, so it won't be included
        # as part of alarm_info
        if ticket.has_key("work"):
            del ticket["work"]

        theAlarm = self.alarm(severity, root_error, pid, uid, source, condition,
                              remedy_type, ticket)

        # send the reply to the client
        ret_ticket = { 'status' : (e_errors.OK, None),
                       enstore_constants.ALARM    : repr(theAlarm.get_id()) }
        Trace.log(e_errors.ALARM, " (%s) %s "%(theAlarm.timedate, theAlarm.short_text(),),
                  Trace.MSG_ALARM)

    def default_action(self, theAlarm, isNew, params=[]):
        if isNew:
            # save it in memory for us now
            self.alarms[theAlarm.get_id()] = theAlarm
        else:
            # this new alarm is the same as an old one.  bump the counter
            # in the old one and rewrite the web pages
	    theAlarm.seen_again()

        # generate a ticket if supposed to
        theAlarm.ticket()
        # write it to the persistent alarm file
        self.write_alarm_file(theAlarm)
        # write it to the web page
        self.write_html_file()

    # all action methods need to call this function
    def action_defaults(self, theAlarm, isNew, params):
        """
        The default alarm action.
        All action methods need to call this function.
        In order to create a new alarm action, do -

        1. add an element to self.ALARM_ACTIONS

        2. add a method with parameters like in send_mail_action

        3. this method should call action_defaults method

        :type theAlarm: :class:`alarm.Alarm`
        :arg theAlarm: alarm
        :type isNew: :obj:`bool`
        :arg isNew: indicates wheter the alarm is new.
        :type params: :obj:`list`
        :arg params: additional parameters- [action_name, 1|\*, optional_params...]
                                            if 1, only send mail the first time the alarm is seen
                                            \*, always send mail when get this alarm

         """

        if isNew:
            self.info_alarms[theAlarm.get_id()] = theAlarm

    def send_mail_action(self, theAlarm, isNew, params):
        """
        Send mail alarm action.

        :type theAlarm: :class:`alarm.Alarm`
        :arg theAlarm: alarm
        :type isNew: :obj:`bool`
        :arg isNew: indicates whether the alarm is new.
        :type params: :obj:`list`
        :arg params: additional parameters. See above
         """
        params_len = len(params)
        alarm_info=theAlarm.alarm_info
        #
        # We expect to get in alarm_info a bunch of key,value pairs
        # like e.g.
        #   alarm_info['patterns']= { 'sg' : 'cms',
        #                           'node' : 'fcdsgi2.fnal.gov' }
        #                      Dmitry Litvintsev (litvinse@fnal.gov)
        #
        if isNew or (params_len > 1 and params[1] == "*"):
            e_mail=""
            if type(params[2]) == types.StringType:
                e_mail=params[2]
                enstore_mail.send_mail(MY_NAME, theAlarm, "Alarm raised", e_mail)
            else:
                try:
                    patterns_in_alarm=alarm_info['text'].get('patterns',
                                                             {}).values()
                    for k in params[2].keys():
                        rp=re.compile(k)
                        for p in patterns_in_alarm:
                            if ( rp.match(p) ) :
                                e_mail=e_mail+params[2][k]+","
                    if (e_mail!=""):
                        enstore_mail.send_mail(MY_NAME, theAlarm, "Alarm raised", e_mail[0:-1])
                    else:
                        self.default_action(theAlarm, isNew)
                except:
                    self.default_action(theAlarm, isNew)
                    Trace.log(e_errors.INFO,
                              "Exception in send_mail_action alarm_info = %s, parameters=%s "%(repr(alarm_info),repr(params)),
                              Trace.MSG_ALARM)
                    pass
        self.action_defaults(theAlarm, isNew, params)

    # handle the alarm
    def process_alarm(self, theAlarm, isNew):
        """
        Handle the alarm.

        :type theAlarm: :class:`alarm.Alarm`
        :arg theAlarm: alarm
        :type isNew: :obj:`bool`
        :arg isNew: indicates wheter the alarm is new.
        """

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

    def find_alarm(self, host, severity, root_error, source, alarm_info,
                   condition, remedy_type):
        """
        Find the alarm that matches the above information.

        :type host: :obj:`str`
        :arg host: hostname
        :type severity: :obj:`str`
        :arg severity: alarm severity
        :type severity: :obj:`str`
        :arg severity: alarm severity
        :type source: :obj:`str`
        :arg source: alarm source
        :type alarm_info: :obj:`str`
        :arg alarm_info: alarm details
        :type condition: :obj:`str`
        :arg condition: alarm condition
        :type remedy_type: :obj:`str`
        :arg remedy_type: alarm remedy type
        """

        ids = self.alarms.keys()
        for an_id in ids:
            if self.alarms[an_id].compare(host, severity, root_error, source, \
                                       alarm_info, condition, remedy_type) == alarm.MATCH:
                return self.alarms[an_id]
        ids = self.info_alarms.keys()
        for an_id in ids:
            if self.info_alarms[an_id].compare(host, severity, root_error, source, \
                                            alarm_info, condition, remedy_type) == alarm.MATCH:
                return self.info_alarms[an_id]
        return None


    # raise the alarm
    def alarm(self, severity=e_errors.DEFAULT_SEVERITY,
              root_error=e_errors.DEFAULT_ROOT_ERROR,
              pid=alarm.DEFAULT_PID, uid=alarm.DEFAULT_UID,
              source=alarm.DEFAULT_SOURCE,
              condition=alarm.DEFAULT_CONDITION,
              remedy_type=alarm.DEFAULT_TYPE,
              alarm_info=None):
        """
        Raise the alarm.

        :type severity: :obj:`str`
        :arg severity: alarm severity
        :type root_error: :obj:`str`
        :arg root_error: alarm cause
        :type pid: :obj:`int`
        :arg pid: alarm client process id
        :type uid: :obj:`int`
        :arg uid: alarm client user id
        :type source: :obj:`str`
        :arg source: alarm source
        :type condition: :obj:`str`
        :arg condition: alarm condition
        :type remedy_type: :obj:`str`
        :arg remedy_type: alarm remedy type
        :type alarm_info: :obj:`str`
        :arg alarm_info: alarm details
        :rtype: :class:`alarm.Alarm` alarm object
        """

        if alarm_info is None:
            alarm_info = {}
        # find out where the alarm came from
	host = hostaddr.address_to_name(self.reply_address[0])
        # we should only get a new alarm if this is not the same alarm as
        # one we already have
        theAlarm = self.find_alarm(host, severity, root_error, source,
                                   alarm_info, condition, remedy_type)
        if not theAlarm:
            # get a new alarm
            theAlarm = alarm.Alarm(host, severity, root_error, pid, uid,
                                   source, condition, remedy_type, alarm_info)
            # process the alarm depending on the action
            self.process_alarm(theAlarm, 1)
	else:
            # this may be the same as an old alarm except now we need
            # to generate a ticket
            theAlarm.set_ticket(condition, remedy_type)
            # process the alarm depending on the action
            self.process_alarm(theAlarm, 0)

        Trace.trace(20, repr(theAlarm.list_alarm()))
        return theAlarm

    def resolve(self, id):
        """
        Resolve alarm.
        An alarm is being resolved, we must do the following -

             1. Remove it from our alarm dictionary.

             2. Rewrite the entire enstore_alarm file (txt and html).

             3. Log this fact.

        :type id: :obj:`int`
        :arg id: alarm id
        :rtype: :obj:`tuple` - (:obj:`str`, :obj:`str`)
        """

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

    def resolve_all(self):
        """
        Resolve all alarms.
        :rtype: :obj:`tuple` - (:obj:`str`, :obj:`str`)
        """

        self.alarms = {}
        self.write_alarm_file()
        self.write_html_file()
        Trace.log(e_errors.INFO, "All Alarms have been resolved")
        return (e_errors.OK, None)

    def resolve_alarm(self, ticket):
        """
        Resolve alarm work.

        :type ticket: :obj:`dict`
        :arg ticket: client ticket with "resolve_alarm" work
        """

        # get the unique identifier for this alarm
        this_id = ticket.get(enstore_constants.ALARM, 0)
	ticket = {}
	if this_id == enstore_html.RESOLVEALL:
            ticket['ALL'] = self.resolve_all()
	else:
	    ticket[this_id] = self.resolve(this_id)

        # send the reply to the client
        self.send_reply(ticket)

    def dump(self, ticket):
        """
        Print all current alarms.

        :type ticket: :obj:`dict`
        :arg ticket: client ticket with "dump" work
        """
	# dump our brains
	for key in self.alarms.keys():
	    print self.alarms[key]
        # send the reply to the client
        ticket['status'] = (e_errors.OK, None)
        self.send_reply(ticket)

    def write_alarm_file(self, alarm=None):
        """
        Write alarm to sorted alarms file.

        :type alarm: :obj:`dict`
        :arg alarm: alarm dictionary
        """

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

    def write_html_file(self):
        """
        Create the web page that contains the list of raised alarms.
        """

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

    def get_alarm_file(self):
        """
        Read the persistent alarm file if it exists.  this reads in all the
        alarms that have not been resolved.
        """

        # the alarm file lives in the same directory as the log file
        self.alarm_file = enstore_files.EnAlarmFile("%s/%s"%(self.get_log_path(),
							     DEFAULT_FILE_NAME))
        self.alarm_file.open('r')
        self.alarms = self.alarm_file.read()
        self.alarm_file.close()


class AlarmServer(AlarmServerMethods, generic_server.GenericServer):
    """
    Implements alarm server functionality.
    """
    def __init__(self, csc):
        """
        :type csc: :class:`configuraion_client.ConfigurationClient`
        :arg csc: configuration client or :obj:`tuple` configuration server address
        """

        # actions supported by the alarm server.  these actions can be
        # specified in the config file for specific severities.  if no
        # action is specified, the default action is to display the alarm
        # on the alarm page
        self.ALARM_ACTIONS = {'html_page' : self.default_action,
                              'send_mail' : self.send_mail_action,
                              }

        generic_server.GenericServer.__init__(self, csc, MY_NAME,
                                              function = self.handle_er_msg,
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

        # setup the communications with the event relay task
        self.erc.start([event_relay_messages.NEWCONFIGFILE])
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
