#!/usr/bin/env python
"""
Alarm client. Sends alarms to alarm server.
"""

# system imports
import sys
import os
import pwd
import errno
import types

# enstore imports
import generic_client
import option
import enstore_constants
import alarm
import Trace
import e_errors

MY_NAME = enstore_constants.ALARM_CLIENT  # "ALARM_CLIENT"
MY_SERVER = enstore_constants.ALARM_SERVER  # "alarm_server"

RCV_TIMEOUT = 5
RCV_TRIES = 2


class Lock:
    def __init__(self):
        self.locked = 0

    def unlock(self):
        self.locked = 0
        return None

    def test_and_set(self):
        s = self.locked
        self.locked = 1
        return s


class AlarmClient(generic_client.GenericClient):
    """
    Implements a client of alarm server.
    """

    def __init__(self, csc, name=MY_NAME, server_name=MY_SERVER,
                 server_address=None, flags=0, logc=None,
                 rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES):
        """
        :type csc: :class:`configuraion_client.ConfigurationClient`
        :arg csc: configuration client or :obj:`tuple` configuration server address
        :type name: :obj:`str`
        :arg name: client name
        :type server_name: :obj:`str`
        :arg server_name: server name
        :type server_address: :obj:`tuple`
        :arg server_address: server address
        :type flags: :obj:`int`
        :arg flags: special flags (see :class:`generic_client.GenericClient`)
        :type logc: :class:`log_client.LogClient`
        :arg logc: log client
        :type rcv_timeout: :obj:`float`
        :type rcv_timeout: :obj:`float`
        :type rcv_tries: :obj:`int`
        :arg rcv_tries: numbre of send retries
        """

        # need the following definition so the generic client init does not
        # get another alarm client
        flags = flags | enstore_constants.NO_ALARM
        generic_client.GenericClient.__init__(self, csc, name, server_address,
                                              flags=flags, logc=logc,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              server_name=server_name)

        try:
            self.uid = pwd.getpwuid(os.getuid())[0]
        except:
            self.uid = "unknown"
        #self.server_address = self.get_server_address(servername, rcv_timeout,
        #                                              rcv_tries)
        self.rcv_timeout = rcv_timeout
        self.rcv_tries = rcv_tries
        Trace.set_alarm_func(self.alarm_func)
        self.alarm_func_lock = Lock()

    def alarm_func(self, time, pid, name, root_error,
                   severity, condition, remedy_type, args):
        """
        Alarm function

        :type time: :obj:`float`
        :arg time: time issued. Even though this implementation of alarm_func() does not use the time
                   parameter, others will.
        :type pid: :obj:`int`
        :arg pid: process id
        :type name: :obj:`str`
        :arg name: client name
        :type root_error: :obj:`str`
        :arg root_error: alarm cause
        :type severity: :obj:`str`
        :arg severity: alarm severity
        :type condition: :obj:`str`
        :arg condition: alarm condition
        :type remedy_type: :obj:`str`
        :arg remedy_type: alarm remedy type
        :type args: :obj:`list`
        :arg args: additional arguments
        """

        __pychecker__ = "unusednames=time"

        # prevent infinite recursion (i.e if some function call by this
        # function does a trace and the alarm bit is set
        if self.alarm_func_lock.test_and_set(): return None
        # translate severity to text
        if type(severity) == types.IntType:
            severity = e_errors.sevdict.get(severity,
                                            e_errors.sevdict[e_errors.ERROR])
        ticket = {}
        ticket['work'] = "post_alarm"
        ticket[enstore_constants.UID] = self.uid
        ticket[enstore_constants.PID] = pid
        ticket[enstore_constants.SOURCE] = name
        ticket[enstore_constants.SEVERITY] = severity
        ticket[enstore_constants.ROOT_ERROR] = root_error
        ticket[enstore_constants.CONDITION] = condition
        ticket[enstore_constants.REMEDY_TYPE] = remedy_type
        ticket['text'] = args
        log_msg = "%s, %s (severity : %s)" % (root_error, args, severity)

        #self.send(ticket, self.rcv_timeout, self.rcv_tries )
        self.u.send_no_wait(ticket, self.server_address, unique_id=True)
        # log it for posterity
        Trace.log(e_errors.ALARM, log_msg, Trace.MSG_ALARM)
        return self.alarm_func_lock.unlock()

    def alarm(self, severity=e_errors.DEFAULT_SEVERITY, \
              root_error=e_errors.DEFAULT_ROOT_ERROR,
              alarm_info=None, condition=None,
              remedy_type=None):
        """
        Send alarm

        :type severity: :obj:`str`
        :arg severity: alarm severity
        :type root_error: :obj:`str`
        :arg root_error: alarm cause
        :type alarm_info: :obj:`str`
        :arg alarm_info: alarm details
        :type condition: :obj:`str`
        :arg condition: alarm condition
        :type remedy_type: :obj:`str`
        :arg remedy_type: alarm remedy type
        """

        if alarm_info is None:
            alarm_info = {}
        Trace.alarm(severity, root_error, alarm_info, condition, remedy_type)

    def resolve(self, id):
        """
        This alarm has been resolved.  we need to tell the alarm server

        :type id: :obj:`int`
        :arg id: alarm id
        :rtype: :obj:`dict` reply from alarm server
        """

        ticket = {'work': "resolve_alarm",
                  enstore_constants.ALARM: id}
        return self.send(ticket, self.rcv_timeout, self.rcv_tries)

    def get_patrol_file(self):
        """
        This functionality is deprecated
        """
        ticket = {'work': 'get_patrol_filename'}
        return self.send(ticket, self.rcv_timeout, self.rcv_tries)


class AlarmClientInterface(generic_client.GenericClientInterface):
    """
    Defines alarm client command inteface and legal commands
    """

    def __init__(self, args=sys.argv, user_mode=1):
        #self.do_parse = flag
        #self.restricted_opts = opts
        # fill in the defaults for the possible options
        # we always want a default timeout and retries so that the alarm
        # client/server communications does not become a weak link
        self.alive_rcv_timeout = RCV_TIMEOUT
        self.alive_retries = RCV_TRIES
        self.alarm = 0
        self.resolve = 0
        self.dump = 0
        self.severity = e_errors.DEFAULT_SEVERITY
        self.root_error = e_errors.DEFAULT_ROOT_ERROR
        self.get_patrol_file = 0
        self.client_name = ""
        self.message = ""
        self.condition = alarm.DEFAULT_CONDITION
        self.remedy_type = alarm.DEFAULT_TYPE
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.trace_options, self.alive_options,
                self.alarm_options)

    alarm_options = {
        option.CLIENT_NAME: {option.HELP_STRING: "set alarm client name",
                             option.VALUE_TYPE: option.STRING,
                             option.VALUE_USAGE: option.REQUIRED,
                             option.VALUE_LABEL: "client_name",
                             option.USER_LEVEL: option.ADMIN,
                             },
        option.CONDITION: {option.HELP_STRING: "condition used when generating a remedy ticket",
                           option.VALUE_TYPE: option.STRING,
                           option.VALUE_USAGE: option.REQUIRED,
                           option.VALUE_LABEL: "condition",
                           option.USER_LEVEL: option.ADMIN,
                           },
        option.DUMP: {option.HELP_STRING:
                      "print (stdout) alarms the alarm server has in memory",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.VALUE_USAGE: option.IGNORED,
                      option.USER_LEVEL: option.ADMIN,
                      },
        option.MESSAGE: {option.HELP_STRING: "message along with raise option",
                         option.VALUE_TYPE: option.STRING,
                         option.VALUE_USAGE: option.REQUIRED,
                         option.VALUE_LABEL: "message",
                         option.USER_LEVEL: option.ADMIN,
                         },
        option.RAISE: {option.HELP_STRING: "raise an alarm",
                       option.DEFAULT_TYPE: option.INTEGER,
                       option.DEFAULT_VALUE: option.DEFAULT,
                       option.DEFAULT_NAME: "alarm",
                       option.VALUE_USAGE: option.IGNORED,
                       option.USER_LEVEL: option.ADMIN,
                       },
        option.RESOLVE: {option.HELP_STRING:
                         "resolve the previously raised alarm whose key "
                         "matches the entered value",
                         option.VALUE_TYPE: option.STRING,
                         option.VALUE_USAGE: option.REQUIRED,
                         option.VALUE_LABEL: "key",
                         option.USER_LEVEL: option.ADMIN,
                         },
        option.ROOT_ERROR: {option.HELP_STRING:
                            "error which caused an alarm to be raised "
                            "[D: UNKONWN]",
                            option.VALUE_TYPE: option.STRING,
                            option.VALUE_USAGE: option.REQUIRED,
                            option.USER_LEVEL: option.ADMIN,
                            },
        option.SEVERITY: {option.HELP_STRING: "severity of raised alarm "
                          "(E, U, W, I, M, C)[D: W]",
                          option.VALUE_NAME: "severity",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.VALUE_LABEL: "severity",
                          option.USER_LEVEL: option.ADMIN,
                          },
        option.REMEDY_TYPE: {option.HELP_STRING: "type used when generating a remedy ticket",
                             option.VALUE_TYPE: option.STRING,
                             option.VALUE_USAGE: option.REQUIRED,
                             option.VALUE_LABEL: "remedy_type",
                             option.USER_LEVEL: option.ADMIN,
                             },
    }


def do_work(intf):
    # get an alarm client name
    if intf.client_name:
        name = intf.client_name
    else:
        name = MY_NAME
    Trace.init(name)
    # now get an alarm client
    alc = AlarmClient((intf.config_host, intf.config_port),
                      rcv_timeout=intf.alive_rcv_timeout,
                      rcv_tries=intf.alive_retries, name=name)
    ticket = alc.handle_generic_commands(MY_SERVER, intf)
    if ticket:
        pass

    elif intf.resolve:
        ticket = alc.resolve(intf.resolve)

    elif intf.dump:
        ticket = alc.dump()

    elif intf.alarm:
        alc.alarm(intf.severity, intf.root_error, intf.message,
                  intf.condition, intf.remedy_type)
        ticket = {}

    else:
        intf.print_help()
        sys.exit(0)
    alc.check_ticket(ticket)


if __name__ == "__main__":   # pragma: no cover
    intf = AlarmClientInterface(user_mode=0)

    do_work(intf)
