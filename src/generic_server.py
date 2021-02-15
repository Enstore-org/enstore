#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# Generic server class for enstore
#
###############################################################################

# system imports
import sys
import string
import socket

# enstore imports
import Trace
import traceback
import timeofday
import e_errors
import option
import generic_client
import event_relay_client
import event_relay_messages
import enstore_constants
import enstore_erc_functions
import hostaddr


class ServerError(generic_client.ClientError):
    def __repr__(self):
        return "ServerError"


class GenericServerInterface(option.Interface):

    def __init__(self):
        self.do_print = []
        self.dont_print = []
        self.do_log = []
        self.dont_log = []
        self.do_alarm = []
        self.dont_alarm = []
        self.help = 0
        self.usage = 0
        option.Interface.__init__(self)

    def valid_dictionaries(self):
        return (self.help_options, self.trace_options)


class GenericServer(generic_client.GenericClient):

    def handle_er_msg(self, fd):
        __pychecker__ = "no-argsused"

        msg = enstore_erc_functions.read_erc(self.erc)
        if msg and msg.type == event_relay_messages.NEWCONFIGFILE:
            self._reinit2()

        return msg

    def _reinit2(self):
        Trace.log(e_errors.INFO,
                  "Received notification of new configuration file.")
        self._reinit()

    def _reinit(self):
        Trace.log(e_errors.INFO, "(Re)loading configuration")

        self.csc.new_config_obj.new_config_msg()
        try:
            hostaddr.update_domains(self.csc)
        except AttributeError:
            # The configuration server itself will fall here.
            # It can't create a client to itself.  However, the
            # configuration server should never call this function
            # either.
            pass

        # Individually defined actions for each Enstore server.
        self.reinit()

    def reinit(self):
        # Need to override.
        pass

    def __init__(self, csc, name, function=None, flags=0,
                 logc=None, alarmc=None):

        # make pychecker happy
        self.socket = None

        # do this in order to centralize getting a log, alarm and configuration
        # client. and to record the fact that we only want to do it once.
        use_flags = enstore_constants.NO_UDP | flags
        generic_client.GenericClient.__init__(self, csc, name,
                                              flags=use_flags,
                                              logc=logc, alarmc=alarmc)

        # Servers need to communicate with the event relay.  Instantiate the
        # event relay client class to facilitate that communication.
        self.erc = event_relay_client.EventRelayClient(self, function)

        # We want the servers to cache the config file contents, because
        # they can wait for the NEWCONFIGFILE message from the event relay.
        try:
            self.csc.new_config_obj.enable_caching()
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()
        except NameError:
            # When 'self' is the configuration server, self.csc does not exist.
            # However, the configuration server does not use this __init__
            # function, so it should never happen...
            Trace.log(e_errors.WARNING, "Configuration server calling itself.")
        except BaseException:
            Trace.log(e_errors.WARNING, "Unable to cache configuration.")

    __pychecker__ = "no-override"

    def handle_generic_commands(self, intf):
        if intf.do_print:
            Trace.do_print(intf.do_print)
        if intf.dont_print:
            Trace.dont_print(intf.dont_print)
        if intf.do_log:
            Trace.do_log(intf.do_log)
        if intf.dont_log:
            Trace.dont_log(intf.dont_log)
        if intf.do_alarm:
            Trace.do_alarm(intf.do_alarm)
        if intf.dont_alarm:
            Trace.dont_alarm(intf.dont_alarm)

    # given a server name, return the name mutated into a name appropriate for
    # identification in log and trace. this means, upcase the name and possibly
    # shorten it.  so if the name can be split into 'part1.part2', shorten
    # part1 to only be 8 characters.  also if there is an alternate part2
    # specified in the instance, use it.
    def get_log_name(self, name):
        parts = string.split(name, '.')
        if len(parts) == 2:
            new_name = "%s.%s" % (string.upper(parts[0][0:8]),
                                  string.upper(self.__dict__.get("name_ext",
                                                                 parts[1])))
        else:
            new_name = string.upper(name)
        return new_name

    # return the server name
    def get_name(self, name):
        config = self.csc.get(name)
        if not config['status'][0] == 'KEYERROR':
            name = config.get('logname', self.get_log_name(name))
        else:
            name = self.get_log_name(name)
        return name

    # this overrides the server_bind in TCPServer for the hsm system
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

    # we got an uncaught error while in serve_forever
    def serve_forever_error(self, id):
        # Get the traceback information.
        exc, msg, tb = sys.exc_info()
        # Extract filename and line number information.
        try:
            filename = tb.tb_frame.f_code.co_filename
            if not filename or not isinstance(filename, type("")):
                filename = "???"
        except BaseException:
            filename = "???"
        try:
            lineno = tb.tb_lineno
        except BaseException:
            lineno = -1

        # Format the error message.
        message = "Exception in file %s at line %s: (%s, %s)." \
                  "  See system log for details." % \
                  (filename, lineno, exc, msg)

        # Log the error to stdout and to the log server.
        Trace.trace(e_errors.ERROR, str(message))
        Trace.alarm(e_errors.ALARM, str(message))

        message2 = "%s argv: %s" % (id, sys.argv)
        Trace.log(e_errors.INFO, message2)

        # Be sure to include a traceback in the log file.
        Trace.handle_error(exc, msg, tb)

        del tb  # Avoid resource leak.

    """
    # send back our response
    def send_reply(self, t):
        try:
            self.reply_to_caller(t)
        except:
            # even if there is an error - respond to caller so he can process it
            exc, msg = sys.exc_info()[:2]
            t["status"] = (str(exc),str(msg))
            self.reply_to_caller(t)
            Trace.trace(enstore_constants.DISPWORKDBG,
                        "exception in send_reply %s" % (t,))
            return
    """

    # get the alive_interval from the server or the default from the inquisitor
    DEFAULT_ALIVE_INTERVAL = 30

    def get_alive_interval(self):
        config = self.csc.get(self.name)
        alive_interval = config.get(enstore_constants.ALIVE_INTERVAL, None)
        if not alive_interval:
            # see if the default is in the inquisitor config dict
            iconfig = self.csc.get(enstore_constants.INQUISITOR)
            alive_interval = iconfig.get(enstore_constants.DEFAULT_ALIVE_INTERVAL,
                                         self.DEFAULT_ALIVE_INTERVAL)
        return alive_interval

    def event_relay_subscribe(self, message_type_list):
        # setup the communications with the event relay task
        self.erc.start(message_type_list)
        # start our heartbeat to the event relay process
        self.alive_interval = self.get_alive_interval()
        self.erc.start_heartbeat(self.name,
                                 self.alive_interval)

    def event_relay_unsubscribe(self):
        # stop the communications with the event relay task
        self.erc.stop()
        # stop our heartbeat to the event relay process
        self.erc.stop_heartbeat()
