import unittest
import sys
import traceback
import os
import mock
import StringIO
import pwd
import time
import types
import threading
import event_relay_messages
import event_relay_client
import e_errors
import enstore_constants

import Trace
from Trace import *

DONT_ALARM_CAPABLE_LIST = [e_errors.EMAIL, e_errors.ERROR, e_errors.USER_ERROR]
# an alarm function to test Trace.py with


def alarm_func_0(time, pid, name, root_error,
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

    # translate severity to text
    if isinstance(severity, types.IntType):
        severity = e_errors.sevdict.get(severity,
                                        e_errors.sevdict[e_errors.ERROR])
    ticket = {}
    ticket['work'] = "post_alarm"
    ticket[enstore_constants.UID] = os.getuid()
    ticket[enstore_constants.PID] = pid
    ticket[enstore_constants.SOURCE] = name
    ticket[enstore_constants.SEVERITY] = severity
    ticket[enstore_constants.ROOT_ERROR] = root_error
    ticket[enstore_constants.CONDITION] = condition
    ticket[enstore_constants.REMEDY_TYPE] = remedy_type
    ticket['text'] = args
    log_msg = "%s, %s (severity : %s)" % (root_error, args, severity)

    Trace.log(e_errors.ALARM, log_msg, Trace.MSG_ALARM)

# a log function to test Trace.py with


def log_func_0(timestamp, pid, name, args):
    # Even though this implimentation of log_func() does not use the time
    # parameter, others will.
    __pychecker__ = "unusednames=time"

    severity = args[0]
    msg = args[1]
    logmsg = 'log_func_0 %s %.6d %.8s %s %s  %s' % (time.ctime(
        timestamp), pid, usr, e_errors.sevdict[severity], name, msg)
    if severity in Trace.STDERR_SEVERITIES:
        print >> sys.stderr, logmsg
    else:
        print logmsg
    return None


class TestTrace(unittest.TestCase):
    def setUp(self):
        self.logname = 'TraceUnitTest'
        self.threadname = 'MainThread'
        init(self.logname, 'yes')

    def test_get_set_logname(self):
        lg = get_logname()
        self.assertEqual(lg, self.logname)
        set_logname('FOO')
        lg = get_logname()
        self.assertEqual(lg, 'FOO')

    def test_get_threadname(self):
        tn = get_threadname()
        self.assertEqual(tn, self.threadname)

    def test_log_thread(self):
        log_thread(1)
        self.assertTrue(Trace.include_threadname)
        log_thread(0)
        self.assertFalse(Trace.include_threadname)

    def test_notify(self):
        msg = 'this is a message'
        notify(msg)

    def test_log_functionality(self):
        msg = 'this is  a log msg'
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            log(e_errors.INFO, msg)
            self.assertTrue(msg in std_out.getvalue(), std_out.getvalue())
        set_log_func(log_func_0)
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            log(e_errors.INFO, msg)
            self.assertTrue(msg in std_out.getvalue(), std_out.getvalue())
            self.assertTrue(
                'log_func_0' in std_out.getvalue(),
                std_out.getvalue())

    def test_log_to_stderr(self):
        msg = 'this is  a log msg'
        for severity in Trace.STDERR_SEVERITIES:
            with mock.patch('sys.stderr', new=StringIO.StringIO()) as std_err:
                log(severity, msg)
                self.assertTrue(msg in std_err.getvalue(), std_err.getvalue())
            set_log_func(log_func_0)
            with mock.patch('sys.stderr', new=StringIO.StringIO()) as std_err:
                log(severity, msg)
                self.assertTrue(msg in std_err.getvalue(), std_err.getvalue())
                self.assertTrue(
                    'log_func_0' in std_err.getvalue(),
                    std_err.getvalue())

    def test_alarm_functionality(self):
        msg = 'this is  an alarm!!!'
        # test default alarm function
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            alarm(e_errors.ALARM, msg)
            self.assertTrue(
                'default_alarm_func' in std_out.getvalue(),
                std_out.getvalue())

        # test setting custom alarm
        set_alarm_func(alarm_func_0)
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            alarm(e_errors.ALARM, msg)
            self.assertTrue(msg in std_out.getvalue(), std_out.getvalue())

    def test_do_and_dont_print(self):
        self.assertEqual(len(print_levels.keys()), 0)
        do_print(Trace.STDERR_SEVERITIES)
        self.assertEqual(len(print_levels.keys()),
                         len(Trace.STDERR_SEVERITIES))
        dont_print(Trace.STDERR_SEVERITIES)
        self.assertEqual(len(print_levels.keys()), 0)

    def test_do_and_dont_log(self):
        self.assertEqual(len(log_levels.keys()), 0)
        do_log(Trace.STDERR_SEVERITIES)
        self.assertEqual(len(log_levels.keys()), len(Trace.STDERR_SEVERITIES))
        dont_log(Trace.STDERR_SEVERITIES)
        self.assertEqual(len(log_levels.keys()), 0)

    def test_do_and_dont_message(self):
        self.assertEqual(len(message_levels.keys()), 0)
        do_message(Trace.STDERR_SEVERITIES)
        self.assertEqual(len(message_levels.keys()),
                         len(Trace.STDERR_SEVERITIES))
        try:
            dont_message(Trace.STDERR_SEVERITIES)
            self.assertTrue(False)
        except ValueError:
            pass
        dont_message(DONT_ALARM_CAPABLE_LIST)
        self.assertEqual(len(message_levels.keys()), 1)

    def test_do_and_dont_alarm(self):
        self.assertEqual(len(alarm_levels.keys()), 0)
        do_alarm(Trace.STDERR_SEVERITIES)
        self.assertEqual(len(alarm_levels.keys()),
                         len(Trace.STDERR_SEVERITIES))
        try:
            dont_alarm(Trace.STDERR_SEVERITIES)
            self.assertTrue(False)
        except ValueError:
            pass
        dont_alarm(DONT_ALARM_CAPABLE_LIST)
        self.assertEqual(len(alarm_levels.keys()), 1)

    def test_trunc_length(self):
        msg = "this is a very very very "
        msg += "long message.  It has no "
        msg += "real  value other than to"
        msg += "be on the verbose side.  "
        msg += "It is possible that in   "
        msg += "the future some  AI will "
        msg += "attempt to analyze this, "
        msg += "perhaps drawing the wrong"
        msg += "conclusions from a spurio"
        msg += "ous correlation. Oh well."

        len1 = len(msg)
        max_msg_size = set_max_message_size(len1 / 2)
        msg2 = trunc(msg)
        len2 = len(msg2)
        self.assertTrue(len2 < len1)

    def test_trace(self):
        ticket = {'status': 'test'}
        dont_print(1)
        do_alarm(1)
        do_log(1)
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            with mock.patch('sys.stderr', new=StringIO.StringIO()) as std_err:
                trace(1, 'a spurious test message: %s' % (ticket,))
                self.assertTrue(
                    'spurious' in std_err.getvalue(), "t2:%s" %
                    (std_err.getvalue()))

    def test_flush_and_sync(self):
        flush_and_sync(sys.stdout)

    def test_handle_error(self):
        try:
            dont_alarm(Trace.STDERR_SEVERITIES)
        except BaseException:
            with mock.patch('sys.stdout', new=StringIO.StringIO()) as _out:
                Trace.handle_error()
                self.assertTrue(
                    'ValueError' in _out.getvalue(),
                    _out.getvalue())

    def test_log_stack_trace(self):
        try:
            with open('/var/lib/does/not/exist', 'r') as not_there:
                lines = not_there.readlines()
        except BaseException:
            with mock.patch('sys.stderr', new=StringIO.StringIO()) as _out:
                Trace.log_stack_trace()
                self.assertTrue(
                    'Failure writing message to log' in _out.getvalue(),
                    _out.getvalue())
                self.assertTrue(
                    'in log_stack_trace' in _out.getvalue(),
                    _out.getvalue())
                self.assertTrue(
                    'in test_log_stack_trace' in _out.getvalue(),
                    _out.getvalue())


if __name__ == "__main__":
    unittest.main()
