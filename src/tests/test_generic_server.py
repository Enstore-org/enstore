import unittest
import generic_server
import sys
import string
import socket
import Trace
import traceback
import timeofday
import mock
import StringIO
import e_errors
import option
import generic_client
import generic_server
import mock_csc
import event_relay_client
import event_relay_messages
import enstore_constants
import enstore_erc_functions
import hostaddr
import udp_client


class TestServerError(unittest.TestCase):
    def setUp(self):
        self.err = generic_server.ServerError('test')

    def test___repr__(self):
        self.assertEqual(self.err.__repr__(), "ServerError")


class TestGenericServerInterface(unittest.TestCase):
    def setUp(self):
        self.gsi = generic_server.GenericServerInterface()

    def test___init__(self):
        self.assertTrue(isinstance(self.gsi, option.Interface))

    def test_valid_dictionaries(self):
        self.assertEqual(
            self.gsi.valid_dictionaries(),
            (self.gsi.help_options,
             self.gsi.trace_options))


class Msg(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class TestGenericServer(unittest.TestCase):
    def setUp(self):
        self.sent_msg = mock.MagicMock()
        udp_client.UDPClient.send = self.sent_msg
        udp_client.UDPClient.send_no_wait = self.sent_msg
        csc = mock_csc.csc()
        self.csc = mock_csc.csc()
        self.svr = generic_server.GenericServer(self.csc, 'test_server')

    def test_handle_er_msg(self):
        with mock.patch('enstore_erc_functions.read_erc') as read_erc:
            read_erc.return_value = Msg(
                type=event_relay_messages.NEWCONFIGFILE)
            with mock.patch('generic_server.GenericServer._reinit2') as _reinit2:
                self.svr.handle_er_msg(1)
                self.assertTrue(_reinit2.called)

    def test__reinit2(self):
        log_contents = 'Received notification of new configuration file.'
        with mock.patch('Trace.log') as log:
            with mock.patch('generic_server.GenericServer._reinit') as _reinit:
                self.svr._reinit2()
                log_str = str(log.mock_calls)
                self.assertTrue(log_contents in log_str, log_str)
                self.assertTrue(_reinit.called)

    def test__reinit(self):
        with mock.patch('hostaddr.update_domains'):
            with mock.patch('generic_server.GenericServer.reinit') as reinit:
                with mock.patch('Trace.log') as log:
                    self.svr._reinit()
                    self.assertTrue(reinit.called)
                    self.assertTrue(log.called)

    def test___init__(self):
        self.assertTrue(isinstance(self.svr, generic_client.GenericClient))
        self.assertTrue(isinstance(self.svr, generic_server.GenericServer))

    def test_handle_generic_commands(self):
        gsi = generic_server.GenericServerInterface()
        with mock.patch('Trace.do_print') as do_print:
            with mock.patch('Trace.dont_print')as dont_print:
                with mock.patch('Trace.do_log') as do_log:
                    with mock.patch('Trace.dont_log') as dont_log:
                        with mock.patch('Trace.do_alarm') as do_alarm:
                            with mock.patch('Trace.dont_alarm') as dont_alarm:
                                self.svr.handle_generic_commands(gsi)
                                self.assertFalse(do_print.called)
                                self.assertFalse(dont_print.called) 
                                self.assertFalse(do_log.called) 
                                self.assertFalse(dont_log.called)   
                                self.assertFalse(do_alarm.called)   
                                self.assertFalse(dont_alarm.called)
                                gsi.do_print.append('test')
                                gsi.dont_print.append('test')
                                gsi.do_log.append('test')   
                                gsi.dont_log.append('test') 
                                gsi.do_alarm.append('test') 
                                gsi.dont_alarm.append('test')   
                                self.svr.handle_generic_commands(gsi)
                                self.assertTrue(do_print.called)
                                self.assertTrue(dont_print.called)
                                self.assertTrue(do_log.called)
                                self.assertTrue(dont_log.called)
                                self.assertTrue(do_alarm.called)
                                self.assertTrue(dont_alarm.called)


    def test_get_log_name(self):
        logn = self.svr.get_log_name('foo.bar')
        self.assertEqual(logn, 'FOO.BAR')
        logn = self.svr.get_log_name('foo.bar.baz')
        self.assertEqual(logn, 'FOO.BAR.BAZ')

    def test_get_name(self):
        nm = self.svr.get_name('foo')
        self.assertEqual(nm,'FOO',nm)

    def test_server_bind(self):
        socket = mock.MagicMock()
        setsockopt = mock.MagicMock()
        bind = mock.MagicMock()
        socket.setsockopt = setsockopt
        socket.bind = bind
        self.svr.socket = socket
        self.svr.server_bind()
        self.assertTrue(setsockopt.called)
        self.assertTrue(bind.called)


    def test_serve_forever_error(self):
        with mock.patch('Trace.alarm') as alarm:
            self.svr.serve_forever_error('whoopsie')
            alarm.assert_called_with(e_errors.ALARM,
                'Exception in file ??? at line -1: (None, None).  See system log for details.')



    def test_get_alive_interval(self):
        self.assertEqual(40,self.svr.get_alive_interval())

    def test_event_relay_subscribe(self):
        erc = mock.MagicMock()
        start = mock.MagicMock()
        erc.start = start
        start_heartbeat = mock.MagicMock()
        erc.start_heartbeat = start_heartbeat
        self.svr.erc = erc
        self.svr.event_relay_subscribe(['foo'])
        self.assertTrue(start.called)
        self.assertTrue(start_heartbeat.called)


    def test_event_relay_unsubscribe(self):
        erc = mock.MagicMock()
        stop = mock.MagicMock()
        erc.stop = stop
        stop_heartbeat = mock.MagicMock()
        erc.stop_heartbeat = stop_heartbeat
        self.svr.erc = erc
        self.svr.event_relay_unsubscribe()
        self.assertTrue(stop.called)
        self.assertTrue(stop_heartbeat.called)


if __name__ == "__main__":
    unittest.main()
