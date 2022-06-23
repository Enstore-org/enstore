"""
test_generic_client.py
unit tests for enstore/src/generic_client.py
Author Dennis Box, dbox@fnal.gov
"""
import unittest
import generic_client
import sys
import errno
import types
import os
import string
import socket
import select
import Trace
import e_errors
import option
import udp_client
import enstore_constants
import enstore_functions2
import configuration_client
import callback
import hostaddr
import mock
import StringIO
import fixtures.config.conf

class TestClientError(unittest.TestCase):
    def setUp(self):
        self.cerr = generic_client.ClientError('something is wrong')
	self.cerr2 = generic_client.ClientError('something else is wrong', 3)
        self.cerr3 = generic_client.ClientError('some other thing  is wrong', 5, e_errors.WRONGPARAMETER)

    def test___init__(self):
        self.assertTrue(isinstance(self.cerr, generic_client.ClientError))
        self.assertTrue(isinstance(self.cerr2, generic_client.ClientError))
        self.assertTrue(isinstance(self.cerr3, generic_client.ClientError))

    def test___str__(self):
        a_str = self.cerr.__str__()
        self.assertEqual('something is wrong', a_str)
        b_str = self.cerr2.__str__()
        self.assertEqual('ESRCH: [ ERRNO 3 ] No such process: something else is wrong', b_str)
        c_str = self.cerr3.__str__()
        self.assertEqual('EIO: [ ERRNO 5 ] Input/output error: some other thing  is wrong', c_str)

    def test___repr__(self):
        self.assertEqual("ClientError", self.cerr.__repr__())
        self.assertEqual("ClientError", self.cerr2.__repr__())
        self.assertEqual("ClientError", self.cerr3.__repr__())

class TestGenericClientInterface(unittest.TestCase):
    def setUp(self):
        self.gci = generic_client.GenericClientInterface()

    def test___init__(self):
        self.assertTrue(isinstance(self.gci, generic_client.GenericClientInterface))

    def test_client_options(self):
        co = self.gci.client_options()
        self.assertEqual(len(co), 3)

    def test_complete_server_name(self):
        csn = self.gci.complete_server_name('rain', 'mover')
        self.assertEqual('rain.mover', csn)
        csn = self.gci.complete_server_name('rain.mover', 'mover')
        self.assertEqual('rain.mover', csn)
        csn = self.gci.complete_server_name('', 'mover')
        self.assertEqual('', csn)


class TestGenericClient(unittest.TestCase):
    def setUp(self):
        self._mocker = mock.MagicMock()
        udp_client.UDPClient.send = self._mocker
        self.csc = configuration_client.ConfigurationClient()
        self.flags = 0 | enstore_constants.NO_LOG | enstore_constants.NO_ALARM
        self.name = 'test.generic.client'
        self.gc = generic_client.GenericClient(self.csc, self.name, flags=self.flags)

    def test___init__(self):
        self.assertTrue(isinstance(self.gc, generic_client.GenericClient))

    def test__is_csc(self):
        self.assertEqual(0, self.gc._is_csc())

    def test__get_csc(self):
        self.assertTrue(isinstance(self.gc._get_csc(), configuration_client.ConfigurationClient))

    def test_get_server_configuration(self):
        cnf1 = self.gc.get_server_configuration(enstore_constants.CONFIGURATION_SERVER)
        self.assertTrue(e_errors.is_ok(cnf1))
        cnf2 = self.gc.get_server_configuration(enstore_constants.MONITOR_SERVER)
        self.assertTrue(e_errors.is_ok(cnf2))
        self._mocker.reset_mock()
        self._mocker.return_value = {'status': ('ok', None), 'host': 'localhost', 'port': 7500, 'hostip': '127.0.0.1'}
        cnf3 = self.gc.get_server_configuration(self.name)
        expected = "call({'new': 1, 'work': 'lookup', 'lookup': '%s'}, ('localhost', 7500), 0, 0)" % self.name
        self.assertEqual(str(self._mocker.call_args), expected, " test_get_server_configuration error")
        self.assertTrue(e_errors.is_ok(cnf3))

    def test_get_server_address(self):
        addy = self.gc.get_server_address(None)
        self.assertIsNone(addy)
        a,b = self.gc.get_server_address(enstore_constants.CONFIGURATION_SERVER)
        self.assertEqual(type(b), type(3))
        a,b = self.gc.get_server_address(enstore_constants.MONITOR_SERVER)
        self.assertEqual(type(b), type(3))
        self._mocker.reset_mock()
        self._mocker.return_value = {'status': ('ok', None), 'host': 'localhost', 'port': 7500, 'hostip': '127.0.0.1'}
        a,b= self.gc.get_server_address(self.name)
        self.assertEqual(a,'127.0.0.1')
        self.assertEqual(b,7500)

    
    def test_send(self):
        ticket  =  {'new': 1, 'work': 'lookup', 'lookup': self.name }
        expected =  "call(%s, None, 0, 0)" % ticket
        self.gc.send(ticket)
        rslt = str(self._mocker.call_args)
        self.assertEqual(expected, rslt, 'test_send expected %s got %s'%(expected,rslt))
        self._mocker.reset_mock()
        self._mocker.side_effect = KeyboardInterrupt('wait! no!')
        try:
            rslt  = self.gc.send(ticket)
            assertTrue(False)
        except KeyboardInterrupt:
            pass

        self._mocker.reset_mock()
        self._mocker.side_effect =  socket.gaierror('my socket is bad')
        rslt  = self.gc.send(ticket)
        expected = "{'status': ('NET_ERROR', 'None: my socket is bad')}"
        self.assertEqual(str(rslt), expected, 'test_send socket.gaierror  test failed returned %s' % str(rslt))

        self._mocker.reset_mock()
        self._mocker.side_effect =  socket.error('my socket is bad')
        rslt  = self.gc.send(ticket)
        expected = "{'status': ('NET_ERROR', 'None: my socket is bad')}"
        self.assertEqual(str(rslt), expected, 'test_send socket.error  test failed returned %s' % str(rslt))

        self._mocker.reset_mock()
        self._mocker.side_effect =  TypeError("not your type")
        rslt  = self.gc.send(ticket)
        expected = "{'status': ('UNKNOWN', 'None: not your type')}"
        self.assertEqual(str(rslt), expected, 'test_send TypeError test failed returned %s'% str(rslt))
        
        self._mocker.reset_mock()
        self._mocker.side_effect =  ValueError("you have strange values")
        rslt  = self.gc.send(ticket)
        expected = "{'status': ('UNKNOWN', 'None: you have strange values')}"
        self.assertEqual(str(rslt), expected, 'test_send ValueError test failed returned %s' % str(rslt))

    def test_get_name(self):
        expected = self.gc.get_name(self.name)
        self.assertEqual(expected, self.name)

    def test_alive(self):

        self._mocker.reset_mock()
        expected =  "call({'new': 1, 'work': 'lookup', 'lookup': '%s'}, ('localhost', 7500), 0, 0)" % self.name
        self.gc.alive(self.name)
        rcvd = str(self._mocker.call_args)
        self.assertEqual(rcvd, expected, "test_alive 1  rcvd=%s expected=%s"%(rcvd,expected))

        self._mocker.reset_mock()
        a2 = self.gc.alive(enstore_constants.CONFIGURATION_SERVER)
        expected = "call({'work': 'alive'}, ('localhost', 7500), 0, 0)"
        rcvd = str(self._mocker.call_args)
        self.assertEqual(rcvd, expected, "test_alive 2  rcvd=%s expected=%s"%(rcvd,expected))


    def test_handle_generic_commands(self):

        intf = generic_client.GenericClientInterface()
        ret = self.gc.handle_generic_commands(self.name, intf)
        self.assertIsNone(ret)
        self._mocker.reset_mock()

        intf.do_print = 3
        self.gc.handle_generic_commands(self.name, intf)
        expected = "call({'work': 'do_print', 'levels': 3}"
        rcvd = str(self._mocker.call_args)
        err_msg = 'test_handle_generic_commands 1 expected=%s rcvd=%s' % (expected, rcvd)
        self.assertTrue(expected in rcvd, err_msg)

        self._mocker.reset_mock()
        intf.do_print = 0
        intf.dont_print = 3
        self.gc.handle_generic_commands(self.name, intf)
        expected = "call({'work': 'dont_print', 'levels': 3}"
        rcvd = str(self._mocker.call_args)
        err_msg = 'test_handle_generic_commands 2 expected=%s rcvd=%s' % (expected, rcvd)
        self.assertTrue(expected in  rcvd, err_msg)
     
        self._mocker.reset_mock()
        intf.dont_print = 0
        intf.do_log = 4
        self.gc.handle_generic_commands(self.name, intf)
        expected = "call({'work': 'do_log', 'levels': 4}"
        rcvd = str(self._mocker.call_args)
        err_msg = 'test_handle_generic_commands 3 expected=%s rcvd=%s' % (expected, rcvd)
        self.assertTrue(expected in  rcvd, err_msg)
     
        self._mocker.reset_mock()
        intf.do_log = 0
        intf.dont_log = 1
        self.gc.handle_generic_commands(self.name, intf)
        expected = "call({'work': 'dont_log', 'levels': 1}"
        rcvd = str(self._mocker.call_args)
        err_msg = 'test_handle_generic_commands 4 expected=%s rcvd=%s' % (expected, rcvd)
        self.assertTrue(expected in  rcvd, err_msg)

        self._mocker.reset_mock()
        intf.dont_log = 0
        intf.do_alarm = 999
        self.gc.handle_generic_commands(self.name, intf)
        expected = "call({'work': 'do_alarm', 'levels': 999}"
        rcvd = str(self._mocker.call_args)
        err_msg = 'test_handle_generic_commands 5 expected=%s rcvd=%s' % (expected, rcvd)
        self.assertTrue(expected in  rcvd, err_msg)

        self._mocker.reset_mock()
        intf.do_alarm = 0
        intf.dont_alarm = 333
        self.gc.handle_generic_commands(self.name, intf)
        expected = "call({'work': 'dont_alarm', 'levels': 333}"
        rcvd = str(self._mocker.call_args)
        err_msg = 'test_handle_generic_commands 5 expected=%s rcvd=%s' % (expected, rcvd)
        self.assertTrue(expected in  rcvd, err_msg)

    def test_check_ticket(self):
        exit_addr = sys.exit
        sys.exit = mock.MagicMock()
        t1 = {'status' : (e_errors.OK, self.name)}
        self.gc.check_ticket(t1)
        self.assertTrue(sys.exit.called_with(0))
        sys.exit.reset_mock()
        t2 = {'status' : (e_errors.BROKEN, self.name)}
        with mock.patch('sys.stderr', new_callable=StringIO.StringIO) as stderr_mock:
            self.gc.check_ticket(t2)
            self.assertTrue(sys.exit.called_with(1))
            self.assertEqual(stderr_mock.getvalue(), "BAD STATUS ('BROKEN', '%s')\n" % self.name)
        t3 = {}
        retval = self.gc.check_ticket(t3)
        self.assertIsNone(retval)
        sys.exit = exit_addr

    def test_dump(self):
        self._mocker.reset_mock()
        self.gc.dump()
        expected = "call({'work': 'dump'}, None, 0, 0)"
        self.assertEqual(str(self._mocker.call_args), expected, "test_dump error")

    def test_quit(self):
        self._mocker.reset_mock()
        self.gc.quit()
        expected = "call({'work': 'quit'}, None, 0, 0)"
        self.assertEqual(str(self._mocker.call_args), expected, "test_quit error")

if __name__ == "__main__":
    unittest.main()
