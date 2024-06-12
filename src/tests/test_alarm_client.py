import unittest
import sys
import os
import time
import mock
import StringIO
import alarm
import alarm_client
import generic_client
import configuration_client
import Trace
import e_errors
import udp_client
import mock_csc


class TestLock(unittest.TestCase):

    def setUp(self):
        self.lock = alarm_client.Lock()

    def test___init__(self):
        self.assertEqual(self.lock.locked, 0)
        self.assertTrue(isinstance(self.lock, alarm_client.Lock))

    def test_unlock(self):
        self.lock.unlock()
        self.assertEqual(self.lock.locked, 0)

    def test_test_and_set(self):
        self.lock.unlock()
        s = self.lock.test_and_set()
        self.assertEqual(s, 0)
        self.assertEqual(self.lock.locked, 1)
        s = self.lock.test_and_set()
        self.assertEqual(s, 1)
        self.assertEqual(self.lock.locked, 1)


class TestAlarmClient(unittest.TestCase):
    def setUp(self):
        self.sent_msg = mock.MagicMock()
        udp_client.UDPClient.send = self.sent_msg
        udp_client.UDPClient.send_no_wait = self.sent_msg
        csc = mock_csc.csc()
        self.alarm_client = alarm_client.AlarmClient(csc)

    def test___init__(self):
        self.assertTrue(isinstance(self.alarm_client, alarm_client.AlarmClient))

    def test_alarm_func(self):
        t = time.time()
        pid = 9999999
        name = self.alarm_client.name
        root_error = "root_error"
        severity = "severity"
        condition = "condition"
        remedy_type = "remedy_type"
        args = ["foo", "bar", "baz"]
        self.alarm_client.alarm_func_lock.unlock()
        self.sent_msg.reset_mock()
        self.alarm_client.alarm_func(t, pid, name, root_error, severity, condition, remedy_type, args)

        # test that a post_alarm message sent to alarm_server
        self.assertEqual('post_alarm', self.sent_msg.mock_calls[0].args[0]['work'])
        # test that a log_message sent to log_server
        self.assertEqual('log_message', self.sent_msg.mock_calls[1].args[0]['work'])

    def test_alarm(self):
        self.sent_msg.reset_mock()
        alarm_info = "This is a test. Do not be alarmed"
        self.alarm_client.alarm(alarm_info)
        # alarm_info should be sent
        self.assertEqual(alarm_info, self.sent_msg.mock_calls[0][1][0]['severity'])
        # test that a post_alarm message sent to alarm_server
        self.assertEqual('post_alarm', self.sent_msg.mock_calls[0].args[0]['work'])   
        # test that a log_message sent to log_server
        self.assertEqual('log_message', self.sent_msg.mock_calls[1].args[0]['work'])


    def test_resolve(self):
        alarm_id = 999
        self.sent_msg.reset_mock()
        self.alarm_client.resolve(alarm_id)
        self.assertEqual(alarm_id, self.sent_msg.mock_calls[0][1][0]['alarm'])
        self.assertEqual('resolve_alarm', self.sent_msg.mock_calls[0][1][0]['work'])


    def test_get_patrol_file(self):
        self.sent_msg.reset_mock()
        self.alarm_client.get_patrol_file()
        self.assertEqual('get_patrol_filename', self.sent_msg.mock_calls[0][1][0]['work'])


class TestAlarmClientInterface(unittest.TestCase):
    def setUp(self):
        self.aci = alarm_client.AlarmClientInterface()

    def test___init__(self):
        self.assertTrue(isinstance(self.aci, alarm_client.AlarmClientInterface))

    def test_valid_dictionaries(self):
        self.assertEqual(4, len(self.aci.valid_dictionaries()))


    def test_do_work(self):
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            with mock.patch("sys.exit") as exit_mock:
                with mock.patch('generic_client.GenericClient.check_ticket') as check_please:
                    alarm_client.do_work(self.aci)
                    exit_mock.assert_called_with(0)
                    self.assertTrue(len(std_out.getvalue()) > 0)


if __name__ == "__main__":
    unittest.main()
