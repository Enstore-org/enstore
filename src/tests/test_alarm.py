import unittest
import mock
import StringIO
import enstore_constants
try:
    import Interfaces
except ImportError:
    import fixtures.mock_imports
from alarm import *

"""
Test alarm.py
"""

class TestGenericAlarm(unittest.TestCase):
    def setUp(self):
        self.al = GenericAlarm()

    def test__init__(self):
        self.assertTrue(isinstance(self.al, GenericAlarm))
        self.assertEqual(self.al.num_times_raised, 1)

    def test_split_severity(self):
        sev, num = self.al.split_severity("1")
        self.assertEqual(sev, "1")
        self.assertEqual(num, 1)

    def test_set_ticket(self):
        self.al.set_ticket("severe", "bad")
        self.assertEqual(self.al.condition, "severe")
        self.assertEqual(self.al.type, "bad")

    def test_prepr(self):
        self.assertTrue("UNKNOWN" in self.al.prepr())

    def test_ticket(self):
        self.al.set_ticket("severe", "bad")
        with mock.patch('sys.stderr', new_callable=StringIO.StringIO) as stderr_mock:
            with mock.patch('sys.stdout', new_callable=StringIO.StringIO) as stdout_mock:
                self.al.ticket()
                self.assertEqual(self.al.ticket_generated, "YES")

    def test_get_id(self):
        self.assertIsNotNone(self.al.get_id())

    def test_seen_again(self):
        self.al.seen_again()
        self.assertEqual(self.al.num_times_raised, 2)

    def test_list_alarm(self):
        self.assertTrue('UNKNOWN' in self.al.list_alarm())

    def test_short_text(self):
        self.assertTrue("UNKNOWN" in self.al.short_text())

    def test_compare(self):
        self.al.set_ticket("severe", "bad")
        self.al.seen_again()
        host = self.al.host
        root_error = self.al.root_error
        severity = self.al.severity
        source = self.al.source
        condition = self.al.condition
        alarm_info = self.al.alarm_info.copy()
        remedy_type = self.al.type
        self.assertEqual(
            MATCH,
            self.al.compare(
                host,
                severity,
                root_error,
                source,
                alarm_info,
                condition,
                remedy_type))

        self.al.alarm_info['foo'] = 'bar'
        self.assertEqual(
            NO_MATCH,
            self.al.compare(
                host,
                severity,
                root_error,
                source,
                alarm_info,
                condition,
                remedy_type))


class TestAlarm(unittest.TestCase):
    def setUp(self):

        host = "host"
        pid = 1
        uid = 1
        root_error = "root_error"
        source = "source"
        severity = 1
        alarm_info = "alarm_info"
        condition = "condition"
        remedy_type = "remedy_type"

        self.al = Alarm(host, severity,
                        pid, uid,
                        root_error, source,
                        alarm_info, condition,
                        remedy_type)

    def test__init__(self):
        self.assertTrue(isinstance(self.al, Alarm))


class TestAsciiAlarm(unittest.TestCase):
    def setUp(self):
        id = 1
        host = "host"
        pid = 1
        uid = 1
        severity = 1
        source = "source"
        root_error = "root_error"
        alarm_info = "alarm_info"

        text = "(%s,%s,%s,%s,%s,%s,%s,%s)" % (id, host, pid,
                                              uid, severity, source, root_error, alarm_info)

        self.al = AsciiAlarm(text)

    def test__init__(self):
        self.assertTrue(isinstance(self.al, AsciiAlarm))


class TestLogFileAlarm(unittest.TestCase):
    def setUp(self):
        self.lfa = LogFileAlarm(
            "11:11:11 foo 1 1 1 1 a LogFileAlarm",
            "2021-01-01")

    def test_unpack_dict(self):
        aDict = {enstore_constants.ROOT_ERROR: "a root error",
                 enstore_constants.SEVERITY: "100000",
                 enstore_constants.ALARM: "an alarm", }
        self.lfa.unpack_dict(aDict)
        self.assertEqual(self.lfa.root_error, "a root error")
        self.assertEqual(self.lfa.severity, "100000")
        self.assertEqual(
            self.lfa.alarm_info[enstore_constants.ALARM], "an alarm")

    def test__init__(self):
        self.assertTrue(isinstance(self.lfa, LogFileAlarm))


if __name__ == "__main__":
    unittest.main()
