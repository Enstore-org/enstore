import unittest
import os
import mock
import sys
import StringIO
sys.modules['enroute'] = mock.MagicMock()
sys.modules['runon'] = mock.MagicMock()
sys.modules['Interfaces'] = mock.MagicMock()
sys.modules['hostaddr'] = mock.MagicMock()
sys.modules['checksum'] = mock.MagicMock()
from get_all_bytes_counter import get_remote_file
from get_all_bytes_counter import ping


class TestGetAllBytesCounter(unittest.TestCase):

    def test_get_remote_file_good(self):
        path = os.environ.get('PATH')
        newpath = "./tests/fixtures:%s" % path
        os.environ['PATH'] = newpath
        rc = get_remote_file('fake_machine', 'fake_file', 'exit_0')
        self.assertEquals(rc, 0, "get_all_bytes_counter.get_remote_file expected rc 0, got %s" % rc)
        os.environ['PATH'] = path

    def test_get_remote_file_bad(self):
        path = os.environ.get('PATH')
        newpath = "./tests/fixtures:%s" % path
        os.environ['PATH'] = newpath
        rc = get_remote_file('fake_machine', 'fake_file', 'exit_1')
        self.assertEquals(rc, 1, "get_all_bytes_counter.get_remote_file expected rc 1, got %s" % rc)
        os.environ['PATH'] = path

    def test_ping_good(self):
        DEAD = 0
        ALIVE = 1
        rc = ping('127.0.0.1')
        self.assertEqual(ALIVE, rc, "get_all_bytes_counter.test_ping to 127.0.0.1 did not succeed")

    def test_ping_bad(self):
        DEAD = 0
        ALIVE = 1
        print "\nIGNORE ping: cannot resolve some.bad.host: Unknown host'\nTODO: SUPPRESS"
        rc = ping('some.bad.host')
        self.assertEqual(
            DEAD, rc, "get_all_bytes_counter.test_ping to some.bad.host succeeded when it should not")


if __name__ == "__main__":
    unittest.main()
