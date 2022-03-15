import unittest
import os
import sys
import mock
import StringIO
try:
    import enroute
except ImportError:
    import fixtures.mock_imports
    sys.stderr.write("WARNING using mocked import of enstore C library\n")

from get_all_bytes_counter import get_remote_file
from get_all_bytes_counter import ping

DEAD = 0
ALIVE = 1


class TestGetAllBytesCounter(unittest.TestCase):

    def test_get_remote_file_good(self):
        path = os.environ.get('PATH')
        # put a dummy enrcp in path
        # just so get_remote_file() has a return code to test
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        newpath = "%s:%s" % (fixture_dir, path)
        os.environ['PATH'] = newpath
        rc = get_remote_file('fake_machine', 'fake_file', 'exit_0')
        self.assertEquals(rc, 0, "get_remote_file expected rc 0, got %s" % rc)
        os.environ['PATH'] = path

    def test_get_remote_file_bad(self):
        path = os.environ.get('PATH')
        # put a dummy enrcp in path
        # just so get_remote_file() has a return code to test
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        newpath = "%s:%s" % (fixture_dir, path)
        os.environ['PATH'] = newpath
        rc = get_remote_file('fake_machine', 'fake_file', 'exit_1')
        self.assertEquals(rc, 1, "get_remote_file expected rc 1, got %s" % rc)
        os.environ['PATH'] = path

    def test_ping_good(self):
        addr = '127.0.0.1'
        rc = ping(addr)
        msg = "test_ping to %s did not succeed"
        self.assertEqual(ALIVE, rc, msg % addr)

    def test_ping_bad(self):
        addr = '0.0.0.1'
        rc = ping(addr)
        msg = "test_ping to %s expected %s, got %s"
        expect = DEAD
        self.assertEqual(expect, rc, msg % (addr, expect, rc))


if __name__ == "__main__":
    unittest.main()
