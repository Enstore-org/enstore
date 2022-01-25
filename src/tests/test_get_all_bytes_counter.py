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
        DEAD = 0
        ALIVE = 1
        rc = ping('github.com')
        self.assertEqual(ALIVE, rc, "test_ping to github.com did not succeed")

    def test_ping_bad(self):
        DEAD = 0
        ALIVE = 1
        # havent figured out how to suppress ping output to stderr
        rc = ping('some.bad.host')
        self.assertEqual(
            DEAD, rc, "test_ping to some.bad.host succeeded when it should not")


if __name__ == "__main__":
    unittest.main()
