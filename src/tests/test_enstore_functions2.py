import unittest
import os
import mock
import sys
import tempfile
import stat
import time
import shutil
try:
    import enroute
except ImportError:
    import fixtures.mock_imports
    print "WARNING using mocked import of enstore C library" 
import enstore_constants
from enstore_functions2 import get_remote_file


class TestEnstoreFunctions2(unittest.TestCase):

    def test_get_remote_file_good(self):
        path = os.environ.get('PATH')
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        newpath = "%s:%s" % (fixture_dir, path)
        os.environ['PATH'] = newpath
        rc = get_remote_file('fake_machine', 'fake_file', 'exit_0')
        errmsg = "enstore_functions2.get_remote_file expected rc 0, got %s"
        self.assertEquals(rc, 0, errmsg % rc)
        os.environ['PATH'] = path

    def test_get_remote_file_bad(self):
        path = os.environ.get('PATH')
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        newpath = "%s:%s" % (fixture_dir, path)
        os.environ['PATH'] = newpath
        errmsg = "enstore_functions2.get_remote_file expected rc 1, got %s"
        rc = get_remote_file('fake_machine', 'fake_file', 'exit_1')
        self.assertEquals(rc, 1, errmsg  % rc)
        os.environ['PATH'] = path



if __name__ == "__main__":
    unittest.main()
