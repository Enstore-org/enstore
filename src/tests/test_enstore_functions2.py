import unittest
import os
import mock
import sys
import tempfile
import stat
import time
import shutil
import socket
try:
    import enroute
except ImportError:
    import fixtures.mock_imports
    print "WARNING using mocked import of enstore C library"
import enstore_constants
import enstore_functions2
from enstore_functions2 import XMODE
from enstore_functions2 import WMODE
from enstore_functions2 import RMODE

# the following are used somewhere in enstore according to grep
# I am using a convention I just made up, where if I
# don't think the code is used externally to enstore_functions2.py
# it is tested as enstore_functions2.method()
# otherwise tested as method()

from enstore_functions2 import bits_to_rwx
from enstore_functions2 import convert_version
from enstore_functions2 import default_file
from enstore_functions2 import default_host
from enstore_functions2 import default_port
from enstore_functions2 import default_value
from enstore_functions2 import expand_path
from enstore_functions2 import format_mode
from enstore_functions2 import format_plot_time
from enstore_functions2 import format_time
from enstore_functions2 import fullpath
from enstore_functions2 import fullpath2
from enstore_functions2 import get_bpd_subdir
from enstore_functions2 import get_days_ago
from enstore_functions2 import get_dir
from enstore_functions2 import get_migrator_status_filename
from enstore_functions2 import get_mover_status_filename
from enstore_functions2 import get_remote_file
from enstore_functions2 import get_status
from enstore_functions2 import is_generic_server
from enstore_functions2 import is_library_manager
from enstore_functions2 import is_media_changer
from enstore_functions2 import is_migrated_state
from enstore_functions2 import is_migration_file_family
from enstore_functions2 import is_migration_related_file_family
from enstore_functions2 import is_migration_state
from enstore_functions2 import is_migrator
from enstore_functions2 import is_mover
from enstore_functions2 import is_on_host
from enstore_functions2 import is_readable_state
from enstore_functions2 import is_readonly_state
from enstore_functions2 import is_udp_proxy_server
from enstore_functions2 import numeric_to_bits
from enstore_functions2 import override_to_status
from enstore_functions2 import ping
from enstore_functions2 import print_list
from enstore_functions2 import shell_command
from enstore_functions2 import shell_command2
from enstore_functions2 import strip_file_dir
from enstore_functions2 import strip_node
from enstore_functions2 import symbolic_to_bits
from enstore_functions2 import unformat_time
from enstore_functions2 import used_default_host
from enstore_functions2 import used_default_port


class TestEnstoreFunctions2(unittest.TestCase):

    def setUp(self):
        this_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(this_dir)
        next_dir = os.path.join(this_dir, 'fixtures')
        next_dir = os.path.join(next_dir, 'config')
        conf_file = os.path.join(next_dir, 'enstore.conf')
        os.environ['ENSTORE_CONF'] = conf_file
        os.environ['ENSTORE_CONFIG_PORT'] = "%s" % enstore_constants.DEFAULT_CONF_PORT
        os.environ['ENSTORE_CONFIG_FILE'] = enstore_constants.DEFAULT_CONF_FILE
        self.config_file = conf_file
        self.td1 = tempfile.mkdtemp()
        self.tf1 = tempfile.NamedTemporaryFile(prefix='enstore_functions2_',
                                               suffix='_test',
                                               dir=self.td1)
        self.migrated = ['migrated', 'duplicated', 'cloned']
        self.migrating = ['migrating', 'duplicating', 'cloning']
        self.readonly = ['full', 'readonly']
        self.readable = ['none', 'full', 'readonly']

    def test__get_mode(self):
        pmode = 1
        rc = enstore_functions2._get_mode(pmode, 0, 0, 1)
        self.assertEqual(XMODE, rc)
        rc = enstore_functions2._get_mode(pmode, 0, 1, 1)
        self.assertEqual(XMODE | WMODE, rc)
        rc = enstore_functions2._get_mode(pmode, 1, 1, 1)
        self.assertEqual(XMODE | WMODE | RMODE, rc)
        rc = enstore_functions2._get_mode(pmode, 1, 0, 1)
        self.assertEqual(XMODE | RMODE, rc)
        pmode = 0
        rc = enstore_functions2._get_mode(pmode, 0, 0, 1)
        self.assertEqual(pmode, rc)
        rc = enstore_functions2._get_mode(pmode, 1, 0, 1)
        self.assertEqual(pmode, rc)

    def test_bits_to_numeric(self):
        os.chmod(self.tf1.name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        st = os.stat(self.tf1.name)
        rc = enstore_functions2.bits_to_numeric(st.st_mode)
        self.assertEqual(rc, '0700')
        bits = st.st_mode | stat.S_IRWXG
        rc = enstore_functions2.bits_to_numeric(bits)
        self.assertEqual(rc, '0770')

    def test_bits_to_rwx(self):
        os.chmod(self.tf1.name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        st = os.stat(self.tf1.name)
        rc = bits_to_rwx(st.st_mode)
        self.assertEqual(rc, '-rwx------')
        bits = st.st_mode | stat.S_IRWXG
        rc = bits_to_rwx(bits)
        self.assertEqual(rc, '-rwxrwx---')

    def test__get_rwx(self):
        os.chmod(self.tf1.name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        st = os.stat(self.tf1.name)
        rc = enstore_functions2._get_rwx(
            st.st_mode, stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR)
        self.assertEqual(rc, 'rwx')
        rc = enstore_functions2._get_rwx(
            st.st_mode, stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP)
        self.assertEqual(rc, '---')

    def test__get_bits(self):
        os.chmod(self.tf1.name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        st = os.stat(self.tf1.name)
        expected = 0
        rc = enstore_functions2._get_bits(
            0, stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR)
        msg = "_get_bits returned %s, expected %s"
        self.assertEqual(rc, expected, msg % (rc, expected))
        expected = 448
        rc = enstore_functions2._get_bits(
            st.st_mode, stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR)
        self.assertEqual(rc, expected, msg % (rc, expected))

    def test_numeric_to_bits(self):
        rc = numeric_to_bits('0700')
        expected = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
        msg = 'numeric_to_bits got %s, expected %s'
        self.assertEqual(rc, expected, msg % (rc, expected))
        expected = expected | stat.S_IRWXG
        rc = numeric_to_bits('0770')
        self.assertEqual(rc, expected, msg % (rc, expected))

    def test_symbolic_to_bits(self):
        rc = symbolic_to_bits("ug=rw")
        expected = 432
        msg = 'symbolic_to_bits got %s, expected %s'
        self.assertEqual(rc, expected, msg % (rc, expected))
        rc = symbolic_to_bits("u=rwx")
        expected = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
        self.assertEqual(rc, expected, msg % (rc, expected))

    def test_print_list(self):
        a_list = ["a", "b", "c"]
        rc = print_list(a_list)
        expected = 'a b c'
        msg = 'test_print returned  "%s", expected "%s"'
        self.assertEqual(rc, expected, msg % (rc, expected))

    def test_get_mover_status_filename(self):
        rc = get_mover_status_filename()
        expected = "enstore_movers.html"
        msg = 'get_mover_status_filename  returned  "%s", expected "%s"'
        self.assertEqual(rc, expected, msg % (rc, expected))

    def test_get_migrator_status_filename(self):
        rc = get_migrator_status_filename()
        expected = "enstore_migrators.html"
        msg = 'get_migrator_status_filename  returned  "%s", expected "%s"'
        self.assertEqual(rc, expected, msg % (rc, expected))

    def test_override_to_status(self):
        for sval in enstore_constants.SAAG_STATUS:
            lval = [sval]
            rc1 = override_to_status(sval)
            rc2 = override_to_status(lval)
            idx = enstore_constants.SAAG_STATUS.index(sval)
            rc3 = enstore_constants.REAL_STATUS[idx]
            msg = 'enstore_constants.SAAG_STATUS mismatch to enstore_constants.REAL_STATUS'
            self.assertEqual(rc1, rc2, msg)
            self.assertEqual(rc2, rc3, msg)

    def test_get_days_ago(self):
        now = time.time()
        sec_in_day = 86400
        msg = 'get_days_ago  returned  %s, expected %s'
        two_days_ago = float(sec_in_day*2)
        expected = now - two_days_ago
        rc = get_days_ago(now, 2)
        self.assertEqual(rc, expected, msg % (rc, expected))
        ten_days_ago = float(sec_in_day*10)
        expected = now - ten_days_ago
        rc = get_days_ago(now, 10)
        self.assertEqual(rc, expected, msg % (rc, expected))

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
        self.assertEquals(rc, 1, errmsg % rc)
        os.environ['PATH'] = path

    def test_ping_good(self):
        addr = '127.0.0.1'
        rc = ping(addr)
        msg = "enstore_functions2.test_ping to %s did not succeed"
        self.assertEqual(enstore_constants.IS_ALIVE, rc, msg % addr)

    def test_ping_bad(self):
        addr = '0.0.0.1'
        rc = ping(addr)
        msg = "enstore_functions2.test_ping to %s expected %s, got %s"
        expect = enstore_constants.IS_DEAD
        self.assertEqual(expect, rc, msg % (addr, expect, rc))

    def test_format_and_unformattime(self):
        now = long(time.time())
        fmted = format_time(now)
        expected = long(unformat_time(fmted))
        msg = "format_time unformat_time disagree returned %s expected %s"
        self.assertEqual(now, expected, msg % (expected, now))

    def test_format_plot_time(self):
        now = time.time()
        rc = format_plot_time(now)
        self.assertNotEqual(rc, None)
        self.assertNotEqual(rc, '')

    def test_get_dir(self):
        expected = os.getcwd()
        rc = get_dir(expected)
        self.assertEqual(rc, expected)
        inp = expected+'/foo/bar/'
        expected = inp[:-1]
        rc = get_dir(inp)
        self.assertEqual(rc, expected)

    def test_strip_file_dir(self):
        inp = "/the/deep/deep/deep/dir"
        expected = "dir"
        rc = strip_file_dir(inp)
        self.assertEqual(expected, rc)

    def test_strip_node(self):
        inp = 'somenode.fnal.gov'
        expected = 'somenode'
        rc = strip_node(inp)
        self.assertEqual(rc, expected)
        inp = None
        expected = None
        rc = strip_node(inp)
        self.assertEqual(inp, expected)

    def test_is_this(self):
        input = 'aview.of.reality'
        self.assertTrue(enstore_functions2.is_this(input, 'reality'))
        self.assertFalse(enstore_functions2.is_this(input, 'fantasy'))

    def test_is_library_manager(self):
        input1 = 'this.is.library_manager'
        input2 = 'this.is.not_library_manager'
        self.assertTrue(is_library_manager(input1))
        self.assertFalse(is_library_manager(input2))

    def test_is_udp_proxy_server(self):
        input1 = 'this.is.udp_proxy_server'
        input2 = 'this.is.not_udp_proxy_server'
        self.assertTrue(is_udp_proxy_server(input1))
        self.assertFalse(is_udp_proxy_server(input2))

    def test_is_mover(self):
        input1 = 'this.is.mover'
        input2 = 'this.is.not_mover'
        self.assertTrue(is_mover(input1))
        self.assertFalse(is_mover(input2))

    def test_is_migrator(self):
        input1 = 'this.is.migrator'
        input2 = 'this.is.not_migrator'
        self.assertTrue(is_migrator(input1))
        self.assertFalse(is_migrator(input2))

    def test_is_media_changer(self):
        input1 = 'this.is.media_changer'
        input2 = 'this.is.not_media_changer'
        self.assertTrue(is_media_changer(input1))
        self.assertFalse(is_media_changer(input2))

    def test_get_name(self):
        self.assertEqual('foo', enstore_functions2.get_name('foo.bar.baz'))

    def test_get_bpd_subdir(self):
        testdir = os.path.dirname(self.tf1.name)
        rc = get_bpd_subdir(testdir)
        self.assertEqual(rc, testdir)
        newtestdir = "%s/%s" % (testdir, enstore_constants.BPD_SUBDIR)
        os.mkdir(newtestdir)
        rc = get_bpd_subdir(testdir)
        self.assertEqual(rc, newtestdir)

    def test_is_generic_server(self):
        for srv in enstore_constants.GENERIC_SERVERS:
            self.assertTrue(is_generic_server(srv))

    def test_get_status(self):
        d1 = {'foo': 'bar'}
        self.assertEqual(None, get_status(d1))
        d1['status'] = ('fine', 'very')
        self.assertEqual('fine', get_status(d1))

    def test_shell_command(self):
        cmd = 'cat /dev/null'
        rslt = shell_command(cmd)
        self.assertEqual('', rslt[0], rslt)
        cmd = 'pwd'
        rslt = shell_command(cmd)
        self.assertNotEqual('', rslt[0], rslt)

    def test_shell_command2(self):
        cmd = 'cat /dev/null'
        rslt = shell_command2(cmd)
        self.assertEqual(0, rslt[0], rslt)
        self.assertEqual('', rslt[1], rslt)
        self.assertEqual('', rslt[2], rslt)
        cmd = 'cat /dev/i_dont_exist'
        rslt = shell_command(cmd)
        self.assertNotEqual(0, rslt[0], rslt)
        self.assertTrue('No such file or directory' in rslt[1], rslt)

    @unittest.skip('private, cannot test without refactor')
    def test__find_config_file(self):
        expct = __find_config_file()
        self.asserEquals(expct, conf_file,
                         "%s and %s should be same" % (expct, conf_file))

    @unittest.skip('private, cannot test without refactor')
    def test___get_wormhole(self):
        expct = enstore_functions2.__get_wormhole()

    @unittest.skip('private, cannot test without refactor')
    def test___get_enstorerc(self):
        expct = enstore_functions2.__get_enstorerc()

    @unittest.skip('private, cannot test without refactor')
    def test___read_enstore_conf(self):
        expct = enstore_functions2.__get_wormhole()

    def test__get_value(self):
        rc = enstore_functions2._get_value('ENSTORE_CONF', 'DEFAULT')
        self.assertEqual(rc[0], self.config_file)
        rc = enstore_functions2._get_value('SHOULD_NOT_EXIST', 'DEFAULT')
        self.assertEqual(rc[0], 'DEFAULT')

    def test_default_value(self):
        rc = default_value('ENSTORE_CONF')
        self.assertEqual(rc, self.config_file)
        rc = default_value('SHOULD_NOT_EXIST')
        self.assertEqual(rc, None)

    def test_used_default_host(self):
        rc = used_default_host()
        self.assertTrue(rc)

    def test_default_host(self):
        rc = default_host()
        self.assertEqual(rc, 'localhost')

    def test_used_default_port(self):
        rc = used_default_port()
        self.assertFalse(rc)

    def test_default_port(self):
        rc = default_port()
        self.assertEqual(rc, 7500)

    def test_default_file(self):
        expected = ['/pnfs/enstore/.(config)(flags)/enstore.conf',
                    '/home/enstore/site_specific/config/']
        rc = default_file()
        self.assertTrue(rc in expected)

    def test_used_default_file(self):
        # there is coupling between default_file() and
        # used_default_file() that looks wrong.
        # see global used_default_config_file
        rc = default_file()
        rc = enstore_functions2.used_default_file()
        self.assertFalse(rc)

    def test_expand_path(self):
        input = 'fixtures/config'
        rc = expand_path(input)
        expected = os.path.dirname(self.config_file)
        self.assertEqual(rc, expected)

    def test_fullpath(self):
        input = 'fixtures/config'
        rc = fullpath(input)
        expected = os.path.dirname(self.config_file)
        self.assertNotEqual(rc, expected)
        self.assertEqual(rc[2], expected)

    def test_fullpath2(self):
        input = 'fixtures/config'
        rc = fullpath2(input)
        expected = os.path.dirname(self.config_file)
        self.assertEqual(rc[3], expected)

    def test_this_host(self):
        rc = enstore_functions2.this_host()
        host = socket.gethostname()
        self.assertTrue(host in rc)

    def test_is_on_host(self):
        host = socket.gethostname()
        rc = is_on_host(host)
        self.assertTrue(rc)

    def test_is_readonly_state(self):
        for st in self.readonly:
            self.assertTrue(is_readonly_state(st))
        for st in self.migrated:
            self.assertTrue(is_readonly_state(st))
        for st in self.migrating:
            self.assertTrue(is_readonly_state(st))
        self.assertFalse(is_readonly_state('unreadable'))

    def test_is_readable_state(self):
        for st in self.readable:
            self.assertTrue(is_readable_state(st))
        for st in self.migrated:
            self.assertTrue(is_readable_state(st))
        for st in self.migrating:
            self.assertTrue(is_readable_state(st))
        self.assertFalse(is_readable_state('unreadable'))

    def test_is_migration_state(self):
        for st in self.readable:
            self.assertFalse(is_migration_state(st))
        for st in self.migrated:
            self.assertTrue(is_migration_state(st))
        for st in self.migrating:
            self.assertTrue(is_migration_state(st))
        self.assertFalse(is_migration_state('unreadable'))

    def test_is_migrated_state(self):
        for st in self.readable:
            self.assertFalse(is_migrated_state(st))
        for st in self.migrated:
            self.assertTrue(is_migrated_state(st))
        for st in self.migrating:
            self.assertFalse(is_migrated_state(st))
        self.assertFalse(is_migrated_state('unreadable'))

    # not used anywhere!
    def test_is_migrating_state(self):
        for st in self.readable:
            self.assertFalse(enstore_functions2.is_migrating_state(st))
        for st in self.migrated:
            self.assertFalse(enstore_functions2.is_migrating_state(st))
        for st in self.migrating:
            self.assertTrue(enstore_functions2.is_migrating_state(st))
        self.assertFalse(enstore_functions2.is_migrating_state('unreadable'))

    def test_is_migration_file_family(self):
        is_m = 'THis-is-MIGRATION-pattern'
        not_m = 'This-Is-NOT-xmigration-pattern'
        self.assertTrue(is_migration_file_family(is_m))
        self.assertFalse(is_migration_file_family(not_m))

    # not used anywhere!
    def test_is_duplication_file_family(self):
        is_dup1 = 'file_copy_1_of_something_important'
        is_dup2 = 'file_copy_2_of_something_important'
        not_dup = 'unduplicated_file'
        self.assertTrue(enstore_functions2.is_duplication_file_family(is_dup1))
        self.assertTrue(enstore_functions2.is_duplication_file_family(is_dup2))
        self.assertFalse(
            enstore_functions2.is_duplication_file_family(not_dup))

    def test_is_migration_related_file_family(self):
        is_dup1 = 'file_copy_1_of_something_important'
        is_dup2 = 'file_copy_2_of_something_important'
        not_dup = 'unduplicated_file'
        is_m = 'THis-is-MIGRATION-pattern'
        not_m = 'This-Is-NOT-xmigration-pattern'

        self.assertTrue(is_migration_related_file_family(is_m))
        self.assertTrue(is_migration_related_file_family(is_dup1))
        self.assertTrue(is_migration_related_file_family(is_dup2))
        self.assertFalse(is_migration_related_file_family(not_m))
        self.assertFalse(is_migration_related_file_family(not_dup))

    @unittest.skip('private, cannot test without refactor')
    def test___int_convert(self):
        pass

    def test_convert_version(self):
        ver1 = "V6_3_4_15"
        ver2 = "V6_3_4_15"
        ver3 = "V6_4_4_15"
        rc1 = convert_version(ver1)
        rc2 = convert_version(ver2)
        rc3 = convert_version(ver3)
        self.assertTrue(rc1 == rc2, "rc1=%s rc2=%s" % (rc1, rc2))
        self.assertTrue(rc3 > rc2, "rc3=%s rc2=%s" % (rc3, rc2))


if __name__ == "__main__":
    unittest.main()
