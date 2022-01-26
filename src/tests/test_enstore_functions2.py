import unittest
import os
import mock
import sys
import tempfile
import stat
try:
    import enroute
except ImportError:
    import fixtures.mock_imports
    print "WARNING using mocked import of enstore C library" 
import enstore_constants
from enstore_functions2 import get_remote_file
from enstore_functions2 import ping
from enstore_functions2 import _get_mode
from enstore_functions2 import bits_to_numeric
from enstore_functions2 import bits_to_rwx
from enstore_functions2 import _get_rwx
from enstore_functions2 import XMODE
from enstore_functions2 import WMODE
from enstore_functions2 import RMODE


class TestEnstoreFunctions2(unittest.TestCase):

    def setUp(self):
        self.tf1 = tempfile.NamedTemporaryFile(prefix='enstore_functions2_',
                                               suffix='_test')

    def test__get_mode(self):
        pmode = 1
        rc = _get_mode(pmode,0,0,1)
        self.assertEqual(XMODE, rc)
        rc = _get_mode(pmode,0,1,1)
        self.assertEqual(XMODE | WMODE, rc)
        rc = _get_mode(pmode,1,1,1)
        self.assertEqual(XMODE | WMODE | RMODE , rc)
        rc = _get_mode(pmode,1,0,1)
        self.assertEqual(XMODE | RMODE , rc)
        pmode = 0
        rc = _get_mode(pmode,0,0,1)
        self.assertEqual(pmode, rc)
        rc = _get_mode(pmode,1,0,1)
        self.assertEqual(pmode, rc)

    def test_bits_to_numeric(self):
        os.chmod(self.tf1.name, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR)
        st = os.stat(self.tf1.name)
        rc = bits_to_numeric(st.st_mode)
        self.assertEqual(rc,'0700')
        bits = st.st_mode | stat.S_IRWXG
        rc = bits_to_numeric(bits)
        self.assertEqual(rc,'0770')

    def test_bits_to_rwx(self):
        os.chmod(self.tf1.name, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR)
        st = os.stat(self.tf1.name)
        rc = bits_to_rwx(st.st_mode)
        self.assertEqual(rc,'-rwx------')
        bits = st.st_mode | stat.S_IRWXG
        rc = bits_to_rwx(bits)
        self.assertEqual(rc,'-rwxrwx---')


    def test__get_rwx(self):
        os.chmod(self.tf1.name, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR)
        st = os.stat(self.tf1.name)
        rc = _get_rwx(st.st_mode, stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR)
        self.assertEqual(rc,'rwx')
        rc = _get_rwx(st.st_mode, stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP)
        self.assertEqual(rc,'---')

    @unittest.skip('not implemented')
    def test__get_bits(self):
        pass

    @unittest.skip('not implemented')
    def test_numeric_to_bits(self):
        pass

    @unittest.skip('not implemented')
    def test_symbolic_to_bits(self):
        pass

    @unittest.skip('not implemented')
    def test_print_list(self):
        pass

    @unittest.skip('not implemented')
    def test_get_mover_status_filename(self):
        pass

    @unittest.skip('not implemented')
    def test_get_migrator_status_filename(self):
        pass

    @unittest.skip('not implemented')
    def test_override_to_status(self):
        pass

    @unittest.skip('not implemented')
    def test_get_days_ago(self):
        pass

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

    def test_ping_good(self):
        addr = '127.0.0.1'
        rc = ping(addr)
        msg = "enstore_functions2.test_ping to %s did not succeed"
        self.assertEqual(enstore_constants.IS_ALIVE, rc, msg % addr)

    def test_ping6_good(self):
        addr = '::1'
        rc = ping(addr,IPv=6)
        msg = "enstore_functions2.test_ping to %s did not succeed"
        self.assertEqual(enstore_constants.IS_ALIVE, rc, msg % addr)


    def test_ping_bad(self):
        addr = '0.0.0.0'
        rc = ping(addr)
        msg = "enstore_functions2.test_ping to %s expected %s, got %s"
        expect = enstore_constants.IS_DEAD
        self.assertEqual(expect, rc, msg % (addr, expect, rc))

    def test_ping6_bad(self):
        addr = '::0'
        rc = ping(addr, IPv=6)
        msg = "enstore_functions2.test_ping to %s expected %s, got %s"
        expect = enstore_constants.IS_DEAD
        self.assertEqual(expect, rc, msg % (addr, expect, rc))


    def test_ping_badIPv(self):
        addr = '::0'
        with self.assertRaises(ValueError):
            rc = ping(addr, IPv=99)

    @unittest.skip('not implemented')
    def test_format_time(self):
        pass

    @unittest.skip('not implemented')
    def test_format_plot_time(self):
        pass

    @unittest.skip('not implemented')
    def test_unformat_time(self):
        pass

    @unittest.skip('not implemented')
    def test_get_dir(self):
        pass

    @unittest.skip('not implemented')
    def test_strip_file_dir(self):
        pass

    @unittest.skip('not implemented')
    def test_strip_node(self):
        pass

    @unittest.skip('not implemented')
    def test_is_this(self):
        pass

    @unittest.skip('not implemented')
    def test_is_library_manager(self):
        pass

    @unittest.skip('not implemented')
    def test_is_udp_proxy_server(self):
        pass

    @unittest.skip('not implemented')
    def test_is_mover(self):
        pass

    @unittest.skip('not implemented')
    def test_is_migrator(self):
        pass

    @unittest.skip('not implemented')
    def test_is_media_changer(self):
        pass

    @unittest.skip('not implemented')
    def test_get_name(self):
        pass

    @unittest.skip('not implemented')
    def test_get_bpd_subdir(self):
        pass

    @unittest.skip('not implemented')
    def test_is_generic_server(self):
        pass

    @unittest.skip('not implemented')
    def test_get_status(self):
        pass

    @unittest.skip('not implemented')
    def test_shell_command(self):
        pass

    @unittest.skip('not implemented')
    def test_shell_command2(self):
        pass

    @unittest.skip('not implemented')
    def test___get_wormhole(self):
        pass

    @unittest.skip('not implemented')
    def test___find_config_file(self):
        pass

    @unittest.skip('not implemented')
    def test___get_enstorerc(self):
        pass

    @unittest.skip('not implemented')
    def test___read_enstore_conf(self):
        pass

    @unittest.skip('not implemented')
    def test__get_value(self):
        pass

    @unittest.skip('not implemented')
    def test_default_value(self):
        pass

    @unittest.skip('not implemented')
    def test_used_default_host(self):
        pass

    @unittest.skip('not implemented')
    def test_default_host(self):
        pass

    @unittest.skip('not implemented')
    def test_used_default_port(self):
        pass

    @unittest.skip('not implemented')
    def test_default_port(self):
        pass

    @unittest.skip('not implemented')
    def test_used_default_file(self):
        pass

    @unittest.skip('not implemented')
    def test_default_file(self):
        pass

    @unittest.skip('not implemented')
    def test_expand_path(self):
        pass

    @unittest.skip('not implemented')
    def test_fullpath(self):
        pass

    @unittest.skip('not implemented')
    def test_fullpath2(self):
        pass

    @unittest.skip('not implemented')
    def test_this_host(self):
        pass

    @unittest.skip('not implemented')
    def test_is_on_host(self):
        pass

    @unittest.skip('not implemented')
    def test_is_readonly_state(self):
        pass

    @unittest.skip('not implemented')
    def test_is_readable_state(self):
        pass

    @unittest.skip('not implemented')
    def test_is_migration_state(self):
        pass

    @unittest.skip('not implemented')
    def test_is_migrated_state(self):
        pass

    @unittest.skip('not implemented')
    def test_is_migrating_state(self):
        pass

    @unittest.skip('not implemented')
    def test_is_migration_file_family(self):
        pass

    @unittest.skip('not implemented')
    def test_is_duplication_file_family(self):
        pass

    @unittest.skip('not implemented')
    def test_is_migration_related_file_family(self):
        pass

    @unittest.skip('not implemented')
    def test___int_convert(self):
        pass

    @unittest.skip('not implemented')
    def test_convert_version(self):
        pass


if __name__ == "__main__":
    unittest.main()
