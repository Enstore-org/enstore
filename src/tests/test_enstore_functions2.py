import unittest
import os
import mock
import sys
sys.modules['enroute'] = mock.MagicMock()
sys.modules['runon'] = mock.MagicMock()
sys.modules['Interfaces'] = mock.MagicMock()
sys.modules['hostaddr'] = mock.MagicMock()
sys.modules['checksum'] = mock.MagicMock()
from enstore_functions2 import get_remote_file
from enstore_functions2 import ping


class TestEnstoreFunctions2(unittest.TestCase):

    @unittest.skip('not implemented')
    def test__get_mode(self):
        pass

    @unittest.skip('not implemented')
    def test_bits_to_numeric(self):
        pass

    @unittest.skip('not implemented')
    def test__get_rwx(self):
        pass

    @unittest.skip('not implemented')
    def test_bits_to_rwx(self):
        pass

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
        newpath = "./tests/fixtures:%s" % path
        os.environ['PATH'] = newpath
        rc = get_remote_file('fake_machine', 'fake_file', 'exit_0')
        self.assertEquals(rc, 0, "enstore_functions2.get_remote_file expected rc 0, got %s" % rc)
        os.environ['PATH'] = path

    def test_get_remote_file_bad(self):
        path = os.environ.get('PATH')
        newpath = "./tests/fixtures:%s" % path
        os.environ['PATH'] = newpath
        rc = get_remote_file('fake_machine', 'fake_file', 'exit_1')
        self.assertEquals(rc, 1, "enstore_functins2.get_remote_file expected rc 1, got %s" % rc)
        os.environ['PATH'] = path

    def test_ping_good(self):
        DEAD = 0
        ALIVE = 1
        rc = ping('127.0.0.1')
        self.assertEqual(ALIVE, rc, "enstore_functions2.test_ping to 127.0.0.1 did not succeed")

    def test_ping_bad(self):
        DEAD = 0
        ALIVE = 1
        print "\nIGNORE ping: cannot resolve some.bad.host: Unknown host'\nTODO: SUPPRESS"
        rc = ping('some.bad.host')
        self.assertEqual(
            DEAD, rc, "enstore_functions2.test_ping to some.bad.host succeeded when it should not")



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
