import unittest
import volume_clerk_client
import sys
import string
import StringIO
import time
import errno
import socket
import re
import pprint
import StringIO
import mock
import hostaddr
import option
import generic_client
import udp_client
import backup_client
import Trace
import e_errors
import file_clerk_client
import cPickle
import info_client
import enstore_constants
from en_eval import en_eval
import mock_csc


class TestMisc(unittest.TestCase):
    def test_timestamp2time(self):
        t = volume_clerk_client.timestamp2time("1980-01-01 00:00:01")
        self.assertEqual(t, 315529201.0)
        t = volume_clerk_client.timestamp2time('1969-12-31 17:59:59')
        self.assertEqual(t, -1)

    def test_capacity_str(self):
        inp = 1024
        c1 = volume_clerk_client.capacity_str(inp, 'B')
        self.assertEqual(c1, "1024.00B ")
        c2 = volume_clerk_client.capacity_str(inp, 'KB')
        self.assertEqual(c2, "1024.00B ")
        inp *= 1024
        c3 = volume_clerk_client.capacity_str(inp)
        self.assertEqual(c3, '   0.00GB')
        inp *= 1024
        c4 = volume_clerk_client.capacity_str(inp)
        self.assertEqual(c4, '   1.00GB')
        inp *= 1024
        c5 = volume_clerk_client.capacity_str(inp)
        self.assertEqual(c5, '1024.00GB')
        inp *= 1024
        c6 = volume_clerk_client.capacity_str(inp, 'TB')
        self.assertEqual(c6, '1024.00TB')
        inp *= 1024
        c7 = volume_clerk_client.capacity_str(inp, 'PB')
        self.assertEqual(c7, '1024.00PB')

    def test_show_volume_header(self):
        cmp = 'label               avail.   system_inhibit                              library          volume_family                        comment     \n'

        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            volume_clerk_client.show_volume_header()
            self.assertEqual(std_out.getvalue(), cmp)

    def test_show_volume(self):
        fake_vol = {
            'comment': 'totally made up',
            'storage_group': 'M8',
            'library': 'LTO8F1',
            'system_inhibit_0': [
                'none',
                'none'],
            'system_inhibit_1': [
                'none',
                'none'],
            'label': 'VR6995M8',
            'wrapper': 'cpio_odc',
            'remaining_bytes': 8786967552000,
            'media_type': 'M8',
            'status': (
                'ok',
                None),
            'si_time': [
                1683067585.0,
                0.0],
            'si_time_0': "2022-01-01 00:00:00",
            'si_time_1': "2022-01-01 12:00:00",
            'si_time_2': "2022-01-02 00:00:00",
            'file_family': 'volume_read_test',
            'volume_family': 'M8.volume_read_test.cpio_odc',
        }

        cmp = "VR6995M8         8183.50GB   (['none', 'none'] "
        cmp += "0101-0000 ['none', 'none'] 0101-1200)   LTO8F1 "
        cmp += "          M8.volume_read_test.cpio_odc         "
        cmp += "totally made up\n"

        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            volume_clerk_client.show_volume(fake_vol)
            self.assertEqual(std_out.getvalue(), cmp)

    def test_my_atol(self):
        self.assertAlmostEqual(volume_clerk_client.my_atol('1 l'), 1.0)
        self.assertAlmostEqual(volume_clerk_client.my_atol('1 MiB'), 1048576)
        self.assertAlmostEqual(volume_clerk_client.my_atol('2 kb'), 2000)
        self.assertAlmostEqual(
            volume_clerk_client.my_atol('3 tb'),
            3000000000000)

        with self.assertRaises(ValueError):
            volume_clerk_client.my_atol('1.0 mib')

        with self.assertRaises(ValueError):
            volume_clerk_client.my_atol('')

    def test_sumup(self):
        self.assertEqual(volume_clerk_client.sumup([]), 0)
        self.assertEqual(volume_clerk_client.sumup("a"), 97)
        self.assertEqual(volume_clerk_client.sumup("A"), 65)
        self.assertEqual(volume_clerk_client.sumup([1, 2, 3]), 6)
        self.assertEqual(volume_clerk_client.sumup({"a": 1, "b": 2}), 198)
        self.assertEqual(volume_clerk_client.sumup(
            {"a": 1, "b": 2, "c": [1, 2, 3]}), 303)

    def test_check_label(self):
        # the following follow the rules declared in the comments
        self.assertEqual(volume_clerk_client.check_label("VR6995L1"), 0)
        self.assertEqual(volume_clerk_client.check_label("VR6995"), 0)
        self.assertEqual(volume_clerk_client.check_label("9R6995"), 1)
        self.assertEqual(volume_clerk_client.check_label("9R6995L1"), 1)
        self.assertEqual(volume_clerk_client.check_label("VR699VL1"), 1)

        # does NOT follow rules in the comments, a bug
        self.assertEqual(volume_clerk_client.check_label("VR6995  "), 1)
        # a used label, this code should be updated to allow it
        self.assertEqual(volume_clerk_client.check_label("VR6995M8"), 1)

    def test_extract_volume(self):
        inp = "VQ0006L8            2.90GB   (NOTALLOWED 0919-1426 full     "
        inp += "0703-1520)   LTO8F            FUJI.fmv18001.cpio_odc"
        cmp = {
            'comment': '',
            'si_time': (
                '0919-1426',
                '0703-1520'),
            'system_inhibit': [
                'NOTALLOWED',
                'full'],
            'library': 'LTO8F',
            'label': 'VQ0006L8',
            'avail': '2.90GB',
            'volume_family': 'FUJI.fmv18001.cpio_odc'}

        self.assertEqual(volume_clerk_client.extract_volume(inp), cmp)


class TestVolumeClerkClient(unittest.TestCase):
    def setUp(self):
        sent_msg = mock.MagicMock()
        udp_client.UDPClient.send = sent_msg
        udp_client.UDPClient.send_no_wait = sent_msg
        self.sent_msg = sent_msg
        csc = mock_csc.csc()
        self.vcc = volume_clerk_client.VolumeClerkClient(csc)

    def test___init__(self):
        self.assertTrue(
            isinstance(
                self.vcc,
                volume_clerk_client.VolumeClerkClient))

    def test_add(self):

        self.vcc.add(
            "library",
            "file_family",
            "storage_group",
            "media_type",
            "external_label",
            "capacity_bytes")

        expected_work_ticket = {
            'storage_group': 'storage_group',
            'eod_cookie': 'none',
            'user_inhibit': [
                'none',
                'none'],
            'sum_rd_access': 0,
            'blocksize': -1,
            'non_del_files': 0,
            'work': 'addvol',
            'external_label': 'external_label',
            'declared': -1,
            'library': 'library',
            'sum_wr_err': 0,
            'sum_wr_access': 0,
            'file_family': 'file_family',
            'wrapper': 'cpio_odc',
            'capacity_bytes': 'capacity_bytes',
            'system_inhibit': [
                'none',
                'none'],
            'media_type': 'media_type',
            'last_access': -1,
            'first_access': -1,
            'sum_rd_err': 0,
            'error_inhibit': 'none'}

        generated_work_ticket = self.sent_msg.mock_calls[1][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_show_state(self):
        self.sent_msg.reset_mock()
        self.vcc.show_state()
        expected_work_ticket = {"work": "show_state"}
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_modify(self):
        self.sent_msg.reset_mock()
        self.vcc.modify({})
        expected_work_ticket = {"work": "modifyvol"}
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_rmvolent(self):
        self.sent_msg.reset_mock()
        self.vcc.rmvolent('external_label')
        expected_work_ticket = {
            "work": "rmvolent",
            "external_label": "external_label"}
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_restore(self):
        self.sent_msg.reset_mock()
        self.vcc.restore('external_label', 1)
        expected_work_ticket = {
            "work": "restorevol",
            "external_label": "external_label",
            "restore": "yes"}
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)
        expected_work_ticket['restore'] = "no"
        self.sent_msg.reset_mock()
        self.vcc.restore('external_label', 0)
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_rebuild_sg_count(self):
        self.sent_msg.reset_mock()
        self.vcc.rebuild_sg_count()
        expected_work_ticket = {"work": "rebuild_sg_count"}
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_sg_count(self):
        self.sent_msg.reset_mock()
        expected_work_ticket = {'work': 'set_sg_count',
                                'library': 'library',
                                'storage_group': 'storage_group',
                                'count': 0
                                }
        self.vcc.set_sg_count('library', 'storage_group', 0)
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_get_sg_count(self):
        expected_work_ticket = {'work': 'get_sg_count',
                                'library': 'library',
                                'storage_group': 'storage_group'}
        self.sent_msg.reset_mock()
        self.vcc.get_sg_count('library', 'storage_group')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_inquire_vol(self):
        expected_work_ticket = {'work': 'inquire_vol',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.inquire_vol('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_touch(self):
        expected_work_ticket = {'work': 'touch',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.touch('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_check_record(self):
        expected_work_ticket = {'work': 'check_record',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.check_record('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_write_protect_on(self):
        expected_work_ticket = {'work': 'write_protect_on',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.write_protect_on('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_write_protect_off(self):
        expected_work_ticket = {'work': 'write_protect_off',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.write_protect_off('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_write_protect_status(self):
        expected_work_ticket = {'work': 'write_protect_status',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.write_protect_status('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_show_quota(self):
        expected_work_ticket = {'work': 'show_quota'}
        self.sent_msg.reset_mock()
        self.vcc.show_quota()
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_new_library(self):
        expected_work_ticket = {'work': 'new_library',
                                'external_label': 'external_label',
                                'new_library': 'new_library'}
        self.sent_msg.reset_mock()
        self.vcc.new_library('external_label', 'new_library')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_writing(self):
        expected_work_ticket = {'work': 'set_writing',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_writing('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_readonly(self):
        expected_work_ticket = {'work': 'set_system_readonly',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_readonly('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_notallowed(self):
        expected_work_ticket = {'work': 'set_system_notallowed',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_notallowed('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_noaccess(self):
        expected_work_ticket = {'work': 'set_system_noaccess',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_noaccess('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_full(self):
        expected_work_ticket = {'work': 'set_system_full',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_full('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_migrated(self):
        expected_work_ticket = {'work': 'set_system_migrated',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_migrated('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_migrating(self):
        expected_work_ticket = {'work': 'set_system_migrating',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_migrating('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_duplicated(self):
        expected_work_ticket = {'work': 'set_system_duplicated',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_duplicated('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_duplicating(self):
        expected_work_ticket = {'work': 'set_system_duplicating',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_duplicating('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_cloned(self):
        expected_work_ticket = {'work': 'set_system_cloned',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_cloned('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_cloning(self):
        expected_work_ticket = {'work': 'set_system_cloning',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_cloning('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_system_none(self):
        expected_work_ticket = {'work': 'set_system_none',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.set_system_none('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_clr_system_inhibit(self):
        expected_work_ticket = {'work': 'clr_system_inhibit',
                                'external_label': 'external_label',
                                'inhibit': None,
                                'position': 0}

        self.sent_msg.reset_mock()
        self.vcc.clr_system_inhibit('external_label')

        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_decr_file_count(self):
        expected_work_ticket = {'work': 'decr_file_count',
                                'external_label': 'external_label',
                                'count': 1}

        self.sent_msg.reset_mock()
        self.vcc.decr_file_count('external_label')

        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_get_remaining_bytes(self):
        expected_work_ticket = {'work': 'get_remaining_bytes',
                                'external_label': 'external_label'}
        self.sent_msg.reset_mock()
        self.vcc.get_remaining_bytes('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_remaining_bytes(self):
        expected_work_ticket = {'work': 'set_remaining_bytes',
                                'external_label': 'external_label',
                                'remaining_bytes': 'remaining_bytes',
                                'eod_cookie': 'eod_cookie',
                                'bfid': 'bfid'}

        self.sent_msg.reset_mock()
        self.vcc.set_remaining_bytes(
            'external_label',
            'remaining_bytes',
            'eod_cookie',
            'bfid')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_update_counts(self):
        expected_work_ticket = bogus_ticket(
            'update_counts',
            'external_label',
            wr_err=0,
            rd_err=0,
            wr_access=0,
            rd_access=0,
            mounts=0)
        #import pdb; pdb.set_trace()
        self.sent_msg.reset_mock()
        self.vcc.update_counts('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_is_vol_available(self):
        expected_work_ticket = bogus_ticket('is_vol_available',
                                            'external_label',
                                            'action',
                                            volume_family=None,
                                            file_size=0
                                            )
        self.sent_msg.reset_mock()
        self.vcc.is_vol_available('action', 'external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_next_write_volume(self):
        expected_work_ticket = bogus_ticket('next_write_volume',
                                            'library',
                                            'min_remaining_bytes',
                                            'volume_family',
                                            'first_found',
                                            vol_veto_list='[]',
                                            mover={},
                                            use_exact_match=0)
        self.sent_msg.reset_mock()
        self.vcc.next_write_volume(
            'library',
            'min_remaining_bytes',
            'volume_family',
            [],
            'first_found')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_can_write_volume(self):
        pass

    def test_clear_lm_pause(self):
        pass

    def test_rename_volume(self):
        pass

    def test_delete_volume(self):
        pass

    def test_erase_volume(self):
        pass

    def test_restore_volume(self):
        pass

    def test_recycle_volume(self):
        pass

    def test_set_ignored_sg(self):
        pass

    def test_clear_ignored_sg(self):
        pass

    def test_clear_all_ignored_sg(self):
        pass

    def test_list_ignored_sg(self):
        pass

    def test_set_comment(self):
        pass

    def test_assign_sg(self):
        pass

    def test_list_migrated_files(self):
        pass

    def test_list_duplicated_files(self):
        pass

    def test_set_migration_history(self):
        pass

    def test_set_migration_history_closed(self):
        pass


def bogus_ticket(*args, **kwargs):
    retval = {}
    retval['work'] = args[0]
    for arg in args[1:]:
        retval[arg] = arg
    for key, value in kwargs.items():
        retval[key] = value
    return retval


class TestVolumeClerkClientInterface(unittest.TestCase):

    def setUp(self):
        self.vci = volume_clerk_client.VolumeClerkClientInterface()

    def test___init__(self):
        self.assertTrue(
            isinstance(
                self.vci,
                volume_clerk_client.VolumeClerkClientInterface))
        self.assertTrue(
            isinstance(
                self.vci,
                generic_client.GenericClientInterface))

    def test_valid_dictionaries(self):
        vd = self.vci.valid_dictionaries()
        self.assertTrue(isinstance(vd, tuple))
        self.assertEqual(len(vd), 4)
        self.assertTrue(isinstance(vd[0], dict))

    def test_do_work(self):
        sent_msg = mock.MagicMock()
        udp_client.UDPClient.send = sent_msg
        udp_client.UDPClient.send_no_wait = sent_msg
        with mock.patch('sys.stderr', new=StringIO.StringIO()) as std_err:
            with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
                with mock.patch("sys.exit") as exit_mock:
                    with mock.patch('generic_client.GenericClient.check_ticket') as check_please:
                        volume_clerk_client.do_work(self.vci)
                        exit_mock.assert_called_with(0)
                        self.assertTrue('help' in std_out.getvalue())


if __name__ == "__main__":
    unittest.main()
