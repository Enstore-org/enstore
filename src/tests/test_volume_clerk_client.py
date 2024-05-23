import unittest
import volume_clerk_client
import sys
import string
import StringIO
import socket
import StringIO
import mock
import option
import generic_client
import udp_client
import Trace
import e_errors
import mock_csc


def bogus_ticket(*args, **kwargs):
    """ Return a bogus ticket for test input
        (aka a dictionary)
    """
    retval = {}
    retval['work'] = args[0]
    for arg in args[1:]:
        retval[arg] = arg
    for key, value in kwargs.items():
        retval[key] = value
    return retval


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
        inp *= -1
        c1 = volume_clerk_client.capacity_str(inp, 'B')
        self.assertEqual(c1, "-1024.00B ")
        c2 = volume_clerk_client.capacity_str(inp, 'KB')
        self.assertEqual(c2, "-1024.00B ")
        inp *= -1024
        c3 = volume_clerk_client.capacity_str(inp, 'KB')
        self.assertEqual(c3, '1024.00KB')

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
        inp *= -1
        c8 = volume_clerk_client.capacity_str(inp, 'PB')
        self.assertEqual(c8, '-1024.00PB')

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
        with self.assertRaises(ValueError):
            volume_clerk_client.my_atol('1.0 quatloo')

    def test_sumup(self):
        self.assertEqual(volume_clerk_client.sumup([]), 0)
        self.assertEqual(volume_clerk_client.sumup("a"), 97)
        self.assertEqual(volume_clerk_client.sumup("A"), 65)
        self.assertEqual(volume_clerk_client.sumup("ABC"), 198)
        self.assertEqual(volume_clerk_client.sumup([1, 2, 3]), 6)
        self.assertEqual(volume_clerk_client.sumup({"a": 1, "b": 2}), 198)
        self.assertEqual(volume_clerk_client.sumup(
            {"a": 1, "b": 2, "c": [1, 2, 3]}), 303)

    def test_check_label(self):
        # the following follow the rules declared in the comments
        self.assertEqual(volume_clerk_client.check_label("VR6995L1"), 0)
        self.assertEqual(volume_clerk_client.check_label("VR6995"), 0)
        self.assertEqual(volume_clerk_client.check_label("9R6995"), 1)
        self.assertEqual(volume_clerk_client.check_label("9R699"), 1)
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
                                            mover={'mover_type': 'DiskMover'},
                                            use_exact_match=1)
        self.sent_msg.reset_mock()
        self.vcc.next_write_volume(
            'library',
            'min_remaining_bytes',
            'volume_family',
            [],
            'first_found',
            {'mover_type': 'DiskMover'})
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_can_write_volume(self):
        expected_work_ticket = bogus_ticket('can_write_volume',
                                            'library',
                                            'min_remaining_bytes',
                                            'volume_family',
                                            'external_label')
        self.sent_msg.reset_mock()
        self.vcc.can_write_volume(
            'library',
            'min_remaining_bytes',
            'volume_family',
            'external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_clear_lm_pause(self):
        expected_work_ticket = bogus_ticket('clear_lm_pause',
                                            'library')
        self.sent_msg.reset_mock()
        self.vcc.clear_lm_pause('library')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_rename_volume(self):
        expected_work_ticket = bogus_ticket('rename_volume',
                                            'old',
                                            'new')
        self.sent_msg.reset_mock()
        self.vcc.rename_volume('old', 'new')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_delete_volume(self):
        expected_work_ticket = bogus_ticket('delete_volume',
                                            'external_label')
        self.sent_msg.reset_mock()
        self.vcc.delete_volume('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)
        expected_work_ticket['check_state'] = True
        self.sent_msg.reset_mock()
        self.vcc.delete_volume('external_label', check_state=True)
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_erase_volume(self):
        expected_work_ticket = bogus_ticket('erase_volume',
                                            'external_label')
        self.sent_msg.reset_mock()
        self.vcc.erase_volume('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_restore_volume(self):
        expected_work_ticket = bogus_ticket('restore_volume',
                                            'external_label')
        self.sent_msg.reset_mock()
        self.vcc.restore_volume('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_recycle_volume(self):
        expected_work_ticket = bogus_ticket('recycle_volume',
                                            'external_label',
                                            reset_declared=True)
        self.sent_msg.reset_mock()
        self.vcc.recycle_volume('external_label')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)
        expected_work_ticket['clear_sg'] = True
        self.sent_msg.reset_mock()
        self.vcc.recycle_volume('external_label', clear_sg=True)
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_ignored_sg(self):
        expected_work_ticket = bogus_ticket('set_ignored_sg',
                                            'sg')
        self.sent_msg.reset_mock()
        self.vcc.set_ignored_sg('sg')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_clear_ignored_sg(self):
        expected_work_ticket = bogus_ticket('clear_ignored_sg',
                                            'sg')
        self.sent_msg.reset_mock()
        self.vcc.clear_ignored_sg('sg')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_clear_all_ignored_sg(self):
        expected_work_ticket = bogus_ticket('clear_all_ignored_sg')
        self.sent_msg.reset_mock()
        self.vcc.clear_all_ignored_sg()
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_list_ignored_sg(self):
        expected_work_ticket = bogus_ticket('list_ignored_sg')
        self.sent_msg.reset_mock()
        self.vcc.list_ignored_sg()
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_comment(self):
        expected_work_ticket = bogus_ticket('set_comment',
                                            'vol',
                                            'comment')
        self.sent_msg.reset_mock()
        self.vcc.set_comment('vol', 'comment')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_assign_sg(self):
        expected_work_ticket = bogus_ticket('reassign_sg',
                                            'external_label',
                                            'storage_group')
        self.sent_msg.reset_mock()
        self.vcc.assign_sg('external_label', 'storage_group')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_list_migrated_files(self):
        expected_work_ticket = bogus_ticket('list_migrated_files',
                                            'src_vol',
                                            'dst_vol')
        self.sent_msg.reset_mock()
        ret_ticket = self.vcc.list_migrated_files('src_vol', 'dst_vol')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_list_duplicated_files(self):
        expected_work_ticket = bogus_ticket('list_duplicated_files',
                                            'src_vol',
                                            'dst_vol')
        self.sent_msg.reset_mock()
        ret_ticket = self.vcc.list_duplicated_files('src_vol', 'dst_vol')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_migration_history(self):
        expected_work_ticket = bogus_ticket('set_migration_history',
                                            'src_vol',
                                            'dst_vol')
        self.sent_msg.reset_mock()
        ret_ticket = self.vcc.set_migration_history('src_vol', 'dst_vol')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)

    def test_set_migration_history_closed(self):
        expected_work_ticket = bogus_ticket('set_migration_history_closed',
                                            'src_vol',
                                            'dst_vol')
        self.sent_msg.reset_mock()
        ret_ticket = self.vcc.set_migration_history_closed(
            'src_vol', 'dst_vol')
        generated_work_ticket = self.sent_msg.mock_calls[0][1][0]
        self.assertEqual(generated_work_ticket, expected_work_ticket)


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
        #import pdb; pdb.set_trace()
        sent_msg = mock.MagicMock()
        udp_client.UDPClient.send = sent_msg
        udp_client.UDPClient.send_no_wait = sent_msg
        with mock.patch('sys.stderr', new=StringIO.StringIO()) as std_err:
            with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
                with mock.patch("sys.exit") as exit_mock:
                    with mock.patch('generic_client.GenericClient.check_ticket') as check_please:
                        # test = Usage
                        volume_clerk_client.do_work(self.vci)
                        exit_mock.assert_called_with(0)
                        self.assertTrue('Usage' in std_out.getvalue())

                        # -------------
                        test = "backup"
                        # -------------
                        self.vci.backup = 1
                        exit_mock.reset_mock()
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        exit_mock.assert_not_called()
                        self.assertEqual(
                            {'work': 'start_backup'}, sent_msg.mock_calls[102][1][0],
                            test + ": " + str(sent_msg.mock_calls))
                        self.assertEqual({'work': 'backup'},
                                         sent_msg.mock_calls[104][1][0],
                                         test + ": " + str(sent_msg.mock_calls))
                        self.assertEqual(
                            {'work': 'stop_backup'}, sent_msg.mock_calls[106][1][0],
                            test + ": " + str(sent_msg.mock_calls))
                        self.vci.backup = 0

                        # -------------
                        test = "show_state"
                        # -------------
                        self.vci.show_state = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertEqual({'work': 'show_state'},
                                         sent_msg.mock_calls[102][1][0],
                                         test + ": " + str(sent_msg.mock_calls))
                        self.vci.show_state = 0

                        # -------------
                        test = "vols"
                        # -------------
                        self.vci.vols = 1
                        sent_msg.reset_mock()
                        std_out.truncate()

                        # test usage string gets printed when wrong number of
                        # args (0) given
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            "Usage" in std_out.getvalue(), std_out.getvalue())

                        # test error string gets printed when wrong number of
                        # args (4) given
                        std_out.truncate()
                        for ch in 'abcd':
                            self.vci.args.append(ch)
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            "Wrong number of arguments" in std_out.getvalue(),
                            std_out.getvalue())
                        self.vci.args = []

                        # now test with some correct args
                        sent_msg.reset_mock()
                        std_out.truncate()
                        self.vci.args.append('noaccess')
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'get_vols3'""" in str(
                                sent_msg.mock_calls), test + ": " + str(
                                sent_msg.mock_calls))
                        self.assertTrue(
                            """'in_state': 'noaccess'""" in str(
                                sent_msg.mock_calls), test + ": " + str(
                                sent_msg.mock_calls))
                        self.vci.vols = 0

                        # -------------
                        test = "pvols"
                        # -------------
                        self.vci.pvols = 1
                        self.vci.force = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'get_pvols2'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.pvols = 0
                        self.vci.force = 0

                        # -------------
                        test = "labels"
                        # -------------
                        self.vci.labels = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'get_vol_list2'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.labels = 0

                        # -------------
                        test = "next"
                        # -------------
                        self.vci.next = 1
                        sent_msg.reset_mock()
                        self.vci.args[0] = 'library'
                        self.vci.args.append('20599088733')
                        self.vci.args.append('volume_family')
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'next_write_volume'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.assertTrue(
                            """'min_remaining_bytes': 20599088733L""" in str(
                                sent_msg.mock_calls), str(
                                sent_msg.mock_calls))
                        self.assertTrue(
                            """'library': 'library'""" in str(
                                sent_msg.mock_calls), str(
                                sent_msg.mock_calls))
                        self.assertTrue(
                            """'volume_family': 'volume_family'""" in str(
                                sent_msg.mock_calls), str(
                                sent_msg.mock_calls))
                        self.vci.next = 0

                        # -------------
                        test = "assign_sg"
                        # -------------
                        self.vci.assign_sg = 'storage_group'
                        self.vci.volume = 'volume'
                        sent_msg.reset_mock()
                        self.vci.args[0] = ''
                        self.vci.args[1] = ''
                        self.vci.args[2] = ''
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'reassign_sg'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.assertTrue(
                            """'external_label': 'volume'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.assertTrue(
                            """'storage_group': 'storage_group'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.assign_sg = 0
                        self.vci.volume = 0

                        # -------------
                        test = "rebuild_sg_count"
                        # -------------
                        self.vci.rebuild_sg_count = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'rebuild_sg_count'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.rebuild_sg_count = 0

                        # -------------
                        test = "ls_sg_count"
                        # -------------
                        self.vci.ls_sg_count = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'list_sg_count2'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.ls_sg_count = 0

                        # -------------
                        test = "get_sg_count"
                        # -------------
                        self.vci.get_sg_count = 1
                        self.vci.storage_group = 'storage_group'
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'get_sg_count'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.get_sg_count = 0

                        # -------------
                        test = "set_sg_count"
                        # -------------
                        self.vci.set_sg_count = 1
                        self.vci.count = 12345
                        self.vci.storage_group = 'storage_group'
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_sg_count'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.assertTrue(
                            """'count': 12345""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.set_sg_count = 0
                        self.vci.count = 0
                        self.vci.storage_group = 0

                        # -------------
                        test = "trim_obsolete"
                        # -------------
                        self.vci.trim_obsolete = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'check_record'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.trim_obsolete = 0

                        # -------------
                        test = "show_quota"
                        # -------------
                        self.vci.show_quota = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'show_quota'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.show_quota = 0

                        # -------------
                        test = "vol"
                        # -------------
                        self.vci.vol = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'inquire_vol'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))

                        self.vci.vol = 0
                        # if intf.force:

                        # -------------
                        test = "gvol"
                        # -------------
                        self.vci.gvol = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'inquire_vol'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        # if intf.force:
                        self.vci.gvol = 0

                        # -------------
                        test = "check"
                        # -------------
                        self.vci.check = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'inquire_vol'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        # if intf.force:
                        self.vci.check = 0

                        # -------------
                        test = "history"
                        # -------------
                        self.vci.history = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'history2'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        # if intf.force:
                        self.vci.history = 0

                        # -------------
                        test = "write_protect_on"
                        # -------------
                        self.vci.write_protect_on = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'write_protect_on'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        # if intf.force:
                        self.vci.write_protect_on = 0

                        # -------------
                        test = "write_protect_off"
                        # -------------
                        self.vci.write_protect_off = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'write_protect_off'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        # if intf.force:
                        self.vci.write_protect_off = 0

                        # -------------
                        test = "write_protect_status"
                        # -------------
                        self.vci.write_protect_status = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'write_protect_status'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        # if intf.force:
                        self.vci.write_protect_status = 0

                        # -------------
                        test = "set_comment"  # set comment of vol
                        # -------------
                        self.vci.set_comment = 1
                        self.vci.comment = 'comment'
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_comment'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.set_comment = 0

                        # -------------
                        test = "export"  # volume export
                        # -------------
                        self.vci.export = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'inquire_vol'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.export = 0

                        # test = "_import"  # volume import

                        # -------------
                        test = "ignore_storage_group"
                        # -------------
                        self.vci.ignore_storage_group = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_ignored_sg'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.ignore_storage_group = 0

                        test = "forget_ignored_storage_group"
                        self.vci.forget_ignored_storage_group = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'clear_ignored_sg'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.forget_ignored_storage_group = 0

                        test = "forget_all_ignored_storage_groups"
                        self.vci.forget_all_ignored_storage_groups = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'clear_all_ignored_sg'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.forget_all_ignored_storage_groups = 0

                        # -------------
                        test = "show_ignored_storage_groups"
                        # -------------
                        self.vci.show_ignored_storage_groups = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'list_ignored_sg'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.show_ignored_storage_groups = 0

                        #test = "add"
                        #test = "modify"

                        # -------------
                        test = "new_library"
                        # -------------
                        self.vci.new_library = 1
                        self.vci.volume = 'volume'
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'new_library'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.new_library = 0

                        # -------------
                        test = "delete"
                        # -------------
                        self.vci.delete = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'delete_volume'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.delete = 0

                        # -------------
                        test = "erase"
                        # -------------
                        self.vci.erase = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'erase_volume'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.erase = 0

                        # -------------
                        test = "restore"
                        # -------------
                        self.vci.restore = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'restore_volume'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.restore = 0

                        # -------------
                        test = "recycle"
                        # -------------
                        self.vci.recycle = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'recycle_volume'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.recycle = 0

                        #test = "clear_sg"
                        #test = "clear"

                        # -------------
                        test = "decr_file_count"
                        # -------------
                        self.vci.decr_file_count = 1
                        self.vci.args.append('decr_file_count')
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'decr_file_count'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.decr_file_count = 0

                        # -------------
                        test = "read_only"
                        # -------------
                        self.vci.read_only = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_system_readonly'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.read_only = 0

                        # -------------
                        test = "full"
                        # -------------
                        self.vci.full = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_system_full'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.full = 0

                        # -------------
                        test = "migrated"
                        # -------------
                        self.vci.migrated = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_system_migrated'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.migrated = 0

                        # -------------
                        test = "duplicated"
                        # -------------
                        self.vci.duplicated = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_system_duplicated'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.duplicated = 0

                        # -------------
                        test = "no_access"
                        # -------------
                        self.vci.no_access = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_system_notallowed'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.no_access = 0

                        # -------------
                        test = "not_allowed"
                        # -------------
                        self.vci.not_allowed = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'set_system_notallowed'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.not_allowed = 0

                        # -------------
                        test = "lm_to_clear"
                        # -------------
                        self.vci.lm_to_clear = 1
                        sent_msg.reset_mock()
                        volume_clerk_client.do_work(self.vci)
                        self.assertTrue(
                            """'work': 'clear_lm_pause'""" in str(
                                sent_msg.mock_calls), test + ': ' + str(
                                sent_msg.mock_calls))
                        self.vci.lm_to_clear = 0

                        #test = "list"
                        #test = "ls_active"


if __name__ == "__main__":
    unittest.main()
