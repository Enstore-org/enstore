""" Unit tests for mover.py
Author: Dennis Box, dbox@fnal.gov
"""
import unittest
import mover
import sys
import os
import threading
import errno
import socket
import time
import string
import struct
import select
import exceptions
import traceback
import fcntl
import random
import copy
import platform
import types
import configuration_client
import generic_server
import monitored_server
import inquisitor_client
import enstore_functions2
import enstore_functions3
import enstore_constants
import option
import dispatching_worker
import volume_clerk_client
import volume_family
import file_clerk_client
import info_client
import media_changer_client
import callback
import checksum
import e_errors
import udp_client
import socket_ext
import hostaddr
import string_driver
import disk_driver
import net_driver
import null_driver
import null_wrapper
import accounting_client
import drivestat_client
import Trace
import generic_driver
import event_relay_messages
import file_cache_status
import scsi_mode_select
import set_cache_status
import log_client
import string

import mover
from mover import MoverError, state_name, mode_name, total_memory, set_max_buffer_limit, get_transfer_notify_threshold, cookie_to_long, is_threshold_passed, shell_command, loc_to_cookie, host_type, create_instance


class TestMoverError(unittest.TestCase):
    def test___init__(self):
        try:
            raise MoverError("something went wrong!")
        except MoverError:
            pass


class TestMoverMisc(unittest.TestCase):
    def test_state_name(self):
        IDLE, SETUP, MOUNT_WAIT, SEEK, ACTIVE, HAVE_BOUND, DISMOUNT_WAIT, DRAINING, OFFLINE, CLEANING, ERROR, FINISH_WRITE = range(
            12)
        self.assertEqual('IDLE', state_name(IDLE))
        self.assertEqual('SETUP', state_name(SETUP))
        self.assertEqual('MOUNT_WAIT', state_name(MOUNT_WAIT))
        self.assertEqual('SEEK', state_name(SEEK))
        self.assertEqual('ACTIVE', state_name(ACTIVE))
        self.assertEqual('HAVE_BOUND', state_name(HAVE_BOUND))
        self.assertEqual('DISMOUNT_WAIT', state_name(DISMOUNT_WAIT))

    def test_mode_name(self):
        self.assertEqual(None, mode_name(None))
        self.assertEqual('READ', mode_name(0))
        self.assertEqual('WRITE', mode_name(1))
        self.assertEqual('ASSERT', mode_name(2))

    def test_total_memory(self):
        tm = total_memory()
        self.assertNotEqual(0, tm)

    def test_set_max_buffer_limit(self):
        set_max_buffer_limit()
        self.assertNotEqual(0, mover.MAX_BUFFER)

    def test_get_transfer_notify_threshold(self):
        tfer = 10000000
        thresh = get_transfer_notify_threshold(tfer)
        self.assertTrue(thresh < tfer)

        tfer = 101 * 2 * 1000000
        thresh = get_transfer_notify_threshold(tfer)
        self.assertTrue(thresh < tfer)

        tfer = 6 * 2 * 1000000
        thresh = get_transfer_notify_threshold(tfer)
        self.assertTrue(thresh < tfer)

    def test_is_threshold_passed(self):
        t = time.time()
        bytes_transfered = 0
        bytes_notified = 0
        bytes_to_transfer = 0
        self.assertEqual(1, is_threshold_passed(
            bytes_transfered, bytes_notified, bytes_to_transfer, t))
        bytes_notified = 10
        bytes_to_transfer = 100
        self.assertEqual(0, is_threshold_passed(
            bytes_transfered, bytes_notified, bytes_to_transfer, t))
        t = t - enstore_constants.MAX_TRANSFER_TIME - 10
        self.assertEqual(1, is_threshold_passed(
            bytes_transfered, bytes_notified, bytes_to_transfer, t))

    def test_shell_command(self):
        res = shell_command("release the kraken")
        self.assertEqual('', res)
        res = shell_command("pwd")
        self.assertNotEqual('', res)

    def test_cookie_to_long(self):
        self.assertEqual(123, cookie_to_long(123))
        self.assertEqual(123, cookie_to_long("123"))
        try:
            self.assertEqual(123.456, cookie_to_long(123.456))
            self.assertEqual(False, True)
        except TypeError:
            pass

    def test_loc_to_cookie(self):
        ck = loc_to_cookie(None)
        self.assertEqual("0000_000000000_0000000", ck)
        ck = loc_to_cookie('123')
        self.assertEqual("0000_000000000_0000123", ck)

    def test_host_type(self):
        x = host_type()
        self.assertEqual(3, host_type())

    @unittest.skip('dont know how to fake tape device in /dev')
    def test_identify_mc_device(self):
        pass

    def test_create_instance(self):
        ins = create_instance('null_driver', 'NullDriver', '')
        self.assertTrue(isinstance(ins, null_driver.NullDriver))


class TestBuffer(unittest.TestCase):

    def setUp(self):
        self.tb = mover.Buffer(0)
        self.nd = null_driver.NullDriver()

    def test___init__(self):
        self.assertTrue(isinstance(self.tb, mover.Buffer))

    def test_set_wrapper(self):
        self.tb.set_wrapper(null_wrapper)
        self.assertEqual(self.tb.wrapper, null_wrapper)

    def test_save_settings(self):
        self.tb.save_settings()
        self.assertEqual(self.tb.sanity_crc, self.tb.saved_sanity_crc)

    def test_restore_settings(self):
        self.tb.save_settings()
        self.tb.sanity_crc = 0
        self.assertNotEqual(self.tb.sanity_crc, self.tb.saved_sanity_crc)
        self.tb.restore_settings()
        self.assertEqual(self.tb.sanity_crc, self.tb.saved_sanity_crc)

    def test_clear(self):
        self.tb.push('some data')
        self.tb.push('more  data')
        self.tb.push('even more  data')
        b1 = len(self.tb._buf)
        self.tb.clear()
        b2 = len(self.tb._buf)
        self.assertTrue(b2 < b1)

    def test_reset(self):
        self.tb.push('some data')
        self.tb.push('more  data')
        self.tb.push('even more  data')
        b1 = self.tb.nbytes()
        self.tb.reset((None, None), True)
        b2 = self.tb.nbytes()
        self.assertTrue(b2 < b1)

    def test_full(self):
        self.tb.push('some data')
        self.tb.push('more  data')
        self.tb.push('even more  data')
        b1 = self.tb.nbytes()
        self.assertFalse(self.tb.full())
        self.tb.max_bytes = b1
        self.assertTrue(self.tb.full())

    def test_empty(self):
        self.tb.clear()
        self.assertTrue(self.tb.empty())

    def test_low(self):
        self.tb.push('some data')
        self.tb.push('more  data')
        self.tb.push('even more  data')
        b1 = self.tb.nbytes()
        self.assertFalse(self.tb.low())
        self.tb.set_min_bytes(b1 + 1)
        self.assertTrue(self.tb.low())

    def test_set_blocksize(self):
        bs = self.tb.blocksize
        bs1 = bs + 1
        self.tb.set_blocksize(bs1)
        self.assertEqual(bs1, self.tb.blocksize)
        self.tb.push('some data')
        self.tb.push('more  data')
        self.tb.push('even more  data')
        try:
            self.tb.set_blocksize(bs)
            self.assertTrue(False)
        except MoverError:
            pass

    def test_pull(self):
        self.tb.push('some data')
        self.tb.push('more  data')
        self.tb.push('even more  data')
        self.assertEqual(self.tb.pull(), 'some data')
        self.assertEqual(self.tb.pull(), 'more  data')
        self.assertEqual(self.tb.pull(), 'even more  data')

    def test_set_crc_seed(self):
        orig = self.tb.crc_seed
        self.tb.set_crc_seed(2147483647)
        new = self.tb.crc_seed
        self.assertTrue(new != orig)

    def test_nonzero(self):
        self.assertFalse(self.tb.nonzero())
        self.tb.push('some data')
        self.tb.push('more  data')
        self.tb.push('even more  data')
        self.assertTrue(self.tb.nonzero())

    def test___repr__(self):
        self.assertTrue(isinstance(self.tb.__repr__(), str))

    def test_dump(self):
        if os.path.exists("dumpfile"):
            os.remove("dumpfile")
        self.tb.push('some data')
        self.tb.push('more  data')
        self.tb.push('even more  data')
        with open("dumpfile", "w") as f:
            self.tb.dump(f)
        self.assertTrue(os.path.exists("dumpfile"))

    def test_block_read(self):
        self.nd.open(mode=0)
        self.tb.set_blocksize(100)
        self.tb.set_wrapper(null_wrapper)
        num = self.tb.block_read(100, self.nd)
        self.assertEqual(num, 100)

    def test_block_write(self):
        buf = ''
        for x in range(0, 10):
            for y in range(0, 10):
                buf = "%s%s%s" % (buf, x, y)
        self.tb.push(buf)
        self.nd.open(mode=1)
        self.tb.set_wrapper(null_wrapper)
        num = self.tb.block_write(200, self.nd)
        self.assertEqual(num, 200)

    def test_stream_read(self):
        buf = ''
        for x in range(0, 10):
            for y in range(0, 10):
                buf = "%s%s%s" % (buf, x, y)
        self.tb.set_blocksize(200)
        sd = string_driver.StringDriver(buf)
        num = self.tb.stream_read(200, sd)
        self.assertEqual(num, 200)

    def test_stream_write(self):
        buf = ''
        for x in range(0, 10):
            for y in range(0, 10):
                buf = "%s%s%s" % (buf, x, y)
        self.tb.push(buf)
        # StringDriver has no 'write' method, use NullDriver
        # sd = string_driver.StringDriver('')
        nd = null_driver.NullDriver()
        nd.open(mode=1)
        # Raises error as CRC doesn't match when writing to /dev/null
        with self.assertRaises(mover.MoverError):
            num = self.tb.stream_write(200, nd)
        # self.assertEqual(num, 200)
        nd.close()

    def test_eof_read(self):
        self.nd.open(mode=0)
        self.tb.set_blocksize(100)
        self.tb.set_wrapper(null_wrapper)
        num = self.tb.block_read(50, self.nd)
        self.tb.eof_read()
        self.assertIsNone(self.tb._reading_block)

    def test_getspace_freespace(self):
        self.tb.set_blocksize(100)
        buf = self.tb._getspace()
        self.assertEqual(len(buf), 100)
        self.tb._freespace(buf)
        self.assertEqual(len(self.tb._freelist), 1)


# commenting these out for now, I am still
# figuring out how to test these nested
# client/server inheritance tangles
# @unittest.skip('empty')
# class TestMover(unittest.TestCase):
#    def test___init__(self):
#        pass
#    def test___setattr__(self):
#        pass
#    def test_dump(self):
#        pass
#    def test_dump_vars(self):
#        pass
#    def test_set_new_bitfile(self):
#        pass
#    def test_init_data_buffer(self):
#        pass
#    def test_return_state(self):
#        pass
#    def test_log_state(self):
#        pass
#    def test_log_processes(self):
#        pass
#    def test_memory_in_use(self):
#        pass
#    def test_memory_usage(self):
#        pass
#    def test_watch_syslog(self):
#        pass
#    def test_lock_state(self):
#        pass
#    def test_unlock_state(self):
#        pass
#    def test_check_sched_down(self):
#        pass
#    def test_set_sched_down(self):
#        pass
#    def test_init_stat(self):
#        pass
#    def test_set_volume_noaccess(self):
#        pass
#    def test_update_tape_stats(self):
#        pass
#    def test_update_stat(self):
#        pass
#    def test_find_mc_drive_address(self):
#        pass
#    def test_fetch_tape_device(self):
#        pass
#    def test_get_tape_device(self):
#        pass
#    def test_start(self):
#        pass
#    def test_restart_lockfile_name(self):
#        pass
#    def test_restart_check(self):
#        pass
#    def test_restart_lock(self):
#        pass
#    def test_restart_unlock(self):
#        pass
#    def test_restart(self):
#        pass
#    def test__reinit(self):
#        pass
#    def test_send_error_and_restart(self):
#        pass
#    def test_device_dump_S(self):
#        pass
#    def test_device_dump(self):
#        pass
#    def test_check_drive_rate(self):
#        pass
#    def test_check_written_file(self):
#        pass
#    def test_nowork(self):
#        pass
#    def test_no_work(self):
#        pass
#    def test_handle_mover_error(self):
#        pass
#    def test_update_lm(self):
#        pass
#    def test_need_update(self):
#        pass
#    def test__do_delayed_update_lm(self):
#        pass
#    def test_delayed_update_lm(self):
#        pass
#    def test_check_dismount_timer(self):
#        pass
#    def test_send_error_msg(self):
#        pass
#    def test_idle(self):
#        pass
#    def test_offline(self):
#        pass
#    def test_reset(self):
#        pass
#    def test_return_work_to_lm(self):
#        pass
#    def test_read_client(self):
#        pass
#    def test_position_for_crc_check(self):
#        pass
#    def test_client_update_enabled(self):
#        pass
#    def test_send_client_update(self):
#        pass
#    def test_selective_crc_check(self):
#        pass
#    def test_write_tape(self):
#        pass
#    def test_read_tape(self):
#        pass
#    def test_write_client(self):
#        pass
#    def test_write_to_hsm(self):
#        pass
#    def test_update_volume_info(self):
#        pass
#    def test_read_from_hsm(self):
#        pass
#    def test_volume_assert(self):
#        pass
#    def test_setup_transfer(self):
#        pass
#    def test_check_connection(self):
#        pass
#    def test_assert_vol(self):
#        pass
#    def test__assert_vol(self):
#        pass
#    def test_finish_transfer_setup(self):
#        pass
#    def test_error(self):
#        pass
#    def test_broken(self):
#        pass
#    def test_position_media(self):
#        pass
#    def test_transfer_failed(self):
#        pass
#    def test_transfer_completed(self):
#        pass
#    def test_maybe_clean(self):
#        pass
#    def test_update_after_writing(self):
#        pass
#    def test_malformed_ticket(self):
#        pass
#    def test_send_client_done(self):
#        pass
#    def test_del_udp_client(self):
#        pass
#    def test_connect_client(self):
#        pass
#    def test_format_lm_ticket(self):
#        pass
#    def test_run_in_thread(self):
#        pass
#    def test_dismount_volume(self):
#        pass
#    def test_unload_volume(self):
#        pass
#    def test_mount_volume(self):
#        pass
#    def test_seek_to_location(self):
#        pass
#    def test_start_transfer(self):
#        pass
#    def test_status(self):
#        pass
#    def test_loadvol(self):
#        pass
#    def test_unloadvol(self):
#        pass
#    def test_viewdrive(self):
#        pass
#    def test_timer(self):
#        pass
#    def test_lockfile_name(self):
#        pass
#    def test_create_lockfile(self):
#        pass
#    def test_remove_lockfile(self):
#        pass
#    def test_check_lockfile(self):
#        pass
#    def test_start_draining(self):
#        pass
#    def test_stop_draining(self):
#        pass
#    def test_warm_restart(self):
#        pass
#    def test_quit(self):
#        pass
#    def test_clean_drive(self):
#        pass
# @unittest.skip('empty')
# class TestMoverInterface(unittest.TestCase):
#    def test___init__(self):
#        pass
#    def test_valid_dictionaries(self):
#        pass
#    #def test_parameters(self):
#        pass
#    def test_parse_options(self):
#        pass
# @unittest.skip('empty')
# class TestDiskMover(unittest.TestCase):
#    def test_device_dump_S(self):
#        pass
#    def test___idle(self):
#        pass
#    def test_idle(self):
#        pass
#    def test_nowork(self):
#        pass
#    def test_no_work(self):
#        pass
#    def test_write_tape(self):
#        pass
#    def test_read_tape(self):
#        pass
#    def test_create_volume_name(self):
#        pass
#    def test_setup_transfer(self):
#        pass
#    def test_finish_transfer_setup(self):
#        pass
#    def test_check_connection(self):
#        pass
#    def test_stage_file(self):
#        pass
#    def test_position_media(self):
#        pass
#    def test_transfer_failed(self):
#        pass
#    def test_transfer_completed(self):
#        pass
#    def test_update_after_writing(self):
#        pass
#    def test_format_lm_ticket(self):
#        pass
#    def test_dismount_volume(self):
#        pass
#    def test_status(self):
#        pass
if __name__ == "__main__":
    unittest.main()
