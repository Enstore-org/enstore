import unittest
import delete_at_exit
import threading
import namespace
import os
import file_clerk_client
import signal
import inspect

from mock import patch
from mock import call
from mock import MagicMock
from mock import ANY

class TestNullDriver(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        # Release lock just in case
        if delete_at_exit.deletion_list_lock.locked():
            delete_at_exit.deletion_list_lock.release()

    def _register_fns(self):
        delete_at_exit.register("fn")
        delete_at_exit.register("fn2")

    def _register_bfids(self):
        delete_at_exit.register_bfid("xxx1234")
        delete_at_exit.register_bfid("xxy2234")

    def _get_tsd(self):
        delete_at_exit.deletion_list_lock.acquire()
        tsd = delete_at_exit.get_deletion_lists()
        delete_at_exit.deletion_list_lock.release()
        return tsd

    def test_get_deletion_lists(self):
        tsd = self._get_tsd()
        self.assertIsInstance(tsd, type(threading.local()))
        self.assertIsInstance(tsd.bfids, type([]))
        self.assertIsInstance(tsd.files, type([]))

    def test_clear_deletion_lists(self):
        self._register_fns()
        self._register_bfids()
        tsd = self._get_tsd()
        self.assertTrue(tsd.files)  # Non-empty lists are True
        self.assertTrue(tsd.bfids)
        delete_at_exit.deletion_list_lock.acquire()
        delete_at_exit.clear_deletion_lists()
        delete_at_exit.deletion_list_lock.release()
        tsd = self._get_tsd()
        self.assertFalse(tsd.files)  # Empty lists are False
        self.assertFalse(tsd.bfids)

    def test_register(self):
        self._register_fns()
        tsd = self._get_tsd()
        self.assertTrue("fn" in tsd.files)
        self.assertTrue("fn2" in tsd.files)

    def test_register_bfid(self):
        self._register_bfids()
        tsd = self._get_tsd()
        self.assertTrue("xxx1234" in tsd.bfids)
        self.assertTrue("xxy2234" in tsd.bfids)

    def test_unregister(self):
        self._register_fns()
        delete_at_exit.unregister("fn")
        tsd = self._get_tsd()
        self.assertFalse("fn" in tsd.files)
        self.assertTrue("fn2" in tsd.files)
        delete_at_exit.unregister("not_present_fn")
        tsd = self._get_tsd()
        self.assertFalse("fn" in tsd.files)
        self.assertTrue("fn2" in tsd.files)

    def test_unregister_bfid(self):
        self._register_bfids()
        delete_at_exit.unregister_bfid("xxx1234")
        tsd = self._get_tsd()
        self.assertFalse("xxx1234" in tsd.bfids)
        self.assertTrue("xxy2234" in tsd.bfids)
        delete_at_exit.unregister_bfid("not_present_bfid")
        tsd = self._get_tsd()
        self.assertFalse("xxx1234" in tsd.bfids)
        self.assertTrue("xxy2234" in tsd.bfids)

    def test_delete(self):
        self._register_fns()
        self._register_bfids()
        storage_fs_mock = MagicMock()
        file_client_mock = MagicMock()
        with patch.object(os.path, "exists", return_value=True):
            with patch.object(namespace, "StorageFS", return_value = storage_fs_mock):
                with patch.object(file_clerk_client, "FileClient",
                                  return_value = file_client_mock) as fcc_mock:
                        delete_at_exit.delete()
                        storage_fs_mock.rm.assert_called_once()
                        fcc_mock.assert_has_calls([call(ANY, "xxx1234"), call(ANY, "xxy2234")])
                        file_client_mock.set_deleted.assert_has_calls([call('yes')])
  
    def test_signal_handler(self):
        with patch.object(delete_at_exit, "delete_and_quit") as mock_daq:
            delete_at_exit.signal_in_progress = True
            delete_at_exit.signal_handler(10, inspect.currentframe())
            mock_daq.assert_not_called()
            delete_at_exit.signal_in_progress = False
            delete_at_exit.signal_handler(10, inspect.currentframe())
            mock_daq.assert_has_calls([call(128 + 10)])
    
    def test_setup_signal_handling(self):
        with patch.object(signal, "signal") as mock_signal:
            delete_at_exit.setup_signal_handling()
            # Ensure handlers called out in signal_handler are assigned
            for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGQUIT]:
                self.assertTrue(call(sig, delete_at_exit.signal_handler) in mock_signal.mock_calls)
            # Ensure not called with some of the leave alone list
            for sig in [signal.SIGCONT, signal.SIGCHLD, signal.SIGPIPE]:
                try:
                    mock_signal.assert_called_with(sig)
                except AssertionError:
                    continue
                raise AssertionError("signal.signal called with signal %s, which "
                                     "should have been excluded." % sig)
  
    def delete_and_quit(self):
        with patch.object(delete_at_exit, 'delete') as mock_delete:
            with patch.object(os, '_exit') as mock__exit:
                delete_and_exit(99)
                mock_delete.assert_called_once()
                mock__exit.assert_called_with(99)
                delete_and_exit()
                mock__exit.assert_called_with(1)
  
if __name__ == "__main__":
    unittest.main()
