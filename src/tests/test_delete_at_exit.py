import unittest
import delete_at_exit
import threading
import namespace
import os
import file_clerk_client
import signal
import inspect

class TestNullDriver(unittest.TestCase):

    def setUp(self):

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
      tsd = _get_tsd()
      assertIsInstance(tsd, type(threading.local()))
      assertIsInstance(tsd.bfids, type([]))
      assertIsInstance(tsd.files, type([]))

    def test_clear_deletion_lists(self):
      _register_fns()
      _register_bfids()
      tsd = _get_tsd()
      assertTrue(tsd.files)  # Non-empty lists are True
      assertTrue(tsd.bfids)
      delete_at_exit.deletion_list_lock.acquire()
      delete_at_exit.clear_deletion_lists()
      delete_at_exit.deletion_list_lock.release()
      tsd = _get_tsd()
      assertFalse(tsd.files)  # Empty lists are False
      assertFalse(tsd.bfids)

    def test_register(self)
      _register_fns()
      tsd = _get_tsd()
      assertTrue("fn" in tsd.files)
      assertTrue("fn2" in tsd.files)

    def test_register_bfid(self)
      _register_bfids()
      tsd = _get_tsd()
      assertTrue("xxx1234" in tsd.bfids)
      assertTrue("xxx2234" in tsd.bfids)

    def test_unregister(self)
      _register_fns()
      delete_at_exit.unregister("fn")
      tsd = _get_tsd()
      assertFalse("fn" in tsd.files)
      assertTrue("fn2" in tsd.files)
      delete_at_exit.unregister("not_present_fn")
      tsd = _get_tsd()
      assertFalse("fn" in tsd.files)
      assertTrue("fn2" in tsd.files)

    def test_unregister_bfid(self)
      _register_bfids()
      delete_at_exit.unregister_bfid("xxx1234")
      tsd = _get_tsd()
      assertFalse("xxx1234" in tsd.bfids)
      assertTrue("xxy2234" in tsd.bfids)
      delete_at_exit.unregister_bfid("not_present_bfid")
      tsd = _get_tsd()
      assertFalse("xxx1234" in tsd.bfids)
      assertTrue("xxy2234" in tsd.bfids)

    def test_delete(self)
      _register_fns()
      _register_bfids()
      with patch.object(os.path, "exists", side_effect=[True, False]):
        with patch.object(namespace.storageFS, "rm") as mock_rm:
          with patch.object(file_clerk_client.FileClient,
                            "set_deleted") as mock_set_deleted:
            delete_at_exit.delete()
            mock_rm.assert_called_once()
            mock_set_deleted.assert_has_calls([call("xxx1234"), call("xxy2234")])

    def test_signal_handler(self):
      with patch.object(delete_at_exit, "delete_and_quit") as mock_daq:
        delete_at_exit.signal_in_progress = True
        signal_handler(10, inspect.currentframe())
        mock_daq.assert_not_called()
        delete_at_exit.signal_in_progress = False
        signal_handler(10, inspect.currentframe())
        mock_daq.assert_has_calls([call(128 + 10)])

    def test_setup_signal_handling(self):
      with patch.object(signal, "signal") as mock_signal:
        delete_at_exit.setup_signal_handling()
        # Ensure handlers called out in signal_handler are assigned
        mock_signal.assert_has_calls([
          call(signal.SIGTERM),
          call(signal.SIGINT),
          call(signal.SIGQUIT),
        ])
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
