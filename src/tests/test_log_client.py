import unittest
import os
import StringIO
import threading
import mock
import e_errors
import enstore_functions
import log_client
import udp_client
import mock_csc
""" Test the log_client module
    The log_client module is used to send log messages to the log server.
    Author: Dennis Box
    Date: 2023-07-10
"""


class TestLoggerClient(unittest.TestCase):

    def setUp(self):
        self.sent_msg = mock.MagicMock()
        udp_client.UDPClient.send = self.sent_msg
        udp_client.UDPClient.send_no_wait = self.sent_msg
        csc = mock_csc.csc()
        self.log_client = log_client.LoggerClient(csc)

    def test___init__(self):
        self.assertTrue(isinstance(self.log_client, log_client.LoggerClient))

    def test_log_func(self):
        self.sent_msg.reset_mock()
        t = 0.0
        pid = 9999999
        name = self.log_client.name
        args = [e_errors.MISC + 1, "an log message", "info"]
        self.log_client.log_func(t, pid, name, args)
        unique_id = True
        p1 = {
            'message': '9999999 %s M LOG_CLIENT  6 an log message' %os.getenv('USER'),
            'work': 'log_message'}
        p2 = ('131.225.214.78', 7504)
        self.sent_msg.assert_called_with(p1, p2, unique_id=True)

    def test_set_get_logpriority(self):
        # NB the log server reacts to the log priority
        # this needs to be tested with the log server running
        # or in the log server test
        log_prio = 99999
        self.assertNotEqual(self.log_client.get_logpriority(), log_prio)
        self.log_client.set_logpriority(log_prio)
        self.assertEqual(self.log_client.get_logpriority(), log_prio)

    def test_get_logfile_name(self):
        self.sent_msg.reset_mock()
        self.log_client.get_logfile_name()
        self.sent_msg.assert_called_with(
            {'work': 'get_logfile_name'}, ('131.225.214.78', 7504), 0, 0)

    def test_get_logfiles(self):
        self.sent_msg.reset_mock()
        self.log_client.get_logfiles('today')
        self.sent_msg.assert_called_with(
            {'work': 'get_logfiles', 'period': 'today'}, ('131.225.214.78', 7504), 0, 0)

    def test_get_last_logfile_name(self):
        self.sent_msg.reset_mock()
        self.log_client.get_last_logfile_name()
        self.sent_msg.assert_called_with(
            {'work': 'get_last_logfile_name'}, ('131.225.214.78', 7504), 0, 0)


class TestTCPLoggerClient(unittest.TestCase):
    def setUp(self):
        def tmp_dir_side_effect(*args, **kwargs):
            return "/tmp"
        enstore_functions.get_enstore_tmp_dir = mock.MagicMock()
        enstore_functions.get_enstore_tmp_dir.side_effect = tmp_dir_side_effect
        threading.Thread = mock.MagicMock()
        self.sent_msg = mock.MagicMock()
        csc = mock_csc.csc()
        self.log_client = log_client.TCPLoggerClient(csc)

    def test___init__(self):
        self.assertTrue(
            isinstance(
                self.log_client,
                log_client.TCPLoggerClient))

    def test_log_func(self):
        self.sent_msg.reset_mock()
        t = 0.0
        pid = 9999999
        name = self.log_client.name
        args = [e_errors.MISC + 1, "an log message", "info"]
        self.log_client.message_buffer.put_nowait = self.sent_msg
        self.log_client.log_func(t, pid, name, args)
        p1 = {
            'message': '9999999 %s M LOG_CLIENT  6 an log message' % os.getenv('USER'),
            'work': 'log_message',
            'sender': mock.ANY}
        self.sent_msg.assert_called_with(p1)

    def test_stop(self):
        self.log_client.stop()
        self.assertFalse(self.log_client.run)

    def test_connect(self):
        with mock.patch('socket.socket.connect', new_callable=mock.MagicMock):
            self.log_client.connect()
            self.assertTrue(self.log_client.connected)

    def test_pull_message(self):
        self.assertEqual(None, self.log_client.pull_message())

    def test_write_to_local_log(self):
        self.log_client.write_to_local_log({'message': 'this is a message'})
        local_log_file = self.log_client.dump_file
        self.assertTrue(os.path.exists(local_log_file))
        os.remove(local_log_file)


class TestMisc(unittest.TestCase):

    def test_genMsgType(self):
        """
        test the gentMsgType function which formats
        the message type for the log server

        the list test_lines was derived from the log_client.py file with the
        following command:
        grep 'if string.find.*lowLine' ../log_client.py | sed -e 's/^.*Line, //g' -e 's/\\(".*"\\)\\(.*\\)/\1/'| sort | tr "\n" ","
        sometimes members in test_lines are repeated, the same input line
        is handled by different if statements
        """
        test_lines = [
            " mover ", " vol", "(re)", "added to mover list", "log server", "log_server",
                       "backup", "bad", "badmount", "busy_vols", "busy_vols", "cantrestart",
                       "completed precautionary", "config server", "configuration server",
                       "dismount", "done", "encp", "exception", "file clerk", "file database",
                       "file", "file_clerk ", "find_mover", "find_mover", "full", "get_suspect_vol",
                       "get_suspect_vol", "get_user_socket", "get_user_socket", "get_work",
                       "get_work", "get_work", "get_work", "getmoverlist", "getmoverlist",
                       "getwork", "getwork", "hurrah", "insert", "insertvol", "library manager",
                       "library_manager ", "load", "load", "media changer", "media_changer ",
                       "next_work", "next_work", "no such vol", "open_file_write",
                       "open_file_write", "performing precautionary", "quit", "read ",
                       "reading", "restart", "root error", "root_error ", "serverdied",
                       "start", "start{", "stop", "tape stall", "tape_stall", "unbind vol",
                       "unbind", "unload", "unmount", "update_mover_list", "update_mover_list",
                       "volume clerk", "volume database", "volume_clerk ", "wrapper.write",
                       "wrapper.write", "write ", "write_to_hsm", "writing"
        ]

        for lowline in test_lines:
            for err in e_errors.stypedict:
                gen_msg = log_client.genMsgType(lowline, '', err)
                #print "debug gen_msg=%s " % gen_msg
                self.assertNotEqual(
                    "", log_client.genMsgType(
                        lowline, lowline, err))

    def test_logit(self):
        sent_msg = mock.MagicMock()
        udp_client.UDPClient.send = sent_msg
        udp_client.UDPClient.send_no_wait = sent_msg
        csc = mock_csc.csc()
        lc = log_client.LoggerClient(csc)
        retval = log_client.logit(lc)
        self.assertTrue('status' in retval)

    def test_logthis(self):
        sent_msg = mock.MagicMock()
        udp_client.UDPClient.send = sent_msg
        udp_client.UDPClient.send_no_wait = sent_msg
        os.environ['ENSTORE_CONFIG_PORT'] = '7777'
        os.environ['ENSTORE_CONFIG_HOST'] = '127.0.0.1'
        with mock.patch('sys.stderr', new=StringIO.StringIO()):
            log_client.logthis()
        formatted_str = "%06d %s I LOGIT  HELLO" % (os.getpid(),os.getenv('USER'))
        param_1 = {'message': formatted_str, 'work': 'log_message'}
        sent_msg.assert_called_with(param_1, None, unique_id=True)

    def test_parse(self):
        keys = ['time', 'host', 'pid', 'user', 'severity', 'server', 'msg']
        s_keys = ['msg_type', 'msg_dict']
        linein = "15:30:11 fmv18019.fnal.gov 052082 %s I TS4500F1MC  FINISHED listDrives returned ('ok', 0, None) Thread MainThread" % os.getenv('USER')
        a_dict = log_client.parse(linein)
        for k in keys:
            self.assertTrue(k in a_dict, k)
        for k in s_keys:
            self.assertFalse(k in a_dict, k)
        linein = "06:10:40 dmsen02.fnal.gov 029136 %s I EVRLY  MSG_TYPE=EVENT_RELAY  Cleaning up ('131.225.80.65', 44501) from clients" % os.getenv('USER')
        a_dict = log_client.parse(linein)
        for k in keys:
            self.assertTrue(k in a_dict, k)
        self.assertTrue('msg_type' in a_dict)


class TestLoggerClientInterface(unittest.TestCase):
    def setUp(self):
        self.lci = log_client.LoggerClientInterface()

    def test___init__(self):
        self.assertTrue(isinstance(self.lci, log_client.LoggerClientInterface))

    def test_valid_dictionaries(self):
        self.assertEqual(4, len(self.lci.valid_dictionaries()))

    def test_do_work(self):
        with mock.patch('sys.stderr', new=StringIO.StringIO()):
            with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
                with mock.patch("sys.exit") as exit_mock:
                    with mock.patch('generic_client.GenericClient.check_ticket'):
                        log_client.do_work(self.lci)
                        exit_mock.assert_called_with(0)
                        self.assertTrue('help' in std_out.getvalue())


if __name__ == "__main__":
    unittest.main()
