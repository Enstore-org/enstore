import callback
import fcntl
import mock
import os
import pytest_socket
import socket
import unittest

import Trace


class TestCallback(unittest.TestCase):

    def setUp(self):
        # Make sure we don't get actual sockets
        # This allows us to mock less
        pytest_socket.disable_socket()

    def test___init__(self):
        pass

    def test_hex8(self):
        self.assertEqual(callback.hex8(12), "0000000c")
        self.assertEqual(callback.hex8(12L), "0000000c")
        self.assertEqual(callback.hex8(0x12c), "0000012c")
        self.assertRaises(TypeError, callback.hex8, "0x12c")
        self.assertEqual(callback.hex8(4294967295), "ffffffff")
        self.assertRaises(OverflowError, callback.hex8, 4294967296)

    def test_get_socket_read_queue_length(self):
        mock_socket = mock.Mock(spec=socket.socket)
        known_op_codes = {
            "Linux": 0x541B,
            "IRIX": 1074030207,
            "SunOS": 1074030207,
            "OSF1": 1074030207,
        }

        # Test fcntl.FIONREAD set
        fcntl.FIONREAD = "FIONREAD"
        fcntl_mock = mock.patch("fcntl.ioctl")
        fcntl_mock.side_effect = lambda fd, op, arg: struct.pack("i", 4) if op == fcntl.FIONREAD else None
        self.assertTrue(get_socket_read_queue_length(mock_socket), 4)

        # Test fcntl.FIONREAD not set but known OS
        fcntl.FIONREAD = None
        uname_mock = mock.patch("os.uname")
        for os_name, op_code in known_op_codes.values():
            uname_mock.return_value = [os_name]
            fcntl_mock.side_effect = lambda fd, op, arg: struct.pack("i", 4) if op == op_code else None
            self.assertTrue(get_socket_read_queue_length(mock_socket), 4)

        # Test fcntl.FIONREAD not set and unknown OS
        uname_mock.return_value = "unknown"
        self.assertRaises(AttributeError, get_socket_read_queue_length(mock_socket))

    def test_get_unacked_packet_count(self):
        pass  # This function is unused

    def test___get_socket_state(self):
        import stat
        os_mock = mock.Mock()
        mock.patch('os', os_mock)
        node_name = "my_inode"

        # Non-Linux OS returns None
        os_mock.uname.return_value = ["Unknown"]
        self.assertIsNone(__get_socket_state(123))

        # Test good responses (only on Linux)
        os_mock.uname.return_value = ["Linux"]
        os_mock.fstat.return_value = {stat.ST_INO: [node_name]}

        net_tcp_mock = mock.Mock()
        test_states = {
            4: "FIN_WAIT1",
            11: "CLOSING",
            18: "UNKNOWN",
        }
        for test_state, expected_state in test_states:
            hex_test_state = str(hex(test_state))
            test_state_string = "0" * (37 - 33 - len(hex_test_state)) + hex_test_state
            net_tcp_mock.readlines.return_value = [
                "x",
                node_name + "0" * (37 - len(node_name) - len(test_state_string)) + test_state_string,
            ]
            self.assertEqual(__get_socket_state(mock.Mock()), expected_state)

        # Test handled errors: these return None
        handled_error_types = [
            socket.error,
            ValueError,
            IOError,
            OSError,
        ]
        for error in handled_error_types:
            os_mock.fstat.side_effect = error()
            self.assertIsNone(__get_socket_state(mock.Mock()))

    def test_log_socket_State(self):
        # Not that much to do here, this function just logs socket state
        # It has no return statements and raises no errors
        mock_sock = mock.Mock(spec=socket.socket)
        mock.patch('socket', mock_sock)
        mock_trace = mock.Mock()
        mock.patch(Trace, mock_trace)

        # Test success even if socket throws errors
        mock_sock.getsockopt.side_effect = socket.error()
        mock_sock.getpeername.side_effect = socket.error()

        log_socket_state(sock)

        # Make sure it logs something: state and/or socket errors
        mock_trace.log.assert_called()

    def test_get_callback(self):
        default_sock_ip = "0.0.0.0"
        arg_sock_ip = "1.1.1.1"

        mock_sock = mock.Mock(spec=socket.socket)
        mock.patch('socket', mock_sock)

        magic_mock_sock = mock.MagicMock()
        mock_sock.socket.return_value = magic_mock_sock

        mock_config = mock.Mock()
        mock_config.get.return_value = default_sock_ip

        mock_host_config = mock.Mock()
        mockpatch(host_config, mock_host_config)
        mock_host_config.get_config.return_value = mock_config

        h, _, s = get_callback()
        assertEqual(h, default_sock_ip)
        magic_mock_sock.assert_called_once('bind', (default_sock_ip, 0))
        assertEqual(s, magic_mock_sock)

        h, _, s = get_callback(arg_sock_ip)
        assertEqual(h, arg_sock_ip)
        magic_mock_sock.assert_called_once('bind', (arg_sock_ip, 0))
        assertEqual(s, magic_mock_sock)


    def test_connect_to_callback(self):
        pass

if __name__ == "__main__":
    unittest.main()
