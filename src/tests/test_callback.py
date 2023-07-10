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
        pass

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
        self.assertTrue(callback.get_socket_read_queue_length(mock_socket), 4)

        # Test fcntl.FIONREAD not set but known OS
        fcntl.FIONREAD = None
        uname_mock = mock.patch("os.uname")
        for os_name, op_code in known_op_codes.values():
            uname_mock.return_value = [os_name]
            fcntl_mock.side_effect = lambda fd, op, arg: struct.pack("i", 4) if op == op_code else None
            self.assertTrue(callback.get_socket_read_queue_length(mock_socket), 4)

        # Test fcntl.FIONREAD not set and unknown OS
        uname_mock.return_value = "unknown"
        self.assertRaises(AttributeError, callback.get_socket_read_queue_length(mock_socket))

    def test_get_unacked_packet_count(self):
        pass  # This function is unused

    @mock.patch('os.fstat')
    @mock.patch('os.uname')
    def test___get_socket_state(self, os_uname_mock, os_fstat_mock):
        import stat
        node_name = "my_inode"
        test_fd = open("/dev/null", "r").fileno()
        _callback__get_socket_state = getattr(callback, '__get_socket_state')

        # Non-Linux OS returns None
        os_uname_mock.return_value = ["Unknown"]
         
        self.assertIsNone(_callback__get_socket_state(test_fd))

        # Test good responses (only on Linux)
        os_uname_mock.return_value = ["Linux"]
        # Mock fstat return my inode
        os_fstat_mock.return_value = {stat.ST_INO: node_name}

        test_states = {
            4: "FIN_WAIT1",
            11: "CLOSING",
            18: "UNKNOWN",
        }
        for test_state, expected_state in test_states.items():
            hex_test_state = str(hex(test_state))
            print(hex_test_state)
            # Pad read data starting with inode
            read_data = node_name + "0" * (33 - len(node_name))
            # End read data with padded hex state
            read_data = read_data + "0x" + "0" * (2 - len(hex_test_state[2:])) + hex_test_state[2:]
            m = mock.mock_open(read_data=read_data)
            with mock.patch('__builtin__.open', m):
                print expected_state
                self.assertEqual(_callback__get_socket_state(test_fd), expected_state)

        # Test handled errors: these return None
        handled_error_types = [
            socket.error,
            ValueError,
            IOError,
            OSError,
        ]
        for error in handled_error_types:
            os_fstat_mock.side_effect = error()
            self.assertIsNone(_callback__get_socket_state(test_fd))

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

    @mock.patch('socket.socket')
    @mock.patch('host_config.get_config')
    @mock.patch('host_config.get_default_interface_ip')
    def test_get_callback(self,
            get_default_interface_ip_mock,
            get_config_mock,
            socket_mock):
        config_sock_ip = "0.1.0.1"
        default_sock_ip = "0.0.0.0"
        arg_sock_ip = "1.1.1.1"

        magic_mock_sock = mock.MagicMock()
        magic_mock_sock.getsockname.return_value = ['a', '2']
        socket_mock.return_value = magic_mock_sock

        get_config_mock.return_value = None
        get_default_interface_ip_mock.return_value = config_sock_ip

        _, _, s = callback.get_callback()
        magic_mock_sock.bind.assert_called_once_with((config_sock_ip, 0))
        self.assertEqual(s, magic_mock_sock)
        magic_mock_sock.reset_mock()

        mock_config = mock.Mock()
        get_config_mock.return_value = mock_config
        mock_config.get.return_value = default_sock_ip

        _, _, s = callback.get_callback()
        magic_mock_sock.bind.assert_called_once_with((default_sock_ip, 0))
        self.assertEqual(s, magic_mock_sock)
        magic_mock_sock.reset_mock()

        _, _, s = callback.get_callback(arg_sock_ip)
        magic_mock_sock.bind.assert_called_once_with((arg_sock_ip, 0))
        self.assertEqual(s, magic_mock_sock)


    def test_connect_to_callback(self):
        pass

if __name__ == "__main__":
    unittest.main()
