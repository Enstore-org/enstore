import unittest
import callback
import mocker


class TestCallback(unittest.TestCase):

    def setUp(self):
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
        mock_socket = mocker.Mock(spec=socket.socket)
        known_op_codes = {
            "Linux": 0x541B,
            "IRIX": 1074030207,
            "SunOS": 1074030207,
            "OSF1": 1074030207,
        }

        # Test fcntl.FIONREAD set
        fcntl.FIONREAD = "FIONREAD"
        fcntl_mock = mocker.patch("fcntl.ioctl")
        fcntl_mock.side_effect = lambda fd, op, arg: struct.pack("i", 4) if op == fcntl.FIONREAD else None
        self.assertTrue(get_socket_read_queue_length(mock_socket), 4)

        # Test fcntl.FIONREAD not set but known OS
        fcntl.FIONREAD = None
        uname_mock = mocker.patch("os.uname")
        for os, op_code in known_op_codes.values():
            uname_mock.return_value = [os]
            fcntl_mock.side_effect = lambda fd, op, arg: struct.pack("i", 4) if op == op_code else None
            self.assertTrue(get_socket_read_queue_length(mock_socket), 4)

        # Test fcntl.FIONREAD not set and unknown OS
        uname_mock.return_value = "unknown"
        self.assertRaises(AttributeError, get_socket_read_queue_length(mock_socket))

    def test_get_unacked_packet_count(self):
        pass  # This function is unused

    def test___get_socket_state(self):
        import stat
        os_mock = mocker.Mock()
        mocker.patch('os', os_mock)
        node_name = "my_inode"

        # Non-Linux OS returns None
        os_mock.uname.return_value = ["Unknown"]
        self.assertIsNone(__get_socket_state(123))

        # Test good responses (only on Linux)
        os_mock.uname.return_value = ["Linux"]
        os_mock.fstat.return_value = {stat.ST_INO: [node_name]}

        net_tcp_mock = mocker.Mock()
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
            self.assertEqual(__get_socket_state(mocker.Mock()), expected_state)

        # Test handled errors: these return None
        handled_error_types = [
            socket.error,
            ValueError,
            IOError,
            OSError,
        ]
        for error in handled_error_types:
            os_mock.fstat.side_effect = error()
            self.assertIsNone(__get_socket_state(mocker.Mock()))


if __name__ == "__main__":
    unittest.main()
