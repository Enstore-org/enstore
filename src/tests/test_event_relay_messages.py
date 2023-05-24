import unittest
import mock
import event_relay_messages as erm
import copy

class TestEventRelayMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayMsg('test_host',999)
        self.sock = mock.MagicMock()
    
    def test___init__(self):
        self.assertTrue(self.msg.host == 'test_host')
        self.assertTrue(self.msg.port == 999)
        self.assertTrue(isinstance(self.msg, erm.EventRelayMsg))
    
    def test_message(self):
        a_msg = self.msg.message()
        self.assertTrue(isinstance(a_msg, str))
        self.assertEqual(a_msg, ' ')

    def test_send(self):
        self.msg.send(self.sock, ' ')
        self.sock.sendto.assert_called_once_with(' ', ' ')

    def test_encode_addr(self):
        self.assertEqual(self.msg.encode_addr(), 'test_host 999')

    def test_encode(self):
        self.msg.encode()


class TestEventRelayNotifyMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayNotifyMsg('test_host',999)
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayNotifyMsg))
    
    def test_encode_decode(self):
        self.msg.encode(['test message'])
        self.assertEqual(self.msg.extra_info, 'test_host 999 test message')
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.NOTIFY) 
        self.assertEqual(self.msg.host, 'test_host')
        self.assertEqual(self.msg.port, '999')
        self.assertEqual(self.msg.msg_types, 'test message')



class TestEventRelayUnsubscribeMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayUnsubscribeMsg('test_host',999)
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayUnsubscribeMsg))

    def test_encode_decode(self):
        self.msg.encode()
        self.assertEqual(self.msg.extra_info, 'test_host 999')
        self.assertEqual(self.msg.type, erm.UNSUBSCRIBE)   
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.host, 'test_host')
        self.assertEqual(self.msg.port, '999')  
        self.assertEqual(erm.decode_type(self.msg.message())[0],erm.UNSUBSCRIBE )  
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())
     

class TestEventRelayAliveMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayAliveMsg('test_host',999)
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayAliveMsg))

    def test_encode_decode(self):
        self.msg.encode('relay_server', 'opt_msg')
        self.assertEqual(self.msg.extra_info, 'test_host 999 relay_server opt_msg')
        self.assertEqual(self.msg.type, erm.ALIVE)   
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.host, 'test_host')
        self.assertEqual(self.msg.port, '999')  
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())


class TestEventRelayNewConfigFileMsg(unittest.TestCase):

    def setUp(self):
        self.msg = erm.EventRelayNewConfigFileMsg('test_host')
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayNewConfigFileMsg))

    def test_encode_decode(self):
        self.msg.encode()
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.NEWCONFIGFILE)
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())



class TestEventRelayTransferMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayTransferMsg('test_host')
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayTransferMsg))

    def test_encode_decode(self):
        self.msg.encode(100, 300)
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.TRANSFER)
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())




class TestEventRelayEncpXferMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayEncpXferMsg('test_host')
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayEncpXferMsg))

    def test_encode_decode(self):
        self.msg.encode()
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.ENCPXFER)
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())

class TestEventRelayDumpMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayDumpMsg('test_host')
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayDumpMsg))

    def test_encode_decode(self):
        self.msg.encode()
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.DUMP)
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())

class TestEventRelayQuitMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayQuitMsg('test_host')
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayQuitMsg))

    def test_encode_decode(self):
        self.msg.encode()
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.QUIT)
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())

class TestEventRelayDoPrintMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayDoPrintMsg('test_host')
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayDoPrintMsg))

    def test_encode_decode(self):
        self.msg.encode()
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.DOPRINT)
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())

class TestEventRelayDontPrintMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayDontPrintMsg('test_host')
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayDontPrintMsg))

    def test_encode_decode(self):
        self.msg.encode()
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.DONTPRINT)
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())

class TestEventRelayHeartbeatMsg(unittest.TestCase):
    def setUp(self):
        self.msg = erm.EventRelayHeartbeatMsg('test_host')
        self.sock = mock.MagicMock()
        self.assertTrue(isinstance(self.msg, erm.EventRelayHeartbeatMsg))


    def test_encode_decode(self):
        self.msg.encode()
        self.msg.decode(self.msg.message())
        self.assertEqual(self.msg.type, erm.HEARTBEAT)
        msg2 = copy.deepcopy(self.msg)
        erm.decode(msg2.message())
        self.assertEqual(msg2.message(),self.msg.message())


if __name__ == "__main__":
    unittest.main()
