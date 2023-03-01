import unittest
import os, sys
import socket
import string, re
import types
import errno
import time
import mock
try:
    import enroute
except ImportError:
    sys.modules['enroute'] = mock.MagicMock()
    sys.modules['runon'] = mock.MagicMock()
    sys.modules['Interfaces'] = mock.MagicMock()
    sys.stderr.write("WARNING using mocked import of enstore C library\n")
import e_errors
import Trace
import hostaddr
import socket

socket.getaddrinfo = mock.MagicMock()
socket.getaddrinfo.return_value = [(2, 1, 6, '', ('131.225.105.239', 0)), (2, 2, 17, '', ('131.225.105.239', 0)), (2, 3, 0, '', ('131.225.105.239', 0))]
socket.getfqdn = mock.MagicMock()
socket.getfqdn.return_value = 'www.fnal.gov'

class TestHostaddr(unittest.TestCase):

    def setUp(self):
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        self.ifconfig_command  = os.path.join(fixture_dir, 'ifconfig')

    def test_is_ip(self):
            self.assertEqual(hostaddr.is_ip("www.fnal.gov"), 0)
            self.assertEqual(hostaddr.is_ip("127.0.0.1"), 1)
            try:
                    hostaddr.is_ip(0)
                    self.assertEqual(True, False)
            except TypeError:
                    pass
    
    def test_gethostinfo(self):
           hostinfo = hostaddr.gethostinfo("www.fnal.gov") 
           self.assertTrue(isinstance(hostinfo, types.ListType))
           self.assertTrue(len(hostinfo) > 0)
    
    def test_getdomainname(self):
            dm = hostaddr.getdomainname()
            self.assertTrue(isinstance(dm, types.StringType))
            self.assertEqual(dm, 'fnal.gov')

    @unittest.skip('private, cannot test') 
    def test__getdomainaddr(self):
            dadr = hostaddr._getdomainaddr('www.fnal.gov')
            self.assertTrue(isinstance(dadr, types.ListType))
            self.assertTrue(len(dadr) > 0)
    
    def test_getdomainaddr(self):
            dadr = hostaddr.getdomainaddr()
            self.assertTrue(isinstance(dadr, types.ListType))
            self.assertTrue(len(dadr) > 0)

    @unittest.skip('private, cannot test') 
    def test___my_gethostbyaddr(self):
            hba = hostaddr._my_gethostbyaddr("www.fnal.gov")
            self.assertTrue(isinstance(hba, types.ListType))
            self.assertTrue(len(hba) > 0)
            self.assertTrue(isinstance(hba[0], types.StringType))
            self.assertTrue(isinstance(hba[1], types.ListType))
            self.assertTrue(isinstance(hba[2], types.ListType))
            self.assertEqual(hba[2][0],"131.225.105.239")
    
    @unittest.skip('private, cannot test') 
    def test___my_gethostbyname(self):
            hbn = hostaddr._my_gethostbyname("www.fnal.gov")
            self.assertTrue(isinstance(hbn, types.StringType))
            # return value set by mock of socket.getaddrinfo
            self.assertEqual(hbn, "131.225.105.239")
    
    def test_address_to_name(self):
            atn = hostaddr.address_to_name("127.0.0.1")
            self.assertTrue(isinstance(atn, types.StringType))
            self.assertTrue(len(atn) > 0)
            self.assertEqual(atn, "localhost")
    
    
    def test_name_to_address(self):
            nta = hostaddr.name_to_address("www.fnal.gov")
            self.assertTrue(isinstance(nta, types.StringType))
            self.assertTrue(len(nta) > 0)
            self.assertEqual(nta, '131.225.105.239')
    
    def test_update_domains(self):
           d = {'invalid_domains':['999.999']} 
           hostaddr.update_domains(d)
           self.assertEqual(hostaddr.known_domains['invalid_domains']['default2'], ['999.999'])
    
    def test_allow(self):
            al = hostaddr.allow('127.0.0.1')
            self.assertTrue(al)

    def test_find_ifconfig_command(self):
            if_cmd = hostaddr.find_ifconfig_command()
            if if_cmd:
                self.assertTrue(isinstance(if_cmd, types.StringType))
                self.assertTrue(len(if_cmd) > 0)
            else:
                self.assertTrue(os.path.exists(self.ifconfig_command))
    
    def test_interface_name(self):
        if not hostaddr.find_ifconfig_command():
            hostaddr.ifconfig_command = self.ifconfig_command
        ifn = hostaddr.interface_name("127.0.0.1")
        self.assertTrue("lo" in ifn)

if __name__ == "__main__":
    unittest.main()
