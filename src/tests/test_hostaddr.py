import unittest
import os, sys
import socket
import string, re
import types
import errno
import time
import mock
import StringIO
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


class TestHostaddr(unittest.TestCase):

    def setUp(self):
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        self.ifconfig_command  = os.path.join(fixture_dir, 'ifconfig')
        self.save_getaddrinfo = socket.getaddrinfo 
        self.save_getfqdn = socket.getfqdn
        socket.getaddrinfo = mock.MagicMock()
        socket.getaddrinfo.return_value = [(2, 1, 6, '', ('131.225.105.239', 0)), (2, 2, 17, '', ('131.225.105.239', 0)), (2, 3, 0, '', ('131.225.105.239', 0))]
        socket.getfqdn = mock.MagicMock()
        socket.getfqdn.return_value = 'www.fnal.gov'

    def tearDown(self):
        socket.getaddrinfo = self.save_getaddrinfo
        socket.getfqdn = self.save_getfqdn

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
           with mock.patch('os.uname', new_callable=mock.MagicMock): 
               with mock.patch('sys.stderr', new_callable=StringIO.StringIO) as stderr_mock:
                   hostaddr.hostinfo = None
                   hostaddr.gethostinfo("www.fnal.gov")
                   self.assertTrue("Warning:  gethostname returns" in stderr_mock.getvalue(), stderr_mock.getvalue())
    
    def test_getdomainname(self):
            dm = hostaddr.getdomainname()
            self.assertTrue(isinstance(dm, types.StringType))
            self.assertEqual(dm, 'fnal.gov')

    def test__getdomainaddr(self):
        dadr = hostaddr._getdomainaddr((10, 1, 6, '', ('2620:6a:0:8421::96', 0, 0, 0)))
        self.assertTrue(isinstance(dadr, types.StringType),dadr)
        self.assertTrue(len(dadr) > 0)
    
    def test_getdomainaddr(self):
            dadr = hostaddr.getdomainaddr()
            self.assertTrue(isinstance(dadr, types.ListType))
            self.assertTrue(len(dadr) > 0)

    @unittest.skip('private, cannot test directly') 
    def test___my_gethostbyaddr(self):
            hba = hostaddr.__my_gethostbyaddr("www.fnal.gov")
            self.assertTrue(isinstance(hba, types.ListType))
            self.assertTrue(len(hba) > 0)
            self.assertTrue(isinstance(hba[0], types.StringType))
            self.assertTrue(isinstance(hba[1], types.ListType))
            self.assertTrue(isinstance(hba[2], types.ListType))
            self.assertEqual(hba[2][0],"131.225.105.239")
    
    @unittest.skip('private, cannot test directly') 
    def test___my_gethostbyname(self):
            hbn = hostaddr.__my_gethostbyname("www.fnal.gov")
            self.assertTrue(isinstance(hbn, types.StringType))
            # return value set by mock of socket.getaddrinfo
            self.assertEqual(hbn, "131.225.105.239")
    
    def test_address_to_name(self):
        atn = hostaddr.address_to_name("127.0.0.1")
        self.assertTrue(isinstance(atn, types.StringType))
        self.assertTrue(len(atn) > 0)
        self.assertEqual(atn, "localhost")
        atn = hostaddr.address_to_name(":::1")
        self.assertEqual(atn, ":::1")
        
    
    
    def test_name_to_address(self):
        nta = hostaddr.name_to_address("www.fnal.gov")
        self.assertTrue(isinstance(nta, types.StringType))
        self.assertTrue(len(nta) > 0)
        self.assertEqual(nta, '131.225.105.239')
        nta2 = hostaddr.name_to_address(None)
        nta3 = hostaddr.name_to_address('foo,bar.baz')
    
    def test_update_domains(self):
           d = {'invalid_domains':['999.999']} 
           hostaddr.update_domains(d)
           self.assertEqual(hostaddr.known_domains['invalid_domains']['default2'], ['999.999'])
   
    def test__allow(self):
        host_list = [('2620:6a:0:8421::96', 0, 0, 0), 
                    ('131.225.191.96', 0),
                    ('abcdefg',0),
                    ('127.0.0.1',0),
                    ('127.0.0.1',0,0)
                    ]
        for hi in host_list:
            hostaddr._allow(hi[0])
        with mock.patch('socket.getfqdn', new_callable=Exception):
            ret = hostaddr._allow('131.225.191.96')
            self.assertEqual(ret,0)
        with mock.patch('socket.getaddrinfo', new_callable=Exception):
            ret = hostaddr._allow('131.225.191.96')
            self.assertEqual(ret,0)
            

    def test_allow(self):
        host_list = [('2620:6a:0:8421::96', 0, 0, 0), 
                    ('131.225.191.96', 0),
                    ('abcdefg',0),
                    ('127.0.0.1',0),
                    ('127.0.0.1',0,0)
                    ]
        for hi in host_list:
            try:
                val = hostaddr.allow(hi)
                self.assertTrue(val in [0,1] , "in=%s val=%s"%(hi,val))
            except TypeError:
                pass

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
        ifn = hostaddr.interface_name(None)
        self.assertTrue(ifn is None)
        ifn = hostaddr.interface_name(":::1")
        self.assertTrue(ifn is None)


if __name__ == "__main__":
    unittest.main()
