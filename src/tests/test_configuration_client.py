import unittest
import time
import ast
import mock
import configuration_client
import sys
import errno
import pprint
import os
import socket
import select
import types
import time
import imp
import getpass
import generic_client
import enstore_constants
import enstore_functions2
import option
import Trace
import callback
import e_errors
import hostaddr
class TestConfigFlag(unittest.TestCase):
    def setUp(self):
        self.cf = configuration_client.ConfigFlag()

    def test___init__(self):
        self.assertTrue(isinstance(self.cf, configuration_client.ConfigFlag))
	self.assertEqual(self.cf.new_config_file, self.cf.MSG_NO)
        self.assertEqual(self.cf.do_caching, self.cf.DISABLE)

    def test_is_caching_enabled(self):
        self.assertNotEqual(self.cf.is_caching_enabled(), self.cf.do_caching)

    def test_new_config_msg(self):
        self.cf.new_config_msg()
        self.assertTrue(self.cf.new_config_file, self.cf.MSG_YES)

    def test_reset_new_config(self):
        self.cf.reset_new_config()
        self.assertEqual(self.cf.new_config_file, self.cf.MSG_NO)

    def test_disable_caching(self):
        self.cf.disable_caching()
        self.assertEqual(self.cf.do_caching, self.cf.DISABLE)

    def test_enable_caching(self):
        self.cf.enable_caching()
        self.assertEqual(self.cf.do_caching, self.cf.ENABLE)

    def test_have_new_config(self):
        self.cf.disable_caching()
        self.assertEqual(self.cf.have_new_config(), self.cf.MSG_YES)
        self.cf.enable_caching()
        self.cf.new_config_msg()
        self.assertEqual(self.cf.have_new_config(), self.cf.MSG_YES)
        self.cf.reset_new_config()
        self.assertEqual(self.cf.have_new_config(), self.cf.MSG_NO)


class TestConfigurationClient(unittest.TestCase):
    
    def setUp(self):
        self.csc = configuration_client.ConfigurationClient()
        self._mocker = mock.MagicMock()
        generic_client.GenericClient.send = self._mocker
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        init_file = os.path.join(fixture_dir, 'csc.prod.dump')
        with open(init_file, 'r') as fd:
            data = fd.read()
        
        self.full_dict = ast.literal_eval(data)
        self.csc.saved_dict = self.full_dict['dump']
        self.csc.timeout = 1
        self.csc.config_load_timestamp=time.time()
        def side_effect(*args, **kwargs ):
            return self.csc.saved_dict.get(args[0]['work'])         
        self._mocker.side_effect = side_effect
        self.csc.new_config_obj.enable_caching()
        self.have_complete_config = 1
       
    def test___init__(self):
        self.assertTrue(isinstance(self.csc, configuration_client.ConfigurationClient))

    def test_get_address(self):
        self.assertEqual(self.csc.server_address, self.csc.get_address())

    def test_get_timeout(self):
        self.assertEqual(self.csc.timeout, self.csc.get_timeout())

    @unittest.skip('no such class member broken code')
    def test_get_retry(self):
        self.assertEqual(self.csc.retry, self.csc.get_retry())

    def test_is_config_current(self):
        self.assertFalse(self.csc.is_config_current())

    @unittest.skip('too much to mock out')
    def test_get_enstore_system(self):
        pass

    def test_do_lookup(self):
        for ky in self.csc.saved_dict:
            dct = self.csc.get(ky,1,0)
            #print "****************************************"
            #print "key %s\ndict %s" % (ky, dct)

    def test_get(self):
        for ky in self.csc.saved_dict:
            dct = self.csc.get(ky,1,0)
            #print "****************************************"
            #print "key %s\ndict %s" % (ky, dct)

    @unittest.skip('doesnt work')
    def test_dump(self):
        import pdb; pdb.set_trace()
        self.csc.dump()

    @unittest.skip('doesnt work')
    def test_dump_old(self):
        self.csc.dump_old()

    @unittest.skip('doesnt work')
    def test_dump_and_save(self):
        pass
    def test_config_load_time(self):
        pass
    def test_get_keys(self):
        pass
    def test_load(self):
        pass
    def test_threaded(self):
        pass
    def test_copy_level(self):
        pass
    def test_get_movers(self):
        pass
    def test_get_movers2(self):
        pass
    def test_get_migrators2(self):
        pass
    def test_get_migrators(self):
        pass
    def test_get_media_changer(self):
        pass
    def test_get_library_managers(self):
        pass
    def test_get_library_managers2(self):
        pass
    def test_get_media_changers(self):
        pass
    def test_get_media_changers2(self):
        pass
    def test_get_migrators_list(self):
        pass
    def test_get_proxy_servers2(self):
        pass
    def test_get_dict_entry(self):
        pass
    def test_reply_serverlist(self):
        pass

class TestConfigurationClientInterface(unittest.TestCase):
    def test___init__(self):
        pass
    def test_valid_dictionaries(self):
        pass
class TestMisc(unittest.TestCase):
    def test_flatten2(self):
            pass
    def test_print_configuration(self):
            pass
    def test_configdict_from_file(self):
            pass
    def test_get_config_dict(self):
            pass
    if __name__ == "__main__":
        unittest.main()
