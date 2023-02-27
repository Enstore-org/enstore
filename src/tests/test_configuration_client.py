import unittest
import string
import StringIO
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
        self.csc.send = mock.MagicMock()

        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        init_file = os.path.join(fixture_dir, 'csc.prod.dump')
        with open(init_file, 'r') as fd:
            data = fd.read()

        self.csc.config_load_timestamp = time.time()
        self.full_dict = ast.literal_eval(data)
        self.full_dict['dump']['config_load_timestamp'] = self.csc.config_load_timestamp
        self.full_dict['dump']['config_timestamp'] = self.csc.config_load_timestamp - 1
        self.csc.saved_dict = self.full_dict['dump']
        self.csc.saved_dict['status'] = ('ok', None)
        self.csc.timeout = 1
        self.this_dir = this_dir

        def send_side_effect(*args, **kwargs):
            """ This mocks the behavior of a ConfigurationClient
                returning working response dictionaries for
                the tests
            """
            ret = args[0]
            ret['status'] = ('ok', None)
            ret['info'] = 'mocked return values from config server for testing'
            akey = ret.get('work')
            if akey == 'config_timestamp':
                akey = 'config_load_timestamp'
            if akey == 'dump2':
                akey = 'dump'
            if akey == 'get_movers':
                akey = 'movers'
            if akey == 'get_library_managers':
                akey = 'library_managers'
            if akey == 'get_media_changers':
                akey = 'media_changers'
            if akey == 'get_dict_element':
                akey = ret.get('keyValue')
            if akey == 'get_migrators':
                akey = 'migrators'
                migs = {}
                for key in self.csc.saved_dict:
                    index = string.find(key, ".migrator")
                    if index != -1:
                        migrator = key[:index]
                        item = self.csc.saved_dict[key]
                        ret[migrator] = {
                            'address': (
                                item['host'],
                                item['port']),
                            'name': key}
                        migs[migrator] = ret[migrator]
                ret['migrators'] = migs
            elif akey == 'get_keys':
                keys = self.csc.saved_dict.keys()
                ret['get_keys'] = keys
            elif akey in self.csc.saved_dict:
                ret[akey] = self.csc.saved_dict[akey]
            else:
                ret[akey] = 'mocked return value'
            return ret

        self.csc.send.side_effect = send_side_effect
        self.csc.new_config_obj.enable_caching()
        self.have_complete_config = 1
        self.csc.server_address = self.csc.saved_dict['known_config_servers']['stken']

    def tearDown(self):
        filename = 'config-filec'
        if os.path.exists(filename):
            os.remove(filename)
        filename = os.path.join(self.this_dir, filename)
        if os.path.exists(filename):
            os.remove(filename)

    def test___init__(self):
        self.assertTrue(
            isinstance(
                self.csc,
                configuration_client.ConfigurationClient))

    def test_get_address(self):
        self.assertEqual(self.csc.server_address, self.csc.get_address())

    def test_get_timeout(self):
        self.assertEqual(self.csc.timeout, self.csc.get_timeout())

    @unittest.skip('no such class member! this is  broken code')
    def test_get_retry(self):
        self.assertEqual(self.csc.retry, self.csc.get_retry())

    def test_is_config_current(self):
        is_c = self.csc.is_config_current()
        self.assertTrue(is_c)

    def test_get_enstore_system(self):
        es = self.csc.get_enstore_system()
        self.assertEqual(es, 'stken')

    def test_do_lookup(self):
        for ky in self.csc.saved_dict:
            dct = self.csc.do_lookup(ky, 1, 0)
            self.assertEqual(dct, self.csc.saved_dict[ky])

    def test_get(self):
        for ky in self.csc.saved_dict:
            dct = self.csc.get(ky, 1, 0)
            self.assertEqual(dct, self.csc.saved_dict[ky])

    def test_dump(self):
        output = self.csc.dump()
        self.assertTrue(output)

    def test_dump_and_save(self):
        output = self.csc.dump_and_save()
        self.assertTrue(output)

    def test_config_load_time(self):
        clt = self.csc.config_load_time()
        self.assertTrue('config_load_timestamp' in clt)

    def test_get_keys(self):
        ret = self.csc.get_keys()
        for a_key in ret['get_keys']:
            self.assertTrue(a_key in self.csc.saved_dict, a_key)

    def test_load(self):
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        conf_dir = os.path.join(fixture_dir, 'config')
        conf_file = os.path.join(conf_dir, 'enstore.conf')
        ret = self.csc.load(conf_file)
        self.assertEqual(ret['load'], 'mocked return value', ret['load'])

    def test_threaded(self):
        ret = self.csc.threaded()
        self.assertTrue(ret['work'] == 'thread_on', ret)

    def test_copy_level(self):
        ret = self.csc.copy_level()
        self.assertTrue(ret['work'] == 'copy_level', ret)

    def test_get_movers(self):
        ret = self.csc.get_movers('TFF2-LTO9M.library_manager')
        self.assertTrue(ret == 'mocked return value', ret)

    def test_get_movers2(self):
        ret = self.csc.get_movers2('', conf_dict={'status': (1, 1)})
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) == 0)
        ret = self.csc.get_movers2('CD-DiskSF3.library_manager')
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) > 0)
        ret = self.csc.get_movers2(
            'CD-DiskSF3.library_manager',
            conf_dict=self.csc.saved_dict)
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) > 0)

    def test_get_migrators2(self):
        ret = self.csc.get_migrators2(conf_dict={'status': (1, 1)})
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) == 0)
        ret = self.csc.get_migrators2()
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) > 0)
        ret = self.csc.get_migrators2(conf_dict=self.csc.saved_dict)
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) > 0)

    def test_get_migrators(self):
        ret = self.csc.get_migrators(conf_dict={'status': (1, 1)})
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) == 0)
        ret = self.csc.get_migrators()
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) > 0)
        ret = self.csc.get_migrators(conf_dict=self.csc.saved_dict)
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(len(ret) > 0)

    def test_get_media_changer(self):
        ret = self.csc.get_media_changer('CD-DiskSF3.library_manager')
        self.assertTrue(isinstance(ret, str))

    def test_get_library_managers(self):
        ret = self.csc.get_library_managers()
        self.assertTrue(isinstance(ret, str), ret)
        self.assertEqual(ret, 'mocked return value', ret)

    def test_get_library_managers2(self):
        ret = self.csc.get_library_managers2()
        self.assertTrue(isinstance(ret, list), ret)
        self.assertTrue(len(ret) > 0)

    def test_get_media_changers(self):
        ret = self.csc.get_media_changers()
        self.assertTrue(isinstance(ret, str), ret)
        self.assertEqual(ret, 'mocked return value', ret)

    def test_get_media_changers2(self):
        ret = self.csc.get_media_changers2()
        self.assertTrue(isinstance(ret, list), ret)
        self.assertTrue(len(ret) > 0)

    def test_get_migrators_list(self):
        ret = self.csc.get_migrators_list()
        self.assertTrue(isinstance(ret, dict), ret)
        self.assertTrue(len(ret) > 0)

    def test_get_proxy_servers2(self):
        ret = self.csc.get_proxy_servers2()
        self.assertTrue(isinstance(ret, list), ret)
        self.assertEqual(ret, [], ret)

    def test_get_dict_entry(self):
        for k in self.csc.saved_dict:
            v = self.csc.get_dict_entry(k)
            self.assertEqual(
                v[k], self.csc.saved_dict[k], "%s %s %s" %
                (k, v[k], self.csc.saved_dict[k]))

    def test_reply_serverlist(self):
        ret = self.csc.reply_serverlist()
        self.assertEqual(ret['reply_serverlist'], "mocked return value")


class TestConfigurationClientInterface(unittest.TestCase):
    def setUp(self):
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            self.cci = configuration_client.ConfigurationClientInterface()

    def test___init__(self):
        self.assertTrue(
            isinstance(
                self.cci,
                configuration_client.ConfigurationClientInterface))

    def test_valid_dictionaries(self):
        vd = self.cci.valid_dictionaries()
        self.assertEqual(len(vd), 4)
        self.assertTrue(isinstance(vd[0], dict))
        self.assertTrue(isinstance(vd[1], dict))
        self.assertTrue(isinstance(vd[2], dict))
        self.assertTrue(isinstance(vd[3], dict))


class TestMisc(unittest.TestCase):

    def setUp(self):
        self.this_dir = os.path.dirname(os.path.abspath(__file__))

    def tearDown(self):
        filename = 'config-filec'
        if os.path.exists(filename):
            os.remove(filename)
        filename = os.path.join(self.this_dir, filename)
        if os.path.exists(filename):
            os.remove(filename)

    def test_misc(self):
        # test config_dict_from_file()
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        conf_dir = os.path.join(fixture_dir, 'config')
        conf_file = os.path.join(conf_dir, 'enstore.conf')
        conf_dict = configuration_client.configdict_from_file(conf_file)
        self.assertTrue(isinstance(conf_dict, dict))

        # test flatten2
        flat_dict = {}
        configuration_client.flatten2("", conf_dict, flat_dict)
        self.assertNotEqual(conf_dict, flat_dict)

        # test print_configuration
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            intf = configuration_client.ConfigurationClientInterface(
                user_mode=0)
            intf.show = 1
            configuration_client.print_configuration(conf_dict, intf)
            self.assertTrue(len(std_out.getvalue()) > 0)

        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            intf.show = 0
            intf.print_1 = 1
            configuration_client.print_configuration(conf_dict, intf)
            self.assertTrue(len(std_out.getvalue()) > 0)


if __name__ == "__main__":
    unittest.main()
