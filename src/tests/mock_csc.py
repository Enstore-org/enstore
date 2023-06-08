import string
import ast
import mock
import atexit
import configuration_client
import os
import time




class MockCSC(object):

    def __init__(self):
        self.csc = configuration_client.ConfigurationClient()
        self.csc.send = mock.MagicMock()
        atexit.register(self.tearDown)

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

        self.csc.send.side_effect = self.send_side_effect
        self.csc.new_config_obj.enable_caching()
        self.have_complete_config = 1
        self.csc.server_address = self.csc.saved_dict['known_config_servers']['stken']

    def get_csc(self):
        return self.csc

    def send_side_effect(self, *args, **kwargs):
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

    def tearDown(self):
        filename = 'config-filec'
        if os.path.exists(filename):
            os.remove(filename)
        filename = os.path.join(self.this_dir, filename)
        if os.path.exists(filename):
            os.remove(filename)


