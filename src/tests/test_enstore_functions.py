import unittest
import os
import socket
from os import path
from mock import patch
from mock import MagicMock
import sys

import configuration_server
import e_errors
import enstore_functions

from enstore_functions import get_config_dict
from enstore_functions import get_from_config_file
from enstore_functions import get_dict_from_config_file
from enstore_functions import get_media
from enstore_functions import get_html_dir
from enstore_functions import get_config_server_info
from enstore_functions import get_www_host
from enstore_functions import inqTrace
from enstore_functions import get_enstore_tmp_dir
import Interfaces

TEST_CONF_FILE=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'fixtures/config/enstore.conf')

class TestEnstoreFunctions(unittest.TestCase):

    def setUp(self):
       # Set environment vars that may be deleted at the beginning of tests
       # to prevent key errors
       os.environ["ENSTORE_CONFIG_FILE"] = TEST_CONF_FILE
       for env_var in ["ENSTORE_CONFIG_HOST", "ENSTORE_OUT", "ENSTORE_HOME", "ENSTORE_DIR"]:
           os.environ[env_var] = 'test'

    def test_get_config_dict(self):
       del os.environ["ENSTORE_CONFIG_FILE"]
       assert get_config_dict() == {}
       os.environ["ENSTORE_CONFIG_FILE"] = TEST_CONF_FILE
       with patch.object(configuration_server.ConfigurationDict,
                        'load_config', return_value=None):
           assert get_config_dict() == {}
       with patch.object(configuration_server.ConfigurationDict,
                        'load_config', return_value=e_errors.OK):
           assert get_config_dict() == {}
       assert isinstance(get_config_dict(),
                         configuration_server.ConfigurationDict)

    def test_get_from_config_file(self):
       server = 'simple_test.server'
       keyword = 'port'
       default = 9999
       actual = 1234
       del os.environ["ENSTORE_CONFIG_FILE"]
       assert get_from_config_file(server, keyword, default) == default
       os.environ["ENSTORE_CONFIG_FILE"] = TEST_CONF_FILE
       assert get_from_config_file(server, keyword, default) == actual
       assert get_from_config_file("bad.SERV", keyword, default) == default

    def test_get_dict_from_config_file(self):
       server = 'simple_test.server'
       actual = {
         'host': 'test',
         'port': 1234,
         'hostip': 'test',
         'status': ('ok', None),
       }
       default = {'def': 'ault'}
       del os.environ["ENSTORE_CONFIG_FILE"]
       assert get_dict_from_config_file(server, default) == default
       os.environ["ENSTORE_CONFIG_FILE"] = TEST_CONF_FILE
       assert get_dict_from_config_file(server, default) == actual
       assert get_dict_from_config_file("bad.SERV", default) == default

    def test_get_media(self):
       with patch('enstore_functions.www_server') as mocked_www_server:
         mocked_www_server.WWW_SERVER='simple_test.server'
         mocked_www_server.MEDIA_TAG='port'
         mocked_www_server.MEDIA_TAG_DEFAULT=9999
         del os.environ["ENSTORE_CONFIG_FILE"]
         assert get_media() == 9999
         os.environ["ENSTORE_CONFIG_FILE"] = TEST_CONF_FILE
         assert get_media() == 1234

    def test_get_html_dir(self):
         del os.environ["ENSTORE_CONFIG_FILE"]
         assert get_html_dir() == "."
         os.environ["ENSTORE_CONFIG_FILE"] = TEST_CONF_FILE
         assert get_html_dir() == "/srv2/enstore/www/web-pages/"

    def test_get_config_server_info(self):
         with patch('enstore_functions.enstore_functions2') as mock_en_f2:
             mock_en_f2.default_port = MagicMock(return_value=1234)
             mock_en_f2.default_host = MagicMock(return_value='enconfig01')
             del os.environ["ENSTORE_CONFIG_FILE"]
             assert get_config_server_info() == {
                 'port': 1234,
                 'host': 'enconfig01',
             }
         os.environ["ENSTORE_CONFIG_FILE"] = TEST_CONF_FILE
         assert get_config_server_info() == {
             'host': 'test', # from ENSTORE_CONFIG_HOST
             'port': 7500
         }

    def test_get_www_host(self):
       with patch('enstore_functions.enstore_functions2') as mock_en_f2:
             mock_en_f2.default_host = MagicMock(return_value='enconfig01')
             del os.environ["ENSTORE_CONFIG_FILE"]
             assert get_www_host() == 'enconfig01'
       os.environ["ENSTORE_CONFIG_FILE"] = TEST_CONF_FILE
       assert get_www_host() == 'http://dmsen02.example.com'

    def test_inqTrace(self):
       with patch.object(enstore_functions.Trace, 'trace') as mock_trace:
           inqTrace(1, 'asd')
           severity, msg = mock_trace.call_args_list[0][0]
           assert severity == 1
           assert "%s" % os.getpid() in msg
           assert 'asd' in msg

    def test_get_enstore_tmp_dir(self):
        for env_var in ["ENSTORE_CONFIG_HOST", "ENSTORE_OUT", "ENSTORE_HOME", "ENSTORE_DIR"]:
            del os.environ[env_var]
        # With all env vars None, return is 'temp_dir' value from config host.
        # Config host DNE so falls back to enstore path env vars
        # These are empty so we get path.join(None, 'tmp')
        assert get_enstore_tmp_dir() == "tmp"
        # Set enstore dir vars in reverse order of preference
        os.environ["ENSTORE_DIR"] = "/enstore/dir"
        assert get_enstore_tmp_dir() == "/enstore/dir/tmp"
        os.environ["ENSTORE_HOME"] = "/enstore/home"
        assert get_enstore_tmp_dir() == "/enstore/home/tmp"
        os.environ["ENSTORE_OUT"] = "/enstore/out"
        assert get_enstore_tmp_dir() == "/enstore/out/tmp"
        # Mock an actual return value from config host
        with patch.object(
          enstore_functions.configuration_client.ConfigurationClient,
          "get", return_value={"temp_dir": "/tmp/from/host"}):
            assert get_enstore_tmp_dir() == "/tmp/from/host"
        # Set config host to local machine (localhost is not good enough)
        # Return value is temp_dir[temp_dir] from local (i.a. test) config
        os.environ["ENSTORE_CONFIG_HOST"] = socket.gethostname()
        assert get_enstore_tmp_dir() == "/example/config/temp"

    def test_run_in_thread(self):
        # TODO: I don't think this is used, run_in_thread functions seem
        # to be defined in several places
        pass
