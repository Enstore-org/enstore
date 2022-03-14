# movcmd_mc includes C module imports.
# If this import fails, please read enstore-pytest-c-module.md,
# and remember to use `python -m pytest`. Thank you.
import movcmd_mc
import os
import pytest
from mock import patch
from movcmd_mc import *

TESTCONFIG = {
    'enstor01.mover': {
         'media_changer': 'enstor06.media_changer',
    },
    'enstor02.mover': {},
    'enstor03.library': {'asd': '1'},
    'enstor04.pagg': {
        '127.9.1': ['asd'],
        '127.3.55a': ['2g'],
    },
}

def test_endswith():
    assert endswith("asd", "sd")
    assert not endswith("asd", "fd")

def test_dict_eval():
    with pytest.raises(ValueError):
        dict_eval("notadict")
    assert dict_eval("{thisdictisjunk}") == {}
    test_value = "{'thisdict': 'is ok'}_even_with_junk"
    test_response = {"thisdict": "is ok"}
    assert dict_eval(test_value) == test_response

def test_get_config():
    expected_res = {'movers': ['enmv1', 'enmv2'], 'host': 'enconf1'}
    test_read_data = "%s" % expected_res
    with patch.object(os, 'popen') as mock_popen:
        mock_popen().read.return_value = test_read_data
        assert get_config() == expected_res

def test_get_movers():
    expected_res = ['enstor01', 'enstor02']
    assert get_movers(config=TESTCONFIG) == expected_res
    with patch.object(movcmd_mc, 'get_config') as mock_get_config:
        mock_get_config.return_value = TESTCONFIG
        assert get_movers() == expected_res

def test_get_media_changer():
    get_media_changer_res = get_media_changer(
      'enstor01.mover', config=TESTCONFIG)
    assert get_media_changer_res == 'enstor06'
    with patch.object(movcmd_mc, 'get_config') as mock_get_config:
        mock_get_config.return_value = TESTCONFIG
        get_media_changer_res = get_media_changer('enstor01.mover')
        assert get_media_changer_res == 'enstor06'
        get_media_changer_res = get_media_changer('enstor01')
        assert get_media_changer_res == 'NotFound'

def test_mc_for_movers():
    expected_res = {'Unknown': ['enstor02'], 'enstor06': ['enstor01']}
    with patch.object(movcmd_mc, 'get_config') as mock_get_config:
        mock_get_config.return_value = TESTCONFIG
        assert mc_for_movers() == expected_res
