# movcmd_mc includes C module imports.
# If this import fails, please read enstore-pytest-c-module.md,
# and remember to use `python -m pytest`.
import module_trace
import pprint
import os
import pytest
from mock import patch
from module_trace import *

TEST_MODULES = {
  'tm1': """import tm2\n"""
         """from tm3 import *""",
  'tm2': """            \n"""
         """import tm1\n # comment"""
         """bonus lines""",
  'tm3': """import tm2, tm4""",
  'tm4': '"""Docstrings"""',
}

# Import tree starting at 'tm1'
FINAL_MTABLE = {
  'tm1': ['tm2', 'tm3'],
  'tm2': ['tm1'],
  'tm3': ['tm2', 'tm4'],
  'tm4': [],
}

# Route to import, by module, starting at 'tm1'
FINAL_RM_TABLE = {
 'tm1': ['tm1'],
 'tm2': ['tm1', 'tm2'],
 'tm3': ['tm1', 'tm3'],
 'tm4': ['tm1', 'tm3', 'tm4'],
}

def _setup_fake_fs(fake_fs):
  for (filename, data) in TEST_MODULES.items():
    fake_fs.create_file(filename, contents=data)

def _fake_get_module_file(m):
  return m

def test_get_module_file():
  assert True # This is just a filepath concatenation.

def test_mtrace(fs):
  _setup_fake_fs(fs)
  with patch("module_trace.get_module_file", wraps=_fake_get_module_file) as _:
    mtrace("tm1")
    assert(module_trace.mtable == FINAL_MTABLE)
    module_trace.mtable = {}

def test_log_trace(fs):
  path = ["a", "s", "d"]
  log_trace(path)
  assert module_trace.rm_table == {"d": path}
  module_trace.rm_table = {}

def test_trace_path(fs):
  _setup_fake_fs(fs)
  with patch("module_trace.get_module_file", wraps=_fake_get_module_file) as _:
    module_trace.trace_path([], "tm1")
    assert(module_trace.rm_table == {})
    mtrace("tm1")
    module_trace.trace_path([], "tm1")
    assert(module_trace.rm_table == FINAL_RM_TABLE)
    module_trace.trace_path([], "module_not_found")
    assert(module_trace.rm_table == FINAL_RM_TABLE)
    module_trace.mtable = {}
    module_trace.rm_table = {}
  
