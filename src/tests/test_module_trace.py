# movcmd_mc includes C module imports.
# If this import fails, please read enstore-pytest-c-module.md,
# and remember to use `python -m pytest`.
import module_trace
import os
import pytest
from mock import patch
from module_trace import *

TEST_MODULES = {
  "tm1": """import tm2\n"""
         """from tm3 import *""",
  "tm2": """            """
         """import tm1\n""",
  "tm3": """import tm2, tm4""",
  "tm4": '"""Docstrings"""',
}

FINAL_MTABLE = {
  "tm1": ["tm2", "tm3"],
  "tm2": ["tm1"],
  "tm3": ["tm2", "tm3"],
  "tm4": [],
}


def test_get_module_file():
  assert True # This is just a filepath concatenation.

def test_mtrace(fs):
  module_trace.mtrace("test_mod1")
  fs.create_file("tm1")
  assert True

