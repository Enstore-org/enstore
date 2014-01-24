#!/usr/bin/env python

"""
Make Sphinx html files.
"""

# Python imports
from __future__ import print_function
import os
import subprocess

# Enstore imports
import configuration_client

# Test presence of Sphinx
try:
    import sphinx
except ImportError:
    exit('Sphinx could not be imported.')

# Determine make directory
make_dir = os.path.abspath(os.path.dirname(__file__))
print('Make directory: {}'.format(make_dir))

# Determine and export build directory
config_client = configuration_client.ConfigurationClient()
build_dir = os.path.join(config_client.get('crons')['html_dir'], 'dev')
os.environ['BUILDDIR'] = build_dir
print('Build directory: {}'.format(build_dir))

# Make
make_cmd = 'make --directory="{}" html'.format(make_dir)
print(make_cmd)
make_exit_status = subprocess.call(make_cmd, shell=True)
# Note: Not using "shell=True" raises "OSError: [Errno 2]".
exit(make_exit_status)
