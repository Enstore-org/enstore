#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Make HTML developer documentation files for Enstore using Sphinx.
"""

# Python imports
from __future__ import print_function
import argparse
import os
import subprocess
import sys

# Parse arguments
parser = argparse.ArgumentParser(description=__doc__.strip())
parser.add_argument('--builddir',
                    help=('Build directory. If not specified, it is determined'
                          ' using the Enstore configuration client.'))
args = parser.parse_args()

# Import sphinx
try:
    import sphinx
except ImportError:
    msg = """Sphinx could not be imported. To install it, run as user enstore:
    wget --no-check-certificate https://raw.github.com/pypa/pip/master/contrib/get-pip.py
    python ./get-pip.py
    pip install sphinx"""
    exit(msg)
# Note: Sphinx version requirement is defined in conf.py.

# Determine make directory
make_dir = os.path.abspath(os.path.dirname(__file__))

# Determine and export build directory
build_dir = args.builddir
if not build_dir:
    try:
        import configuration_client
        config_client = configuration_client.ConfigurationClient()
        build_dir = config_client.get('crons')['html_dir']
        build_dir = os.path.join(build_dir, 'docs')
    except:
        msg = ('Error determining build directory automatically using the '
               'configuration client. It can be specified as a command-line '
               'option. (--builddir)')
        print(msg, end='\n\n', file=sys.stderr)
        raise
build_dir = os.path.abspath(build_dir)
os.environ['BUILDDIR'] = build_dir
# Note: The build directory is created automatically if it doesn't exist.

print("""The HTML directory path is printed after the build has finished.

General troubleshooting steps:
• If docs fail to generate for a module, ensure its .rst file exists.
• If docs fail to update for a module, "touch" its .rst file.
""")

# GNU make
make_cmd = 'make --directory="{}" html'.format(make_dir)
make_exit_status = subprocess.call(make_cmd, shell=True)
# Note: Not using "shell=True" raises "OSError: [Errno 2]".
exit(make_exit_status)
