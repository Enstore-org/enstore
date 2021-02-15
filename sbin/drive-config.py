#!/usr/bin/env python
#
# $Id$
#
# Script to find drive host and device inside of config file
#
from __future__ import print_function
import os
import string
import sys

import configuration_client

if __name__ == "__main__":

    foundit = 0
    host = "UNKNOWN"
    device = "UNKNOWN"
    the_drive = "UNKNOWN"

    if len(sys.argv) <= 1:
        print("mover=%s host=%s device=%s\n" % (the_drive, host, device))
        sys.exit(1)
    else:
        the_drive = sys.argv[1]

    cport = os.environ.get('ENSTORE_CONFIG_PORT', 7500)
    cport = string.atoi(cport)
    chost = os.environ.get('ENSTORE_CONFIG_HOST', 'd0ensrv2')
    csc = configuration_client.ConfigurationClient((chost, cport))
    ckeys = csc.get_keys()
    cdump = csc.dump()
    for item in ckeys['get_keys']:
        if string.find(item, '.mover') != - \
                1 and string.find(item, the_drive) == 0:
            host = cdump['dump'][item].get('host', 'UNKNOWN')
            device = cdump['dump'][item].get('device', 'UNKNOWN')
            print("mover=%s host=%s device=%s\n" % (item, host, device))
            foundit = 1
            sys.exit(0)
    if not foundit:
        print("mover=%s host=%s device=%s\n" % (the_drive, host, device))
        sys.exit(1)
