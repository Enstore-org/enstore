#!/usr/bin/env python
# Use this wrapper as a standalone script to purge files in enstore cache
from __future__ import print_function
import os
import sys
import socket

import configuration_client
import enstore_functions2

config_host = os.getenv('ENSTORE_CONFIG_HOST')
config_port = int(os.getenv('ENSTORE_CONFIG_PORT'))
csc = configuration_client.ConfigurationClient((config_host, config_port))
# find disk libraries to send purge requests for
local_fqdn = socket.getfqdn()

migrators = csc.get_migrators2()

disk_libraries = []
# use migrators running on this node to identify disk libraries
for m in migrators:
    if socket.getfqdn(m['host']) == local_fqdn:
        if m['disk_library'] not in disk_libraries:
            disk_libraries.append(m['disk_library'])
dl = " "
ls = dl.join(disk_libraries)
print("CMD %s %s" % ("$ENSTORE_DIR/src/purge_files.py -p %s", ls))

rc = enstore_functions2.shell_command2(
    "source $ENSTORE_DIR/sbin/krb5_ticket_sourceme; get_krb5_ticket; $ENSTORE_DIR/src/purge_files.py -p %s;destroy_krb5_ticket" %
    (ls,))
sys.exit(rc[0])
