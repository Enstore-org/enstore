#!/usr/bin/env python

from __future__ import print_function
import os
import option
import configuration_client
import pprint
import sys
import e_errors
import time


def usage():
    print("usage: %s enstoredb|accounting|drivestat|operation" % (sys.argv[0]))


# needs an argument
if len(sys.argv) < 2:
    usage()
    sys.exit(0)

# instantiate a configuration client
intf = option.Interface()
csc = configuration_client.ConfigurationClient(
    (intf.config_host, intf.config_port))

# who am I?
host = os.uname()[1].split('.')[0]

# check for known database
if sys.argv[1] == 'enstoredb':
    server = csc.get('database')
elif sys.argv[1] == 'accounting':
    server = csc.get('accounting_server')
elif sys.argv[1] == 'drivestat':
    server = csc.get('drivestat_server')
elif sys.argv[1] == 'operation':
    server = csc.get('operation')
else:
    usage()
    sys.exit(0)

# ignore missing database
if server['status'][0] != e_errors.OK:
    usage()
    sys.exit(0)

db_host = server['dbhost'].split('.')[0]

# check if this is the right host to run
if host != db_host:
    # not on this node
    print("wrong host: expecting %s and find %s" % (db_host, host))
    sys.exit(0)

cmd = "vacuumdb -p %d -d %s -v -z" % (server['dbport'], server['dbname'])

print(cmd)
os.system(cmd)
