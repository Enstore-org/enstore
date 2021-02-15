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
    print("usage: %s accounting|drivestat|operation" % (sys.argv[0]))


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
backupinfo = csc.get('backup')
backup_host = backupinfo['host'].split('.')[0]

# check if this is the right host to run
if host != backup_host:
    # not on this node
    print("wrong host: expecting %s and find %s" % (backup_host, host))
    sys.exit(0)

# check for known database
if sys.argv[1] == 'accounting':
    server = csc.get('accounting_server')
elif sys.argv[1] == 'drivestat':
    server = csc.get('drivestat_server')
elif sys.argv[1] == 'operation':
    server = csc.get('operation_db')
else:
    usage()
    sys.exit(0)

# ignore missing database
if server['status'][0] != e_errors.OK:
    usage()
    sys.exit(0)

# make up the out file
# do not take more backup in the same hour!
localtime = time.localtime()
suffix = "%02d-%02d" % (localtime.tm_mday, localtime.tm_hour,)
out_dir = os.path.join(backupinfo['dir'], 'ACC-DST')
# if not os.access(out_dir, os.F_OK):
#	os.makedirs(out_dir)
if not os.path.exists(out_dir):
    print(out_dir, " does not exist, creating ")
    os.makedirs(out_dir)
outfile = os.path.join(
    backupinfo['dir'],
    'ACC-DST',
    sys.argv[1] +
    '.backup.' +
    suffix)

cmd = "pg_dump -p %d -h %s -U %s -F c -f %s %s" % (
    server['dbport'], server['dbhost'], server['dbuser'], outfile, server['dbname'])

print(cmd)
err = os.system(cmd)
sys.exit(err)
