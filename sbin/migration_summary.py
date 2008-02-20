#!/usr/bin/env python

import pg
import configuration_client
import option
import time
import sys

intf = option.Interface()
csc = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))

dbinfo = csc.get('database')

if len(sys.argv) > 1 and sys.argv[1] == '--default-log':
	outfile = csc.get('crons').get('html_dir')+'/tape_inventory/MIGRATION_SUMMARY'
	sys.stdout = open(outfile, 'w')

db = pg.DB(host=dbinfo['dbhost'], port=dbinfo['dbport'], dbname=dbinfo['dbname'])
print time.ctime(time.time())
print
print "================="
print "Migration per day"
print "================="
print
print  db.query("select date(time) as date, count(distinct src) from migration_history where time > '2008-01-01' group by date order by date;")
print
print db.query("select count(label) as \"migrated volumes\" from volume where system_inhibit_1 = 'migrated' and not label like 'PRI%';")
print
print db.query("select count(distinct dst) as \"tapes written\" from migration_history where not src like 'PRI%';")

