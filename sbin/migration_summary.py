#!/usr/bin/env python

import pg
import configuration_client
import option
import time

intf = option.Interface()
csc = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))

dbinfo = csc.get('database')
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

