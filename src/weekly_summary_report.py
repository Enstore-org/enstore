#!/usr/bin/env python
import pg
import time
import option
import configuration_client

# get a configuration client
intf = option.Interface()
csc = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))

# get db hosts, ports, and names for enstore db and accounting db
database = csc.get('database')
accounting_server = csc.get('accounting_server')
# use volume_clerk['host'] to determine the system
volume_clerk = csc.get('volume_clerk')

# connections to the databases
enstoredb = pg.DB(host=database['db_host'], port=database['db_port'], dbname=database['dbname'])
accdb = pg.DB(host=accounting_server['dbhost'], port=accounting_server.get('db_port', 5432), dbname=accounting_server['dbname'])

print "This report is generated at",
print time.ctime(time.time()),
print "for",
if volume_clerk['host'][:3] == "d0e":
	print "D0",
else:
	print  volume_clerk['host'][:3].upper(),
print "system."
# calculate the reporting period
print "Reporting period:",
t = time.localtime(time.time())
t1 = time.mktime((t[0], t[1], t[2], 0, 0, 0, 0, 0, 0))
t2 = t1 - 60*60*24*7
print time.ctime(t2), "--", time.ctime(t1-1)
print
print
print "======================="
print "Transfer in last 7 days"
print "======================="
print accdb.query("select * from data_transfer_last_7days() order by storage_group;")
print "============================="
print "Tapes recycled in last 7 days"
print "============================="
print enstoredb.query("select * from tapes_recycled_last_7days() order by media_type;")
print "============================="
print "Bytes recycled in last 7 days"
print "============================="
print enstoredb.query("select * from bytes_deleted_last_7days() order by storage_group;")
print "================"
print "Remaining blanks"
print "================"
print enstoredb.query("select * from remaining_blanks order by media_type;")
print "==========================="
print "Blanks drawn in last 7 days"
print "==========================="
print accdb.query("select * from blanks_drawn_last_7days() order by media_type;")
