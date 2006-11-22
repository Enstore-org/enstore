#!/usr/bin/env python
"""
weekly_summary_report.py [<log_file>]

Generates weekly summary report and prints it through stdout.
If <log_file> is specified:
[1] a copy of the report is written to <log_file>, and
[2] <log_file> will be "enrcp-ed" to
    *srv2:/diska/tape-inventory/WEEKLY_SUMMARY, and
[3] the content of <log_file> will be mailed to $ENSTORE_MAIL
"""

import pg
import time
import option
import configuration_client
import sys
import os

# mailing adress, if it is needed
mail_address = os.environ.get("ENSTORE_MAIL", "hyp-enstore@hppc.fnal.gov")

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


def eprint(ff, s):
	if ff:
		print >> ff, s
	print s

if len(sys.argv) > 1:
	f = open(sys.argv[1], 'w')
else:
	f = None

if volume_clerk['host'][:3] == "d0e":
	system = "D0"
else:
	system = volume_clerk['host'][:3].upper()

# add cope@fnal.gov to CDF and D0 report
# add oleynik@fnal.gov to all reports
if system == 'D0' or system == 'CDF':
	mail_address = mail_address + ' cope@fnal.gov'
if system == 'CDF':
	mail_address = mail_address + ' genser@fnal.gov'
mail_address = mail_address + ' oleynik@fnal.gov'

eprint(f, "This report is generated at %s for %s system"%(
	time.ctime(time.time()), system))

# calculate the reporting period
t = time.localtime(time.time())
t1 = time.mktime((t[0], t[1], t[2], 0, 0, 0, 0, 0, 0))
t2 = t1 - 60*60*24*7
eprint(f, "Reporting period: %s -- %s\n\n"%(
	time.ctime(t2), time.ctime(t1-1)))
eprint(f, "=======================")
eprint(f, "Transfer in last 7 days")
eprint(f, "=======================")
eprint(f, accdb.query("select * from data_transfer_last_7days() order by storage_group;"))
eprint(f, "=============================")
eprint(f, "Tapes recycled in last 7 days")
eprint(f, "=============================")
eprint(f, enstoredb.query("select * from tapes_recycled_last_7days() order by media_type;"))
eprint(f, "=============================")
eprint(f, "Bytes recycled in last 7 days")
eprint(f, "=============================")
eprint(f, enstoredb.query("select * from bytes_deleted_last_7days() order by storage_group;"))
eprint(f, "================")
eprint(f, "Remaining blanks")
eprint(f, "================")
eprint(f, enstoredb.query("select * from remaining_blanks order by media_type;"))
eprint(f, "===========================")
eprint(f, "Blanks drawn in last 7 days")
eprint(f, "===========================")
eprint(f, accdb.query("select * from blanks_drawn_last_7days() order by media_type;"))

# if there is a file, copy it to *srv2:/diska/tape_intventory
if f:
	f.close()
	# copy it to *srv2
	cmd = "enrcp %s %sensrv2:/diska/tape-inventory/WEEKLY_SUMMARY"%(
		sys.argv[1], system.lower())
	print cmd
	try:
		os.system(cmd)
	except:
		print "Error: Can not", cmd
		sys.exit(1)
	# mail it out to $ENSTORE_MAIL
	cmd = 'mail -s "Weekly Summary Report for %s System" %s < %s'%(
		system, mail_address, sys.argv[1])
	print cmd
	try:
		os.system(cmd)
	except:
		print "Error: Can not", cmd
		sys.exit(1)
