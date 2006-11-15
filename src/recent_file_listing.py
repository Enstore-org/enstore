#!/usr/bin/env python

from edb import timestamp2time
import time
import sys
import option
import configuration_client
import os

SG = 'cms'
DURATION = 12
PREFIX = 'RECENT_FILES_'

if __name__ == '__main__':
	intf = option.Interface()
	csc = configuration_client.ConfigurationClient(
		(intf.config_host, intf.config_port))
	database = csc.get('database')
	if len(sys.argv) > 1:
		SG = sys.argv[1]
	if len(sys.argv) > 2:
		DURATION = int(sys.argv[2])
	if len(sys.argv) > 3:
		PREFIX = sys.argv[3]

	t = time.localtime()
	t1 = (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, 0, 0,
		t.tm_wday, t.tm_yday, t.tm_isdst)
	t0 = int(time.mktime(t1)) - 60*60*DURATION
	bfid1 = 'CDMS'+str(t0)+'00000'
	now = time.time()
	bfid2 = 'CDMS'+str(int(now))+'00000'

	out_file = PREFIX+SG.upper()
	if os.access(out_file, os.F_OK):
		os.rename(out_file, out_file+'.old')
	f = open(out_file, 'w')
	head = "Recent files in %s between %s and %s"%(
		SG, time.ctime(t0), time.ctime(now))
	f.write("Date this listing was generated: %s\n"%time.ctime(now))
	f.write("\n%s\n\n"%(head))
	f.close()
	query = "select update as time, label as volume, \
			location_cookie, pnfs_path from file, volume \
			where \
				bfid >= '%s' and bfid < '%s' and \
				file.volume = volume.id and \
				volume.storage_group = 'cms' and \
				file.deleted = 'n' \
				order by update;"%(bfid1, bfid2)
	cmd = 'psql -p %d -h %s %s -c "%s" >> %s'%(
		database['db_port'], database['db_host'],
		database['dbname'], query, out_file)
	print cmd
	os.system(cmd)
