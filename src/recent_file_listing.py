#!/usr/bin/env python

# $Id$

# system imports
from edb import timestamp2time
import time
import sys
import os

# enstore modules
import option
import configuration_client
import file_clerk_client
import e_errors

SG = 'cms'  # hard-coded!!!
DURATION = 12
PREFIX = 'RECENT_FILES_'

if __name__ == '__main__':
	#Get inforation from the Enstore servers.
	intf = option.Interface()
	csc = configuration_client.ConfigurationClient(
		(intf.config_host, intf.config_port))
	database = csc.get('database')
	if not e_errors.is_ok(database):
		sys.stdout.write("No database information.\n")
		sys.exit(1)
	fcc = file_clerk_client.FileClient(csc, rcv_timeout = 10,
					   rcv_tries = 5)
	bfid_brand = fcc.get_brand()
	if bfid_brand == None:
		sys.stdout.write("No bfid brand found.\n")
		sys.exit(1)

	#Parse any command line arguements.
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
	bfid1 = bfid_brand+str(t0)+'00000'
	now = time.time()
	bfid2 = bfid_brand+str(int(now))+'00000'

	#If we write to a temp file, and swap in it when we are done, there
	# will not any time when the page is empty becuase the scipt is still
	# writing the file.
	out_file = PREFIX+SG.upper()
	temp_file = out_file + ".temp"
	if os.access(out_file, os.F_OK):
		os.rename(out_file, out_file+'.old')

	#Write the output.
	f = open(temp_file, 'w')
	head = "Recent files in %s between %s and %s"%(
		SG, time.ctime(t0), time.ctime(now))
	f.write("Date this listing was generated: %s\n"%time.ctime(now))
	f.write("Brought to You by: %s\n" % (os.path.basename(sys.argv[0]),))
	f.write("\n%s\n\n"%(head))
	f.close()
	query = "select update as time, label as volume, file_family, \
			location_cookie, pnfs_path from file, volume \
			where \
				bfid >= '%s' and bfid < '%s' and \
				file.volume = volume.id and \
				volume.storage_group = 'cms' and \
				file.deleted = 'n' \
				order by update;"%(bfid1, bfid2)
	cmd = 'psql -p %d -h %s %s -c "%s" >> %s'%(
		database['db_port'], database['db_host'],
		database['dbname'], query, temp_file)
	print cmd
	os.system(cmd)

	os.rename(temp_file, out_file)   #Do the temp file swap.
