#!/usr/bin/env python

# $Id$


# system imports
from edb import timestamp2time
import time
import sys
import os
import pg
import errno

# enstore modules
import option
import configuration_client
import file_clerk_client
import e_errors
import log_trans_fail #for copy_it


DURATION = 12 #hours
PREFIX = 'RECENT_FILES_'


class RecentFileListingInterface(option.Interface):
	def __init__(self, args=sys.argv, user_mode=0):
		
		self.duration = DURATION #hours
		self.output_dir = None
		option.Interface.__init__(self, args=args, user_mode=user_mode)
		

	def valid_dictionaries(self):
		return (self.help_options, self.rfl_options)

	#  define our specific parameters
	parameters = [
		"[[storage_group1 [storage_group2] ...]]",
		]
	
	rfl_options = {
		option.DURATION:{option.HELP_STRING:
				 "Duration in hours to report.  "
				 "(Default 12 hours)",
				 option.VALUE_USAGE:option.REQUIRED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.ADMIN,},
		option.OUTPUT_DIR:{option.HELP_STRING:
				   "Specify a directory to place the output.  "
				   "(Default is the tape_inventory dir.)",
				   option.VALUE_USAGE:option.REQUIRED,
				   option.VALUE_TYPE:option.STRING,
				   option.USER_LEVEL:option.ADMIN,},
		}

def make_recent_file(storage_group, duration, bfid_brand, database,
		     out_dir, temp_dir):
	t = time.localtime()
	t1 = (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, 0, 0,
		t.tm_wday, t.tm_yday, t.tm_isdst)
	t0 = int(time.mktime(t1)) - 60 * 60 * duration
	bfid1 = bfid_brand+str(t0)+'00000'
	now = time.time()
	bfid2 = bfid_brand+str(int(now))+'00000'

	#If we write to a temp file, and swap in it when we are done, there
	# will not any time when the page is empty becuase the scipt is still
	# writing the file.
	out_file = os.path.join(out_dir, PREFIX + storage_group.upper())
	temp_file = os.path.join(temp_dir, PREFIX + storage_group.upper() + ".temp")

	#Write the output.
	f = open(temp_file, 'w')
	head = "Recent files in %s between %s and %s"%(
		storage_group, time.ctime(t0), time.ctime(now))
	f.write("Date this listing was generated: %s\n"%time.ctime(now))
	f.write("Brought to You by: %s\n" % (os.path.basename(sys.argv[0]),))
	f.write("\n%s\n\n"%(head))
	f.close()
	query = "select update as time, label as volume, file_family, \
			location_cookie, pnfs_id, pnfs_path from file, volume \
			where \
				bfid >= '%s' and bfid < '%s' and \
				file.volume = volume.id and \
				volume.storage_group = 'cms' and \
				file.deleted = 'n' \
				order by update;"%(bfid1, bfid2)
	cmd = 'psql -p %d -h %s -U %s %s -c "%s" >> %s'%(
		database['db_port'], database['db_host'],
		database['dbuser'], database['dbname'], query, temp_file)
	print cmd
	os.system(cmd)

	# update the file
	if os.access(out_file, os.F_OK):
		#os.rename(out_file, out_file+'.old')
		os.remove(out_file)

	try:
		os.rename(temp_file, out_file)   #Do the temp file swap.
	except (OSError, IOError), msg:
		if msg.errno == errno.EXDEV:
			log_trans_fail.copy_it(temp_file, out_file)
		else:
			raise 

def main(intf):
	#Get some configuration information.
	csc = configuration_client.ConfigurationClient(
		(intf.config_host, intf.config_port))
	database = csc.get('database')
	if not e_errors.is_ok(database):
		sys.stdout.write("No database information.\n")
		sys.exit(1)
	crons_dict = csc.get('crons')
	if not e_errors.is_ok(crons_dict):
		sys.stdout.write("No crons information.\n")
		sys.exit(1)
	temp_dir = crons_dict.get("tmp_dir", "/tmp")
	fcc = file_clerk_client.FileClient(csc, rcv_timeout = 10,
					   rcv_tries = 5)
	bfid_brand = fcc.get_brand()
	if bfid_brand == None:
		sys.stdout.write("No bfid brand found.\n")
		sys.exit(1)

	

	#Grab the list of storage_groups.
	sg_list = []
	for item in intf.args:
		sg_list.append(item)
	#If no storage groups on the command line, do all of them.
	if not sg_list:#Get the connection to the database.
		edb   = pg.DB(
			host   = database.get('dbhost', "localhost"),
			port   = database.get('dbport', 5432),
			dbname = database.get('dbname', "accounting"),
			user   = database.get('dbuser', "enstore"),
			)
		q = "select distinct storage_group from volume;"
		res = edb.query(q).getresult()
		for row in res:
			#row[0] is the storage_group
			sg_list.append(row[0])
			
	#By default stuff things into the tape_inventory directory,
	# however if the user specifies a different directory, use that.
	if intf.output_dir:
		if not os.path.exists(intf.output_dir):
			sys.stdout.write("Output directory not found.\n")
			sys.exit(1)
		out_dir = intf.output_dir
	else:
		#Make the default path the tape inventory dir.  Put only
		# if the html_dir exists.
	       	if not crons_dict.get("html_dir", None):
			sys.stdout.write("No html_dir information.\n")
			sys.exit(1)
		if not os.path.exists(crons_dict["html_dir"]):
			sys.stdout.write(
				"No html_dir found.  Consider using "
				"--output-dir.\n")
			sys.exit(1)
		inventory_dir = os.path.join(crons_dict["html_dir"],
					     "tape_inventory")
		if not os.path.exists(inventory_dir):
			os.mkdir(inventory_dir)

		out_dir = inventory_dir

	#Make the page for each storage group.
	for sg in sg_list:
		make_recent_file(sg, intf.duration, bfid_brand, database,
				 out_dir, temp_dir)

if __name__ == "__main__":   # pragma: no cover
	#Get inforation from the Enstore servers.
	rfl_intf = RecentFileListingInterface()

	main(rfl_intf)

