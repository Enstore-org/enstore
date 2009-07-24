#!/usr/bin/env python
######################################################################
# src/$RCSfile$   $Revision$
#
import string
import sys
import os
import configuration_client
import enstore_constants
import inventory
import Trace
import pg

CDF = "cdf"
D0 = "d0"
STK = "stk"

LIBRARIES = {CDF : ["cdf", "CDF-9940B", "CDF-LTO3", "CDF-LTO4"],
	     D0 : ["mezsilo", "samlto", "samm2", "sammam", "D0-9940B",
                   "samlto2", "shelf-samlto", "D0-LTO3", "D0-LTO4"],
	     STK : ["9940", "CD-9940B", "CD-LTO3", "CD-LTO4"]
	     }
def work(total_bytes, vq_output_file, vq_output_file2):
	output_file = open(vq_output_file, 'w')
	output_file.write("%.3f TB\n"%(total_bytes/1099510000000.0))
	output_file.close()
	output_file = open(vq_output_file2, 'w')
	output_file.write("%.3f\n"%(total_bytes))
	output_file.close()
    
def go(system, vq_file_name, vq_output_file, vq_output_file2):

    if system and system in LIBRARIES.keys():
	vq_libs = LIBRARIES[system]
    else:
	# assume all systems
	vq_libs = []
	for system in LIBRARIES.keys():
	    vq_libs = vq_libs + LIBRARIES[system]

    # read it in and pull out the libraries that we want
    vq_file = open(vq_file_name, 'r')
    total_bytes = 0.0
    for line in vq_file.readlines():
	fields = string.split(line)
	if len(fields) == 2:
	    lib = fields[0]
	    bytes = fields[1]
	else:
	    # this line has the wrong format, skip it
	    continue
	if lib in vq_libs:
	    # get rid of the newline
	    bytes = string.strip(bytes)
	    total_bytes = total_bytes + float(bytes)
    else:
	# output the file that has the number of bytes in it.
	vq_file.close()
	output_file = open(vq_output_file, 'w')
	output_file.write("%.3f TB\n"%(total_bytes/1099510000000.0))
	output_file.close()
	output_file = open(vq_output_file2, 'w')
	output_file.write("%.3f\n"%(total_bytes))
	output_file.close()


if __name__ == "__main__":
    intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc   = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
    system_name = csc.get_enstore_system(timeout=1,retry=0)        
    config_dict={}
    
    if system_name:
        config_dict = csc.dump(timeout=1, retry=3)
        config_dict = config_dict['dump']
    else:
        try: 
            configfile = os.environ.get('ENSTORE_CONFIG_FILE')
            print "Failed to connect to config server, using configuration file %s"%(configfile,)
            f = open(configfile,'r')
            code = string.join(f.readlines(),'')
            configdict={}
            exec(code)
            config_dict=configdict
        except:
            pass
    inq_d = config_dict.get(enstore_constants.INQUISITOR, {})
    dir = inq_d["html_file"]
    vq_output_file = os.path.join(dir,"enstore_system_user_data.html")
    vq_output_file2 = "%s2"%(vq_output_file,)
    system=""
    vq_file_dir = os.path.join(dir,"tape_inventory")
    vq_file_name = os.path.join(vq_file_dir,"VOLUME_QUOTAS_FORMATED")
#    go(system, vq_file_name, vq_output_file, vq_output_file2)

    acc = config_dict.get("database",{})
    total_bytes=0.
    q="select coalesce(sum(size),0) from file, volume where file.volume = volume.id and system_inhibit_0 != 'DELETED' and media_type!='null'"
    if system_name.find("stken") != -1 or system_name.find("d0en") != -1 or system_name.find("cdfen") or system_name.find("gccen") :
	    q="select sum(deleted_bytes+unknown_bytes+active_bytes)  from volume where system_inhibit_0!='DELETED' and media_type!='null' and library not like '%shelf%' and library not like '%test%'"
    try: 
        db = pg.DB(host  = acc.get('db_host', "localhost"),
                   dbname= acc.get('dbname', "enstoredb"),
                   port  = acc.get('db_port', 5432),
                   user  = acc.get('dbuser', "enstore"))
        res=db.query(q)
        for row in res.getresult():
            if not row:
                continue
            total_bytes=row[0]
        db.close()
    except:
        Trace.handle_error()
        pass

    work(total_bytes, vq_output_file, vq_output_file2)
    

	# get the system from the args
#	if argc > 2:
#	    system = sys.argv[2]
#	else:
#	    system = ""
#
#	# get the file we need to read
#	if argc > 3:
#	    vq_file_name = sys.argv[3]
#	else:
#	    # we were not passed a name, get the default name from the 
#	    # inventory file
#	    dirs = inventory.inventory_dirs()
#	    vq_file_name = inventory.get_vq_format_file(dirs[0])
#
#	go(system, vq_file_name, vq_output_file, vq_output_file2)
#    else:
#	# this is an error we need to be given the file to write
#	pass
#
