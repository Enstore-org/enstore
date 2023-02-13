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
import dbaccess

def work(total_bytes, vq_output_file, vq_output_file2):
	output_file = open(vq_output_file, 'w')
	output_file.write("Total/Active : %.3f/%.3f TB\n"%(total_bytes["total"]/1099510000000.0,total_bytes["active"]/1099510000000.))
	output_file.close()
	output_file = open(vq_output_file2, 'w')
	output_file.write("%.3f %.3f\n"%(total_bytes["total"],
					 total_bytes["active"]))
	output_file.close()

if __name__ == "__main__":   # pragma: no cover
    config_dict=configuration_client.get_config_dict()
    intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
    csc   = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
    system_name = csc.get_enstore_system(timeout=10,retry=2)
    config_dict={}

    if system_name:
        config_dict = csc.dump(timeout=10, retry=2)
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

    acc = config_dict.get("database",{})
    total_bytes={}

    q="select sum(deleted_bytes+unknown_bytes+active_bytes) as total, sum(active_bytes) as active from volume where system_inhibit_0!='DELETED' and media_type not in ('null', 'disk')"
    if system_name.find("stken") != -1 or system_name.find("d0en") != -1 or system_name.find("cdfen") != -1  or system_name.find("gccen") != -1 :
	    q="select sum(deleted_bytes+unknown_bytes+active_bytes) as total, sum(active_bytes) as active  from volume where system_inhibit_0!='DELETED' and media_type not in ('null', 'disk') and library not like '%shelf%'"
    try:
        db = dbaccess.DatabaseAccess(maxconnections=1,
				     host     = acc.get('db_host', "localhost"),
				     database = acc.get('dbname', "enstoredb"),
				     port     = acc.get('db_port', 5432),
				     user     = acc.get('dbuser', "enstore"))
        res=db.query_dictresult(q)
        for row in res:
            if not row:
                continue
	    for key,value in row.iteritems():
		    total_bytes[key]=float(value)
        db.close()
    except:
        Trace.handle_error()

    work(total_bytes, vq_output_file, vq_output_file2)
