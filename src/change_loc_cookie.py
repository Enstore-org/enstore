#!/usr/bin/env python
import sys
import os
import types
import db
import string
import regsub
import configuration_client
import file_clerk_client
import Trace

port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
port = string.atoi(port)
if port:
    # we have a port
    host = os.environ.get('ENSTORE_CONFIG_HOST', 0)
    if host:
        # we have a host
        csc = configuration_client.ConfigurationClient((host, port))
    else:
        print "cannot find config host"
        sys.exit(-1)
else:
    print "cannot find config port"
    sys.exit(-1)
# create file clerk client
fcc = file_clerk_client.FileClient(csc)
# get file list for the specified tape
if len(sys.argv) != 2:
    print "Usage: %s volume_label"%sys.argv[0]
    sys.exit(1)

ticket = fcc.tape_list(sys.argv[1])
print ticket['tape_list']
records = string.split(ticket['tape_list'],"\n")
del(records[0])
entries = []
for record in records:
    enti = string.split(record,' ')
    size = len(enti)
    ent = []
    for i in range(0,size):
        if enti[i] != '':
            ent.append(enti[i])
    
    if len(ent) == 6:
        element = {"file" : ent[5],
                   "bfid" : ent[1],
                   "loc_cookie": ent[3]
                   }
        entries.append(element)

dbInfo = csc.get('database')
dbHome = dbInfo['db_dir']
try:  # backward compatible
    jouHome = dbInfo['jou_dir']
except:
    jouHome = dbHome
print "dbHome", dbHome
print "jouHome", jouHome
dict = db.DbTable("file", dbHome, jouHome, [])

for entry in entries:
    f_entry = dict[entry['bfid']]
    ell = string.split(f_entry['location_cookie'],'_')
    l = len(ell[1])
    ell[1] = '0'*l
    new_cookie = string.join(tuple(ell),'_')
    print "changing location cookie for %s. Was:%s. Will be:%s"%\
          (entry['file'],f_entry['location_cookie'],new_cookie)
    f_entry['location_cookie'] = new_cookie
    dict[entry['bfid']] = f_entry
sys.exit(0)

