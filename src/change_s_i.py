import sys
import os
import types
import db
import string
import configuration_client
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

dbInfo = csc.get('database')
dbHome = dbInfo['db_dir']
try:  # backward compatible
    jouHome = dbInfo['jou_dir']
except:
    jouHome = dbHome
print "dbHome", dbHome
print "jouHome", jouHome
dict = db.DbTable("volume", dbHome, jouHome, [])
dict.cursor("open")
key,value=dict.cursor("first")
while key:
    update = 0
    print "KEY",key
    print "VALUE",value
    old_system_inhibit = value["system_inhibit"]
    if type(old_system_inhibit) != types.TupleType:
        if (old_system_inhibit == "readonly" or
            old_system_inhibit == "full"):
            new_system_inhibit = ["none",old_system_inhibit]
        else:
            new_system_inhibit = [old_system_inhibit,"none"]
        value["system_inhibit"] = new_system_inhibit
        update = 1
    old_user_inhibit = value["user_inhibit"]
    if type(old_user_inhibit) != types.TupleType:
        if (old_user_inhibit == "readonly" or
            old_user_inhibit == "full"):
            new_user_inhibit = ["none",old_user_inhibit]
        else:
            new_user_inhibit = [old_user_inhibit,"none"]
        value["user_inhibit"] = new_user_inhibit
        update = 1
    if update:
        print "UPDATING",key
        dict.cursor("update",value)
    key,value=dict.cursor("next")
dict.cursor("close")
