#!/bin/sh
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

# bin/$RCSfile conf.sh $  $Revision$
# returns servers configured for a node

USAGE="`basename $0` [node]"
if [ ! "${1-}" ];then 
    host=""
else
    host=$1
fi
python -c '
import configuration_client
intf=configuration_client.ConfigurationClientInterface()
csc=configuration_client.ConfigurationClient("'$ENSTORE_CONFIG_HOST'"',$ENSTORE_CONFIG_PORT',intf.config_list)
t=csc.u.send({"work":"reply_serverlist"},csc.config_address)
servers = t["server_list"]
#import pprint;pprint.pprint(t)
for key in servers.keys():
    try:
        ahost,ip,port = servers[key]
	#print key,ahost,ip,port
	if  "'$host'" == "" or ahost == "'$host'" or key=="file_clerk" or key=="volume_clerk" or key=="admin_clerk" or key=="logserver"\
                                                  or key=="inquisitor" :
            print "%s:%s:%s" % (key,ahost,port)
    except:
        pass
del csc.u
'
