#!/bin/sh
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

# bin/$RCSfile conf.sh $  $Revision$
# returns servers configured for a node

USAGE="`basename $0` <node>"
if [ ! "${1-}" ];then 
    echo "$USAGE"
    exit 1
fi

host=$1

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
        if ahost == "'$host'":
            print "%s:%s" % (key,port)
    except:
        pass
del csc.u
'
