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
t=csc.u.send({"work":"hi"},csc.config_address)
configdict = t["list"]
for key in configdict.keys():
    try:
        if configdict[key]["host"] == "'$host'":
            print "%s:%s" % (key,configdict[key]["port"])
    except:
        pass
del csc.u
'
