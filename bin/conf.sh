#!/bin/sh
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

# bin/$RCSfile conf.sh $  $Revision$
# returns servers configured for a node or looks up a key

USAGE="`basename $0` [node | -k lookup_item] [timeout] [tries]"
if [ ! "${1-}" ];then 
    host=""
    lookup=0
    timeout=10
    tries=1
else
    if [ "${1}" = "-k" ] ; then
      if [ -z "${2-}" ] ; then
        echo USAGE
	exit 1
      fi
      shift
      lookup=$1
      host=0
    else
      lookup=0
      host=$1
    fi
    shift
    if [ -n "${1-}" ] ; then
	timeout=$1
	shift
	if [ -n "${1-}" ] ; then
	    tries=$1
	else
	    tries=1
	fi
    else
	timeout=10
	tries=1
    fi
fi

# check if config server is up
udp_sendWaitReply-withTimeout.sh $ENSTORE_CONFIG_HOST $ENSTORE_CONFIG_PORT $timeout >/dev/null
if [ $? -ne 0 ] ; then echo "Configuration Server is Dead"; exit 1; fi

python -c '
import configuration_client
intf=configuration_client.ConfigurationClientInterface()
csc=configuration_client.ConfigurationClient("'$ENSTORE_CONFIG_HOST'"',$ENSTORE_CONFIG_PORT',intf.verbose)
if "'$lookup'" != "0":
 t=csc.u.send({"work":"lookup","lookup": "'$lookup'"},csc.config_address'${2:+,$2}${3:+,$3}')
 import pprint
 pprint.pprint(t)
else:  
 t=csc.u.send({"work":"reply_serverlist"},csc.config_address'${2:+,$2}${3:+,$3}')
 servers = t["server_list"]
 for key in servers.keys():
     try:
         ahost,ip,port = servers[key]
	 #print key,ahost,ip,port
	 if  "'$host'" == "" or ahost == "'$host'" or key=="file_clerk" or key=="volume_clerk" or key=="logserver" or key=="inquisitor" :
             print "%s:%s:%s" % (key,ahost,port)
     except:
         pass
del csc.u
'
exit 0
