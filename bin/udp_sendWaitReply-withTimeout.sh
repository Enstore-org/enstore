#!/bin/sh
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

# bin/$RCSfile udp_send.sh $  $Revision$
# returns servers configured for a node

USAGE="\
usage: `basename $0` <host> <port> [work=alive] [additional_ticket]
    example: `basename $0` work 7528 set_timeout '\"timeout\":1'"
if [ $# -lt 3 ];then 
    echo "$USAGE"
    exit 1
fi

host=$1
port=$2
timeout=$3

if [ "${4-}" ];then
    work=$4
else
    work=alive
fi
if [ "${4-}" ];then
    additional=,$4
else
    additional=
fi

python -c '
import udp_client
import sys
u = udp_client.UDPClient()
try:
    t = u.send( {"work":"'$work'"'$additional'}, ("'$host'",'$port'), '$timeout' )
    print "t=",t
    sts=0
except:
    print "timedout"
    sts=1
del u
sys.exit( sts )
'
