#!/bin/sh
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

# bin/$RCSfile udp_send.sh $  $Revision$
# returns servers configured for a node

opts_wo_args='pprint'
opts_w_args='work|additional_ticket|tries'
USAGE="`basename $0` <host> <port>  <timeout> [--{$opts_w_args} <arg>] [--{$opts_wo_args}]"
if [ $# -lt 3 ];then 
    echo "$USAGE"
    exit 1
else
    host=$1
    port=$2
    timeout=$3
    shift 3
fi
while opt=`expr "${1-}" : '--\(.*\)'`;do
    shift
    eval "case \$opt in
    \\?) echo \"$USAGE\"; exit 0;;
#uncomment next 2 lines if we get some opts_wo_args
    $opts_wo_args)
        eval opt_\$opt=1;;
    $opts_w_args)
        if [ $# = 0 ];then echo option $opt requires argument; exit 1; fi
        eval opt_\$opt=\"'\$1'\";shift ;;
    *)  echo \"invalid option: \$opt\"; exit 1;;
    esac"
done


# if additional items in the ticket - add the comma
# maybe the user should be required to specify this since if she were adding
# more than 1 item, commas would be needed to be added explicitly
if [ "${opt_additional_ticket-}" ];then
    additional=,$opt_additional_ticket
else
    additional=
fi

python -c '
import udp_client
import sys
import pprint
u = udp_client.UDPClient()
t={}
try:
    t = u.send( {"work":"'${opt_work:-alive}'"'$additional'}, ("'$host'",'$port'), '$timeout', '${opt_tries:-0}' )
    if '${opt_pprint-0}': pprint.pprint( t )
    else:                 print( t )
    sts=0
except:
    print "timedout"
    sts=1
del u
sys.exit( sts )
'
