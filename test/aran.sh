#!/bin/sh
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

# test/$RCSfile$  $Revision$
# returns integer random number between 2 values

USAGE="`basename $0` a b"
if [ -z "${1-}" -o -z "{$2-}" ];then 
    echo "$USAGE"
    exit 1
else
    a=$1
    b=$2
fi
python -c '
import whrandom
print whrandom.randint('$a','$b')
'
