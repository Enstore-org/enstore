#!/bin/bash 

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
mail="${ENSTORE_MAIL:-litvinse@fnal.gov}"


./read.py > read.log 2>&1

/bin/mail -s "message from ${node}: read test exited, see log file" $mail < read.log 
exit 0

