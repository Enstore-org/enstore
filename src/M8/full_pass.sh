#!/bin/bash 

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
mail="${ENSTORE_MAIL:-litvinse@fnal.gov}"


./full_pass.py > full_pass.log 2>&1

/bin/mail -s "message from ${node}: full_pass test exited, see log file" $mail < full_pass.log 
exit 0

