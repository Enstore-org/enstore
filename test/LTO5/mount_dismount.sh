#!/bin/bash 

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
mail="${ENSTORE_MAIL:-litvinse@fnal.gov}"


./mount_dismount.py > mount_dismount.log 2>&1

/bin/mail -s "message from ${node}: mount_dismount test exited, see log file" $mail < mount_dismount.log 
exit 0

