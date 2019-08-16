#!/bin/bash 

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
mail="moibenko@fnal.gov"


./write.py > write.log 2>&1

/bin/mail -s "message from ${node}: write test exited, see log file" $mail < write.log 


exit 0

