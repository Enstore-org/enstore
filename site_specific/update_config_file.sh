#!/bin/sh -x 
###############################################################################
#
# $Id$
#
###############################################################################
set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

# setup enstore first
if [ -f /usr/local/etc/setups.sh ]; then
    # this check is required by current rpm build process, do not remove!!!!
    . /usr/local/etc/setups.sh
    setup enstore
fi

krbdir="/usr/krb5/bin"
defaultDomain=".fnal.gov"
   
user=`whoami`
host=`uname -n`

# we need the full domain name, if no domain is there, add default one on
if expr $host : '.*\.' >/dev/null;then 
    thisHost=$host;
else 
    thisHost=${host}${defaultDomain};
fi



dir=`dirname $ENSTORE_CONFIG_FILE`
file=`basename $ENSTORE_CONFIG_FILE`
command="source /usr/local/etc/setups.sh ; setup enstore; cd $dir; cvs update -A $file"

# we should be enstore when update config file
 if [ "$user" = "root" ]; then
      OLDKRB5CCNAME=${KRB5CCNAME:-NONE}
      KRB5CCNAME=/tmp/krb5cc_enstore_$$;export KRB5CCNAME
      ${krbdir}/kinit -k -t /local/ups/kt/enstorekt enstore/cd/${thisHost}
      cd $dir
      cvs update -A $file
      ${krbdir}/kdestroy
      # if we had an old ticket cache, restore it
      if [ $OLDKRB5CCNAME != "NONE" ]; then KRB5CCNAME=$OLDKRB5CCNAME; fi
      
 elif [ "$user" = "enstore" ]; then
      cd $dir
      cvs update -A $file

else
     echo "You must be root or enstore to run this command"
fi

