if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

if [ -n "${PS1-}" ] ; then
   if [ -r ~/.bashrc ] ; then source ~/.bashrc; fi

else
   if [ -r /usr/local/etc/fermi.profile ] ; then 
      set +u
      source /usr/local/etc/fermi.profile
      set -u
   fi
fi

PATH=/usr/krb5/bin:/home/enstore/pgsql/bin:$PATH
source /home/enstore/gettkt
umask 002
