if [ -n "${PS1-}" ] ; then
   if [ -r ~/.bashrc ] ; then
      source ~/.bashrc
   fi
elif [ -r /usr/local/etc/fermi.profile ] ; then 
   set +u
   source /usr/local/etc/fermi.profile
   set -u
fi

if [ -d /home/enstore/pgsql/bin ] ; then
   PATH=/home/enstore/pgsql/bin:`echo $PATH | sed s=/home/enstore/pgsql/bin:==`
fi

PATH=/usr/krb5/bin:`echo $PATH | sed s=/usr/krb5/bin:==`

umask 002
