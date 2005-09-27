if [ -n "${PS1-}" ] ; then
   if [ -r ~/.bashrc ] ; then
      source ~/.bashrc
	PS1="[\u@\h \W]\\$" 
   fi
elif [ -r /usr/local/etc/fermi.profile ] ; then 
   set +u
   source /usr/local/etc/fermi.profile
   set -u
fi

if [ -r /usr/local/bin/ENSTORE_HOME ] ; then
   source /usr/local/bin/ENSTORE_HOME
fi

if [ -d /home/enstore/pgsql/bin ] ; then
   PATH=/home/enstore/pgsql/bin:`echo $PATH | sed s=/home/enstore/pgsql/bin:==`
fi

PATH=/usr/krb5/bin:`echo $PATH | sed s=/usr/krb5/bin:==`

umask 002
