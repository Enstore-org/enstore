if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

if [ -z "${PS1-}" ] ; then return; fi

if [ -r /usr/local/etc/setups.sh ] ; then
   set +u
   source /usr/local/etc/setups.sh
   setup enstore
   set -u
fi

driverate() {   o=/tmp/$1.data; rm -f $o >/dev/null 2>&1;
                echo grep $1 /diska/enstore-log/$2
                grep $1  /diska/enstore-log/$2  |grep " ENCP " | while read line; do  t=`echo $line | awk '{print $1}'`; r=`echo $line|sed -e "s/.*(//"  -e "s/ MB.*//"`; echo $t $r ; done >$o
                oo=/tmp/$1.gnu; rm -f $oo >/dev/null 2>&1;
                ooo=/tmp/$1.ps; rm -f $ooo >/dev/null 2>&1;
                echo "set terminal postscript color solid"        >> $oo
                echo "set output \"$ooo\""                        >> $oo
                echo "set title \"$1\""                           >> $oo
                echo "set xlabel \"$2\""                          >> $oo
                echo "set ylabel \"Rate\""                        >> $oo
                echo "set timefmt \"%H:%M:%S\""                   >> $oo
                echo "set xdata time"                             >> $oo
                echo "plot \"$o\" using 1:2 t '' with points 1 1" >> $oo
                gnuplot $oo
                gv $ooo
     }

getticket() { 
              krbdir="/usr/krb5/bin"
              defaultDomain=".fnal.gov"
              host=`uname -n`
              if expr $host : '.*\.' >/dev/null;then 
                thisHost=$host;
              else 
                thisHost=${host}${defaultDomain};
              fi
              OLDKRB5CCNAME=${KRB5CCNAME:-NONE}
              KRB5CCNAME=/tmp/krb5cc_enstore_$$;export KRB5CCNAME
              ${krbdir}/kinit -k -t /local/ups/kt/enstorekt enstore/cd/${thisHost}
    }

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

case $node in
  stken*) gang=stken
          V8()     { enstore lib --vols eagle.library_manager
                     enstore med --get_work stk.media_changer
                     stk_qd| grep "9840"
                     enstore lib --status eagle.library_manager
                     if [ -n "${1-}" ]; then enstore lib --get_queue "" eagle.library_manager; fi
                   }
          VT8()    { enstore lib --vols test.library_manager
                     enstore med --get_work stk.media_changer
                     stk_qd| grep "9840"
                     enstore lib --status test.library_manager
                     if [ -n "${1-}" ]; then enstore lib --get_queue "" test.library_manager; fi
                   }
          V9 ()    { enstore lib --vols 9940.library_manager
                     enstore med --get_work stk.media_changer
                     stk_qd| grep "9940"
                     enstore lib --status 9940.library_manager
                     if [ -n "${1-}" ]; then enstore lib --get_queue "" 9940.library_manager; fi
                   }
          nospaces()     { echo "$1" | sed -e 's/ //g' ; }
          stk_qd()       { /usr/bin/rsh fntt -l acsss "echo query drive  `nospaces "${1:-all}"`            |/export/home/ACSSS/bin/cmd_proc 2>> /tmp/garb" < /dev/null ; }
          stk_qv()       { /usr/bin/rsh fntt -l acsss "echo query vol    ${1:-VOLUME}                      |/export/home/ACSSS/bin/cmd_proc 2>> /tmp/garb" < /dev/null ; }
          stk_mount()    { /usr/bin/rsh fntt -l acsss "echo mount ${1:-VOLUME} `nospaces "${2:-DRIVE}"`    |/export/home/ACSSS/bin/cmd_proc 2>> /tmp/garb" < /dev/null ; }
          stk_dismount() { /usr/bin/rsh fntt -l acsss "echo dismount VOLUME `nospaces "${1:-DRIVE}"` force |/export/home/ACSSS/bin/cmd_proc 2>> /tmp/garb" < /dev/null;  }
	  stk_msg()      { /usr/bin/rsh fntt -l acsss "tail -${1:-50} log/acsss_event.log" | awk '/20[0-9][0-9]/ { printf("%s",$0) ; getline ;  getline; printf("\t%s\n",$0)}' ; }
	  stk_log_get()  { /usr/bin/rcp acsss@fntt:log/acsss_event.log . ;}
	  stk_log()      { /usr/bin/rsh -l acsss fntt 'tail -175 log/acsss_event.log ' | more ;}

          ;;
  cdfen*) gang=cdfen
          V  ()    { enstore lib --vols cdf.library_manager
                     enstore med --get_work stk.media_changer
                     stk_qd| grep "9940"
                     enstore lib --status cdf.library_manager
                     if [ -n "${1-}" ]; then enstore lib --get_queue "" cdf.library_manager; fi
                   }
          VT ()    { enstore lib --vols test.library_manager
                     enstore med --get_work stk.media_changer
                     stk_qd| grep "9940"
                     enstore lib --status test.library_manager
                     if [ -n "${1-}" ]; then enstore lib --get_queue "" test.library_manager; fi
                   }
          nospaces()     { echo "$1" | sed -e 's/ //g' ; }
          stk_qd()       { /usr/bin/rsh fntt2 -l acsss "echo query drive  `nospaces "${1:-all}"`            |/export/home/ACSSS/bin/cmd_proc 2>> /tmp/garb" < /dev/null ; }
          stk_qv()       { /usr/bin/rsh fntt2 -l acsss "echo query vol    ${1:-VOLUME}                      |/export/home/ACSSS/bin/cmd_proc 2>> /tmp/garb" < /dev/null ; }
          stk_mount()    { /usr/bin/rsh fntt2 -l acsss "echo mount ${1:-VOLUME} `nospaces "${2:-DRIVE}"`    |/export/home/ACSSS/bin/cmd_proc 2>> /tmp/garb" < /dev/null ; }
          stk_dismount() { /usr/bin/rsh fntt2 -l acsss "echo dismount VOLUME `nospaces "${1:-DRIVE}"` force |/export/home/ACSSS/bin/cmd_proc 2>> /tmp/garb" < /dev/null;  }
	  stk_msg()      { /usr/bin/rsh fntt2 -l acsss "tail -${1:-50} log/acsss_event.log" | awk '/20[0-9][0-9]/ { printf("%s",$0) ; getline ;  getline; printf("\t%s\n",$0)}' ; }
	  stk_log_get()  { /usr/bin/rcp acsss@fntt2:log/acsss_event.log . ;}
	  stk_log()      { /usr/bin/rsh -l acsss fntt2 'tail -175 log/acsss_event.log ' | more ;}

          ;;
   d0en*) gang=d0en
          V2 ()    { enstore lib --vols samm2.library_manager
                     enstore med --get_work aml2r2.media_changer
                     enstore med --get_work aml2r1.media_changer
                     dasadmin listd2| grep -v "volser:   cleaning"| grep DI
                     enstore lib --status samm2.library_manager
                     if [ -n "${1-}" ]; then  enstore lib --get_queue "" samm2.library_manager; fi
                   }
          VT()     { enstore lib --vols test.library_manager
                     enstore med --get_work aml2r2.media_changer
                     enstore med --get_work aml2r1.media_changer
                     dasadmin listd2| grep -v "volser:   cleaning"| grep DC
                     enstore lib --status test.library_manager
                     if [ -n "${1-}" ]; then  enstore lib --get_queue "" test.library_manager; fi
                   }

          VT2()    { enstore lib --vols testm2.library_manager
                     enstore med --get_work aml2r2.media_changer
                     enstore med --get_work aml2r1.media_changer
                     dasadmin listd2| grep -v "volser:   cleaning"| grep DI
                     enstore lib --status testm2.library_manager
                     if [ -n "${1-}" ]; then  enstore lib --get_queue "" testm2.library_manager; fi
                   }

          VN ()    { enstore lib --vols samnull.library_manager
                     enstore med --get_work samnull.media_changer
                     enstore lib --status samnull.library_manager
                     if [ -n "${1-}" ]; then  enstore lib --get_queue "" samnull.library_manager; fi
                   }

          VTO()    { enstore lib --vols testlto.library_manager
                     #enstore med --get_work aml2r2.media_changer
                     #enstore med --get_work aml2r1.media_changer
                     #dasadmin listd2| grep -v "volser:   cleaning"| grep DI
                     enstore lib --status testlto.library_manager
                     if [ -n "${1-}" ]; then  enstore lib --get_queue "" testlto.library_manager; fi
                   }
          ;;
       *) gang=UNKNOWN
          ;;
esac

bakken() { . /home/bakken/.bash_profile; }

export PATH=/usr/krb5/bin:/home/enstore/pgsql/bin:$PATH
 
umask 002
