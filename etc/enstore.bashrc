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

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

case $node in
  stken*) gang=stken
	  fntt=fntt
	  V9()     { enstore lib --vols 9940.library_manager
		     enstore med --get_work stk.media_changer
		     stk_qd| grep "9940"
		     enstore lib --status 9940.library_manager
		     if [ -n "${1-}" ]; then enstore lib --get_queue "" 9940.library_manager; fi
		   }
	  V()      { enstore lib --vols dlt.library_manager
		     enstore med --get_work aml2r1.media_changer
		     dasadmin listd2| grep -v "volser:   cleaning"| grep DC
		     enstore lib --status dlt.library_manager
		     if [ -n "${1-}" ]; then  enstore lib --get_queue "" dlt.library_manager; fi
		   }
	  ;;
  cdfen*) gang=cdfen
	  fntt=fntt2
	  V9()     { enstore lib --vols cdf.library_manager
		     enstore med --get_work stk.media_changer
		     stk_qd| grep "9940"
		     enstore lib --status cdf.library_manager
		     if [ -n "${1-}" ]; then enstore lib --get_queue "" cdf.library_manager; fi
		   }
	  ;;
   d0en*) gang=d0en
	  fntt=fntt
	  V()      { enstore lib --vols samlto.library_manager
		     enstore med --get_work aml2.media_changer
		     dasadmin listd2| grep -v "volser:   cleaning"| grep DC
		     enstore lib --status samlto.library_manager
		     if [ -n "${1-}" ]; then  enstore lib --get_queue "" samlto.library_manager; fi
		   }
	  VN()     { enstore lib --vols samnull.library_manager
		     enstore med --get_work samnull.media_changer
		     enstore lib --status samnull.library_manager
		     if [ -n "${1-}" ]; then  enstore lib --get_queue "" samnull.library_manager; fi
		   }
	  ;;
       *) gang=UNKNOWN
	  ;;
esac

stk_qd()	{ /usr/bin/rsh $fntt -l acsss "echo query drive   ${1:-all}    '\r' logoff | bin/cmd_proc -l -q 2> /dev/null" < /dev/null; }
stk_qr()	{ /usr/bin/rsh $fntt -l acsss "echo query request ${1:-all}    '\r' logoff | bin/cmd_proc -l -q 2> /dev/null" < /dev/null; }
stk_qv()	{ /usr/bin/rsh $fntt -l acsss "echo query volume  ${1:-VOLUME} '\r' logoff | bin/cmd_proc -l -q 2> /dev/null" < /dev/null; }
stk_mount()	{ /usr/bin/rsh $fntt -l acsss "echo mount ${1:-VOLUME} ${2:-DRIVE} '\r' logoff | bin/cmd_proc -l -q 2> /dev/null" < /dev/null; }
stk_dismount()	{ /usr/bin/rsh $fntt -l acsss "echo dismount ${2:-VOLUME} ${1:-DRIVE} force '\r' logoff | bin/cmd_proc -l -q 2> /dev/null" < /dev/null; }
stk_msg()	{ /usr/bin/rsh $fntt -l acsss "tail -${1:-64}  log/acsss_event.log" | awk '/^20[0-9][0-9]-/ { printf("%s",$0); getline; getline; printf("\t%s\n",$0)}'; }
stk_log()	{ /usr/bin/rsh $fntt -l acsss 'tail -${1:-256} log/acsss_event.log' | more; }
stk_log_get()	{ /usr/bin/rcp acsss@$fntt:log/acsss_event.log . ; }

bakken() { . /home/bakken/.bash_profile; }
