# flip_tab_acsls.sh handles write protect tab flipping for the STK 9310 and 8500
# libraries, using the ACSLS command interface.

# This script must be sourced from tape_aid_wrapper.

if [ "$icap" != "$ocap" ]; then
  /usr/bin/rsh $sun -l acsss "echo set cap mode automatic $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"
fi

for i in `ls -vI \*.\*`; do
  t0=`date +'%s'`
  echo "`date` Group $i" >> $output
  . $i |tee -a $output 2>&1
  echo
  echo
  if [ "$icap" = "$ocap" ]; then
    /usr/bin/rsh $sun -l acsss "echo set cap mode automatic $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"
    echo
    echo
    /bin/echo -n "Press Enter when all tapes have been loaded with tabs in ${action}ed position"
    read ans
    msgflag=
    while /bin/true; do
      cap=`/usr/bin/rsh $sun -l acsss "echo q cap $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"`
      capstate=`echo $cap | sed -e 's#.*automatic ##' | cut -f1 -d\ `
      if [ "$capstate" = "available" ]; then break; fi
      if [ -z "$msgflag" ]; then
	echo "`date` Waiting for cap to be unloaded ..."
	msgflag=done
      fi
      sleep 5
    done
    /usr/bin/rsh $sun -l acsss "echo set cap mode manual $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"
    echo
    echo
  fi
  touch $i
  t1=`date +'%s'`
  delta=`expr $t1 - $t0`
  deltam=`expr $delta / 60`
  deltas=`expr $delta % 60`
  echo That cycle took $deltam minutes $deltas seconds.
  # extract the volumes about to be entered
  set `awk '{split($0,vols); for (v in vols) if (vols[v] ~ /^[[:upper:]]+[[:digit:]]+$/) print vols[v]}' $i | sort`
  /bin/echo -n "Have $# tapes been loaded in cap $icap with tabs in ${action}ed position [y/n]? "
  if read ans && expr "$ans" : '[Yy]' >/dev/null; then
     echo "... success acknowledged"
     echo
     echo "Now updating write-protect status in enstore..."
     # set the volumes successfully entered as write-protected or write-permitted
     for vol; do
       enstore vol --write-protect-$prot $vol
     done
     mv $i ${i}.done
  else
     echo "... failure acknowledged"
     mv $i ${i}.fail
  fi
  echo
  echo
  rem=`ls -vI \*.\* | awk 'END {print NR}'`
  if expr $rem : 0 >/dev/null; then
     echo "`date` There are no more groups to $action. Thank you."
     break
  else
     if [ $rem -gt 1 ]; then
       /bin/echo -n "There are $rem groups remaining. Do another [y/n]? "
     else
       /bin/echo -n "There is only one group remaining! Do it [y/n]? "
     fi
     if read ans && expr "$ans" : '[Yy]' >/dev/null; then
	continue
     else
	break
     fi
  fi
done

check_work

if [ "$icap" != "$ocap" ]; then
  msgflag=
  while /bin/true; do
    cap=`/usr/bin/rsh $sun -l acsss "echo q cap $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"`
    capstate=`echo $cap | sed -e 's#.*automatic ##' | cut -f1 -d\ `
    if [ "$capstate" = "available" ]; then break; fi
    if [ -z "$msgflag" ]; then
      echo "`date` Waiting for cap to be unloaded ..."
      msgflag=done
    fi
    sleep 5
  done
  /usr/bin/rsh $sun -l acsss "echo set cap mode manual $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"
fi
