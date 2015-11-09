# flip_tab_acsls.sh handles write protect tab flipping for the STK 9310 and 8500
# libraries, using the ACSLS command interface.

# This script must be sourced from tape_aid_wrapper.

cap=`/usr/bin/rsh $sun -l acsss "echo q cap $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"`
if echo $cap | grep manual >/dev/null; then
  /usr/bin/rsh $sun -l acsss "echo set cap mode automatic $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"
fi

for i in `ls -vI \*.\*`; do
  t0=`date +'%s'`
  echo "`date` Group $i" >> $output
  . ./$i |tee -a $output 2>&1
  echo
  echo
  if [ "$icap" = "$ocap" ]; then
    echo
    echo
    if [ $action = reload ]; then
      prompt="Press Enter when all tapes have been reloaded"
    else
      prompt="Press Enter when all tapes have been loaded with tabs in ${action}ed position"
    fi
    read -p "$prompt" ans
    msgflag=
    while /bin/true; do
      cap=`/usr/bin/rsh $sun -l acsss "echo q cap $icap '\r' logoff | bin/cmd_proc -l -q 2>/dev/null"`
      capstate=`echo $cap | sed -e 's#.*automatic ##' | cut -f1 -d\ `
      if [ "$capstate" = "available" ]; then
	 break
      fi
      if [ -z "$msgflag" ]; then
	echo "Waiting for cap to be unloaded ..."
	msgflag=done
      fi
      sleep 5
    done
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
  if [ $action = reload ]; then
    case `YesNo "Have $# tapes been reloaded in cap $icap?"` in
      Yes)
	 echo "`date` ... success acknowledged"
	 mv $i ${i}.done
	 ;;
      No)
	 echo "`date` ... failure acknowledged"
	 mv $i ${i}.fail
	 ;;
    esac
  else
    case `YesNo "Have $# tapes been loaded in cap $icap with tabs in ${action}ed position?"` in
      Yes)
	 echo "`date` ... success acknowledged"
	 echo
	 echo "Now updating write-protect status in enstore..."
	 # set the volumes successfully entered as write-protected or write-permitted
	 for vol; do
	   enstore vol --write-protect-$prot $vol
	 done
	 mv $i ${i}.done
	 ;;
      No)
	 echo "`date` ... failure acknowledged"
	 mv $i ${i}.fail
	 ;;
    esac
  fi
  echo
  echo
  rem=`ls -vI \*.\* | awk 'END {print NR}'`
  if expr $rem : 0 >/dev/null; then
     echo "`date` There are no more groups to $action. Thank you."
     break
  else
     if [ $rem -gt 1 ]; then
       prompt="There are $rem groups remaining. Do another?"
     else
       prompt="There is only one group remaining! Do it?"
     fi
     case `YesNo "$prompt"` in
       Yes)
	  continue
	  ;;
       No)
	  break
	  ;;
     esac
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
      echo "Waiting for cap to be unloaded ..."
      msgflag=done
    fi
    sleep 5
  done
fi
