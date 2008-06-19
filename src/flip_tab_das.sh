# flip_tab_das.sh handles write protect tab flipping for the ADIC AML/2
# library, using the DAS command interface.

# This script must be sourced from tape_aid_wrapper.

for i in `ls -vI \*.\*`; do
  t0=`date +'%s'`
  # must wait for this box to finish any previous insert
  ibox=`awk 'NR == 1 {sub("^E","I",$NF); print $NF}' $i`
  if jobs %?$ibox >/dev/null 2>&1 | grep Running >/dev/null
  then
     echo "Waiting for I/O box $ibox to be unloaded ..."
     wait %?$ibox >/dev/null 2>&1
  fi
  echo "`date` Group $i" >> $output
  . $i |tee -a $output 2>&1
  echo
  echo
  touch $i
  t1=`date +'%s'`
  delta=`expr $t1 - $t0`
  deltam=`expr $delta / 60`
  deltas=`expr $delta % 60`
  echo That cycle took $deltam minutes $deltas seconds.
  # extract the volumes about to be entered
  set `awk '{gsub(",","\n",$(NF-1)); print $(NF-1)}' $i`
  case `YesNo "Have $# tapes been loaded in I/O box $ibox with tabs in ${action}ed position?"` in
    Yes)
       echo "`date` ... success acknowledged"
       echo
       echo "Now updating write-protect status in enstore..."
       # set the volumes successfully entered as write-protected or write-permitted
       for vol; do
	 enstore vol --write-protect-$prot $vol
       done
       mv $i ${i}.done
       secs=40
       ;;
    No)
       echo "`date` ... failure acknowledged"
       mv $i ${i}.fail
       secs=80
       ;;
  esac
  echo
  echo "Now inserting volumes from I/O box $ibox..."
  # wait for successful insert in the background
  until dasadmin insert2 -n $ibox >/dev/null 2>&1; do sleep $secs; [ $secs -gt 10 ] && secs=`expr $secs / 2`; done &
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

echo "Waiting for I/O box(es) to be unloaded ..."
jobs
wait
