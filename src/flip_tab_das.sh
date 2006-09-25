# flip_tab_das.sh handles write protect tab flipping for the ADIC AML/2
# library, using the DAS command interface.

# This script must be sourced from tape_aid_wrapper.

for i in `ls -vI \*.\*`; do
  t0=`date +'%s'`
  pid=
  stat=
  # must wait for this box to finish any previous insert
  ibox=`awk 'NR == 1 {sub("^E","I",$NF); print $NF}' $i`
  eval `jobs -l | awk -v Ibox=$ibox '$0 ~ Ibox {printf "pid=%s;stat=%s",$2,$3}'`
  [ "$stat" = Running ] && wait $pid 2>&1
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
  /bin/echo -n "Have $# tapes been loaded in I/O box $ibox with tabs in ${action}ed position [y/n]? "
  if read ans && expr "$ans" : '[Yy]' >/dev/null; then
     echo "... success acknowledged"
     echo
     echo "Now updating write-protect status in enstore..."
     # set the volumes successfully entered as write-protected or write-permitted
     for vol; do
       enstore vol --write-protect-$prot $vol
     done
     mv $i ${i}.done
     secs=40
  else
     echo "... failure acknowledged"
     mv $i ${i}.fail
     secs=80
  fi
  echo
  echo "Now inserting volumes from I/O box $ibox..."
  # wait for successful insert in the background
  until dasadmin insert2 -n $ibox 2>/dev/null 1>&2; do sleep $secs; [ $secs -gt 10 ] && secs=`expr $secs / 2`; done &
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

echo "`date` Waiting for I/O box(es) to be unloaded ..."
jobs
wait
