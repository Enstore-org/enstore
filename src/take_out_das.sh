# take_out_das.sh handles tape shelving and removal for the ADIC AML/2
# library, using the DAS command interface.

# This script must be sourced from tape_aid_wrapper.

for i in `ls -vI \*.\*`; do
  t0=`date +'%s'`
  obox=`awk 'NR == 1 {print $NF}' $i`
  echo "`date` Group $i" >> $output
  # extract the volumes about to be ejected
  set `awk '{gsub(",","\n",$(NF-1)); print $(NF-1)}' $i`
  case $action in
    shelve)
      echo "Now moving volumes to be ejected to shelf library..."
      for vol in $*; do
	if enstore info --check $vol; then
	  lib=`enstore info --vol $vol | awk -F\' '$2 == "library" {print $4}'`
	  if ! echo $lib | grep "^shelf-" >/dev/null; then
	    enstore vol --new-library=shelf-$lib $vol
	  fi
	fi
      done
      ;;
    remove)
      echo "Now deleting volumes to be ejected from enstore..."
      if ! klist -s; then
	echo "`date` ERROR. Check kerberos credentials."
	exit 1
      fi
      if ! rsh $clerk ". /local/ups/etc/setups.sh; setup enstore; for vol in $*; do enstore vol --delete \$vol; done" 2>/dev/null; then
	echo "`date` ERROR. Failed to delete volumes!"
	exit 1
      fi
      ;;
  esac

  echo
  . $i |tee -a $output 2>&1
  echo
  echo
  touch $i
  t1=`date +'%s'`
  delta=`expr $t1 - $t0`
  deltam=`expr $delta / 60`
  deltas=`expr $delta % 60`
  echo That cycle took $deltam minutes $deltas seconds.
  /bin/echo -n "Have $# tapes been ${action%e}ed from I/O box $obox [y/n]? "
  if read ans && expr "$ans" : '[Yy]' >/dev/null; then
     echo "... success acknowledged"
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
