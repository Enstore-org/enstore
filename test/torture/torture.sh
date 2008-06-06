#!/bin/bash 
pnfs_path="${1:-/pnfs/cdfen/test/sl8500/test/}"
node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
mail="${ENSTORE_MAIL:-litvinse@fnal.gov}"
RANDOM=$$$(date +"%s")

i=0

  file_family=${node}
  destination=${pnfs_path}${node}
  if [ ! -e ${destination} ] 
  then 
      mkdir -p ${destination}
  fi
  cd $destination
  enstore pnfs --file_family=${file_family}
  enstore pnfs --file_family_width=1
  cd -  > /dev/null 2>&1

while [ 1 ] 
do
  name=${node}_4000_`date +"%s"`.data
  sz=`gauss 4000|awk '{ print $1}'`
  createfile $sz $name 
  encp --threaded $name $destination
  if [ $? -ne 0 ]
      then
      /bin/mail -s "message from ${node}: encp ${name} ${destination} failed" $mail
      rm $name
      exit 1
  fi
  rm ${name}

  name=${node}_500_`date +"%s"`.data
  sz=`gauss 500|awk '{ print $1}'`
  createfile $sz $name 
  encp --threaded $name $destination
  if [ $? -ne 0 ]
      then
      /bin/mail -s "message from ${node}: encp ${name} ${destination} failed" $mail
      rm $name
      exit 1
  fi
  rm ${name}
  #
  # Now read file back
  # 
  label=`cat "$destination/.(use)(4)($name)" | head -1`
  files=`enstore volume --ls-active $label`
  j=0
  lfiles=()
  for file in ${files}
  do
    lfiles[j]=$file
  let j=j+1
  done

  iread=0
  while [ $iread -lt 2 ] 
  do 
    if [ $j -ne 0 ] 
    then 
      r=`expr \( $RANDOM % $j \)`
      f=${lfiles[$r]}
      if [ -e $f ] 
      then
        s=`stat -t $f | awk '{ print $2 }'`
        if [ $s -ne 0 ] 
        then
          encp --threaded $f tmp_$$.data
          if [ $? -ne 0 ]
          then
            /bin/mail -s "message from ${node}: encp ${f} tmp_$$.data failed ( $r $s $node )" $mail
            rm tmp_$$.data
            exit 1
          fi
          rm tmp_$$.data
        else
          echo "file $f has zero length"
        fi
      fi  
    fi
    let iread=iread+1
  done
let i=i+1
echo "Finished $i iteration, label $label"
done 
exit 0
