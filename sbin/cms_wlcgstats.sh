#!/bin/bash

#get some CMS tape stats for wlcg
#Gene Oleynik 3/27/2018

set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi


out_file=`enstore conf --show crons html_dir`/cms_wlcgstats.json
rc=$?
if [ ${rc} -ne 0 ]
then
	exit $rc
fi

stken=`enstore conf --show database db_host`
stkenstat=`enstore conf --show accounting_server host`

lastupdate=$(enrsh $stken date \"+%s\")
yest=$[ lastupdate  -  86400]
start=$(enrsh $stken date -d @$yest \"+%Y-%m-%d %H:%M:%S\")
rc=$?
if [ ${rc} -ne 0 ]
then
	exit $rc
fi
end=$(enrsh $stken date -d @$lastupdate \"+%Y-%m-%d %H:%M:%S\")
rc=$?
if [ ${rc} -ne 0 ]
then
	exit $rc
fi



eversion=$(enrsh $stken rpm -q enstore | sed -e 's/enstore-//' -e 's/\.[^.]*$//')
rc=$?
if [ ${rc} -ne 0 ]
then
	exit $rc
fi

q="select round( sum(active_bytes), -9),round(sum(active_bytes+deleted_bytes+unknown_bytes), -9) from volume where storage_group = 'cms' and system_inhibit_0!='DELETED' and media_type!='null' and library not like '%shelf%' and library not like '%test%' and media_type !='disk'"
enrsh  ${stken} "psql -t -A -F ' ' -p 8888 enstoredb -U enstore_reader -c \"$q\"" | awk '{print $1 " " $2}' > wlcg-temp.txt
rc=$?
if [ ${rc} -ne 0 ]
then
	exit $rc
fi

usedsize=$(cat wlcg-temp.txt | awk '{ print $1}' )
occupiedsize=$(cat wlcg-temp.txt | awk '{print $2}')

#get list of volumes dismounted in last 24 hours (same difference as mounts)

q="select volume, reads, writes  from tape_mounts where state in ('D','d') and start between '$start' and '$end'  and storage_group = 'cms' order by volume"
enrsh ${stkenstat} "psql -t -A -F ' ' -p 8800 accounting -U enstore_reader -c \"$q\"" > wlcg-temp.txt
rc=$?
if [ ${rc} -ne 0 ]
then
	exit $rc
fi


mounts=$(wc -l wlcg-temp.txt | awk '{print $1}')
uniqmounts=$( cat wlcg-temp.txt | awk '{print $1}' | uniq | wc -l | awk '{print $1}')
avgtaperemounts=$( echo "$mounts $uniqmounts" | awk '{ printf (" %.2f", $1/$2)}')

rm -f wlcg-temp.txt

tmp_out_file=$out_file.tmp
rm -f $tmp_out_file
#echo "Content-type: application/json" > $tmp_out_file
#echo " " >> $tmp_out_file
echo "{">> $tmp_out_file
echo "      \"storageservice\": {" >> $tmp_out_file
echo "           \"name\": \"FNAL-ENSTORE\"," >> $tmp_out_file
echo "           \"implementation\": \"Enstore\"," >> $tmp_out_file
echo "           \"implementationversion\": \"$eversion\"," >> $tmp_out_file
echo "           \"latestupdate\": $lastupdate," >> $tmp_out_file
echo "           \"storageshares\": [" >> $tmp_out_file
echo "               {" >> $tmp_out_file
echo "                     \"name\": \"CMS\"," >> $tmp_out_file
echo "                     \"usedsize\": $usedsize," >> $tmp_out_file
echo "                     \"occupiedsize\": $occupiedsize," >> $tmp_out_file
echo "                     \"avgtaperemounts\": $avgtaperemounts," >> $tmp_out_file
echo "                     \"timestamp\": $lastupdate," >> $tmp_out_file
echo "                     \"vos\": [\"cms\"]" >> $tmp_out_file
echo "               }" >> $tmp_out_file
echo "            ]" >> $tmp_out_file
echo "       }" >> $tmp_out_file
echo "}" >> $tmp_out_file
mv $tmp_out_file $out_file
exit 0
