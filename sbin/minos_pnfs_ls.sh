#! /bin/bash
cd /fnal/ups/prd/www_pages/enstore

html_file=minos_pnfs_ls.html
tmp_file=${html_file}.tmp

theDate=`date`

rm -f $tmp_file
echo "<HTML><HEAD><TITLE>MINOS PNFS DIRECTORY LISTING</TITLE></HEAD>" > $tmp_file
echo "<BODY TEXT=#000066 BGCOLOR=#FFFFFF LINK=#0000EF VLINK=#55188A ALINK=#FF0000 BACKGROUND=enstore.gif>" >> $tmp_file
echo "<BR><BR><FONT SIZE=+4><B>Directory Listing of /pnfs/minos</B></FONT>" >> $tmp_file
echo "<PRE>" >> $tmp_file
echo "  " >> $tmp_file
echo "  " >> $tmp_file
echo "  " >> $tmp_file
echo $theDate >> $tmp_file
echo "  " >> $tmp_file
echo "  " >> $tmp_file

ls -l -F -R /pnfs/minos >> $tmp_file 2>/dev/null

echo "</PRE></BODY></HTML>" >> $tmp_file
mv $tmp_file $html_file

