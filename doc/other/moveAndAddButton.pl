#!/usr/local/bin/perl

# move all specified files from the source directory to the target directory.
# during the move add the top button onto the top of the file if it was not
# already done.
#
#   calling format:
#                   moveAndAddButton.pl <targetDir> <files>
#
# first get the contents of the header text to add.  this way we can pick out
# the top line.  if this line is the first line in a source file than just
# copy the file.  if it is not, then add the header to the file and coy it to
# the target directory.
#
$tempfile = "temp.html";
$hfile = "header.html";

$target = @ARGV[0];

foreach $i (1 .. $#ARGV) {
  $sfile = "@ARGV[$i]";
  @filename = split /\//, $sfile;
  $tfile = "$target/@filename[$#filename]";

  $rtn = `cat $hfile $sfile>$tfile`;

}
