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
$text =  "grep -i -F -e \"<BODY>\" -e \"<HTML>\" ";
$hfile = "header.html";
$hlines = `cat $hfile`;

$target = @ARGV[0];

sub add_and_copy {
  my ($sfile, $tfile) = @_;
  # first see if the header has a <HTML> line.  if it does not, then just
  # tack on the header at the beginning.  otherwise we must read in the initial
  # file and figure out where to put the header.
  $lines = `$text $sfile`;
  if ("$lines" ne "") {
    # we must try to figure out where to put the header, get the original file
    $lines = `cat $sfile`;
    @lines = split /\n/, $lines;
    $wrote_header = 0;
    $has_html = 0;
    open NEWFILE, ">$tfile" or die "Cannot open new file - $tfile\n";
    foreach $line (@lines) {
      if (! $wrote_header) {
	if ("$line" =~ /<BODY>/i) {
	  # this is the beginning of body statement, write the header after it
	  if (! $has_html) {
	    # no <HTML> line was found so add one
	    print NEWFILE "<HTML>\n";
	  }
	  print NEWFILE "$line\n";
	  print NEWFILE "$hlines\n";
	  $wrote_header = 1;
	} elsif ("$line" =~ /<HTML>/i) {
	  # mark that this file has a <HTML> construct
	  print NEWFILE "$line\n";
	  $has_html = 1;
	} else {
	  print NEWFILE "$line\n";
	}
      } else {
	# we already wrote the header just write this line into the new file
	print NEWFILE "$line\n";
      }
    }
    close NEWFILE;
  } else {
    # nope, just add the header at the beginning
    $rtn = `cat $hfile $sfile>$tfile`;
  }
}

foreach $i (1 .. $#ARGV) {
  $sfile = "@ARGV[$i]";
  @filename = split /\//, $sfile;
  $filename = @filename[$#filename];
  $tfile = "$target/$filename";
  # if the filename does not end with '.html' then just move it
  if (($filename =~ m/.html$/) && ("$filename" ne "index.html") &&
      ("$filename" ne "header.html")) {
    # this is an html file, add the header and copy the file
    add_and_copy("$sfile", "$tfile");
  } else {
    # this is not an html file, just copy it to the new area
    $rtn = `cp $sfile $tfile`;
  }
}

