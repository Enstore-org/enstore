#! /usr/bin/env perl
#  This file (msg_filter.pl) was created by Ron Rechenmacher <ron@fnal.gov> on
#  Jun  4, 1999. "TERMS AND CONDITIONS" governing this file are in the README
#  or COPYING file. If you do not have such a file, one can be obtained by
#  contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#  $RCSfile$
#  $Revision$
#  $Date$


# must have header!
$_=<>;
if (/(.*[^ ]+) +message/)
{   $len = length( $1 );
    print;
    $_=<>;
    print;
    while (<>)
    {   if (!/^.{$len} *(call |ret  |exc  )/)
        {   $_ =~ /^(.{$len})\s+(.*)/;
	    print "${1}msg: $2\n";
        }
        else
        {   print $_;
        }
    }
}
else
{   print "no header -- no processing done\n";
    print;
    while (<>) { print; }
}
