/*  This file (t_read.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Nov 25, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    $RCSfile$
    $Revision$
    $Date$
    */
/* compile: cc -Wall -g -o t_read t_read.c
 */

#include <sys/types.h>		/* open */
#include <sys/stat.h>		/* open */
#include <fcntl.h>		/* open */
#include <stdio.h>		/* printf */
#include <unistd.h>		/* close */
#include <stdlib.h>		/* atoi, malloc */

char	*USAGE="\
%s <ofile> [nMB=102] [blksiz(KB)=32]\n\
examples:\n\
   xxxx /dev/nst0 102\n\
   xxxx test_file 102\n\
";

void
main(  int	argc
     , char	*argv[] )
{
	int	fd, idx;
	int	nKB=32;
	char	*buf_p;
	char	*file;
	int	nMB=102;

    if (argc <= 1)
    {   printf( USAGE, argv[0] );
	exit( 1 );
    }

    file = argv[1];

    if (argc > 2)
	nMB = atoi( argv[2] );

    if (argc > 3)
	nKB = atoi( argv[3] );

    fd = open( file, O_RDONLY );
    if (fd < 0)
    {   printf( "open error\n" );
	exit( 1 );
    }

    buf_p = malloc( 1024*nKB );

    printf( "1024*1024*%d=%d   1024*%d=%d\n", nMB, 1024*1024*nMB, nKB, 1024*nKB );
    for (idx=0; idx<((1024*nMB)/nKB); idx++)
    {
	 read( fd, buf_p, 1024*nKB );
    }

    close( fd );
}
