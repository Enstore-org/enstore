/*  This file (tape.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    May  7, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    */
/* compile: cc -Wall -g -o t_rand t_rand.c
 */

#include <sys/types.h>		/* open */
#include <sys/stat.h>		/* open */
#include <fcntl.h>		/* open */
#include <stdio.h>		/* printf */
#include <unistd.h>		/* close */
#include <stdlib.h>		/* rand */


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
	int	*i_p; /* to fill buf w/ random ints */
	char	*file;
	int	nMB=102;
	int	sts;

    if (argc <= 1)
    {   printf( USAGE, argv[0] );
	exit( 1 );
    }

    file = argv[1];

    if (argc > 2)
	nMB = atoi( argv[2] );

    if (argc > 3)
	nKB = atoi( argv[3] );

    fd = open( file, O_WRONLY|O_CREAT, 0x1b6 );	/* mode 0x1b6 = octal 666 */
    if (fd < 0)
    {   printf( "open error\n" );
	exit( 1 );
    }

    buf_p = malloc( 1024*nKB );
    i_p = (int *)buf_p;
    srand(1);			/* seed */
    for (idx=(1024*nKB)/4; idx--; )
	*i_p++ = rand();

    printf( "1024*1024*%d=%d   1024*%d=%d\n", nMB, 1024*1024*nMB, nKB, 1024*nKB );
    for (idx=0; idx<((1024*nMB)/nKB); idx++)
    {
	 sts = write( fd, buf_p, 1024*nKB );
	 if (sts == -1)
	 {   perror( "write error" );
	     break;
	 }
    }

    close( fd );
}
