
#include <stdio.h>

static	char	*version = "$Revision$ $Date$ $Author$";

void
main(  int	argc
     , char	**argv )
{
	double	x, y, delta, total;
	char	buf[200], buf2[200];
	int	opt, opt_reverse=0;
	double	nanoseconds=1.0;
	int	tot_bytes=0;


    opt=1;
    for (opt=1; (opt<argc) && (strncmp(argv[opt],"-",1)==0); opt++)
    {   
	if        (strcmp(argv[opt],"-r") == 0)
	{   opt_reverse = 1;
	}
    }

    while (fscanf(stdin,"%[^\n]\n",buf2) && !sscanf(buf2,"%lf%[^\n]\n",&x,buf))
	fprintf( stdout, "%s\n", buf2 );

    y = x; total = delta = 0;
    fprintf( stdout, "%18.6lf %18.6lf%s\n", delta, x, buf );

    while (fscanf( stdin,"%Lf%[^\n]\n", &x, buf ) == 2)
    {   
	if (opt_reverse)   delta = x - y;
	else               delta = y - x;

	total += delta;

	if (nanoseconds != 1.0)
	    fprintf( stdout, "%11.2fns ", nanoseconds*delta );

	fprintf( stdout, "%18.6lf %18.6lf%s\n", delta, x, buf );
        y = x;
    }

}
