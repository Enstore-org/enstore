
#include <stdio.h>
#include <string.h>		/* strcmp */
#include <time.h>

static	char	*version = "$Revision$ $Date$ $Author$";

int
main(  int	argc
     , char	*argv[] )
{
	double	x, sav, delta;
	char	buf[200], buf2[200];
	int	opt, opt_reverse=0, opt_absolute=0, opt_ct=0;


    opt=1;
    for (opt=1; (opt<argc) && (strncmp(argv[opt],"-",1)==0); opt++)
    {   
	if      (strcmp(argv[opt],"-r") == 0)
	{   opt_reverse = 1;
	}
	else if (strcmp(argv[opt],"-nr") == 0)
	{   opt_reverse = 0;
	}
	else if (strcmp(argv[opt],"-a") == 0)
	{   opt_absolute = 1;
	}
	else if (strcmp(argv[opt],"-ct") == 0)
	{   opt_ct = 1;
	}
	else if (strcmp(argv[opt],"--version") == 0)
	{   printf( "%s\n", version );
	    return (0);
	}
    }

    while (fscanf(stdin,"%[^\n]\n",buf2) && !sscanf(buf2,"%lf%[^\n]\n",&x,buf))
	fprintf( stdout, "%s\n", buf2 );

    sav = x; delta = 0;
    if (opt_absolute || opt_ct)
    {
	if (opt_ct)	/* opt_ct overrides */
	{   time_t	xx=(time_t)x;
	    char	*c_p = ctime(&xx);
	    c_p[strlen(c_p)-1] = ' '; /* strip '\n' */
	    c_p[strlen(c_p)] = '\0'; /* strip '\n' */
	    fprintf( stdout, "%18.6f %s %s\n", delta, c_p, buf );
	}
	else
	{   fprintf( stdout, "%18.6f %18.6f%s\n", delta, x, buf );
	}
    }
    else
	fprintf( stdout, "%18.6f %s\n", delta, buf );

    while (fscanf( stdin,"%lf%[^\n]\n", &x, buf ) == 2)
    {   
	if (opt_reverse)   delta = x - sav;
	else               delta = sav - x;

	if (opt_absolute || opt_ct)
	{
	    if (opt_ct)	/* opt_ct overrides */
	    {   time_t	xx=(time_t)x;
		char	*c_p = ctime(&xx);
		c_p[strlen(c_p)-1] = ' '; /* strip '\n' */
		c_p[strlen(c_p)] = '\0'; /* strip '\n' */
		fprintf( stdout, "%18.6f %s %s\n", delta, c_p, buf );
	    }
	    else
	    {   fprintf( stdout, "%18.6f %18.6f%s\n", delta, x, buf );
	    }
	}
	else
	    fprintf( stdout, "%18.6f %s\n", delta, buf );
        sav = x;
    }
    return (0);
}
