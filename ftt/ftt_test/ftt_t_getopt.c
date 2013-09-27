static char *rcsid = "@(#) $Id$";
/* this is the getopt function missing on Windows NT */
#include <string.h>

#ifdef WIN32
char *optarg;
int getopt(int argc,char **argv,char *what) 
{
	static int i=1;
	char *a;
	char s;

	int k = 0;
	optarg = 0;
	if ( i  >= argc ) return -1;

	a = argv[i++];
	/* parmaters are like '-f' or '/f' */

	if (a[0] != '/' && a[0] != '-') return '?'; 
	if (strlen(a) != 2 )            return '?';
	s = a[1];
	while (what[k] != 0 ) {
		if (s == what[k] ) {
			if ( what[k+1] == ':' ) {
				if (i > argc ) return '?';
				a = argv[i];
				if (a[0] == '/' || a[0] == '-' ) return '?';
				i++;
				optarg = a;
			}
			return s;
		}
		k++;
	}
	return '?';
}
#endif
