/*
 * $Id$
 */

#include "volume_import.h"

int
join_path(char *dest, char *a, char *b)
{
    char tmp[MAX_PATH_LEN];
    char *cp1, *cp2;

    if (!a || !b || !dest){
	fprintf(stderr,"%s: NULL pointer error in join_path\n", 
		progname);
	return -1;
    }

    if (strlen(a)+strlen(b)>MAX_PATH_LEN){
	fprintf(stderr, "%s: path too long: %s/%s\n",
		progname, a, b);
	return -1;
    }
    
    sprintf(tmp, "%s/%s", a, b);
    
    for (cp1=tmp, cp2=dest; *cp1; ++cp1){
	if (cp1!=tmp && *cp1=='/' && *cp1==*(cp1-1)) /* repeated /'s */
	    continue;
	*cp2++=*cp1;
    }
    *cp2='\0';
    return 0;
}

int 
strip_path(char *dest, char *base, char *name)
{
    int l;

    if (!dest || !base || !name){
	fprintf(stderr,"%s: NULL pointer error in strip_path\n", 
		progname);
	return -1;
    }
    l = strlen(base);
    if (strlen(name)<=l 
	||strncmp(base, name, l))
	{
	    fprintf(stderr,"%s: base path %s not found in %s\n", 
		    progname, base, name);
	    return -1;
	}
    strcpy(dest, name+l);
    return 0;
}

    
