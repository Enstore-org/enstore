/*
 * $Id$
 */

#include "volume_import.h"

int
join_path(char *dest, char *a, char *b)
{
    int l1, l2;
    char result[MAX_PATH_LEN];

    if (!a || !b || !dest){
	fprintf(stderr,"%s: NULL pointer error in join_path\n", 
		progname);
	return -1;
    }

    l1=strlen(a);
    l2=strlen(b);
    
    /*special cases*/
    if (l1==0){
	strcpy(dest,b);
	return 0;
    } else if (l2==0){
	strcpy(dest,a);
	return 0;
    }

    if (l1+l2>MAX_PATH_LEN){
	fprintf(stderr, "%s: path too long: %s/%s\n",
		progname, a, b);
	return -1;
    }
    
    strcpy(result, a);
    if (result[l1-1] != '/')
	result[l1++]= '/';
    if (*b=='/') 
	++b;
    strcpy(result+l1, b);
    strcpy(dest, result);
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

    
