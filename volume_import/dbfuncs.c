/* $Id$
 Simple "inode" database
*/

#include "volume_import.h"


extern char *progname;

static int 
write_db_fmt(char *db_path, char *fmt, char *key, int value)
{
    FILE *fp;
    char path[MAX_PATH_LEN];

    sprintf(path, "%s/%s", db_path, key);
    if (!(fp=fopen(path,"w"))){
	fprintf(stderr,"%s: cannot open ",progname);
	perror(path);
	return -1;
    }
    fprintf(fp, fmt, value); /* XXX check return value from fprintf */
    fclose(fp);
    return 0;
}

int
write_db_s(char *db_path, char *key, char *value)
{
    return write_db_fmt(db_path, "%s\n", key, (int)(value ? value:""));
}


int
write_db_i(char *db_path, char *key, int value)
{
    return write_db_fmt(db_path,"%d\n", key, value);
}

int
write_db_ul(char *db_path, char *key, unsigned long int value)
{
    return write_db_fmt(db_path, "%ul\n", key, value);
}
