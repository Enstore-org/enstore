/* $Id$
 Simple "inode" database
*/

#include "volume_import.h"

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
    if (fprintf(fp, fmt, value)<=0
	||fclose(fp)
	) return 1;
    return 0;
}

int
write_db_s(char *db_path, char *key, char *value)
{
    verbage("adding %s=%s to database\n", key, value);
    return write_db_fmt(db_path, "%s\n", key, (int)(value ? value:""));
}

int
write_db_i(char *db_path, char *key, int value)
{
    verbage("adding %s=%d to database\n", key, value);
    return write_db_fmt(db_path,"%d\n", key, value);
}

int
write_db_u(char *db_path, char *key, unsigned int value)
{
    verbage("adding %s=%u to database\n", key, value);
    return write_db_fmt(db_path, "%u\n", key, value);
}

static int 
read_db_fmt(char *db_path, char *fmt, char *key, void *value)
{
    FILE *fp;
    char path[MAX_PATH_LEN];
    
    sprintf(path, "%s/%s", db_path, key);
    if (!(fp=fopen(path,"r"))){
	fprintf(stderr,"%s: cannot open ",progname);
	perror(path);
	return -1;
    }
    if (fscanf(fp, fmt, value)<=0){
	fprintf(stderr,"%s: cannot read database file %s\n", progname, path);
	return -1;
    }
    if (fclose(fp)){
	fprintf(stderr,"%s: close: ",progname);
	perror(path);
	return -1;
    }
    return 0;
}

int 
read_db_i(char *db_path, char *key, int *value)
{
    return read_db_fmt(db_path, "%d", key, (void *)value);
}
