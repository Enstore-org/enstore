/* 
   $Id$
*/

#include "volume_import.h"


static char * 
match_opt(char *optname, char *arg)
{
    int n;
    char *cp;

    /* be friendly about _ vs - */
    for (cp=arg; *cp && *cp!='='; ++cp)
	if (*cp=='_')
	    *cp = '-';

    n = strlen(optname);
    if (strlen(arg)<n){
	return (char *)0;
    }
    if (!strncmp(optname, arg, n)){
	return arg+n;
    }
    return (char *)0;
}

static char 
echo_cat(char *fname){
    char buf[256];
    int nbytes;
    int fd;

    fd = open(fname,0);
    if (fd<0){
	fprintf(stderr,"%s: ", progname);
	perror(fname);
	return -1;
    }
    
    write (1, fname, strlen(fname));
    write (1, " ", 1);
    while ( (nbytes=read(fd, buf, 256)) > 0){
	if (write(1, buf, nbytes) != nbytes){
	    fprintf(stderr,"%s: ", progname);
	    perror("write");
	    close(fd);
	    return -1;
	}
    }
    close(fd);
    return 0;
}


int
dump_db_main(int argc, char **argv){
    char *cp;
    DIR *vold, *subd, *filed, *itemd;
    struct dirent *vol, *sub, *file, *item;
    
    char path[MAX_PATH_LEN];
    
    tape_db = getenv("TAPE_DB");
    
    if (argc>2){
      Usage: fprintf(stderr,"Usage: %s --dump-db [--tape-db=dbdir]\n\
   tape-db (db directory) can be set using environment variable $TAPE_DB\n", 
		     progname);
      return -1;
    }
    
    if (argc==2){
	if ((cp=match_opt("--tape-db=", argv[1])))
	    tape_db = cp;
	else {
	    fprintf(stderr, "%s: unknown option %s\n", 
		    progname, argv[1]);
	    goto Usage;
	}
    }

    if (!tape_db || verify_tape_db(0)) {
	fprintf(stderr, "%s: no tape db specified\n", progname);
	goto Usage;
    }
    
    if (chdir(tape_db)){
	fprintf(stderr,"%s: chdir ", progname);
	perror(tape_db);
	return -1;
    }
    
    if ( (vold = opendir("volumes")) == (DIR*)0){
	fprintf(stderr,"%s: opendir: ", progname);
	perror("volumes");
	return -1;
    }

    while ( (vol=readdir(vold)) ){      
	if (vol->d_name[0]=='.')
	    continue;
	sprintf(path, "volumes/%s", vol->d_name);
	if ( (subd = opendir(path)) == (DIR*)0){
	    fprintf(stderr,"%s: opendir: ", progname);
	    perror(vol->d_name);
	    return -1;
	}
	while ( (sub=readdir(subd)) ){
	    if (sub->d_name[0]=='.')
		continue;
	    if (!strcmp(sub->d_name,"files")){ /*per-file data*/
		sprintf(path, "volumes/%s/files", vol->d_name);
		if ( (filed = opendir(path)) == (DIR*)0){
		    fprintf(stderr,"%s: opendir: ", progname);
		    perror(path);
		}
		while ( (file=readdir(filed)) ){
		    if (file->d_name[0]=='.')
			continue;
		    sprintf(path, "volumes/%s/files/%s", vol->d_name,
			    file->d_name);
		    if ( (itemd = opendir(path)) == (DIR*)0){
			fprintf(stderr, "%s: opendir: ", progname);
			perror(path);
			return -1;
		    }
		    while ( (item=readdir(itemd)) ){
			if (item->d_name[0]=='.')
			    continue;
			sprintf(path, "volumes/%s/files/%s/%s",
				vol->d_name, file->d_name, item->d_name);;
			echo_cat(path);
		    }
		    closedir(itemd);
		}
		closedir(filed);
	    } else { /*per-volume data*/
		sprintf(path, "volumes/%s/%s", vol->d_name,
			sub->d_name);
		echo_cat(path);
	    }
	}
	closedir(subd);
    }
    closedir(vold);
    return 0;
}

	    
