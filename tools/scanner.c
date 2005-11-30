#include <dirent.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

int selector(const struct dirent* d) {
	if (strcmp(d->d_name,".") ==0) return 0;
	if (strcmp(d->d_name,"..")==0) return 0;
	return 1;
}

int scan_dir(const char* dirname) { 
	DIR* dp;
	struct dirent** namelist;
	int n;
	int i;
	int n_links=0;
	int n_files=0;
	int n_dirs=0;
	char type[2];
	int rc=0;
	struct stat buf;
        rc=lstat(dirname, &buf);
	if (rc!=0) {
		fprintf(stdout,"Can't lstat %s\n",dirname);	
		return 0;
	}
        if ( S_ISLNK(buf.st_mode) ) {
		return 0;
        }
	n = scandir(dirname, &namelist, selector, alphasort);
	if (n < 0)
		perror("scandir");
	for (i=0; i<n; i++) {
		char fullpath[512]="";
		strcat(fullpath,dirname);
		strcat(fullpath,"/");
		strcat(fullpath,namelist[i]->d_name);
		rc=lstat(fullpath,&buf);
		if (rc==0) { 
			if ( S_ISLNK(buf.st_mode) ) { 
				n_links++;
			}
			else if  (  S_ISDIR(buf.st_mode) ) {
				n_dirs++;
			}
			else if ( S_ISREG(buf.st_mode) ) {
				n_files++;
			}
		}
	}
	fprintf(stdout,"%8d %8d %6d %6d %s \n",n,n_files,n_dirs,n_links,dirname);
	
	for (i=0; i<n; i++) {
		char fullpath[512]="";
		strcat(fullpath,dirname);
		strcat(fullpath,"/");
		strcat(fullpath,namelist[i]->d_name);
		free(namelist[i]);
		
		if ( (dp=opendir(fullpath)) == NULL) { 
			switch(errno) {
			case EACCES:
				fprintf(stdout,"Can't access %s\n",fullpath);
				continue;
		  case EMFILE:
			  fprintf(stdout,"Too many files in directory  %s\n",fullpath);
			  continue;
			case ENOTDIR:
		    continue;
			default:
				fprintf(stdout,"Failed to open  %s\n",fullpath);
				continue;
			}			
		}
		closedir(dp);
		scan_dir(fullpath);
	}
	free(namelist);
	return 0;
}


int main(int argc, char* argv[]) {
	char path[512];
	if (argc < 2) {
		int return_code = getcwd(path,512);
	}
	else { 
		strcpy(path,argv[1]);
	}
	scan_dir(path);
	return 0;
}  

