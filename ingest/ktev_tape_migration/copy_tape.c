#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include "ftt.h"

extern char *optarg;
extern int optind;
extern int opterr;
extern int optopt;
extern errno;

#define FTT_ERROR_REPORT(routine)   {           \
   ftt_errstr = ftt_get_error(&ftt_errno);      \
   fprintf (stderr,"%s \n",ftt_errstr);         \
   if (ifd) ftt_close(ifd);                     \
   if (ofd) close(ofd);                         \
   exit(1);                                     \
   }

/* returns: 
   0 - OK
   1 - error
   2 - no space
*/
int main(int argc, char **argv)
{
 
  int 		opt;			/* command line option */
  char            *indev= NULL;		/* input filename */
  char            *outdir=NULL;	/* output filename */
  char            *outfile[200];
  ftt_descriptor  ifd = NULL;           /* in and out file descriptors */
  int            ofd=0;
  char            label[80];		/* data buffer for 1-st record*/
  char           *data;		        /* data buffer */
  int             nfile,nblock, filenum;		/* counters */
  int             position = 0,in_pos;         /* starting tape position */
  char            *ftt_errstr;		/* error string */
  int             len,status,failed=0;		/* statuses */
  int             bs=128*1024; 
  int             files_to_read=1, err=0, rc=0;
  time_t          tm, t1,t;
  float           bytes,mb=1024.*1024.,rate;
  /* get the command line switches 
     -i for input file
     -o for output file
     ============================== */
  while ((opt = getopt(argc,argv,"n:o:i:s:p:")) != -1)
    {
      switch (opt) {
      case 'i' :
	{ indev = strdup(optarg); break; }
      case 'o' :
	{ outdir = strdup(optarg); break; }
      case 'p' :
	{ position = atoi(optarg); break; }
      case 'n' :
	{ files_to_read = atoi(optarg); break; }
      case 's' :
	{ bs = atoi(optarg); break; }
      case '?' :
	fprintf(stderr, "Usage: %s -i <input device> -p <statring position> -o <output dir> -s <block size> -n <number of files to read>\n",argv[0]);
	exit(1);
      }
    }

  if (!(indev)) {
    fprintf(stderr, "Usage: %s -i <input device> -p <statring position> -o <output dir> -s <block size> -n <number of files to read>\n", argv[0]);
    exit(1);
  }
  if (!(outdir)) {
    fprintf(stderr, "Usage: %s -i <input device> -p <statring position> -o <output dir> -s <block size> -n <number of files to read>\n", argv[0]);
    exit(1);
  }
  
  /* open the drives and rewind the output device
     note that we do not set the mode on the output file, 
     but are using the default device
     ==================================================== */
  
  ifd = ftt_open (indev,1);
  if (!ifd) FTT_ERROR_REPORT("ftt_open - input");
 
  printf("rewind %s\n",indev);
  status = ftt_rewind(ifd);
  if (status == -1) FTT_ERROR_REPORT("ftt_rewind");
 
  /* do the copy by reading until we get an error
     ============================================ */
  
  nfile = 0;
  nblock = 0;
  in_pos = position;
  data = (char *)malloc(bs);
  if (position == 0) {
    printf("throw away label\n");
    len = ftt_read(ifd,label,80);
    printf("read label returned %d\n",len);
    if (len != 80) {
      exit(1);
    }
  }
  else {
    status = ftt_skip_fm(ifd, position+1);
  }

  filenum = 0;
  for (nfile = 0; nfile < files_to_read; nfile++) {
    sprintf(outfile,"%s/f%d",outdir, filenum+in_pos);
    ofd = open(outfile,O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (!ofd) FTT_ERROR_REPORT("open - output");
    bytes=0.;
    t1 = time(NULL);
    while (1)
      { 
	len = ftt_read(ifd,data+4,bs);
	if (len > 0)
	  {
	    memcpy(data, &len, 4);
	    memcpy(data+4+len, &len, 4);
	    status = write(ofd,data,len+8);
	    if (status == -1){
	      perror(strerror(errno));
	      failed = 1;
	      break;
	    }
	    bytes=bytes+len;
	    nblock++;
	  }
	else if (len == 0)
	  {
	    tm = time(NULL);
	    t = tm-t1; 
	    if (t == 0) t = 1;
	    rate = bytes/mb/(t);
	    fprintf (stdout," %s file %d had %d blocks %f Mb/s\n",ctime(&tm),filenum+in_pos,nblock, rate);
	    break;
	  }
	else
	  {
	    err=errno;
	    fprintf(stderr,"errno %d\n",err);
	    perror(strerror(err));
	    break;
	  }
	/* printf("file#=%d block#=%d length=%d\n",nfile, nblock, len); */
      }
    close(ofd);
    if ((bytes == 0) || failed || (err == ENOSPC)) {
      fprintf(stderr, "Unlink %s\n",outfile);
      unlink(outfile);
      if (failed) {
	rc = 1;
	break;
      }
      else if (err == ENOSPC){
	printf("last file %d\n",filenum+in_pos);
	rc = 2;
	break;
      }
    }
    else {
      filenum++;
    }

    nblock = 0;
  }
  free(data);
  
  /* done copying
     ============ */
  exit(rc);
}
