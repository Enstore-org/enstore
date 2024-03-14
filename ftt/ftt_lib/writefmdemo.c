/*
  This demo shows what data write rates are achived
  with or without buffered tape marks
*/
#include <stdlib.h>
#include <stdio.h>
#include <ftt_private.h>
#include <time.h>

#ifndef WIN32
#include <unistd.h>
#endif

#define KB 1024L
#define MB KB*KB
#define GB MB*KB

extern int ftt_writefm_buffered(ftt_descriptor d);
extern int ftt_flush_data(ftt_descriptor d);

long write_data(ftt_descriptor  fd,
		                 long f_size,
		                char *block,
		                int block_size,
		                int buffered)
{
  long tot_written;
  int bytes_written, cnt, blocks, rc;
  time_t t1, t2;

  tot_written = 0l;
  blocks = f_size / block_size;
  // blocks = 1;
  for (cnt=0; cnt < blocks; cnt++)
  {
    bytes_written = ftt_write(fd,block,block_size);
    tot_written = tot_written + bytes_written;
  }
  t1 = time(NULL);
  if (buffered) {
    rc = ftt_writefm_buffered(fd);
  }
  else {
    rc = ftt_writefm(fd);
  }
  t2 = time(NULL);
  printf("writefm %d\n", t2-t1);
  printf("written %ld\n", tot_written);
  return tot_written;
}


int
demo(char *dev, int buffered) {
	ftt_descriptor d;
	char *pc, *block;
	int err, res, block_size=MB;
	long total=0l, f_size=500*MB, bytes_written;
	int files=10, file_cnt;
	time_t start, stop;
	float tmb, mbf;

	d = ftt_open(dev, 0);
	printf("Using ");
	if (!buffered) {
	  printf("not ");
	}
	printf("buffered tape marks\n");

	printf("demo calling ftt_rewind\n");
	res = ftt_rewind(d);
	if (res < 0){
	  perror("ftt_rewind error\n");
	  printf("RES %d\n", res);
	  return -1;
	}
	block = (char *)malloc(MB);
	start = time(NULL);
	for (file_cnt=0; file_cnt<files;file_cnt++)
	  {
	    bytes_written = write_data(d, f_size, block, block_size, buffered);
	    total = total + bytes_written;
	  }

	stop = time(NULL);
	tmb = (float)total;
	mbf = (float)MB;
	tmb = tmb/mbf;
	printf("total written %f MB in %d seconds average rate %f MB/s\n", tmb, stop-start, tmb/(float)(stop-start));
	start = time(NULL);
	ftt_flush_data(d);
	stop = time(NULL);
	printf("data flushed in %d sec\n", stop-start);
	ftt_rewind(d);
	ftt_close(d);
	return 0;
}

void usage(char *cmd) {
  printf("usage:%s [-h?b] mt_device\n", cmd);
  printf("-b - buffered tape mark\n");
}

int
main(int argc, char **argv) {
  int opt;
  int buffered=0;

  if (argc < 2) {
    usage(argv[0]);
    return -1;
   }
  while ((opt = getopt(argc, argv, "bh?")) != -1) {
    switch (opt) {
    case 'h': usage(argv[0]); return 0;
    case '?': usage(argv[0]); return 0;
    case 'b': buffered = 1; break;
    }
  }
  demo(argv[optind], buffered);
  return 0;
}
