#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ftt_private.h>

void
usage(void) {
   fprintf(stderr, "usage: ftt_suid -s basename		   # print stats\n");
   fprintf(stderr, "       ftt_suid -c basename		   # clear stats\n");
   fprintf(stderr, "       ftt_suid -b dens bsize basename # set mode\n");
   exit(-1);
}

int
main(int argc, char **argv) {
	ftt_descriptor d;
	ftt_stat_buf b;
	int n, res;
	char *pc;
	char *basename;
	char command;
	int dens, bsize;

	if (argc <= 2 || argv[1][0] != '-') {
		usage();
	}

	switch (argv[1][1]) {
	case 'c':
	case 's': 
		if (argc != 3) {
			usage();
		}
		command = argv[1][1];
		basename = argv[2];
		break;
	case 'b': 
		if (argc != 5) {
			usage();
		}
		command = argv[1][1];
		dens = atoi(argv[2]);
		bsize = atoi(argv[3]);
		basename = argv[4];
		break;
	default:
		usage();
	}

	if (geteuid() != 0) {
		fprintf(stderr,"ftt_suid executable not setuid root!\n");
		exit(-2);
	}

	/* ftt_debug = 3; */
	d = ftt_open(basename,FTT_RDONLY);

	if (0 == d) {
		printf("-1\n");
		pc = ftt_get_error(&n);
		printf("%d\n%s\n", n, pc);
	}

	/* attach our ftt_report() channel to stdout */
	d->async_pf = stdout;

	switch(command){
	case 's':
		b = ftt_alloc_stat(); 		if (b == 0) break;
		res = ftt_get_stats(d,b);	if (res < 0) break;
		ftt_dump_stats(b,stdout);
		ftt_free_stat(b);
		break;
	case 'c':
		ftt_clear_stats(d);
		break;
	case 'b':
		res = ftt_set_hwdens_blocksize(d,dens,bsize);
		if (res > 0){
			ftt_open_dev(d);
		}
		break;
	}
	ftt_report(d);
	return 0;
}
