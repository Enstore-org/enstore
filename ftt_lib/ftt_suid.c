static char rcsid[] = "@(#)$Id$";
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ftt_private.h>

#ifndef WIN32
#include <unistd.h>
#endif

void
usage(void) {
   fprintf(stderr, "usage: ftt_suid [-w] -s basename	 # print stats\n");
   fprintf(stderr, "       ftt_suid -c basename		 # clear stats\n");
   fprintf(stderr, "       ftt_suid -b arg basename      # set blocksize\n");
   fprintf(stderr, "       ftt_suid -C arg basename      # set compression\n");
   fprintf(stderr, "       ftt_suid -d arg basename      # set density\n");
   fprintf(stderr, "       ftt_suid -p basename          # get/dump parts\n");
   fprintf(stderr, "       ftt_suid -u basename          # undump/write parts\n");
   fprintf(stderr, "       ftt_suid -l blck basename  	 # locate to blk\n");
   fprintf(stderr, "       ftt_suid -L blck prt basename # locate w/partition\n");
   fprintf(stderr, "       ftt_suid -M prt basename      # set mount partition\n");
   exit(-1);
}

int
main(int argc, char **argv) {
	ftt_descriptor d;
	ftt_stat_buf b;
	ftt_partbuf pb;
	int n, res;
	char *pc;
	char *basename;
	char command;
	int  arg, arg2;
	int direction = FTT_DIR_READING;

	if (argc <= 2 || argv[1][0] != '-') {
		usage();
	}

	while(1){
		switch (argv[1][1]) {
		case 'x':
			ftt_debug = 4;
			argv++;
			argc--;
			continue;
		case 'w':
			direction = FTT_DIR_WRITING;
			argv++;
			argc--;
			continue;
		case 'e':
		case 'c':
		case 's': 
		case 'i':
		case 'v':
	        case 'p':
	        case 'u':
			if (argc != 3) {
				usage();
			}
			command = argv[1][1];
			basename = argv[2];
			break;
		case 'C':
		case 'b': 
		case 'd': 
		case 'l': 
		case 'M': 
			if (argc != 4) {
				usage();
			}
			command = argv[1][1];
			arg = atoi(argv[2]);
			basename = argv[3];
			break;
		case 'L': 
			if (argc != 5) {
				usage();
			}
			command = argv[1][1];
			arg = atoi(argv[2]);
			arg2 = atoi(argv[3]);
			basename = argv[4];
			break;
		default:
			usage();
		}
		break;
	}

	if (geteuid() != 0) {
		fprintf(stderr,"ftt_suid executable not setuid root!\n");
		exit(-2);
	}

	/* ftt_debug = 3; */
	d = ftt_open(basename,FTT_RDONLY);

	d->data_direction = direction;

	if (0 == d) {
		/* fake an ftt_report to stdout */
		printf("-1\n");
		pc = ftt_get_error(&n);
		printf("%d\n%s\n", n, pc);
		exit(1);
	}

	/* attach our ftt_report() channel to stdout */
	d->async_pf_parent = stdout;

	switch(command){
	case 'i':
		printf("%s\n", d->prod_id );
		res = 0;
		break;
	case 'e':
		res = ftt_erase(d);
		break;
	case 's':
		b = ftt_alloc_stat(); 		if (b == 0) break;
		res = ftt_get_stats(d,b);	if (res < 0) break;
		ftt_dump_stats(b,stdout);
		ftt_free_stat(b);
		break;
	case 'c':
		ftt_clear_stats(d);
		break;
	case 'C':
		res = ftt_set_compression(d,arg);
		break;
	case 'b':
		res = ftt_set_blocksize(d,arg);
		break;
	case 'd':
		ftt_set_mode_dev(d, basename,0);
		res = ftt_set_hwdens(d,arg);
		break;
	case 'v':
		res = ftt_verify_blank(d);
		break;
        case 'p':
		pb = ftt_alloc_parts();
		res = ftt_get_partitions(d,pb);
		ftt_dump_partitions(pb,stdout);
		ftt_free_parts(pb);
		break;
        case 'u':
		pb = ftt_alloc_parts();
		ftt_undump_partitions(pb,stdin);
		res = ftt_write_partitions(d,pb);
		ftt_free_parts(pb);
		break;
        case 'l':
		res = ftt_scsi_locate(d, arg);
		break;
        case 'L':
		res = ftt_locate_part(d, arg, arg2);
		break;
        case 'M':
                ftt_set_mount_partition(d, arg);
		break;
	}
	ftt_report(d);
	return 0;
}
