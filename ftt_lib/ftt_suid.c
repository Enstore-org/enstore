#include <stdio.h>
#include <string.h>
#include <ftt_private.h>

usage() {
   fprintf(stderr, "usage: ftt_suid basename\n");
   exit(-1);
}

main(int argc, char **argv) {
    ftt_descriptor d;
    ftt_stat_buf b;
    int n, res;
    char *pc;

    if (argc != 2) {
       usage();
    }

    /* ftt_debug = 3; */
    d = ftt_open(argv[1],FTT_RDONLY);

    if (0 == d) {
	printf("-1\n");
	pc = ftt_get_error(&n);
	printf("%d\n%s\n", n, pc);
    }

    b = ftt_alloc_stat();
    res = ftt_get_stats(d,b);
    printf("%d\n", res);

    if (res < 0) {
	pc = ftt_get_error(&n);
	printf("%d\n%s\n", n, pc);
    } else {
	ftt_dump_stats(b,stdout);
    }
    ftt_free_stat(b);
    ftt_close(d);
    exit(0);
}
