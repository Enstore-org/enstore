#include <stdio.h>
#include <unistd.h>
#include <ftt_private.h>

void
demo_with_delay(int n) {
	ftt_descriptor d;
	char *pc; int err, res;
	extern int ftt_debug, ftt_errno;

	d = ftt_open("/dev/rmt0", 0);
	printf( "opened...\n");
	ftt_open_dev(d);
	printf( "open_deved...\n");
	ftt_rewind(d);
	printf( "rewound...\n");
	ftt_debug = 3;
	switch(ftt_fork(d)){

	case 0:	/* child */
		printf( "childs play...\n");
		ftt_skip_fm(d,2);
		ftt_rewind(d);
		ftt_report(d);

	default: /* parent */
		printf( "parenting ...\n");
		res = ftt_check(d);
		printf("ftt_check returns %d ftt_errno %d\n", res, ftt_errno);
		system("ps -l");
		sleep(n);
		system("ps -l");
		res = ftt_check(d);
		printf("ftt_check returns %d ftt_errno %d\n", res, ftt_errno);
		res = ftt_wait(d);
		printf("ftt_wait returns %d ftt_errno %d\n", res, ftt_errno);
		break;

	case -1: /* error */
		printf( "fork error ...\n");
		pc = ftt_get_error(&err);
		fprintf(stderr,pc);
		exit(0);
	}
	ftt_close(d);
}

int
main() {
/* 	demo_with_delay(10); */
	printf( "starting...\n");
	demo_with_delay(0);
	return 0;
}
