#include <stdio.h>
#include <ftt_private.h>

main() {
	demo_with_delay(10);
	demo_with_delay(0);
}

demo_with_delay(int n) {
	ftt_descriptor d;
	char *pc; int err, res;
	extern int ftt_debug, ftt_errno;

	d = ftt_open("/dev/rmt/tps0d3", 0);
	ftt_rewind(d);
	ftt_debug = 0;
	switch(ftt_fork(d)){

	case 0:	/* child */
		ftt_skip_fm(d,2);
		ftt_rewind(d);
		ftt_report(d);

	default: /* parent */
		break;

	case -1: /* error */
		pc = ftt_get_error(&err);
		fprintf(stderr,pc);
		exit(0);
	}
	res = ftt_check(d);
	printf("ftt_check returns %d ftt_errno %d\n", res, ftt_errno);
	system("ps -l");
	sleep(n);
	system("ps -l");
	res = ftt_check(d);
	printf("ftt_check returns %d ftt_errno %d\n", res, ftt_errno);
	res = ftt_wait(d);
	printf("ftt_wait returns %d ftt_errno %d\n", res, ftt_errno);
	ftt_close(d);
}
