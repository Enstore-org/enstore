static char rcsid[] = "$Id$";
#include <stdio.h>
#include <ftt.h>

ftt_descriptor d;

char buf[128];

main() {
	ftt_debug = 3;
	d = ftt_open("/dev/bogus", 1);
	printf("result %s\n", ftt_get_error(0));
	d = ftt_open("/dev/rmt/jag1d10nrv.8500", 1);
	printf("result %s\n", ftt_get_error(0));
	if (d  == 0) {
		exit(1);
	}
	ftt_read(d,buf,128);
	printf("result %s\n", ftt_get_error(0));
	ftt_read(d,buf,128);
	printf("result %s\n", ftt_get_error(0));
	ftt_close(d);
}
