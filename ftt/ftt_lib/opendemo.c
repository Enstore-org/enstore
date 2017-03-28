static char rcsid[] = "@(#)$Id$";

#include <stdlib.h>
#include <stdio.h>
#include <ftt_private.h>

#ifndef WIN32
#include <unistd.h>
#endif

void
demo(char *dev) {
	ftt_descriptor d;
	char *pc; int err, res;
	extern int ftt_debug, ftt_errno;

	d = ftt_open(dev, 0);
	printf( "opened...\n");
	if (res < 0){
	  perror("ftt_open error \n");
	}
	printf("demo calling ftt_open_dev\n");
	res = ftt_open_dev(d);
	if (res < 0){
	  perror("ftt_open_dev error\n");
	  printf("RES %d\n", res);
	}
	
	printf( "open_dev... %d\n", res);
	//ftt_rewind(d);
	//printf( "rewound...\n");
	ftt_close(d);
	
}

int
main(int argc, char **argv) {
  ftt_debug=3;
  demo(argv[1]); 
  return 0;
}
