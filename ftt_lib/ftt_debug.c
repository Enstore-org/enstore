#include<stdio.h>

ftt_debug_dump(char *pc, int n){
	int i, j;

	if( 0 == pc ){
		fprintf(stderr,"(null)");
		return 0;
	}
	for (i = 0; i < n-16; i += 16) {
		for (j = 0; j < 16; j++) {
			fprintf(stderr,"%02x", pc[i+j] & 0xff );
		}
		putc('\t',stderr);
		for (j = 0; j < 16; j++) {
			putc( isprint(pc[i+j]) ? pc[i+j] : '.' , stderr);
		}
		putc('\n', stderr);
	}
	for ( j = 0; j < n - i ; j++ ) {
		fprintf(stderr,"%02x", pc[i+j] & 0xff );
	}
	for ( ; j < 16; j++ ) {
		fprintf(stderr,"  ");
	}
	putc('\t',stderr);
	for ( j = 0; j < n - i ; j++ ) {
		putc( isprint(pc[i+j]) ? pc[i+j] : '.' ,stderr );
	}
	putc('\n',stderr);
	fflush(stderr);
}
