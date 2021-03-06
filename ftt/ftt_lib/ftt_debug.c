static char rcsid[] = "@(#)$Id$";

#include <stdio.h>
#include <ctype.h>

int
ftt_dump(FILE *pf, unsigned char *pc, int n, int do_offsets, int do_chars) {
    int i, j;

    if( 0 == pc ){
        fprintf(stderr,"0");
        return 0;
    }
    for (i = 0; i < n-16; i += 16) {

        if (do_offsets) {
            fprintf(pf, "%04x: ", i);
        }

        for (j = 0; j < 16; j++) {
            fprintf(pf,"%02x", pc[i+j] & 0xff );
        }

        if (do_chars) {
            putc('\t',pf);
            for (j = 0; j < 16; j++) {
                putc( isprint(pc[i+j]) ? pc[i+j] : '.' , pf);
            }
        }
        putc('\n', pf);
    }
    if (do_offsets) {
	fprintf(pf, "%04x: ", i);
    }
    for ( j = 0; j < n - i ; j++ ) {
        fprintf(pf,"%02x", pc[i+j] & 0xff );
    }
    if (do_chars) {
	for ( ; j < 16; j++ ) {
	    fprintf(pf,"  ");
	}
	putc('\t',pf);
	for ( j = 0; j < n - i ; j++ ) {
	    putc( isprint(pc[i+j]) ? pc[i+j] : '.' ,pf );
	}
    }
    putc('\n',pf);
    fflush(pf);
    return 0;
}

int
ftt_debug_dump(unsigned char *pc, int n) {
    return ftt_dump(stderr,pc,n,1,1);
}

