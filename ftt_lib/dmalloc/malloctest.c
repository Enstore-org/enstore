#include "debugmalloc.h"

main()
{
    char *ap[10];
    int test, i, j;

    for( test = 0; test < 10; test++ ) {
	for( i = 1; i < 10; i++ ) {
		ap[i] = malloc( 10 * i );
		bcopy ("012345678",ap[i],9);
	}
	if (test == 9) {
		bcopy( "0123456789abcdefg",ap[1],17);
	}
	for( i = 1; i < 10; i++ ) {
		free(ap[i]);
	}
    }
}
