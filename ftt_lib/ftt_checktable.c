static char rcsid[] = "#(@)$Id$";
#include <stdio.h>
#include <unistd.h>
#include <ftt_private.h>

main() {
    ftt_check_table(stdout);
}

ftt_check_table(FILE *pf) {
    ftt_descriptor d1, d2;
    ftt_devinfo *pd;
    int i,j,k,l;
    int first_seen;

    fprintf( pf, "Stage 0: 'first' flag checks\n");

    for(ftt_first_supported(&i); d1 = ftt_next_supported(&i); ) {
	pd = d1->devinfo;
	for( k = 0; 0 != pd[k].device_name; k++ ) {
	    first_seen = 0;
	    for( l = 0; l < k ; l++ ) {
		if (0 == strcmp(pd[k].device_name, pd[l].device_name) 
				&& 1 == pd[l].first ) {
		    first_seen = 1;
		}
	    }
	    if (first_seen == 0 && ! d1->devinfo[k].first ) {
		fprintf(pf, 
		     "entry for os '%s' basename '%s' prod_id '%s' controller '%s':\n\
		      missing 'first' flag in slot %d, name '%s'\n",
		      d1->os, d1->basename, d1->prod_id,
		      d1->controller, k, pd[k].device_name);
		/* change it for now, so we don't keep warning about it */
		pd[k].first = 1;
	    }
	}
    }

    fprintf( pf, "Stage 1: obscured entry checks\n");

    for(ftt_first_supported(&i); d1 = ftt_next_supported(&i); ) {
        for(ftt_first_supported(&j); (d2 = ftt_next_supported(&j)) && j < i; ) {
	    if (0 == strncmp(d1->os,d2->os,strlen(d1->os)) &&
	      0 == strncmp(d1->prod_id,d2->prod_id,strlen(d1->prod_id)) &&
	      0 == strcmp(d1->basename,d2->basename)) {
		fprintf(pf, 
		     "entry for os '%s' basename '%s' prod_id '%s' controller '%s':\n\
obscures entry for os '%s' basename '%s' prod_id '%s' controller '%s':\n",
		      d1->os, d1->basename, d1->prod_id,
		      d2->os, d2->basename, d2->prod_id);
	    }
	}
    }
}
