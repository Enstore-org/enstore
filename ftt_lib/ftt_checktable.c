static char rcsid[] = "@(#)$Id$";

#include <stdio.h>
#include <ftt_private.h>
#include <string.h>

void ftt_check_table();

static int table_debug;
int
main() {
   
    table_debug = 0;
    ftt_check_table(stdout);
	return 0;
}

void ftt_check_table(FILE *pf) {
    ftt_descriptor d1, d2;
    ftt_dev_entry *dt;
    ftt_devinfo *pd;
    int i,j,k,l;
    int first_seen;
    extern int devtable_size;

    fprintf( pf, "Stage 0: whole table reachable check\n");

    for(ftt_first_supported(&i); (d1 = ftt_next_supported(&i)) ; ) {
	if (table_debug) fprintf(pf, 
	     "DEBUG: os '%s' basename '%s' prod_id '%s' controller '%s':\n",
		      d1->os, d1->basename, d1->prod_id, d1->controller);
    }
    if (i < (int)(devtable_size / sizeof(ftt_dev_entry) - 1)) {
	fprintf(pf, "Premature end of table scan, slot %d of %d/%d\n", 
		i, devtable_size, sizeof(ftt_dev_entry));
    }

    fprintf( pf, "Stage 1: 'first' flag checks\n");

    for(ftt_first_supported(&i); (d1 = ftt_next_supported(&i)) ; ) {
	if (table_debug) fprintf(pf, 
	     "DEBUG: os '%s' basename '%s' prod_id '%s' controller '%s':\n",
		      d1->os, d1->basename, d1->prod_id, d1->controller);
	pd = d1->devinfo;
	for( k = 0; 0 != pd[k].device_name; k++ ) {
	    first_seen = 0;
	    for( l = 0; l < k ; l++ ) {
		if (0 == strcmp(pd[k].device_name, pd[l].device_name) 
				&& 1 == pd[l].first ) {
		    if (first_seen) {
			/* already saw a first flag for this name */
			fprintf(pf, 
			      "entry for os '%s' basename '%s' prod_id '%s' controller '%s':\n\
			       extra 'first' flag in slot %d, name '%s'\n",
			       d1->os, d1->basename, d1->prod_id,
			       d1->controller, l, pd[l].device_name);
			/* change it for now, so we don't keep warning about it */
			pd[l].first = 0;
		    }
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

    fprintf( pf, "Stage 2: obscured entry checks\n");

    for(ftt_first_supported(&i); (d1 = ftt_next_supported(&i) ); ) {
	if (table_debug) fprintf(pf, 
	     "DEBUG: os '%s' basename '%s' prod_id '%s' controller '%s':\n",
		      d1->os, d1->basename, d1->prod_id, d1->controller);
        for(ftt_first_supported(&j); (d2 = ftt_next_supported(&j)) && j < i; ) {
	    if (0 == strncmp(d1->os,d2->os,strlen(d1->os)) &&
	      0 == strncmp(d1->prod_id,d2->prod_id,strlen(d2->prod_id)) &&
	      0 == strcmp(d1->basename,d2->basename)) {
		fprintf(pf, 
		     "entry for os '%s' basename '%s' prod_id '%s' controller '%s':\n\
obscures entry for os '%s' basename '%s' prod_id '%s' controller '%s':\n",
		      d1->os, d1->basename, d1->prod_id, d1->controller,
		      d2->os, d2->basename, d2->prod_id, d1->controller);
	    }
	}
    }


    fprintf( pf, "Stage 3: format string, etc. checks\n");

    for(dt = devtable; dt->os != 0; dt++ ) {
	int slashes, prcnt, prcnts, prcntd;
	char *pc;

	if (table_debug) fprintf(pf, 
	     "DEBUG: os '%s' baseconv_out '%s' drivid '%s' controller '%s':\n",
		      dt->os, dt->baseconv_out, dt->drivid, dt->controller);

	if (dt->errortrans == 0 ) {
	     fprintf(pf, 
	         "missing errortrans: os '%s' baseconv_out '%s' drivid '%s' controller '%s':\n",
		      dt->os, dt->baseconv_out, dt->drivid, dt->controller);
	}
	if (dt->densitytrans == 0 ) {
	     fprintf(pf, 
	         "missing densitytrans: os '%s' baseconv_out '%s' drivid '%s' controller '%s':\n",
		      dt->os, dt->baseconv_out, dt->drivid, dt->controller);
	}

	pd = dt->devs;
	for( k = 0; 0 != pd[k].device_name; k++ ) {
	    if (table_debug) fprintf(pf, "devslot %d, name %s\n", k, pd[k].device_name);
	    slashes = 0;
	    prcnt = 0;
	    prcnts = 0;
	    prcntd = 0;
	    for (pc = pd[k].device_name; 0 != pc && 0 != *pc ; pc++ ) {
		if ('/' == pc[0] ) {
		    slashes++;
		}
		if ('%' == pc[0] ) {
		    prcnt++;
		    if ('%' == pc[1]) { /* "%%" doesnt count */
			 prcnt--;
		    }
		    if ('s' == pc[1]) {
			 prcnts++;
		    }
		    if ('d' == pc[1]) {
			 prcntd++;
		    }
		}
	    }
	    if (slashes > 1) {
		fprintf(pf, 
		     "entry for os '%s' baseconv_out '%s' drivid '%s' controller '%s':\n\
		      too many '/' characters in format slot %d, name '%s'\n",
		      dt->os, dt->baseconv_out, dt->drivid,
		      dt->controller, k, pd[k].device_name);
	    }
	    if (prcnt > 2) {
		fprintf(pf, 
		     "entry for os '%s' baseconv_out '%s' drivid '%s' controller '%s':\n\
		      too many '%%' characters in format slot %d, name '%s'\n",
		      dt->os, dt->baseconv_out, dt->drivid,
		      dt->controller, k, pd[k].device_name);
	    }
	    if (prcnts > 1) {
		fprintf(pf, 
		     "entry for os '%s' baseconv_out '%s' drivid '%s' controller '%s':\n\
		      too many '%%s' specs in format slot %d, name '%s'\n",
		      dt->os, dt->baseconv_out, dt->drivid,
		      dt->controller, k, pd[k].device_name);
	    }
	    if (prcntd > 2) {
		fprintf(pf, 
		     "entry for os '%s' baseconv_out '%s' drivid '%s' controller '%s':\n\
		      too many '%%d' specs in format slot %d, name '%s'\n",
		      dt->os, dt->baseconv_out, dt->drivid,
		      dt->controller, k, pd[k].device_name);
	    }
	}
    }
}
