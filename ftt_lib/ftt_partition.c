static char rcsid[] = "@(#)$Id$";
#include <ftt_private.h>

typedef struct { 
	int n_parts; 
	int max_parts; 
	long int partsizes[64]
} *ftt_partbuf;

ftt_partbuf 	
ftt_alloc_parts() {
     ftt_partbuf res;
     res = malloc(sizeof(*res));
     if (res != 0) {
            memset(res, sizeof(*res)),0);
     }
     return res;
}

void 		
ftt_free_parts(ftt_partbuf p) {
   free(p);
}

int 		
ftt_extract_nparts(partbuf p) {
   return p->nparts;
}

int 		
ftt_extract_maxparts(partbuf) {
   return p->maxparts;
}
long 		
ftt_extract_part_size(partbuf p,int n) {
    if ( n > p->nparts || n < 0) {
        ftt_eprintf("not that many partitions in buffer");
	ftt_errno = FTT_EFAULT;
        return -1;
    }
    return p->partsizes[n];
}

int 		
ftt_set_nparts(partbuf p,int n) {
    p->nparts = n;
}

int 		
ftt_set_part_size(partbuf p,int n,long sz) {
    if ( n > p->nparts || n < 0) {
        ftt_eprintf("not that many partitions in buffer");
	ftt_errno = FTT_EFAULT;
        return -1;
    }
    p->partsizes[n] = sz;
    return 0;
}

int		
ftt_get_partitions(ftt_descriptor d,partbuf p) {
    static unsigned char cdb_msnp11[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    res = ftt_do_scsi_command(d,"Get Partition table", cdb_msnp11, 6, 0, 0, 10, 0);
    if ( res < 0) return res;
    p->nparts = pack(...);
    p->maxparts = pack(...);
    for( i = 0 ; i < p->nparts; i++ ) {
        p->partsizes[i] = pack(...);
    }
    return 0;
}

int		
ftt_write_partitions(ftt_descriptor,partbuf);
    static unsigned char cdb_mslp11[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    cdb_msp11[3] = p->nparts;
    for( i = 0 ; i < p->nparts; i++ ) {
        cdb_msp11[x + 2*i + 0] = (p->partsizes[i] & 0xff00) >> 8;
        cdb_msp11[x + 2*i + 1] = p->partsizes[i] & 0x00ff
    }
    res = ftt_do_scsi_command(d,"Get Partition table", cdb_mslp11, 6, 0, 0, 10, 0);
    return res;
}

int		
ftt_skip_part(ftt_descriptor,nparts) {
 /* locate */
}
