static char rcsid[] = "@(#)$Id$";
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ftt_private.h"

ftt_partbuf 	
ftt_alloc_parts() {
     ftt_partbuf res;
     res = malloc(sizeof(*res));
     if (res != 0) {
            memset(res, sizeof(*res),0);
     }
     return res;
}

void 		
ftt_free_parts(ftt_partbuf p) {
   free(p);
}

int 		
ftt_extract_nparts(ftt_partbuf p) {
   return p->n_parts;
}

int 		
ftt_extract_maxparts(ftt_partbuf p) {
   return p->max_parts;
}

long 		
ftt_extract_part_size(ftt_partbuf p,int n) {
    if ( n > p->n_parts || n < 0) {
        ftt_eprintf("not that many partitions in buffer");
	ftt_errno = FTT_EFAULT;
        return -1;
    }
    return p->partsizes[n];
}

int 		
ftt_set_nparts(ftt_partbuf p,int n) {
    if ( n <= p->max_parts) {
	p->n_parts = n;
        return 0;
    } else {
        return -1;
    }
}

int 		
ftt_set_part_size(ftt_partbuf p,int n,long sz) {
    if ( n > p->n_parts || n < 0) {
        ftt_eprintf("not that many partitions in buffer");
	ftt_errno = FTT_EFAULT;
        return -1;
    }
    p->partsizes[n] = sz;
    return 0;
}
#define pack(a,b,c,d) \
     (((unsigned long)(a)<<24) + ((unsigned long)(b)<<16) + ((unsigned long)(c)<<8) + (unsigned long)(d))


int		
ftt_get_partitions(ftt_descriptor d,ftt_partbuf p) {
    static char buf[136];
    static unsigned char cdb_modsen11[6] = {0x1a, 0x08, 0x11, 0x00,140, 0x00};
    int res;
    int i;

    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
	ftt_close_dev(d);
	switch(ftt_fork(d)){
	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		fflush(d->async_pf_parent);
		close(1);
		dup2(fileno(d->async_pf_parent),1);
		if (ftt_debug) {
		    execlp("ftt_suid", "ftt_suid", "-x",  "-p", d->basename, 0);
		} else {
		     execlp("ftt_suid", "ftt_suid", "-p", d->basename, 0);
		}
		break;

	default: /* parent */
		ftt_undump_partitions(p,d->async_pf_child);
		res = ftt_wait(d);
	}

    } else {

	res = ftt_do_scsi_command(d,"Get Partition table", cdb_modsen11, 6, buf, 140, 10, 0);
	if (res < 0) return res;
	p->n_parts = buf[4+3];
	p->max_parts = buf[4+2];
	for( i = 0 ; i <= p->n_parts; i++ ) {
	    p->partsizes[i] = pack(0,0,buf[4+8+2*i],buf[4+8+2*i+1]);
	}
	return 0;
   }
}

int		
ftt_write_partitions(ftt_descriptor d,ftt_partbuf p) {
    static unsigned char buf[140];
    static unsigned char cdb_modsen11[6] = {0x1a, 0x08, 0x11, 0x00,140, 0x00};
    static unsigned char cdb_modsel[6] = {0x15, 0x10, 0x00, 0x00,140, 0x00};
    int res, i;
    int len;

    /*  
    ** note that we use async_pf_{parent,child} backwards here
    ** because we are blowing data down to the child...
    */
    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
	ftt_close_dev(d);
	switch(ftt_fork(d)){
	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdin);	/* make async_pf stdin */
		fflush(d->async_pf_parent);
		close(0);
		dup2(fileno(d->async_pf_child),0);
		if (ftt_debug) {
		    execlp("ftt_suid", "ftt_suid", "-x",  "-u", d->basename, 0);
		} else {
		     execlp("ftt_suid", "ftt_suid", "-u", d->basename, 0);
		}
		break;

	default: /* parent */
		ftt_dump_partitions(p,d->async_pf_parent);
		res = ftt_wait(d);
	}

    } else {
	res = ftt_do_scsi_command(d,"Get Partition table", cdb_modsen11, 6, buf, 140, 10, 0);
	if (res < 0) return res;

	buf[0] = 0;
	buf[1] = 0;

	len = buf[4+1] + 6;
	/* set number of partitions */
	buf[4+3] = p->n_parts;

	/* set to write initiator defined partitions, in MB */
	buf[4+4] = 0x20 | 0x10;

	/* fill in partition sizes... */
	for( i = 0 ; i <= p->n_parts; i++ ) {
	    buf[4+8 + 2*i + 0] = (p->partsizes[i] & 0xff00) >> 8;
	    buf[4+8 + 2*i + 1] = p->partsizes[i] & 0x00ff;
	}
	res = ftt_do_scsi_command(d,"Put Partition table", cdb_modsel, 6, buf, len, 3600, 1);
	return res;
    }
}

int
ftt_cur_part(ftt_descriptor d) {
    int res;
    static unsigned char buf[20];
    static unsigned char cdb_read_position[]= {0x34, 0x00, 0x00, 0x00, 0x00,
					    0x00, 0x00, 0x00, 0x00, 0x00};

    res = ftt_do_scsi_command(d,"Read Position", cdb_read_position, 10, 
				  buf, 20, 10, 0);
	
    if (res < 0) {
        return -1;
    } else {
        return buf[1];
    }
}

int		
ftt_skip_part(ftt_descriptor d,int nparts) {
    int cur;
    int res = 0;
    static unsigned char 
        locate_cmd[10] = {0x2b,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};

    cur = ftt_cur_part(d);
    cur += nparts;
    locate_cmd[1] = 0x02;
    locate_cmd[8] = cur;
    res = ftt_do_scsi_command(d,"Locate",locate_cmd,10,NULL,0,60,0);
    res = ftt_describe_error(d,0,"a SCSI pass-through call", res,"Locate", 0);

    return res;
}
int		
ftt_locate_part(ftt_descriptor d, int blockno, int part) {
    int cur;
    int res = 0;
    static unsigned char 
        locate_cmd[10] = {0x2b,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};

    locate_cmd[1] = 0x02;
    locate_cmd[3] = (blockno >> 24) & 0xff;
    locate_cmd[4] = (blockno >> 16) & 0xff;
    locate_cmd[5] = (blockno >> 8)  & 0xff; 
    locate_cmd[6] = blockno & 0xff;
    locate_cmd[8] = part;

    res = ftt_do_scsi_command(d,"Locate",locate_cmd,10,NULL,0,60,0);
    res = ftt_describe_error(d,0,"a SCSI pass-through call", res,"Locate", 0);

    return res;
}

/* shared printf formats for dump/undump */
char *curfmt = "Cur: %d\n";
char *maxfmt = "Max: %d\n";
char *parfmt = "P%d: %d MB\n";

ftt_dump_partitions(ftt_partbuf parttab, FILE *pf) {
    int i;

    fprintf(pf,"Partition table:\n");
    fprintf(pf,"================\n");
    fprintf(pf, curfmt, ftt_extract_nparts(parttab));
    fprintf(pf, maxfmt, ftt_extract_maxparts(parttab));
    for( i = 0; i <= parttab->n_parts; i++) {
	 fprintf(pf,parfmt, i, ftt_extract_part_size(parttab,i));
    }
    return;
}

ftt_undump_partitions(ftt_partbuf p, FILE *pf) {
    char buf[80];
    int i,junk;

    fgets(buf,80,pf);
    fgets(buf,80,pf);
    fscanf(pf, curfmt, &(p->n_parts));
    fscanf(pf, maxfmt, &(p->max_parts));
    for( i = 0 ; i < p->n_parts; i++ ) {
	fscanf(pf, parfmt, &junk, &(p->partsizes[i]));
    }
}
