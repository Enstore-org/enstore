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

void 		
ftt_set_maxparts(ftt_partbuf p, int n) {
   p->max_parts = n;
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

#include "ftt_dbd.h"

int		
ftt_get_partitions(ftt_descriptor d,ftt_partbuf p) {
    static unsigned char buf[BD_SIZE+136];
    static unsigned char cdb_modsen11[6] = {0x1a, DBD, 0x11, 0x00,140, 0x00};
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
		fclose(d->async_pf_parent);

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

	res = ftt_do_scsi_command(d,"Get Partition table", cdb_modsen11, 6, buf, BD_SIZE+136, 10, 0);
	if (res < 0) return res;
	p->max_parts = buf[BD_SIZE+2];
	p->n_parts = buf[BD_SIZE+3];
	for( i = 0 ; i <= p->n_parts; i++ ) {
	    p->partsizes[i] = pack(0,0,buf[BD_SIZE+8+2*i],buf[BD_SIZE+8+2*i+1]);
	}
	return 0;
   }
}


static unsigned char wp_buf[BD_SIZE+136];

int
ftt_part_util_get(ftt_descriptor d) {
    static unsigned char cdb_modsen11[6] = {0x1a, DBD, 0x11, 0x00,BD_SIZE+136, 0x00};
    return  ftt_do_scsi_command(d,"Get Partition table", cdb_modsen11, 6, wp_buf, BD_SIZE+136, 10, 0);
}

int ftt_part_util_set(ftt_descriptor d,  ftt_partbuf p ) {
    int res, i;
    int len;
    static unsigned char cdb_modsel[6] = {0x15, 0x10, 0x00, 0x00,BD_SIZE+136, 0x00};

    wp_buf[0] = 0;
    wp_buf[1] = 0;

    len = wp_buf[BD_SIZE+1] + BD_SIZE + 2;

    if ( len < BD_SIZE + 10 + 2 * p->n_parts ) {
	len =  BD_SIZE + 10 + 2 * p->n_parts;
	wp_buf[BD_SIZE + 1] = 8 + 2 * p->n_parts;
    }

    cdb_modsel[4] = len;

    DEBUG3(stderr,"Got length of %d\n", len);

    /* set number of partitions */
    wp_buf[BD_SIZE+3] = p->n_parts;

    /* set to write initiator defined partitions, in MB */
    wp_buf[BD_SIZE+4] = 0x20 | 0x10;

    /* fill in partition sizes... */
    for( i = 0 ; i <= p->n_parts; i++ ) {
	wp_buf[BD_SIZE+8 + 2*i + 0] = (p->partsizes[i] & 0xff00) >> 8;
	wp_buf[BD_SIZE+8 + 2*i + 1] = p->partsizes[i] & 0x00ff;
    }
    for( i = p->n_parts + 1 ; i <= p->max_parts; i++ ) {
	wp_buf[BD_SIZE+8 + 2*i + 0] = 0;
	wp_buf[BD_SIZE+8 + 2*i + 1] = 0;
    }
    res = ftt_do_scsi_command(d,"Put Partition table", cdb_modsel, 6, wp_buf, len, 3600, 1);
    return res;
}

int		
ftt_write_partitions(ftt_descriptor d,ftt_partbuf p) {
    int res, i;
    int len;
    int pd[2];
    FILE *topipe;


    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {

        res = pipe(pd); if (res < 0) return -1; 

	DEBUG2(stderr,"pipe is (%d,%d)\n", pd[0], pd[1]);
        fflush(stderr);
  
	ftt_close_dev(d);

	switch(ftt_fork(d)){
	case -1:
		return -1;

	case 0:  /* child */
		/* output -> async_pf */
		fflush(stdout);	/* make async_pf stdout */
		fflush(d->async_pf_parent);
		close(1);
		dup2(fileno(d->async_pf_parent),1);
                fclose(d->async_pf_parent);

		/* stdin <- pd[0] */
                fclose(stdin);
		close(pd[1]);
		dup2(pd[0],0);
                close(pd[0]);

		if (ftt_debug) {
		    execlp("ftt_suid", "ftt_suid", "-x",  "-u", d->basename, 0);
		} else {
		     execlp("ftt_suid", "ftt_suid", "-u", d->basename, 0);
		}
		break;

	default: /* parent */
		/* close the read end of the pipe... */
                close(pd[0]);

		/* send the child the partition data */
		topipe = fdopen(pd[1],"w");
		ftt_dump_partitions(p,topipe);
  		fclose(topipe);

		res = ftt_wait(d);
	}

    } else {
        res = ftt_part_util_get( d );
	if (res < 0) return res;
	res =  ftt_part_util_set(d, p);
	if (res < 0) return res;
    }
    return res;
}

int
ftt_cur_part(ftt_descriptor d) {
    static ftt_stat_buf b;
    
    if (0 == b) {
	b = ftt_alloc_stat();
    }
    ftt_get_stats(d,b);
    return atoi(ftt_extract_stats(b,FTT_CUR_PART));
}

int		
ftt_skip_part(ftt_descriptor d,int nparts) {
    int cur;

    cur = ftt_cur_part(d);
    cur += nparts;
    return ftt_locate_part(d, 0, cur);
}

int		
ftt_locate_part(ftt_descriptor d, int blockno, int part) {
    int cur;
    int res = 0;

    if ( blockno == 0 ) {
	d->current_block = 0;
	d->current_file = 0;
	d->current_valid = 1;
    } else {
	d->current_valid = 0;
    }
    d->data_direction = FTT_DIR_READING;
    d->last_pos = -1;   /* we skipped backwards, so this can't be valid */

    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
        static char buf1[10],buf2[10];

        sprintf(buf1,"%d",blockno);
        sprintf(buf2,"%d",part);

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
		    execlp("ftt_suid", "ftt_suid", "-x",  "-L", buf1, buf2, d->basename, 0);
		} else {
		     execlp("ftt_suid", "ftt_suid", "-L", buf1, buf2, d->basename, 0);
		}
		break;

	default: /* parent */
		res = ftt_wait(d);
	}

    } else {

	static unsigned char 
	    locate_cmd[10] = {0x2b,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};

	locate_cmd[1] = 0x02;
	locate_cmd[3] = (blockno >> 24) & 0xff;
	locate_cmd[4] = (blockno >> 16) & 0xff;
	locate_cmd[5] = (blockno >> 8)  & 0xff; 
	locate_cmd[6] = blockno & 0xff;
	locate_cmd[8] = part;

	res = ftt_do_scsi_command(d,"Locate",locate_cmd,10,NULL,0,300,0);
	res = ftt_describe_error(d,0,"a SCSI pass-through call", res,res,"Locate", 0);

    }
    return res;
}

/* shared printf formats for dump/undump */
char *curfmt = "Cur: %d\n";
char *maxfmt = "Max: %d\n";
char *parfmt = "P%d: %u MB\n";

ftt_dump_partitions(ftt_partbuf parttab, FILE *pf) {
    int i;

    fprintf(pf,"Partition table:\n");
    fprintf(pf,"================\n");
    fprintf(pf, curfmt, ftt_extract_nparts(parttab));
    fprintf(pf, maxfmt, ftt_extract_maxparts(parttab));
    for( i = 0; i <= parttab->n_parts; i++) {
	 fprintf(pf,parfmt, i, ftt_extract_part_size(parttab,i));
    }
    fflush( pf );
    return;
}

ftt_undump_partitions(ftt_partbuf p, FILE *pf) {
    static char buf[80];
    int i,junk;

    buf[0] = 'x';
    while (buf[0] != '=') {
	fgets(buf,80,pf);
	DEBUG2(stderr,"skipping line %s\n", buf);
    }
    fscanf(pf, curfmt, &(p->n_parts));
    DEBUG2(stderr,"got n_parts of %d\n", p->n_parts);
    fscanf(pf, maxfmt, &(p->max_parts));
    DEBUG2(stderr,"got max_parts of %d\n", p->max_parts);
    for( i = 0 ; i <= p->n_parts; i++ ) {
	fscanf(pf, parfmt, &junk, &(p->partsizes[i]));
    }
}

int		
ftt_set_mount_partition(ftt_descriptor d, int partno) {
    int res = 0;

    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
        static char buf2[10];

        sprintf(buf2,"%d",partno);

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
		    execlp("ftt_suid", "ftt_suid", "-x",  "-M", buf2, d->basename, 0);
		} else {
		     execlp("ftt_suid", "ftt_suid", "-M", buf2, d->basename, 0);
		}
		break;

	default: /* parent */
		res = ftt_wait(d);
	}

    } else {

	ftt_partbuf p;
	static unsigned char buf[BD_SIZE+6];
	static unsigned char cdb_modsense[6] = {0x1a, DBD, 0x21, 0x00, BD_SIZE+6, 0x00};
	static unsigned char cdb_modsel[6] = {0x15, 0x10, 0x00, 0x00, BD_SIZE+6, 0x00};
	int len;
	int max;

	/* get maximum number of partitions.. */
	p = ftt_alloc_parts();
	ftt_get_partitions(d,p);
	max = ftt_extract_maxparts(p);
	ftt_free_parts(p);

	/* -1 means highest supported partition */
	if ( partno < 0 || partno > max ) partno = max;

	res = ftt_do_scsi_command(d,"Mode Sense, 0x21", cdb_modsense, 6, buf, 10, 10, 0);
	if (res < 0) return res;

	buf[0] = 0;
	buf[1] = 0;

	len = buf[BD_SIZE+1] + BD_SIZE + 2;

	/* set load partition */
	buf[BD_SIZE+3] &= ~0x7e;
	buf[BD_SIZE+3] |= (partno << 1) & 0x7e;

	/* reserved fields */
	buf[BD_SIZE+2] = 0;
	buf[BD_SIZE+4] = 0;
	buf[BD_SIZE+5] = 0;

	res = ftt_do_scsi_command(d,"Mode Select, 0x21", cdb_modsel, 6, buf, len, 10, 1);
    }
    return res;
}


int
ftt_format_ait(ftt_descriptor d, int on, ftt_partition_table *pb) {

   int   res;
   int   i;

    static unsigned char
        mod_sen31[6] = {0x1a, 0x00, 0x31, 0x00, 0x16, 0x00 },
        mod_sel31[6] = {0x15, 0x10, 0x00, 0x00, 0x16, 0x00 },

        ait_conf_buf[4+8+10];


    ENTERING("ftt_format_ait");

    CKNULL("ftt_descriptor", d);
    DEBUG2(stderr, "Entering ftt_format_ait\n");
    res = 0;
    if ((d->flags&FTT_FLAG_SUID_SCSI) == 0 || 0 == geteuid()) {

        res = ftt_open_scsi_dev(d);
        if(res < 0) return res;

        res = ftt_part_util_get(d);
        if(res < 0) return res;
        
        /* get the AIT Device Configuration page 0x31 */
        DEBUG2(stderr, "CALLING ----- mod_sen31\n");
        res = ftt_do_scsi_command(d, "Mode Sense 0x31", mod_sen31, 6,
           ait_conf_buf, 4+8+10, 5, 0);
        if (res < 0) return res;

        /* switch device into native AIT mode */
        ait_conf_buf[0] = 0x00;                        /* reserved */
        ait_conf_buf[1] = 0x00;                        /* reserved */
        ait_conf_buf[2] = 0x7f;                        /* reserved */
        ait_conf_buf[5] = 0x00;                        /* reserved */
        ait_conf_buf[6] = 0x00;                        /* reserved */
        ait_conf_buf[7] = 0x00;                        /* reserved */
        ait_conf_buf[8] = 0x00;                        /* reserved */
        ait_conf_buf[4+8+0] &= 0x3f;                   /* reserved */
        if ( on ) {
          if ((ait_conf_buf[4+8+4] & 0x80) != 0 ) {
            /* volume has a MIC */
            ait_conf_buf[4+8+2] = 0xf3;               /* enable full AIT mode */
          } else {
            /* volume has no MIC */
            ait_conf_buf[4+8+2] = 0xc0;               /* enable AIT mode */
          }
          ait_conf_buf[4+8+4] &= 0x80;                /* reserved */
       } else {
          ait_conf_buf[4+8+2] &= ~0xf3;             /* disable AIT mode */
       }

        /* set the AIT Device Configuration page and switch format mode */
        DEBUG2(stderr, "CALLING ----- mod_sel31\n");
        res = ftt_do_scsi_command(d, "Mode Select 0x31", mod_sel31, 6,
           ait_conf_buf, 4+8+10, 180, 1);

        if(res < 0) return res;

	res =  ftt_part_util_set(d, pb);

    } else {
        int pd[2];
        FILE *topipe;
        pipe(pd);
        ftt_close_dev(d);
        ftt_close_scsi_dev(d);
	switch(ftt_fork(d)){
	static char s1[10];
	case -1:
		return -1;

	case 0:  /* child */
		/* output -> async_pf */
		fflush(stdout);	/* make async_pf stdout */
		fflush(d->async_pf_parent);
		close(1);
		dup2(fileno(d->async_pf_parent),1);
                fclose(d->async_pf_parent);

		/* stdin <- pd[0] */
                fclose(stdin);
		close(pd[1]);
		dup2(pd[0],0);
                close(pd[0]);

		sprintf(s1, "%d", on);

		if (ftt_debug) {
		    execlp("ftt_suid", "ftt_suid", "-x",  "-A", s1, d->basename, 0);
		} else {
		     execlp("ftt_suid", "ftt_suid", "-A", s1, d->basename, 0);
		}
		break;

	default: /* parent */
		/* close the read end of the pipe... */
                close(pd[0]);

		/* send the child the partition data */
		topipe = fdopen(pd[1],"w");
		ftt_dump_partitions(pb,topipe);
  		fclose(topipe);

		res = ftt_wait(d);
	}
    }
    return res;
}

