#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include "ftt_private.h"
#ifndef  _FTT_PARTION_H
#define _FTT_PARTION_H

ftt_partbuf  ftt_alloc_parts();
void ftt_free_parts(ftt_partbuf p) ;
int ftt_extract_nparts(ftt_partbuf p);
void   ftt_set_maxparts(ftt_partbuf p, int n);
int   ftt_extract_maxparts(ftt_partbuf p); 
long   ftt_extract_part_size(ftt_partbuf p,int n); 
int   ftt_set_nparts(ftt_partbuf p,int n);
int   ftt_set_part_size(ftt_partbuf p,int n,long sz);
int ftt_part_util_get(ftt_descriptor d);
int ftt_part_util_set(ftt_descriptor d,  ftt_partbuf p );
int  ftt_get_partitions(ftt_descriptor d,ftt_partbuf p);
int  ftt_write_partitions(ftt_descriptor d,ftt_partbuf p);
int ftt_cur_part(ftt_descriptor d);
int  ftt_skip_part(ftt_descriptor d,int nparts);
int  ftt_locate_part(ftt_descriptor d, int blockno, int part);
void ftt_dump_partitions(ftt_partbuf parttab, FILE *pf);
void ftt_undump_partitions(ftt_partbuf p, FILE *pf);
int  ftt_set_mount_partition(ftt_descriptor d, int partno);
int ftt_format_ait(ftt_descriptor d, int on, ftt_partbuf pb);

#endif
