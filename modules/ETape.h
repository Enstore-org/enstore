/*  This file (ETape.h) was created by Ron Rechenmacher <ron@fnal.gov> on
    May  1, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    */

/* this header is shared between ETape and EXfer */

#include <ftt.h>

typedef struct s_ETdesc
{
    ftt_descriptor	ftt_desc;
    char		*buffer;
    int			block_size;
    char		*bufptr;    /* buffer to build output blk - write only*/
    int			hadeof;	    /* seen an eof on this file - read only */
    long		bytes_xferred;   /* # of bytes */
} ET_descriptor;

