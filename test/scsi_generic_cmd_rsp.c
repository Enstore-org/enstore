/*  Example program to demonstrate the generic SCSI interface
    For linux, do not forget about the "echo" commands to /proc/scsi/scsi
    documented in drivers/scsi/scsi.c.
        scsi log <all|none>
        scsi log <error|timeout|scan|mlqueue|mlcomplete|llqueue|llcomplete|hlqueue|hlcomplete|ioctl> <0-7>
	i.e.:
        echo "scsi log all" >/proc/scsi/scsi
        OR
	echo "scsi log scan 6" >/proc/scsi/scsi
 */



/* compile:
   cc -o scsi_generic_cmd_rsp scsi_generic_cmd_rsp.c
   */

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <scsi/sg.h>
#include <assert.h>		/* void assert (int expression); */
#include <stdlib.h>		/* atoi, rand */

#if 1
/*  Note, although print will not occur, parens IS valid "C" and the
    code does get executed; watch out for, for example, var++. */
# define PRINTF
#else
# define PRINTF printf
#endif

#define SCSI_OFF sizeof(struct sg_header)


/* process a complete SCSI cmd. Use the generic SCSI interface. */

static int
handle_SCSI_cmd(  int		fd	/* handle */
		, unsigned	cmd_len /* command length */
		, unsigned	in_size /* input data size */
		, unsigned char	*i_buff /* input buffer */
		, unsigned 	out_size/* output data size */
		, unsigned char *o_buff	/* output buffer */
		)
{
	int			status = 0;
	struct sg_header	*sg_hd;

    /* safety checks */
    if (!cmd_len) return -1;            /* need a cmd_len != 0 */
    if (!i_buff) return -2;             /* need an input buffer != NULL */
#ifdef SG_BIG_BUFF
    if (SCSI_OFF + cmd_len + in_size > SG_BIG_BUFF) return -3;
    if (SCSI_OFF + out_size > SG_BIG_BUFF) return -4;
#else
    if (SCSI_OFF + cmd_len + in_size > 4096) return -5;
    if (SCSI_OFF + out_size > 4096) return -6;
#endif

    if (!o_buff) out_size = 0;      /* no output buffer, no output size */

    /* generic SCSI device header construction */
    sg_hd = (struct sg_header *)i_buff;
    sg_hd->reply_len   = SCSI_OFF + out_size;
    sg_hd->twelve_byte = cmd_len == 12;

    /* send command */
    PRINTF( "i_buff[SCSI_OFF+1]=0x%02x\n", i_buff[SCSI_OFF+1] );
    status = write( fd, i_buff, SCSI_OFF + cmd_len + in_size );
    if ( status < 0 || status != SCSI_OFF + cmd_len + in_size ||
                         sg_hd->result )
    {   /* some error happened */
	fprintf( stderr, "write(generic) result = 0x%x cmd = 0x%x\n",
                      sg_hd->result, i_buff[SCSI_OFF] );
	perror("");
	return status;
    }

    /* XXX THIS SHOULD BE AN ERROR IF out_size IS NONE-ZERO! */
    if (!o_buff) o_buff = i_buff;       /* buffer pointer check */

    /* retrieve result */
    sg_hd->sense_buffer[0] = 0;
    status = read( fd, o_buff, SCSI_OFF+out_size );
    PRINTF(  "status=%d SCSI_OFF=%d sizeof(struct sg_header)=%d\n"
	   , status, SCSI_OFF, sizeof(struct sg_header) );
    PRINTF(  "sg.result=0x%02x sg.sense[0]=0x%02x\n"
	   , sg_hd->result, sg_hd->sense_buffer[0] );
    if (   status < 0 
	|| status != SCSI_OFF+out_size 
	|| sg_hd->result
	|| sg_hd->sense_buffer[0] )
    {   /* some error happened */
	fprintf(  stderr
		, "read(generic) status=0x%x read_req_size=%d "
		   "sg.result=0x%x sg.sense_buffer[0]=0x%02x cmd=0x%x\n"
		,  status, SCSI_OFF+out_size, sg_hd->result
		, sg_hd->sense_buffer[0]
		, i_buff[SCSI_OFF] );
	fprintf( stderr, "read(generic) sense(hex) "
		"%x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x\n",
		sg_hd->sense_buffer[0],         sg_hd->sense_buffer[1],
		sg_hd->sense_buffer[2],         sg_hd->sense_buffer[3],
		sg_hd->sense_buffer[4],         sg_hd->sense_buffer[5],
		sg_hd->sense_buffer[6],         sg_hd->sense_buffer[7],
		sg_hd->sense_buffer[8],         sg_hd->sense_buffer[9],
		sg_hd->sense_buffer[10],        sg_hd->sense_buffer[11],
		sg_hd->sense_buffer[12],        sg_hd->sense_buffer[13],
		sg_hd->sense_buffer[14],        sg_hd->sense_buffer[15]);
    }
    /* Look if we got what we expected to get */
    if (status == SCSI_OFF + out_size) status = 0; /* got them all */

    return status;  /* 0 means no error */
}




#define CMDLEN_MAX	10
#define REPLY_LEN_MIN	99	/* incase error - need romm for sense data */

char *USAGE="\
%s <generic_device> [loops [6cmdBytes [4moreBytes]]]\n\
0 loops means a lot\n";

int
main(  int	argc
     , char	*argv[] )
{
	unsigned char		*cmd_buffer=0;
	unsigned char		*rsp_buffer=0;
	unsigned char		cmdblk[CMDLEN_MAX];
	int			fd;           /* SCSI device/file descriptor */
	int			i;
	int			sts, loops=1;
	int			cmdlen,datalen,blk_mode;
	char			direction;
	

    /* argv[1]   is device
       argv[2]   is loops     (defaults to 1)
       argv[3-8] are
       argv[9-12]
    */
       
    if ((argc!=2) && (argc!=3) && !(argc>=9))
    {   printf( USAGE, basename(argv[0]) );
	return (1);
    }
    if (argc >= 3) loops = atoi( argv[2] );

    cmdblk[0] = (unsigned char)strtol(argv[3],NULL,0);
    if (cmdblk[0] & 0x60) cmdlen=10;
    else                  cmdlen=6;
    if ((cmdlen==10) && (argc<13))
    {   printf( "error: not enough arguments for scsi cmd 0x%02x\n", cmdblk[0] );
	printf( USAGE, basename(argv[0]) );
	return (1);
    }
    for (i=0; i<cmdlen; i++)
    {
#       ifdef DEBUG
	printf( "cmdblk%d = 0x%x\n", i, (unsigned)strtol(argv[3+i],NULL,0) );
#       endif
	cmdblk[i] = (unsigned char)strtol(argv[3+i],NULL,0);
    }

    /* defaults */
    direction = 'o';
    datalen = blk_mode = 0;
#   define D2_4(cc) (cc[2]<<16)|(cc[3]<<8)|cc[4]
#   define D3_4(cc) (cc[3]<<8)|cc[4]
#   define D4_4(cc) cc[4]
#   define D6_8(cc) (cc[6]<<16)|(cc[7]<<8)|cc[8]
#   define D7_8(cc) (cc[7]<<8)|cc[8]
    switch (cmdblk[0])
    {
    case 0x03: direction='i'; datalen=D4_4(cmdblk); break;
    case 0x04: direction='i'; datalen=6           ; break;
    case 0x08: direction='i'; datalen=D2_4(cmdblk); blk_mode=cmdblk[1]&1; break;
    case 0x0a: direction='o'; datalen=D2_4(cmdblk); blk_mode=cmdblk[1]&1; break;
    case 0x12: direction='i'; datalen=D4_4(cmdblk); break;
    case 0x15: direction='o'; datalen=D4_4(cmdblk); break;
    case 0x1a: direction='i'; datalen=D4_4(cmdblk); break;
    case 0x1c: direction='i'; datalen=D3_4(cmdblk); break;
    case 0x1d: direction='o'; datalen=D3_4(cmdblk); break;
    case 0x34: direction='i'; datalen=20;           break;
    case 0x3b: direction='o'; datalen=D6_8(cmdblk); break;
    case 0x3c: direction='i'; datalen=D6_8(cmdblk); break;
    case 0x4c: direction='o'; datalen=D7_8(cmdblk); break;
    case 0x4d: direction='i'; datalen=D7_8(cmdblk); break;
    case 0x5a: direction='i'; datalen=D7_8(cmdblk); break;
    }

#   ifdef DEBUG
    printf( "loops=%d cmdlen=%d", loops, cmdlen );
    if (datalen) printf( " direction=%c, datalen=0x%x\n", direction, datalen );
    else         printf( "\n" );
#   endif

    if ((fd=open(argv[1],O_RDWR)) < 0)
    {   printf( "Open failure\n" );
	return (1);
    }

    cmd_buffer = malloc( SCSI_OFF
			+cmdlen
			+((REPLY_LEN_MIN>(direction=='o'?datalen:0))
			  ?REPLY_LEN_MIN
			  :datalen) );
    memcpy( cmd_buffer+SCSI_OFF, cmdblk, cmdlen );
    if (direction == 'o')
    {   srand(1);			/* seed */
	for (i=0; i<datalen; i++)
	    cmd_buffer[SCSI_OFF+cmdlen+i] = (unsigned char)rand();
	if (argc > 3+cmdlen)
	    for (i=0; i<(argc-(3+cmdlen)); i++)
	    {    PRINTF(  "buffer%d = 0x%x\n"
			, i
			, (unsigned)strtol(argv[3+cmdlen+i],NULL,0) );
		 cmd_buffer[SCSI_OFF+cmdlen+i] = (unsigned char)strtol(argv[3+cmdlen+i],NULL,0);
	    }
    }
    else
    {   rsp_buffer = malloc( SCSI_OFF
			    +cmdlen
			    +((REPLY_LEN_MIN>datalen)?REPLY_LEN_MIN:datalen) );
    }

    do
    {   /*
	 * +------------------+
	 * | struct sg_header | <- cmd
	 * +------------------+
	 * | copy of cmdblk   | <- cmd + SCSI_OFF
	 * +------------------+
	 */
	sts = handle_SCSI_cmd(  fd, cmdlen
			      , (direction=='o'?datalen:0)
			      , cmd_buffer
			      , (direction=='i'?datalen:0)+REPLY_LEN_MIN
			      , rsp_buffer );
	PRINTF( "rsp_buffer=%p 0x%02x\n", rsp_buffer, cmd_buffer[SCSI_OFF+cmdlen] );
	if (sts)
	{   fprintf( stderr, "cmd failed; sts=%d\n", sts );
	    return (2);
	}

    } while ((sts==0) && --loops);

    if (direction == 'i')
    {
	for (i=0; i<datalen; i++)
	{   printf( "%2d: 0x%02x\n", i, rsp_buffer[SCSI_OFF+i] );
	}
    }
    return (sts);
}


