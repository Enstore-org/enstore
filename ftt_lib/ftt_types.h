/*
** Private data structurs for FTT internals
*/


#define FTT_EPRINT_BUF_SIZE 512
extern char ftt_eprint_buf[];

#define MAX_TRANS_ERRNO 50	/* maximum error number we translate */

#define MAX_TRANS_DENSITY 10	/* maximum density number we translate */

/* device information structure */
#define MAXDEVSLOTS 80

typedef struct {		
	char *device_name;	/* pathname for device 		*/
	short int density;	/* density code  		*/
	short int mode;		/* compression, etc.		*/
	short int hwdens;	/* hardware density code for (density,mode) */
	short int passthru;	/* scsi passthru device	        */
	short int fixed;	/* fixed blocksize */
	short int rewind;	/* rewind on close, ret on open */
	short int first;	/* first time this name appears in table */
	int  max_blocksize;	/* maximum blocksize allowed in this mode */
} ftt_devinfo;

typedef struct {
	ftt_devinfo 	devinfo[MAXDEVSLOTS];	/* table of above */
	char 		*basename;		/* basename of device */
	char            *prod_id;		/* SCSI ID prefix */
	int 		**errortrans;		/* errno translation table */
	char		**densitytrans;		/* density names */
	char 		readonly;		/* we were opened readonly */
	char 		unrecovered_error;	/* waiting for rewind... */
	int 		file_descriptor;	/* fd or scsi handle */
	char 		current_valid;		/* see below */
	long 		current_block;		/* postion on tape */
	long 		current_file;
	FILE * 		async_pf_parent;	/* pipe fd for async ops */
	FILE * 		async_pf_child;		/* pipe fd for async ops */
	int 		async_pid;		/* proc id for async ops */
	int 		last_operation;		/* operation num last done */
	long		scsi_ops;		/* operation nums to passthru*/
	long 		flags;			/* other flags */
	long		readkb, readlo;		/* kb and remainder read */
	long		writekb, writelo;	/* kb and remainder written */
	char *		controller;		/* controller type */
	int 		which_is_open;		/* devinfo index open now */
	int 		which_is_default;	/* devinfo index for open_dev*/
	int		default_blocksize;	/* blocksize for open_dev */
	int		current_blocksize;	/* blocksize for open_dev */
	int		density_is_set;		/* we already set density */
	int		data_direction;		/* are we reading/writing */
	int		nreads, nwrites;	/* operation counts */
	scsi_handle     scsi_descriptor;	/* descriptor feild */
	int 		last_pos;		/* have we moved data */
	char *		os;			/* operating system */
	int		nretries, nfailretries;	/* retried reads/writes */
	int		nresets;		/* unexpected BOT's */
	int		nharderrors;		/* unrecovered r/w errors */
} ftt_descriptor_buf, *ftt_descriptor;


/* operation flags for last_operation, scsi_ops */
#define FTT_OPN_READ		 1
#define FTT_OPN_WRITE		 2
#define FTT_OPN_WRITEFM		 3
#define FTT_OPN_SKIPREC		 4
#define FTT_OPN_SKIPFM		 5
#define FTT_OPN_REWIND		 6
#define FTT_OPN_UNLOAD		 7
#define FTT_OPN_RETENSION	 8
#define FTT_OPN_ERASE		 9
#define FTT_OPN_STATUS		10
#define FTT_OPN_GET_STATUS	11
#define FTT_OPN_ASYNC 		12 
#define FTT_OPN_PASSTHRU        13
#define FTT_OPN_CHALL           14
#define FTT_OPN_OPEN            15
#define FTT_OPN_RSKIPREC	16
#define FTT_OPN_RSKIPFM		17
#define FTT_OPN_SETDENSITY	18
#define FTT_OPN_SETCOMPRESSION	19

/* operation masks */
#define FTT_OP_READ 		(1 <<  FTT_OPN_READ )
#define FTT_OP_WRITE 		(1 <<  FTT_OPN_WRITE )
#define FTT_OP_WRITEFM 		(1 <<  FTT_OPN_WRITEFM )
#define FTT_OP_SKIPREC 		(1 <<  FTT_OPN_SKIPREC )
#define FTT_OP_SKIPFM 		(1 <<  FTT_OPN_SKIPFM )
#define FTT_OP_REWIND 		(1 <<  FTT_OPN_REWIND )
#define FTT_OP_UNLOAD 		(1 <<  FTT_OPN_UNLOAD )
#define FTT_OP_RETENSION 	(1 <<  FTT_OPN_RETENSION )
#define FTT_OP_ERASE 		(1 <<  FTT_OPN_ERASE )
#define FTT_OP_STATUS 		(1 <<  FTT_OPN_STATUS )
#define FTT_OP_GET_STATUS 	(1 <<  FTT_OPN_GET_STATUS )
#define FTT_OP_ASYNC 		(1 <<  FTT_OPN_ASYNC )
#define FTT_OP_PASSTHRU      	(1 <<  FTT_OPN_PASSTHRU )
#define FTT_OP_CHALL           	(1 <<  FTT_OPN_CHALL )
#define FTT_OP_OPEN            	(1 <<  FTT_OPN_OPEN )
#define FTT_OP_RSKIPREC		(1 <<  FTT_OPN_RSKIPREC)
#define FTT_OP_RSKIPFM		(1 <<  FTT_OPN_RSKIPFM)
#define FTT_OP_SETDENSITY	(1 <<  FTT_OPN_SETDENSITY)
#define FTT_OP_SETCOMPRESSION	(1 <<  FTT_OPN_SETCOMPRESSION)

/* flags values (system/device dependant) */
#define FTT_FLAG_FSF_AT_EOF	0x00000001 /* fsf to get past eof  */
#define FTT_FLAG_REOPEN_AT_EOF	0x00000002 /* reopen    "          */
#define FTT_FLAG_HOLD_SIGNALS	0x00000004 /* sighold reads/writes */
#define FTT_FLAG_REOPEN_R_W	0x00000008 /* reopen on r/w switch */
#define FTT_FLAG_SUID_SCSI	0x00000010 /* must be root to do scsi */
#define FTT_FLAG_CHK_BOT_AT_FMK	0x00000020 /* check for reset/rewinds */
#define FTT_FLAG_BSIZE_AFTER	0x00000040 /* set blocksize after open */
#define FTT_FLAG_VERIFY_EOFS	0x00000080 /* check whether EOF is EOT */
#define FTT_FLAG_SUID_DRIVEID	0x00000100 /* need root to get driveid */
#define FTT_FLAG_MODE_AFTER     0x00000200 /* set mode After dev is opened */
#define FTT_FLAG_NO_DENSITY	0x00000400 /* dont actually set density */

typedef struct {
	char *value[FTT_MAX_STAT];
} ftt_stat, *ftt_stat_buf;

/* internally used routines */
extern char *ftt_get_os(void);			/* get os release */

extern char *ftt_get_driveid(char *,char *);	/* find drive type given */
						/* os release & basename */
extern char *ftt_strip_to_basename(const char *, char*);
extern int ftt_translate_error(ftt_descriptor , int, char *, int , char *, int); 

typedef struct {
    char *os;			/* OS+Version (i.e. IRIX+5.3) string */
    char *drivid;		/* SCSI Drive-id prefix */
    char *controller;		/* controller name string */
    long flags;			/* FTT_FLAG_XXX bits for behavior */
    long scsi_ops;		/* FTT_OP_XXX bits for ops to use SCSI */
    int **errortrans;		/* errortrans[FTT_OPN_XXX][errno]->ftt_errno */
    char **densitytrans;	/* density names */
    char *baseconv_in;		/* basename parser scanf string */
    char *baseconv_out;		/* basename parser scanf string */
    int nconv;			/* number of items scanf should return */
    char *drividcmd;		/* printf this to get shell command->driveid */
    ftt_devinfo devs[MAXDEVSLOTS]; /* drive specs with printf strings */
} ftt_dev_entry;

extern ftt_dev_entry devtable[];

typedef struct {
	char 		*name;			/* SCSI  drive id 	*/
	long		stat_ops;		/* statisics to get 	*/
} ftt_stat_entry;

extern ftt_stat_entry ftt_stat_op_tab[];

/* stat_ops flags */
#define FTT_DO_INQ    0x00000001   /* do basic scsi inquiry */
#define FTT_DO_SN     0x00000002   /* do inquiry with serial number page */
#define FTT_DO_MS     0x00000004   /* do basic modes sense */
#define FTT_DO_RS     0x00000008   /* do basic request sense */
#define FTT_DO_EXBRS  0x00000010   /* do Exabyte vendor specifics */
#define FTT_DO_LS     0x00000020   /* do log sense for read/write stats */
#define FTT_DO_VSRS   0x00000040   /* to request sense for vendor specific stuff */
#define FTT_DO_05RS   0x00000100   /* EXABYTE 8x05 Request sense added bytes */
#define FTT_DO_DLTRS  0x00000200   /* DLT Request sense added bytes */
#define FTT_DO_TUR    0x00000400   /* do a test unit ready */
#define FTT_DO_RP     0x00000800   /* do a read position */
#define FTT_DO_RP_SOMETIMES 	0x00001000 /* do a read position,okay if fails*/
#define FTT_DO_MS_Px10     	0x00002000 /* do a ModeSense p.0x10 */
#define FTT_DO_MS_Px20_EXB	0x00004000 /* do a ModeSense p.0x20(EXB) */
#define FTT_DO_EXB82FUDGE       0x00008000 /* fudge read/write counts with remain tape */
#define FTT_DO_MS_Px0f     	0x00010000 /* do a ModeSense p.0x0f to set density */
#define FTT_DO_MS_Px21     	0x00020000 /* do a ModeSense p.0x10 */

extern int ftt_write_fm_if_needed(ftt_descriptor);
extern int ftt_matches(const char*, const char*);
extern int ftt_do_scsi_command(ftt_descriptor, char *,unsigned char *, 
				int, unsigned char *, int, int, int);
extern int ftt_set_hwdens(ftt_descriptor, int); 
extern int ftt_set_compression(ftt_descriptor, int); 
extern int ftt_set_blocksize(ftt_descriptor, int); 
extern int ftt_get_hwdens(ftt_descriptor, char*); 
extern int ftt_findslot(char*, char*, char*, void*, void*, void*);
extern void ftt_set_transfer_length(unsigned char *, int);
extern int ftt_skip_fm_internal(ftt_descriptor, int);
extern int ftt_open_scsi_dev(ftt_descriptor d);
extern int ftt_close_scsi_dev(ftt_descriptor d);
extern char *ftt_find_last_part(char*);

