/*
** Private data structurs for FTT internals
*/

/* device information structure */

typedef struct {		
	char *device_name;	/* pathname for device 		*/
	char density;		/* density code  		*/
	char mode;		/* compression, etc.		*/
	char hwdens;		/* hardware density code for (density,mode) */
	char rewind;		/* rewind on close, ret on open */
	char passthru;		/* scsi passthru device	        */
	char fixed;		/* fixed blocksize */
	char first;		/* first time this name appears in table */
} ftt_devinfo;

typedef struct {
	ftt_devinfo 	devinfo[32];		/* table of above */
	char 		*basename;		/* basename of device */
	char            *prod_id;		/* SCSI ID prefix */
	int 		**errortrans;		/* errno translation table */
	char 		readonly;		/* we were opened readonly */
	char 		current_valid;		/* see below */
	char 		unrecovered_error;	/* waiting for rewind... */
	long 		file_descriptor;	/* fd or scsi handle */
	long 		current_block;		/* postion on tape */
	long 		current_file;
	int 		max_async;		/* async level */
	int 		async_fd;		/* pipe fd for async ops */
	int 		async_pid;		/* proc id for async ops */
	int 		last_operation;		/* operation num last done */
	long		scsi_ops;		/* operation nums to passthru*/
	long 		flags;			/* other flags */
	long		readkb, readlo;		/* kb and remainder read */
	long		writekb, writelo;	/* kb and remainder written */
	char *		controller;		/* controller type */
	int 		which_is_open;		/* devinfo index open now */
	int		current_blocksize;	/* blocksize set now */
	int 		which_is_default;	/* devinfo index for open_dev*/
	int		default_blocksize;	/* blocksize for open_dev */
	int		data_direction;		/* are we reading/writing */
} ftt_descriptor_buf, *ftt_descriptor;

/* current values */
#define FTT_CURRENT_BLOCK_VALID		0x01
#define FTT_CURRENT_FILE_VALID		0x02

/* data directions */
#define FTT_DIR_READING 0
#define FTT_DIR_WRITING 1

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

/* flags values (system/device dependant) */
#define FTT_FLAG_FSF_AT_EOF	0x00000001	/* fsf to get past eof  */
#define FTT_FLAG_REOPEN_AT_EOF	0x00000002	/* reopen    "          */
#define FTT_FLAG_HOLD_SIGNALS	0x00000004	/* sighold reads/writes */
#define FTT_FLAG_REWIND_WAIT	0x00000010	/* rewind returns immed.*/
#define FTT_FLAG_REOPEN_R_W	0x00000020	/* reopen on r/w switch */
#define FTT_FLAG_ASYNC_REWIND	0x00000040	/* Async. rewind call 	*/
#define FTT_FLAG_SUID_SCSI	0x00000080	/* must be root to do scsi */

typedef struct {
	char *value[50];
} ftt_stat, *ftt_stat_buf;

/* internally used routines */
extern char *ftt_get_os();			/* get os release */

extern char *ftt_get_driveid(char *,char *);	/* find drive type given */
						/* os release & basename */
extern char *ftt_strip_to_basename(char *, char*);
extern char *ftt_get_controller(char *, char*);
extern int ftt_translate_error(ftt_descriptor , int, char *, int , char *); 

#define DEBUG1 (ftt_debug>=1)&&fprintf
#define DEBUG2 (ftt_debug>=2)&&fprintf
#define DEBUG3 (ftt_debug>=3)&&fprintf
#define DEBUGDUMP1 (ftt_debug>=1)&&ftt_debug_dump
#define DEBUGDUMP2 (ftt_debug>=2)&&ftt_debug_dump
#define DEBUGDUMP3 (ftt_debug>=3)&&ftt_debug_dump

typedef struct {
    char *os;
    char *drivid;
    long flags;
    long scsi_ops;
    int **errortrans;
    struct {
        char *string;
        char density;
        char mode;
	char hwdens;
        char passthru;
	char fixed;
        char rewind;
        char suffix;
	char first;
    } devs[64];
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
#define FTT_DO_LSRW   0x00000020   /* do log sense for read/write stats */
#define FTT_DO_LSC    0x00000080   /* do log sense for compression rate */
#define FTT_DO_05RS   0x00000100   /* EXABYTE 8x05 Request sense added bytes */
#define FTT_DO_DLTRS  0x00000200   /* DLT Request sense added bytes */
#define FTT_DO_TUR    0x00000400   /* do a test unit ready */
#define FTT_DO_RP     0x00000800   /* do a read position */

extern int ftt_write_fm_if_needed(ftt_descriptor);
extern int ftt_matches(char*, char*);
extern int ftt_do_scsi_command(ftt_descriptor, char *,unsigned char *, 
				int, unsigned char *, int, int, int);
extern int ftt_set_hwdens_blocksize(ftt_descriptor, int, int); 
