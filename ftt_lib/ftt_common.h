
/* statistics */
#define FTT_VENDOR_ID		0
#define FTT_PRODUCT_ID		1
#define FTT_FIRMWARE		2
#define FTT_SERIAL_NUM		3
#define FTT_HOURS_ON		4
#define FTT_CLEANING_BIT	5
#define FTT_READ_COUNT		6
#define FTT_WRITE_COUNT		7
#define FTT_READ_ERRORS		8
#define FTT_WRITE_ERRORS	9
#define FTT_FTT_DENSITY		10
#define FTT_READ_COMP		11
#define FTT_FILE_NUMBER		12
#define FTT_BLOCK_NUMBER	13
#define FTT_BOT			14
#define FTT_READY		15
#define FTT_WRITE_PROT		16
#define FTT_FMK			17
#define FTT_EOM			18
#define FTT_PEOT		19
#define FTT_MEDIA_TYPE		20
#define FTT_BLOCK_SIZE		21
#define FTT_BLOCK_TOTAL		22
#define FTT_TRANS_DENSITY	23
#define FTT_TRANS_COMPRESS	24
#define FTT_REMAIN_TAPE		25
#define FTT_USER_READ		26
#define FTT_USER_WRITE		27
#define FTT_CONTROLLER		28
#define FTT_DENSITY		29
#define FTT_ILI			30
#define FTT_SCSI_ASC		31
#define FTT_SCSI_ASCQ		32
#define FTT_PF			33
#define FTT_CLEANED_BIT	        34
#define FTT_WRITE_COMP		35
#define FTT_TRACK_RETRY		36
#define FTT_UNDERRUN		37
#define FTT_MOTION_HOURS	38
#define FTT_POWER_HOURS		39
#define FTT_TUR_STATUS		40
#define FTT_BLOC_LOC		41
#define FTT_COUNT_ORIGIN	42
#define FTT_N_READS		43
#define FTT_N_WRITES		44
#define FTT_MAX_STAT		45

extern int ftt_errno, ftt_debug;
extern char ftt_eprint_buf[];
extern char *ftt_stat_names[];
extern int ftt_numeric_tab[];
/*
** ftt entry points
*/

extern ftt_stat_buf	ftt_alloc_stat(void);
extern char *		ftt_avail_mode(ftt_descriptor, int, int, int);
extern char *		ftt_get_mode(ftt_descriptor, int *, int* mode, int *);
extern void 		ftt_add_stats(ftt_stat_buf,ftt_stat_buf,ftt_stat_buf);
extern int		ftt_chall(ftt_descriptor, int, int, int);
extern int 		ftt_check(ftt_descriptor);
extern int		ftt_close(ftt_descriptor);
extern int		ftt_close_dev(ftt_descriptor);
extern int 		ftt_describe_dev(ftt_descriptor,char*,FILE*);
extern int 		ftt_dump_stats(ftt_stat_buf, FILE*);
extern void		ftt_eprintf(char *, ...);
extern int		ftt_erase(ftt_descriptor);
extern char *		ftt_extract_stats(ftt_stat_buf, int n);
extern int		ftt_fork(ftt_descriptor);
extern int 		ftt_format_label(char*,int,char*,int,int);
extern int		ftt_free_stat(ftt_stat_buf);
extern char *		ftt_get_basename(ftt_descriptor d);
extern char *		ftt_get_error(int *);
extern int		ftt_get_mode_dev(ftt_descriptor,char*,int*,int*,int*,int*);
extern int		ftt_get_position(ftt_descriptor, int *, int *);
extern char *		ftt_get_scsi_devname(ftt_descriptor);
extern int		ftt_get_stats(ftt_descriptor, ftt_stat_buf);
extern int		ftt_guess_label(char *,int, char**, int *);
extern ftt_stat_buf *	ftt_init_stats(ftt_descriptor);
extern char **		ftt_list_all(ftt_descriptor d);
extern ftt_descriptor	ftt_open(char*, int);
extern int		ftt_open_dev(ftt_descriptor);
extern ftt_descriptor	ftt_open_logical(char*,char*,char*,int);
extern int		ftt_read(ftt_descriptor, char*, int);
extern void		ftt_report(ftt_descriptor);
extern int		ftt_retension(ftt_descriptor);
extern int		ftt_rewind(ftt_descriptor);
extern char * 		ftt_set_mode(ftt_descriptor, int density, int,  int );
extern int 		ftt_set_mode_dev(ftt_descriptor, char *, int , int );
extern int		ftt_skip_fm(ftt_descriptor, int);
extern int		ftt_skip_rec(ftt_descriptor, int);
extern int 		ftt_skip_to_double_fm(ftt_descriptor d);
extern int		ftt_status(ftt_descriptor,int);
extern void 		ftt_sub_stats(ftt_stat_buf,ftt_stat_buf,ftt_stat_buf);
extern int		ftt_unload(ftt_descriptor);
extern int 		ftt_update_stats(ftt_descriptor,ftt_stat_buf *);
extern int 		ftt_verify_vol_label(ftt_descriptor,int,char*,int,int);
extern int 		ftt_wait(ftt_descriptor);
extern int		ftt_write(ftt_descriptor, char*, int);
extern int 		ftt_write_vol_label(ftt_descriptor,int,char*);
extern int		ftt_writefm(ftt_descriptor);

/* rewind/retension/etc. flags */
#define FTT_RWOC 0x00000001	/* rewind on close */
#define FTT_RTOO 0x00000002	/* retension on open */
#define FTT_BTSW 0x00000004	/* byte swap on read/write */

/* error returns */

#define FTT_SUCCESS		 0
#define FTT_EPARTIALSTAT	 1
#define FTT_EUNRECOVERED	 2
#define FTT_ENOTAPE		 3
#define FTT_ENOTSUPPORTED 	 4
#define FTT_EPERM		 5
#define FTT_EFAULT		 6
#define FTT_ENOSPC		 7
#define FTT_ENOENT		 8
#define FTT_EIO			 9
#define FTT_EBLKSIZE		10
#define FTT_ENOEXEC		11
#define FTT_EBLANK		12
#define FTT_EBUSY		13		
#define FTT_ENODEV		14
#define FTT_ENXIO		15
#define FTT_ENFILE		16
#define FTT_EROFS		17
#define FTT_EPIPE		18
#define FTT_ERANGE		19
#define FTT_ENOMEM		20
#define FTT_ENOTTAPE		21
#define FTT_E2SMALL		22
#define FTT_ERWFS		23
#define FTT_EWRONGVOL		24
#define FTT_EWRONGVOLTYP	25
#define FTT_ELEADER		26
#define FTT_EFILEMARK		27

extern char *ftt_ascii_error[]; /* maps error numbers to their names */


/* ftt_status return bitflags */
#define FTT_ABOT	0x01
#define FTT_AFM		0x02
#define FTT_AEOT	0x04
#define FTT_AEW 	0x08
#define FTT_PROT	0x10
#define FTT_ONLINE	0x20
#define FTT_BUSY	0x40

/* header types */
#define FTT_ANSI_HEADER 	0
#define FTT_FMB_HEADER  	1
#define FTT_TAR_HEADER  	2
#define FTT_CPIO_HEADER 	3
#define FTT_UNKNOWN_HEADER 	4
#define FTT_BLANK_HEADER	5
#define FTT_DONTCHECK_HEADER	6

/* readonly falues */

#define FTT_RDWR   0
#define FTT_RDONLY 1
