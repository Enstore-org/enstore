
/* ftt_defines.h
**
** this file contains type definitions, prototypes, and #defines that
** are visible to the end user and within the library.
*/

/* statistics 
**
** If you add or change statistics, you MUST also update:
** -- ftt_numeric_tab[] in ftt_stats.c
** -- ftt_stat_names[] in ftt_higher.c
** so that ftt_dump_stats(), ftt_undump_stats(), etc. know how
** to print 'em.
*/
extern char *ftt_stat_names[];	/* ascii strings names of each stat */
extern int ftt_numeric_tab[];	/* table for ftt_{add,sub}_stats */

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
#define FTT_TNP			45
#define FTT_SENSE_KEY		46
#define FTT_TRANS_SENSE_KEY	47
#define FTT_RETRIES		48
#define FTT_FAIL_RETRIES	49
#define FTT_RESETS		50
#define FTT_HARD_ERRORS		51
#define FTT_MAX_STAT		52

extern int ftt_errno;
extern int ftt_debug;
/* debug levels
**
** you can set ftt_debug to these levels to get assorted stuff spewed
** to stderr.  Low mostly prints "Entering xyz"  when entering
** subroutines. MED prints out at various points, HI prints inside
** loops, if/else sub-statements, etc.
*/
#define FTT_DEBUG_NONE  0
#define FTT_DEBUG_LOW   1
#define FTT_DEBUG_MED   2
#define FTT_DEBUG_HI	3

/* rewind/retension/etc. flags 
**
** If you add/change these, you need to update
** -- ftt_ascii_rewindflags[] in ftt_higher.c
** so ftt_describe_devs() knows how to print 'em out.
*/
extern char *ftt_ascii_rewindflags[];

#define FTT_RWOC 0x00000001	/* rewind on close */
#define FTT_RTOO 0x00000002	/* retension on open */
#define FTT_BTSW 0x00000004	/* byte swap on read/write */
#define FTT_RDNW 0x00000008	/* we can read this density, but not write it */


/* error returns 
**
** If you add or change error numbers, you MUST also update
** -- ftt_ascii_error[] in ftt_error.c
** -- messages[] in ftt_error.c
** -- any affected error tranlation tables in ftt_tables.c
*/
extern char *ftt_ascii_error[]; /* maps error numbers to their names */

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
#define FTT_ELOST		28
#define FTT_ENOTBOT		29
#define FTT_EBOT        30
#define FTT_EEOM        31


/* ftt_status return bitflags 
*/
#define FTT_ABOT	0x01
#define FTT_AEOT	0x04
#define FTT_AEW 	0x08
#define FTT_PROT	0x10
#define FTT_ONLINE	0x20
#define FTT_BUSY	0x40

/* header types 
** if you add/change these, you need to update
** -- ftt_label_type_names[] in ftt_higher.c
** so that it can print reasonable error messages.
*/
extern char *ftt_label_type_names[];
#define FTT_ANSI_HEADER 	0
#define FTT_FMB_HEADER  	1
#define FTT_TAR_HEADER  	2
#define FTT_CPIO_HEADER 	3
#define FTT_UNKNOWN_HEADER 	4
#define FTT_BLANK_HEADER	5
#define FTT_DONTCHECK_HEADER	6
#define FTT_MAX_HEADER		7

/* readonly Values 
*/

#define FTT_RDWR   0
#define FTT_RDONLY 1


/* compression Values 
*/
#define FTT_UNCOMPRESSED 0
#define FTT_COMPRESSED   1

/* data directions
*/
#define FTT_DIR_READING 0
#define FTT_DIR_WRITING 1
