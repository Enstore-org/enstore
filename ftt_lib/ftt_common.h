/* ftt_common.h
**
** this file contains type definitions, prototypes, and #defines that
** are visible to the end user and within the library.
*/


/* ftt entry points
** See docs for what they do, arguments, etc.
*/

extern ftt_stat_buf	ftt_alloc_stat(void);
extern int		ftt_all_scsi(ftt_descriptor);
extern char *		ftt_avail_mode(ftt_descriptor, int, int, int);
extern char *		ftt_get_mode(ftt_descriptor, int *, int* mode, int *);
extern void 		ftt_add_stats(ftt_stat_buf,ftt_stat_buf,ftt_stat_buf);
extern int		ftt_chall(ftt_descriptor, int, int, int);
extern int 		ftt_check(ftt_descriptor);
extern int		ftt_clear_stats(ftt_descriptor);
extern int		ftt_close(ftt_descriptor);
extern int		ftt_close_dev(ftt_descriptor);
extern char *		ftt_density_to_name(ftt_descriptor, int);
extern int 		ftt_describe_dev(ftt_descriptor,char*,FILE*);
extern int		ftt_debug_dump(unsigned char *, int);
extern int 		ftt_dump_stats(ftt_stat_buf, FILE*);
extern void		ftt_eprintf(char *, ...);
extern int		ftt_erase(ftt_descriptor);
extern char *		ftt_extract_stats(ftt_stat_buf, int n);
extern int		ftt_fork(ftt_descriptor);
extern int 		ftt_format_label(char*,int,char*,int,int);
extern int		ftt_free_stat(ftt_stat_buf);
extern char *		ftt_get_basename(ftt_descriptor d);
extern char *		ftt_get_error(int *);
extern int 		ftt_get_max_blocksize(ftt_descriptor);
extern int		ftt_get_mode_dev(ftt_descriptor,char*,int*,int*,int*,int*);
extern int		ftt_get_position(ftt_descriptor, int *, int *);
extern char *		ftt_get_scsi_devname(ftt_descriptor);
extern int		ftt_get_stats(ftt_descriptor, ftt_stat_buf);
extern int		ftt_guess_label(char *,int, char**, int *);
extern ftt_stat_buf *	ftt_init_stats(ftt_descriptor);
extern char **		ftt_list_all(ftt_descriptor);
extern char * 		ftt_make_os_name(char *, char *, char *);
extern int		ftt_name_to_density(ftt_descriptor, char *);
extern ftt_descriptor	ftt_open(const char*, int);
extern int		ftt_open_dev(ftt_descriptor);
extern ftt_descriptor	ftt_open_logical(const char*,char*,char*,int);
extern int		ftt_read(ftt_descriptor, char*, int);
extern int		ftt_report(ftt_descriptor);
extern int		ftt_retension(ftt_descriptor);
extern int 		ftt_retry(ftt_descriptor, 
				  int, 
				  int (*)(ftt_descriptor, char *, int),
				  char *buf, 
				  int len);
extern int		ftt_rewind(ftt_descriptor);
extern int		ftt_scsi_locate(ftt_descriptor, int);
extern int		ftt_set_data_direction(ftt_descriptor, int);
extern char * 		ftt_set_mode(ftt_descriptor, int density, int,  int );
extern int 		ftt_set_mode_dev(ftt_descriptor, char *, int );
extern int		ftt_setdev(ftt_descriptor);
extern int		ftt_skip_fm(ftt_descriptor, int);
extern int		ftt_skip_rec(ftt_descriptor, int);
extern int 		ftt_skip_to_double_fm(ftt_descriptor d);
extern int		ftt_status(ftt_descriptor,int);
extern void 		ftt_sub_stats(ftt_stat_buf,ftt_stat_buf,ftt_stat_buf);
extern int		ftt_unload(ftt_descriptor);
extern int 		ftt_update_stats(ftt_descriptor,ftt_stat_buf *);
extern int		ftt_undump_stats(ftt_stat_buf, FILE *);
extern int 		ftt_verify_vol_label(ftt_descriptor,int,char*,int,int);
extern int 		ftt_wait(ftt_descriptor);
extern int		ftt_write(ftt_descriptor, char*, int);
extern int 		ftt_write_vol_label(ftt_descriptor,int,char*);
extern int		ftt_writefm(ftt_descriptor);
extern int		ftt_write2fm(ftt_descriptor);

extern int		ftt_close_scsi_dev(ftt_descriptor);

extern void		ftt_first_supported(int *);
extern ftt_descriptor	ftt_next_supported(int *);
