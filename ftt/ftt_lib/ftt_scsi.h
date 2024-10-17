#ifndef FTT_SCSI_H_INCLUDED
#define FTT_SCSI_H_INCLUDED
typedef long scsi_handle;
extern int ftt_scsi_check(scsi_handle,char *, int, int);
extern scsi_handle ftt_scsi_open(const char*);
extern int ftt_scsi_close(scsi_handle);
extern int ftt_scsi_command(scsi_handle, char*, unsigned char*, int, unsigned char*, int, int, int);
#endif

