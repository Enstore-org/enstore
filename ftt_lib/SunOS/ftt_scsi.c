/*
 * ftt_scsi_sun.c
 */

#include <stdio.h>
#ifdef FILENAME_MAX     /* defined in stdio.h only in SYSV systems */
#define SYSV
#endif

#include <sys/param.h>
#include <values.h>
#include <sys/types.h>
#include <fcntl.h>
#include "ftt_private.h"

#ifdef SYSV

#include <sys/scsi/generic/mode.h>
#include <sys/scsi/generic/commands.h>
#include <sys/scsi/impl/types.h>
#include <sys/scsi/impl/uscsi.h>
#include <sys/systeminfo.h>
#define gethostname(b, l) sysinfo(SI_HOSTNAME, b, (long)l)
#define bzero(b,l) memset(b,0,l)

#else /* SYSV */

#include <scsi/impl/types.h>
#include <scsi/impl/uscsi.h>

#endif /* SYSV */


int
ftt_scsi_open(const char *pcDevice)
{
	scsi_handle n;
        DEBUG2(stderr,"entering ftt_scsi_open(%s,..)\n",pcDevice);
        n = (scsi_handle)open(pcDevice, O_RDWR, 0);
        DEBUG2(stderr,"filehandle == %d\n",  n );
	return n;

}

int 
ftt_scsi_close(scsi_handle n)
{
	close(n);
}

int
ftt_scsi_command(scsi_handle fd, char *pcOp,unsigned char *pcCmd, int nCmd, unsigned char *pcRdWr, int nRdWr, int delay, int iswrite)
{
        struct uscsi_cmd cmd;
        union scsi_cdb cdb;
	int scsistat, res;

        cmd.uscsi_cdb=(caddr_t)pcCmd;
        cmd.uscsi_cdblen=nCmd; /* SCSI Group 0 cmd */
        cmd.uscsi_bufaddr=(caddr_t)pcRdWr;
        cmd.uscsi_buflen=nRdWr;
        cmd.uscsi_flags=USCSI_SILENT|(iswrite?USCSI_WRITE:USCSI_READ);

        DEBUG2(stderr,"sending scsi frame:\n");
        DEBUGDUMP2(pcCmd,nCmd);

        scsistat = ioctl(fd, USCSICMD, &cmd);
	if (-1 == scsistat || 5 == errno )
                res = UNIX_ERRNO(errno);
        else
                res = ftt_scsi_check(fd,pcOp,cmd.uscsi_status);
        if (pcRdWr != 0 && nRdWr != 0){
                DEBUG2(stderr,"got back:\n");
                DEBUGDUMP2(pcRdWr,nRdWr);
        }
	return res;
}
