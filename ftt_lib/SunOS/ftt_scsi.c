static char rcsid[] = "@(#)$Id$";
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

extern int errno;

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


scsi_handle
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
	static int havesense;


        /*
        ** small kluge -- if we're being asked to send a request sense
        ** command, and we have sense data from the last command, we
        ** just return it.  There doesn't seem to be an easy way to make
        ** the SunOS 5 code not grab the sense data...
        */
#ifdef ARQ
        if ( 0x03 == pcCmd[0] && havesense ) {
            havesense = 0;
            if (pcRdWr != jgp_acSensebuf) {
                bcopy(jgp_acSensebuf, pcRdWr, nRdWr<19?nRdWr:19);
            }
            return ftt_scsi_check(fd,pcOp,0,0);
        }
#endif

        cmd.uscsi_cdb=(caddr_t)pcCmd;
        cmd.uscsi_cdblen=nCmd;
        cmd.uscsi_bufaddr=(caddr_t)pcRdWr;
        cmd.uscsi_buflen=nRdWr;
        cmd.uscsi_flags=USCSI_SILENT|(iswrite?USCSI_WRITE:USCSI_READ);
	cmd.uscsi_timeout=delay;
#ifdef ARQ
        cmd.uscsi_rqbuf = jgp_acSensebuf;
        cmd.uscsi_rqlen = 19;
#else
        cmd.uscsi_rqbuf = 0;
        cmd.uscsi_rqlen = 0;
#endif

        DEBUG2(stderr,"sending scsi frame:\n");
        DEBUGDUMP2(pcCmd,nCmd);

        res = ioctl(fd, USCSICMD, &cmd);
	DEBUG3(stderr, "USCSICMD ioctl returned %d, errno %d\n", res, errno);
	if (-1 == res && errno != 5 ) {
                res = ftt_scsi_check(fd,pcOp, 255,
				cmd.uscsi_buflen - cmd.uscsi_resid);
        } else {
                res = ftt_scsi_check(fd,pcOp,cmd.uscsi_status,
				cmd.uscsi_buflen - cmd.uscsi_resid);
	}
        if (pcRdWr != 0 && nRdWr != 0) {
                DEBUG2(stderr,"got back:\n");
                DEBUGDUMP2(pcRdWr,nRdWr);
        }
#ifdef ARQ
        havesense = (cmd.uscsi_rqstatus == 0);
#endif
	return res;
}
