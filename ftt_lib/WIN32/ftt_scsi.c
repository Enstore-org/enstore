static char rcsid[] = "@(#)$Id$";

#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <ftt_private.h>

#ifdef WIN32
#include <windows.h>
#include <winioctl.h>
#endif


scsi_handle
ftt_scsi_open(const char *pc)
{

	return (scsi_handle) -1;
	
}

int
ftt_scsi_close(scsi_handle n) {
	
	return -1;
}

int
ftt_scsi_command(scsi_handle n, char *pcOp,unsigned char *pcCmd, int nCmd, unsigned char *pcRdWr, int nRdWr, int delay, int writeflag)
{
	
	return -1;
}
