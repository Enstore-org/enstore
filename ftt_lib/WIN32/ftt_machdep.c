static char rcsid[] = "@(#)$Id$";
#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <ftt_private.h>

#include <windows.h>
#include <winioctl.h>
#include <stdlib.h>
int ftt_translate_error_WIN();
int ftt_open_io_dev();

DWORD 
ftt_win_get_paramters(ftt_descriptor d, TAPE_GET_MEDIA_PARAMETERS *gmp,
					  TAPE_GET_DRIVE_PARAMETERS *gdp) {
	DWORD fres1=0,fres2=0;
	int i;
	int size;
	HANDLE fh = (HANDLE)d->file_descriptor;
	if (gdp) {
		i = 0;
D_loop:
		fres1 = GetTapeParameters(fh,GET_TAPE_DRIVE_INFORMATION,&size,gdp);
		DEBUG2(stdout,"GetTapeParam-Drive returned %d - size %d - iter %d \n",(int)fres1,size,i);
		i++;
		if (fres1 == ERROR_MORE_DATA ) {
			if ( i < 10 ) {
				Sleep(10);
				goto D_loop;
			}
		}
	}
	if (gmp ) {
		i=0;
M_loop:
		fres2 = GetTapeParameters(fh,GET_TAPE_MEDIA_INFORMATION,&size,gmp);
		DEBUG1(stdout,"GetTapeParam-Media returned %d - size %d - iter %d \n",(int)fres2,size,i);
		i++;
		if (fres2 == ERROR_MORE_DATA ) {
			if ( i < 10 ) {
				Sleep(10);
				goto M_loop;
			}
		}
	}
	
	if ( fres2 > 0 ) fres1 = fres2;
	return fres2; /* Latest error */

}

int
ftt_status(ftt_descriptor d, int time_out) {
    DWORD fres, fres2,fres3,pos=(DWORD)-1,pos2=(DWORD)-1,par=(DWORD)-1;
    int res=0;
	TAPE_GET_MEDIA_PARAMETERS gmp;
	HANDLE fh;

    ENTERING("ftt_status");
    CKNULL("ftt_descriptor", d);

    

	if (0 >  ftt_open_io_dev(d)) { 
		if( FTT_EBUSY == ftt_errno )return FTT_BUSY;
		return -1;
	}
	
	fh = (HANDLE)d->file_descriptor;

    fres = GetTapeStatus(fh);
   
	while(time_out > 0 && 
		 ( fres == ERROR_BUS_RESET || fres == ERROR_NO_MEDIA_IN_DRIVE) ) {
		Sleep(1000); /* in miliseconds*/
		fres = GetTapeStatus(fh);
		time_out--;
	}

    res = FTT_ONLINE;
    
    if     (fres == ERROR_BEGINNING_OF_MEDIA )		res |= FTT_ABOT;
    else if(fres == ERROR_BUS_RESET)				;
	else if(fres == ERROR_END_OF_MEDIA)				res |= FTT_AEOT | FTT_AEW;
    else if(fres == ERROR_FILEMARK_DETECTED)		;
    else if(fres == ERROR_SETMARK_DETECTED)			;
    else if(fres == ERROR_NO_DATA_DETECTED)			;
    else if(fres == ERROR_PARTITION_FAILURE)		;
    else if(fres == ERROR_INVALID_BLOCK_LENGTH)		;
    else if(fres == ERROR_DEVICE_NOT_PARTITIONED)	;
    else if(fres == ERROR_MEDIA_CHANGED)			res = 0;
    else if(fres == ERROR_NO_MEDIA_IN_DRIVE)		res = 0;
    else if(fres == ERROR_NOT_SUPPORTED)			;
    else if(fres == ERROR_UNABLE_TO_LOCK_MEDIA)		;
    else if(fres == ERROR_UNABLE_TO_UNLOAD_MEDIA)	;
    else if(fres == ERROR_WRITE_PROTECT)			res |= FTT_PROT;

	fres2 = ftt_win_get_paramters(d,&gmp,0);

	if ( fres2 == NO_ERROR ) {
		if (gmp.WriteProtected ) res |= FTT_PROT;
	}
	
	fres3 = GetTapePosition(fh,TAPE_LOGICAL_POSITION,&par,&pos,&pos2);

	if(fres3==NO_ERROR && pos==0) res |= FTT_ABOT;
    return res;
}

int
ftt_set_compression(ftt_descriptor d, int compression) {
	TAPE_GET_DRIVE_PARAMETERS gdp;
	TAPE_SET_DRIVE_PARAMETERS sdp;
	int res=0;
	HANDLE fh;
	DWORD fres;

	DEBUG1(stderr,"entering ftt_set_compression %d\n",compression);
   
    if ( 0 > ftt_open_io_dev(d)) return -1; 

	fh = (HANDLE)d->file_descriptor;

	fres = ftt_win_get_paramters(d,0,&gdp);

	if ( fres != NO_ERROR ) {
		res = ftt_translate_error_WIN(d,"ftt_set_compression", "Set compression ", 
			fres, "GetTapeParameters",1);
		return res;
	}
	else {
		if ( gdp.FeaturesLow & TAPE_DRIVE_COMPRESSION ) {
			sdp.ECC = gdp.ECC;
			sdp.Compression = (BOOLEAN) compression;
			sdp.DataPadding = gdp.DataPadding;
			sdp.ReportSetmarks = gdp.ReportSetmarks;
			sdp.EOTWarningZoneSize = gdp.EOTWarningZoneSize;
			fres = SetTapeParameters(fh,SET_TAPE_DRIVE_INFORMATION,&sdp);
			if ( fres != NO_ERROR ) {
				res = ftt_translate_error_WIN(d,"ftt_set_compression", "Set compression ", 
					fres, "SetTapeParameters",1);
				return res;
			}
		}
		else if (compression){
			ftt_eprintf("ftt_set_compression: Drive does NOT support compression ");
			ftt_errno=FTT_ENOTSUPPORTED;
			return 0;
		}
	}
	return 0;
}

int
ftt_set_hwdens(ftt_descriptor d, int hwdens) {
    /* ignore hwdens, 'cause we opened the right device node */
   ftt_eprintf("ftt_set_hwdens: Not able to set density ");
   ftt_errno=FTT_ENOTSUPPORTED;
   if ( hwdens != 0 ) return -1;
   return 0;
}

int
ftt_set_blocksize(ftt_descriptor d, int blocksize) {
    int res=0;
	DWORD fres;
	TAPE_SET_MEDIA_PARAMETERS sp;
    
	sp.BlockSize = blocksize;
	
    DEBUG1(stderr,"entering ftt_set_hwdens_blocksize %d\n", blocksize);

    if (  0 >  ftt_open_io_dev(d) ) return -1; 
	
	fres = SetTapeParameters((HANDLE)d->file_descriptor,
			                  SET_TAPE_MEDIA_INFORMATION,(LPVOID)&sp);

	if ( fres != NO_ERROR ) {
			res = ftt_translate_error_WIN(d,"ftt_set_blocksize", "Set Block size", 
				fres, "SetTapeParameters",1);
	}
	return res;
}

int
ftt_get_hwdens(ftt_descriptor d, char *devname) {
    int res;

    res = d->devinfo[d->which_is_default].hwdens;
    return res;
}

