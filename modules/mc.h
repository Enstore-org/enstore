/* c source common to media changers.   I know executable code
   should not be .h files but Setup would require another Makefile scheme
*/
enum e_type {MC_OK=0,           /* mount successful */
                MC_UNKOWN,      /* something wrong with library system */
                MC_DRIVE,       /* a drive problem - retry another drive */
                MC_MEDIA};      /* a cartridge problem - drive ok */

/*
        Convert the STK error code to a canonical code
                e_type will also be defined in media_changer.py
        stat - status returned by the dismount
        drive_errs - a list of status codes which are drive errors
	media_errs - a list of status codes which are media errors
*/


int status_class(int stat, int* drive_errs, int* media_errs)
{
int *e;
  if (stat == 0) return(MC_OK);
  for (e=drive_errs; *e; e++)
        if (stat == *e) return(MC_DRIVE);
  for (e=media_errs; *e; e++)
        if (stat == *e) return(MC_MEDIA);
  return(MC_UNKOWN);
}

