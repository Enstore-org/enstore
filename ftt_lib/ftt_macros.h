

#define DEBUG1 if(ftt_debug>=1) (void)fprintf
#define DEBUG2 if(ftt_debug>=2) (void)fprintf
#define DEBUG3 if (ftt_debug>=3) (void)fprintf
#define DEBUGDUMP1 if (ftt_debug>=1) (void)ftt_debug_dump
#define DEBUGDUMP2 if (ftt_debug>=2) (void)ftt_debug_dump
#define DEBUGDUMP3 if (ftt_debug>=3) (void)ftt_debug_dump

#define ENTERING(name) 						\
    char *_name = name;						\
								\
    DEBUG1(stderr, "Entering %s\n", _name);	 		\
    ftt_eprintf("Ok\n"); 					\
    ftt_errno = FTT_SUCCESS;					\

#define CKNULL(what,p)						\
    if( 0 == p ) {  						\
	ftt_eprintf("%s called with NULL %s\n", _name, what);	\
	ftt_errno = FTT_EFAULT;					\
	return -1;						\
    }

#define PCKNULL(what,p)						\
    if( 0 == p ) {  						\
	ftt_eprintf("%s called with NULL %s\n", _name, what);	\
	ftt_errno = FTT_EFAULT;					\
	return 0;						\
    }

#define VCKNULL(what,p)						\
    if( 0 == p ) {  						\
	ftt_eprintf("%s called with NULL %s\n", _name, what);	\
	ftt_errno = FTT_EFAULT;					\
	return;							\
    }

#define CKOK(d,name,writes,recovers) \
    char *_name = name;							\
									\
    DEBUG1(stderr, "Entering %s\n", _name);	 			\
    if ( d && d->unrecovered_error && !recovers )  {			\
	ftt_errno = FTT_EUNRECOVERED;					\
	return -1;							\
    }									\
    if ( d && d->unrecovered_error && recovers )  {			\
	d->unrecovered_error = 0;					\
    }									\
    if ( d && d->readonly && writes ) {					\
	ftt_eprintf("%s called on read only ftt descriptor",_name);	\
	ftt_errno = FTT_EROFS;						\
	return -1;							\
    }									\
    ftt_eprintf("Ok\n"); 						\
    ftt_errno = FTT_SUCCESS;
