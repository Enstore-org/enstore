
#define DEBUG1 (ftt_debug>=1)&&fprintf
#define DEBUG2 (ftt_debug>=2)&&fprintf
#define DEBUG3 (ftt_debug>=3)&&fprintf
#define DEBUGDUMP1 (ftt_debug>=1)&&ftt_debug_dump
#define DEBUGDUMP2 (ftt_debug>=2)&&ftt_debug_dump
#define DEBUGDUMP3 (ftt_debug>=3)&&ftt_debug_dump

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
