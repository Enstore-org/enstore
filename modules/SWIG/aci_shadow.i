/************************************************************
 *
 *   $Id$
 *
 * SWIG input file for aci_shadowcmodule
 *
 ************************************************************/

%module aci_shadow

/* build default constructors/destructors for shadow classes */
%pragma make_default

%{
#include "aci.h"
#include "aci_typedefs.h"
%}

/*-------------------------------------------------------------------------*/
/*                                                                         */
/*  aci.h - the header file for ACI users                                  */
/*                                                                         */
/*  version: 1.30C3 or later                                               */
/*                                                                         */
/*-------------------------------------------------------------------------*/

#include <sys/types.h>
#include <rpc/rpc.h>    /* for bool_t   */
#include "derrno.h"     /* das errnos   */
#include <netinet/in.h>

%include "typemaps.i"   /* SWIG standard typemaps */
%include "aci_typedefs.h"
%include "aci_typemaps.i"



/*-------------------------------------------------------------------------*/
/*                                                                         */
/*-------------------------------------------------------------------------*/

enum aci_media { ACI_3480 = 1, ACI_OD_THICK, ACI_OD_THIN,
                 ACI_DECDLT, ACI_8MM, ACI_4MM, ACI_D2, ACI_VHS, ACI_3590,
                 ACI_CD, ACI_TRAVAN, ACI_DTF, ACI_BETACAM, ACI_AUDIO_TAPE
                 };

enum aci_command {ACI_ADD = 1, ACI_MODIFY, ACI_DELETE};
enum aci_drive_status {ACI_DRIVE_DOWN = 1, ACI_DRIVE_UP};

/*-------------------------------------------------------------------------*/
/*                                                                         */
/*-------------------------------------------------------------------------*/

#define ACI_COORD_LEN   11      /* should match das.h setting */
#define ACI_VOLSER_LEN  17      /* should match das.h setting */
#define ACI_NAME_LEN    65      /* should match das.h setting */
#define ACI_REQTYPE_LEN 10      /* should match das.h setting */
#define ACI_DRIVE_LEN   10      /* should match das.h setting */
#define ACI_AMU_DRIVE_LEN 3     /* should be das.h setting +1 */
#define ACI_OPTIONS_LEN 3       /* should match das.h setting */
#define ACI_MAX_RANGES  10      /* should match das.h setting */
#define ACI_RANGE_LEN   100     /* should match das.h setting */
#define ACI_POOLNAME_LEN  17    /* should match das.h setting */

#define ACI_MAX_REQ_ENTRIES 15  /* should match das.h setting */
#define ACI_MAX_DRIVE_ENTRIES 15 /* should match das.h setting */
#define ACI_MAX_DRIVE_ENTRIES2 250 /* should match das.h setting */

#define ACI_MAX_VERSION_LEN 20   /*           including '\0'   */
#define ACI_MAX_QUERY_VOLSRANGE 1000


#define ACI_VOLSER_ATTRIB_MOUNTED   'M'
#define ACI_VOLSER_ATTRIB_EJECTED   'E'
#define ACI_VOLSER_ATTRIB_OCCUPIED  'O'
#define ACI_VOLSER_ATTRIB_UNDEFINED 'U'


/*-------------------------------------------------------------------------*/
/*                                                                         */
/*-------------------------------------------------------------------------*/

struct aci_vol_desc {
        char coord[ACI_COORD_LEN];
        char owner;
        char attrib;
        char type;
        char volser[ACI_VOLSER_LEN];
        char vol_owner;
        int use_count;
        int crash_count;
};


struct aci_drive_entry {
    char drive_name[ACI_DRIVE_LEN];
    char amu_drive_name[ACI_AMU_DRIVE_LEN];
    enum aci_drive_status drive_state;
    char type;
    char system_id[ACI_NAME_LEN];
    char clientname[ACI_NAME_LEN];
    char volser[ACI_VOLSER_LEN];
    bool_t cleaning;
    short clean_count;
};

/*cgw*/
struct in_addr {
	unsigned int s_addr;
};

struct aci_client_entry {
        char clientname[ACI_NAME_LEN];
        struct in_addr ip_addr;
        bool_t avc;
        bool_t complete_access;
        bool_t dismount;
        aci_range volser_range;
        char drive_range [ACI_RANGE_LEN];
};


struct aci_req_entry {
        u_long request_no;
        u_long individ_no;
        char clientname [ACI_NAME_LEN];
        char req_type [ACI_REQTYPE_LEN + 1];
};

/*-------------------------------------------------------------------------*/
/*                                                                         */
/*-------------------------------------------------------------------------*/

struct aci_volserinfo
{
       char            volser [ ACI_VOLSER_LEN ];
       enum aci_media  media_type;
       char            attrib;
};




/*-------------------------------------------------------------------------*/
/*                                                                         */
/*-------------------------------------------------------------------------*/

int aci_robhome (char *); /*cgw*/
int aci_robstat (char *, char *); /*tgj*/

int aci_cancel (int);
extern int aci_clientaccess (char *, enum aci_command, char *, enum aci_media,
			     char *); /*cgw*/
extern int aci_clientstatus (char *, struct aci_client_entry *); /*cgw*/
extern int aci_dismount (char *, enum aci_media);
extern int aci_driveaccess (char *, char *, enum aci_drive_status);
extern int aci_drivestatus (char *, struct aci_drive_entry *[ACI_MAX_DRIVE_ENTRIES]);
extern int aci_drivestatus2 (char *, struct aci_drive_entry *[ACI_MAX_DRIVE_ENTRIES]);
extern int aci_eject (char *, char *, enum aci_media);
extern int aci_eject_complete( char *, char *, enum aci_media );
extern int aci_force (char *);
extern int aci_foreign (enum aci_command, char *, enum aci_media, char *,short);
extern int aci_init (void);
extern int aci_insert (char *, char *volser_ranges[ACI_MAX_RANGES], enum aci_media *); /*tgj*/
extern int aci_list (char *, struct aci_req_entry *[ACI_MAX_REQ_ENTRIES]);
extern int aci_mount (char *, enum aci_media, char *);
extern int aci_move (char *, enum aci_media, char *);
extern int aci_carry( char *, char *, char *, int *);
extern void aci_perror (char *);
extern int aci_register (char *, char *, enum aci_command,bool_t,bool_t,bool_t);
extern int aci_shutdown (char *);
extern int aci_view (char *, enum aci_media, struct aci_vol_desc *);

extern int aci_initialize( void );


extern int aci_qversion( version_string, version_string);
extern int aci_qvolsrange( char *, char *, int, char *, int *,
                           struct aci_volserinfo *               );
extern int aci_partial_inventory( char *, char *);
extern int aci_inventory( void );
extern int aci_scratch_set (char *, enum aci_media , char * );
extern int aci_scratch_get (char *, enum aci_media , char * );
extern int aci_scratch_unset (char *, enum aci_media , char * );
extern int aci_scratch_info (char *,  enum aci_media , long *, long *);


/*-------------------------------------------------------------------------*/
/*                                                                         */
/*-------------------------------------------------------------------------*/

extern int d_errno;     /*  global object for error return from aci_ call */

extern char *szRetDrive[15]; /*  global object for drive return from aci_mount-call  */
                             /*  without Drive ---> generic mount                    */

extern  int   iMediaType;
extern  char  szMediaType[];

extern  char  szVolser[];
extern  char  szSourceCoord[];
extern  char  szTargetCoord[];

/*-------------------------------------------------------------------------*/
/*  E n d                                                                  */
/*-------------------------------------------------------------------------*/




