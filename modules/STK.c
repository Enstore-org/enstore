#include <unistd.h>
#include <Python.h>
#include "acssys.h"
#include "acsapi.h"

/*
        See media_changer.py
        This is a python interface to the stk acsls system.
        It requires -
                1) STK product to compile and link - See Setup
                2) stkssi and mini_el daemons be running.
                   They are normally started with "$STKDIR/bin/stkssi start"
                   run at boot time.
                3) The ACSAPI_PACKET_VERSION=2, ACSAPI_SSI_SOCKET=50015
                   which are definded with setup stk
*/
/*
 *  Get the acknowledgement response
 */
#define SSI_TIMEOUT 300

static PyObject *STKErrObject;

static char volStatus[1000];
static char volStatus2[1000];

static struct typename {
    ACS_RESPONSE_TYPE rtype;
    char *tname_string;
}
tname_table[] = {
    RT_FIRST,
    "RT_FIRST",
    RT_ACKNOWLEDGE,
    "RT_ACKNOWLEDGE",
    RT_INTERMEDIATE,
    "RT_INTERMEDIATE",
    RT_FINAL,
    "RT_FINAL",
    RT_LAST,
    "RT_LAST",
};

static char unknown[80];

void sleep4IPC()
{
    printf("\nSLEEP4IPC\n");
    usleep((unsigned long)500000);
}

char *type_response (ACS_RESPONSE_TYPE rtype)
{

    unsigned short i;

    /* Look for the status parameter in the table. */
    for (i = 1; tname_table[i].rtype != RT_LAST; i++) {
        if (rtype == tname_table[i].rtype) {
            return tname_table[i].tname_string;
        }
    }

    /* untranslatable status */
    sprintf (unknown, tname_table[i].tname_string);
    return unknown;
}

STATUS get_ack(SEQ_NO seq,                /* common get acknowledgment function*/
               ACS_RESPONSE_TYPE *p_type) /* common get final (or next)func */
{
    SEQ_NO                seq_nmbr;     /* command identification number     */
    REQ_ID                req_id;       /* response request identification   */
    STATUS                status;       /* command return status structure   */
    ACS_RESPONSE_TYPE     type;         /* final response structure          */
    ALIGNED_BYTES         rbuf[(MAX_MESSAGE_SIZE/sizeof(ALIGNED_BYTES)+1)];
    int                   maxtries=3;

    /*
    **  Call acs_response to get the acknowledgement response.
    */

    *p_type = RT_LAST;
    while (maxtries>0) {
        printf("\nACS_RESPONSE (get_ack) for seq %d\n",seq);
        status = acs_response(SSI_TIMEOUT,&seq_nmbr,&req_id,&type, rbuf);
        printf("\nACS_RESPONSE (get_ack) for seq=%d: seq=%d, req_id=%d, type=%d %s, status=%d %s\n",
               seq,seq_nmbr,req_id,type,type_response(type),status,cl_status(status));
        if (seq==seq_nmbr) {
            *p_type = type;
            return(status);
        } else {
            printf("\nMISMATCH acs_response (get_ack) for seq=%d: seq=%d MISMATCH!\n",seq,seq_nmbr);
            maxtries--;
        }
    }
    return(STATUS_NONE);
}
/*
 *  Get the final (or next) response
 */

STATUS get_next(SEQ_NO seq,
                ALIGNED_BYTES *p_rbuf,
                int p_size,
                ACS_RESPONSE_TYPE *p_type) /* common get final (or next)func */

{

    SEQ_NO                seq_nmbr;     /* command identification number     */
    REQ_ID                req_id;       /* response request identification   */
    STATUS                status;       /* command return status structure   */
    ACS_RESPONSE_TYPE     type;         /* final response structure          */
    ALIGNED_BYTES         rbuf[(MAX_MESSAGE_SIZE/sizeof(ALIGNED_BYTES)+1)];
    int                   maxtries=3;

    /*
    **  Call acs_response to get an intermediate and/or final response.
    */

    *p_type = RT_LAST;
    while (maxtries>0) {
        printf("\nACS_RESPONSE (get_next) for seq %d\n",seq);
        status = acs_response(SSI_TIMEOUT,&seq_nmbr,&req_id,&type, rbuf);
        printf("\nACS_RESPONSE (get_next) for seq=%d: seq=%d, req_id=%d, type=%d %s, status=%d %s\n",
               seq,seq_nmbr,req_id,type,type_response(type),status,cl_status(status));
        if (seq==seq_nmbr) {
            memcpy((ALIGNED_BYTES)p_rbuf,rbuf,p_size);
            *p_type = type;
            return(status);
        } else {
            printf("\nMISMATCH acs_response (get_next) for seq=%d: seq=%d MISMATCH!\n",seq,seq_nmbr);
            maxtries--;
        }
    }
    return(STATUS_NONE);
}

STATUS STKmount(SEQ_NO p_s,
             char p_volume[],
             DRIVEID p_drv_id,
             BOOLEAN p_readonly,
             LOCKID p_lock_id)          /* mount function                    */

{
    VOLID               vol_id;         /* volume structure                  */
    ACS_MOUNT_RESPONSE  *mp;            /* mount response structure          */
    ALIGNED_BYTES       rbuf[(MAX_MESSAGE_SIZE/sizeof(ALIGNED_BYTES)+1)];
    STATUS              status;         /* command return status structure   */
    ACS_RESPONSE_TYPE   type;           /* final response structure          */
    int                 size;           /* final response size               */
    BOOLEAN             bypass = FALSE; /* bypass checking flag              */
    char                *drive_name;    /* drive name                        */

    printf("\nSTKMOUNT: seq=%d, vol=%s drive=%d/%d/%d/%d, RO=%d,lockid=%d\n",p_s,p_volume,
           p_drv_id.panel_id.lsm_id.lsm, p_drv_id.panel_id.lsm_id.acs, p_drv_id.panel_id.panel, p_drv_id.drive,
           p_readonly, p_lock_id );

    (void)strcpy(vol_id.external_label,p_volume);

    status = acs_mount(
                       p_s,             /* client defined number returned in the response */
                       p_lock_id,       /* Lock the drive with this id or NO_LOCK */
                       vol_id   ,       /* Id of the tape cartridge to be mounted */
                       p_drv_id,        /* Id of the drive where the tape cartridge is mounted */
                       p_readonly,      /* If TRUE, the volume will be mounted readonly */
                       bypass);         /* If TRUE, bypass volser and media verification */
    printf("\nACS_MOUNT transmit. seq=%d, vol=%s, code=%d %s\n",p_s,p_volume,status,cl_status(status));
    if (status != STATUS_SUCCESS){ sleep4IPC(); return(status); }

    if ((status = get_ack(p_s,&type)) != STATUS_SUCCESS) { sleep4IPC(); return(status); }
    if (type == RT_FINAL | type== RT_LAST) { sleep4IPC(); return(STATUS_NONE); }

    size = sizeof(ACS_MOUNT_RESPONSE);
    if ((status = get_next(p_s,rbuf,size,&type)) != STATUS_SUCCESS) { sleep4IPC(); return(status); }

    mp = (ACS_MOUNT_RESPONSE *)rbuf;
    status=mp->mount_status;
    printf("\nSTKMOUNT returning. seq=%d, vol=%s, code=%d %s\n",p_s,p_volume,status,cl_status(status));
    sleep4IPC(); return(status);

}

STATUS STKdismount(SEQ_NO p_s,
                char p_volume[],
                DRIVEID p_drv_id,
                LOCKID p_lock_id)      /* dismount function                 */

{

    ACS_DISMOUNT_RESPONSE *dp;          /* dismount response structure       */
    VOLID                 vol_id;       /* volume id structure               */
    char                  cmd_buf[256]; /* system call command buffer        */
    LOCKID                lock_id;      /* lock value structure              */
    ALIGNED_BYTES         rbuf[(MAX_MESSAGE_SIZE/sizeof(ALIGNED_BYTES)+1)];
    STATUS                status;       /* command return status structure   */
    ACS_RESPONSE_TYPE     type;         /* final response structure          */
    int                   size;         /* final response structure size     */
    char                  *drive_name;  /* drive name                        */
    int                   ret;          /* function return code              */

    printf("\nSTKDISMOUNT: seq=%d, vol=%s drive=%d/%d/%d/%d, lockid=%d\n",p_s,p_volume,
           p_drv_id.panel_id.lsm_id.lsm, p_drv_id.panel_id.lsm_id.acs, p_drv_id.panel_id.panel, p_drv_id.drive,
           p_lock_id );

    (void)strcpy(vol_id.external_label,p_volume);

    status = acs_dismount(
                          p_s,          /* client defined number returned in the response*/
                          p_lock_id,    /* Lock the drive with this id */
                          vol_id,       /* Id of the tape cartridge to be mounted */
                          p_drv_id,     /* Id of the drive where the tape cartridge is mounted */
                          TRUE);        /* Force Dismount */
    printf("\nACS_DISMOUNT transmit. seq=%d, vol=%s, code=%d %s\n",p_s,p_volume,status,cl_status(status));
    if(status != STATUS_SUCCESS) { sleep4IPC(); return(status); }

    if ((status = get_ack(p_s,&type)) != STATUS_SUCCESS) { sleep4IPC(); return(status); }
    if (type == RT_FINAL | type== RT_LAST) { sleep4IPC(); return(STATUS_NONE); }

    size = sizeof(ACS_DISMOUNT_RESPONSE);
    if ((status = get_next(p_s,rbuf,size,&type)) != STATUS_SUCCESS) { sleep4IPC(); return(status); }

    dp = (ACS_DISMOUNT_RESPONSE *)rbuf;
    status=dp->dismount_status;
    printf("\nSTKDISMOUNT returning. seq=%d, vol=%s, code=%d %s\n",p_s,p_volume,status,cl_status(status));
    sleep4IPC(); return(status);
}

STATUS STKquery_volume(SEQ_NO p_s,
                    char p_volume[])    /* query volume function             */

{

    VOLID                  vol_id[MAX_ID]; /* volume array structure         */
    ACS_QUERY_VOL_RESPONSE *qp;         /* volume response structure         */
    QU_VOL_STATUS          *sp;         /* volume status structure           */
    ALIGNED_BYTES          rbuf[(MAX_MESSAGE_SIZE/sizeof(ALIGNED_BYTES)+1)];
    STATUS                 status;      /* command return status structure   */
    ACS_RESPONSE_TYPE      type;        /* final response structure          */
    int                    size;        /* final response size               */
    int                    i;           /* loop counter                      */
    char                   *drive_name; /* drive name                        */

    printf("\nSTKQUERY_VOLUME: seq=%d, vol=%s\n",p_s,p_volume);

    /*
    **  Call query_volume to get the status of the requested volume.  If the
    **  query itself, or the query_volume status fails, write an error message,
    **  otherwise write the query volume results to stdout.
    */

    (void)strcpy(vol_id[0].external_label,p_volume);
    (void)strcpy(volStatus,"??");
    (void)strcpy(volStatus2,"??");

    status = acs_query_volume(p_s,vol_id,(ushort) 1);
    printf("\nACS_QUERY_VOLUME transmit. seq=%d, vol=%s, code=%d %s\n",p_s,p_volume,status,cl_status(status));
    if(status != STATUS_SUCCESS) { sleep4IPC(); return(status); }

    if ((status = get_ack(p_s,&type)) != STATUS_SUCCESS) { sleep4IPC(); return(status); }
    if (type == RT_FINAL | type== RT_LAST) { sleep4IPC(); return(STATUS_NONE); }

    while (1) {

        size = sizeof(ACS_QUERY_VOL_RESPONSE);
        if ((status = get_next(p_s,rbuf,size,&type)) != STATUS_SUCCESS) { sleep4IPC(); return(status); }

        qp = (ACS_QUERY_VOL_RESPONSE *)rbuf;
        status=qp->query_vol_status;

        if (status != STATUS_SUCCESS) {
            printf("\nSTKQUERY_VOLUME status. seq=%d, vol=%s, volStatus=%s, code=%d %s\n",p_s,p_volume,volStatus,status,cl_status(status));
        }  else {
            /* I don't see how qp->count can ever be anything but 1 */
            for (i = 0; i < (int)qp->count; i++) {
                sp = &qp->vol_status[i];
               (void)strcpy(volStatus,(char *)cl_status(sp->status));
            }
        }
        printf("\nSTKQUERY_VOLUME status. seq=%d, vol=%s, qp->count=%d, volStatus=%s, code=%d %s\n",p_s,p_volume,(int)qp->count,volStatus,status,cl_status(status));
        if (type == RT_FINAL) break;
    }

    printf("\nSTKQUERY_VOLUME returning. seq=%d, vol=%s, volStatus=%s, code=%d %s\n",p_s,p_volume,volStatus,status,cl_status(status));
    sleep4IPC(); return(status);
}

/*
   Convert a drive from ascii of the form "lsm,acs,panel,drive" to a DRIVEID struct.
   see  ./h/api/ident_api.h.   There must be a stk api but I can't find it.
   There is NO ERROR CHECKING - intended for use in enstore
*/
void  asc2STKdrv( char *drive,  DRIVEID *stkdrv)
{
  stkdrv->panel_id.lsm_id.lsm=strtol(drive,&drive,0); drive++;
  stkdrv->panel_id.lsm_id.acs=strtol(drive,&drive,0); drive++;
  stkdrv->panel_id.panel =    strtol(drive,&drive,0); drive++;
  stkdrv->drive =             strtol(drive,&drive,0);
}

/*
        Convert the STK error code to a canonical code
*/
char* status_class(int stat)
{
    static int drive_errs[]={STATUS_DRIVE_IN_USE, STATUS_DRIVE_NOT_IN_LIBRARY, STATUS_DRIVE_OFFLINE,
                             STATUS_DRIVE_RESERVED,STATUS_INVALID_DRIVE,STATUS_INVALID_DRIVE_TYPE,0};
    static int media_errs[]={STATUS_MISPLACED_TAPE,STATUS_UNREADABLE_LABEL,STATUS_INVALID_VOLUME,
                             STATUS_VOLUME_IN_TRANSIT,STATUS_VOLUME_NOT_FOUND,STATUS_VOLUME_DELETED,
                             STATUS_VOLUME_ACCESS_DENIED,STATUS_VOLUME_NOT_IN_LIBRARY,0};
    int *e;
    if (stat == 0) return("ok");
    for (e=drive_errs; *e; e++)
        if (stat == *e) return("DRIVE");
    for (e=media_errs; *e; e++)
        if (stat == *e) return("TAPE");
    return("BAD");
}


/*
        Documentation for python
*/
static char STK_Doc[] =  "STK Robot load and unload";
static char Mount_Doc[] =  "Mount a tape: STK.mount(vol,drive,media_type,seq)";
static char Dismount_Doc[] =  "Dismount a tape: STK.dismount(vol,drive,media_type,seq)";
static char Query_Volume_Doc[] =  "Query volume: STK.query(vol,media_type,seq)";
static char Test_Doc[] =  "Test routine: (vol,drive,media_type,seq)";
/*      mount

        Arguments{
                vol - cartridge id
                drive - drive name
                media_type_s - mediatype
        Returns
                int - status returned by robot
                int - canonical status 0=> ok, 1=> unknow error, 2=>drive problem, 3=>nedia problem
                char* text desciption of error
*/

static PyObject* mount(PyObject *self, PyObject *args)
{
  char *vol;
  char *drive;
  char *media_type;
  DRIVEID stkdrv;
  int seqNo;
  SEQ_NO seq;
  int stat;

  if(!PyArg_ParseTuple(args, "sssi", &vol, &drive, &media_type, &seqNo)) {
      printf("\nMOUNT - invalid arguments\n");
      return (NULL);
  }
  seq = seqNo;
  printf("\nMOUNT: vol=%s drive=%s, media_type=%s seq=%d\n",vol,drive,media_type,seq);

  asc2STKdrv(drive, &stkdrv);
  stat = STKmount(seq, vol, stkdrv, 0, NO_LOCK_ID);
  return(Py_BuildValue("sis", status_class(stat), stat, cl_status(stat)));
}

/*
        dismount - see mount parameters and return values
*/
static PyObject* dismount(PyObject *self, PyObject *args)
{
  char *vol;
  char *drive;
  char *media_type;
  DRIVEID stkdrv;
  int seqNo;
  SEQ_NO seq;
  int stat;

  if(!PyArg_ParseTuple(args, "sssi", &vol, &drive, &media_type, &seqNo)) {
      printf("\nDISMOUNT - invalid arguments\n");
      return (NULL);
  }
  seq = seqNo;
  printf("\nDISMOUNT: vol=%s drive=%s, media_type=%s seq=%d\n",vol,drive,media_type,seq);

  asc2STKdrv(drive, &stkdrv);
  stat = STKdismount(seq, vol, stkdrv, NO_LOCK_ID);
  return(Py_BuildValue("sis", status_class(stat), stat, cl_status(stat)));
}

static PyObject* query_volume(PyObject *self, PyObject *args)
{
  char *vol;
  char *media_type;
  int stat;
  int seqNo;
  SEQ_NO seq;

  if(!PyArg_ParseTuple(args, "ssi", &vol, &media_type, &seqNo)) {
      printf("\nQUERY_VOLUME - invalid arguments\n");
      return (NULL);
  }
  seq = seqNo;
  printf("\nQUERY_VOLUME: vol=%s media_type=%s seq=%d\n",vol,media_type,seq);

  stat = STKquery_volume(seq, vol);

  /* printf("\nSTK.QUERY_VOLUME returned %s\n",volStatus); *
  /* try to put this into same format as aml/2 state */
  if (strcmp("STATUS_VOLUME_IN_DRIVE",volStatus)==0) {
      strcpy(volStatus2,"M"); /* mounted */
  } else if (strcmp("STATUS_VOLUME_HOME",volStatus)==0) {
      strcpy(volStatus2,"O"); /* occupied */
  } else if (strcmp("STATUS_VOLUME_NOT_IN_LIBRARY",volStatus)==0) {
      volStatus[0]=0; /* not present - no info */
  } else if (strcmp("STATUS_VOLUME_IN_TRANSIT",volStatus)==0) {
      strcpy(volStatus2,"T"); /* aml doesnot have this - but shorten */
  }
  return(Py_BuildValue("sisss", status_class(stat), stat, cl_status(stat),volStatus2,volStatus));
}

static PyObject* test(PyObject *self, PyObject *args)
{
  char *vol;
  char *media_type;
  char *drive;
  int stat;
  char *sc;
  int seqNo;
  SEQ_NO seq;
  STATUS status;
  ALIGNED_BYTES       rbuf[(MAX_MESSAGE_SIZE/sizeof(ALIGNED_BYTES)+1)];
  ACS_RESPONSE_TYPE   type;           /* final response structure          */
  int                 size;           /* final response size               */
  ACS_QUERY_SRV_RESPONSE *sp;         /* server response structure         */
  QU_SRV_STATUS          *rp;         /* query server  status structure */
  int                    i;           /* loop counter                      */


  if(!PyArg_ParseTuple(args, "sssi", &vol, &drive, &media_type, &seqNo)) {      /* get args */
      printf("\nDISMOUNT - invalid arguments\n");
      return (NULL);
  }
  seq = seqNo;
  printf("\nTEST: vol=%s drive=%s, media_type=%s seq=%d\n",vol,drive,media_type,seq);

  status = acs_query_server(seq);
  printf("\nACS_QUERY_SERVER transmit seq=%d, code=%d %s\n",seq,status,cl_status(status));
  if (status != STATUS_SUCCESS){
      sleep4IPC();
      return(Py_BuildValue("sisisii", status_class(status), status, cl_status(status),STATE_LAST,cl_state(STATE_LAST),seq));
  }

  if ((status = get_ack(seq,&type)) != STATUS_SUCCESS) {
      sleep4IPC();
      return(Py_BuildValue("sisisii", status_class(status), status, cl_status(status),STATE_LAST,cl_state(STATE_LAST),seq));
  }
  if (type == RT_FINAL | type== RT_LAST) {
      sleep4IPC();
      status=STATUS_NONE;
      return(Py_BuildValue("sisisii", status_class(status), status, cl_status(status),STATE_LAST,cl_state(STATE_LAST),seq));
  }
  size = sizeof(ACS_MOUNT_RESPONSE);
  if ((status = get_next(seq,rbuf,size,&type)) != STATUS_SUCCESS) {
      sleep4IPC();
      return(Py_BuildValue("sisisii", status_class(status), status, cl_status(status),STATE_LAST,cl_state(STATE_LAST),seq));
  }
  sp = (ACS_QUERY_SRV_RESPONSE *)rbuf;
  status=sp->query_srv_status;

  if (status != STATUS_SUCCESS) {
      printf("\nTEST bad status. seq=%d, code=%d %s\n",seq,status,cl_status(status));
      sleep4IPC;
      return(Py_BuildValue("sisisii", status_class(status), status, cl_status(status),STATE_LAST,cl_state(STATE_LAST),seq));
  }  else {
      /* I don't see how sp->count can ever be anything but 1 */
      for (i = 0; i < (int)sp->count; i++) {
          rp = &sp->srv_status[i];
      }
  }
  printf("\nTEST returning: status_class=%s, status=%d %s state=%d %s seq=%d, count=%d\n",status_class(status), status, cl_status(status),rp->state,cl_state(rp->state),seq,(int)sp->count);
  sleep4IPC();
  return(Py_BuildValue("sisisii", status_class(status), status, cl_status(status),rp->state,cl_state(rp->state),seq));
}

/*
   Module Methods table.

   There is one entry with four items for for each method in the module

   Entry 1 - the method name as used  in python
         2 - the c implementation function
         3 - flags
         4 - method documentation string
*/

static PyMethodDef STK_Methods[] = {
  { "mount", mount, 1, Mount_Doc},
  { "dismount", dismount, 1, Dismount_Doc},
  { "query_volume", query_volume, 1, Query_Volume_Doc},
  { "test", test, 1, Test_Doc},

  {0,     0}        /* Sentinel */
};

/*
   Module initialization.   Python call the entry point init<module name>
   when the module is imported.  This should the only non-static entry point
   so it is exported to the linker.

   The Py_InitModule4 is not in the python 1.5 documentation but is copied
   from the oracle module.  It extends Py_InitModule with documentation
   and seems useful.

   First argument must be a the module name string.

   Seond        - a list of the module methods

   Third        - a doumentation string for the module

   Fourth & Fifth - see Python/modsupport.c

*/

void initSTK()
{
  PyObject *m, *d;

  m=  Py_InitModule4("STK", STK_Methods, STK_Doc,
                        (PyObject*)NULL,PYTHON_API_VERSION);
  d = PyModule_GetDict(m);
  STKErrObject = PyErr_NewException("STK.error", NULL, NULL);
  if (STKErrObject != NULL)
             PyDict_SetItemString(d,"error",STKErrObject);
}

