/*
   EMASS.c - wrapper for emass library
  
*/
#include <Python.h>
#include <aci.h>
#include <derrno.h>

#define EMASS_SKIPINITIALIZEB4ALLCMD 0

/*
	See media_changer.py 
*/
static PyObject *EMASSErrObject;

int EMASSerr(char *caller, char *location, int status)
{
 printf("EMASS err: %s - %s - code %d \n", caller, location, status);
 return(status);
}

static char EMASS_Doc[] =  "EMASS Robot operations";
static char Mount_Doc[] =  "mount <vol> <drive> <media type> ";
static char Dismount_Doc[] =  "dismount <vol> <drive> <media type>";
static char Home_Doc[] = "home <robot>";
static char View_Doc[] = "view <vol> <media type>";
/* static char Initialize_Doc[] = "initialize"; */

/*
	Convert Acsii media type to ACI enum - page 1-8 DAS ref guide
*/
struct media_struct {
  enum aci_media media_enum;
  char *    media_string;
}  media_table[] = {
  ACI_VHS,	"VHS",
  ACI_DECDLT, 	"DECDLT",
  ACI_3480,	"3480",
  ACI_3590,	"3590",
  ACI_OD_THIN,	"OD_THIN",
  ACI_OD_THICK,	"OD_THICK",
  ACI_D2,	"D2",
  ACI_8MM,	"8MM",
  ACI_4MM,	"4MM",
  ACI_DTF,	"DTF",
  ACI_BETACAM,	"BETACAM",
  ACI_TRAVAN,	"TRAVAN",
  ACI_CD,	"CD",
  ACI_AUDIO_TAPE, "AUDIO_TAPE",
  (enum aci_media)NULL,"INVALIDMEDIA"
};

enum aci_media stoi_mediatype(char *media_type)
{
struct media_struct *m;
  for (m=media_table;m->media_enum; m++) 
  {
    if (strcmp(media_type, m->media_string) == 0) 
      return(m->media_enum);
  }
  return(m->media_enum);		/* return "INVALID" */
}
/*
        Convert the STK error code to a canonical code
                e_type will also be defined in media_changer.py
        stat - status returned by the dismount
*/


struct stat_struct{
	char *status;
	char *status_descrip;
	} status_table[]
  = {
	"ok",	"request successful",				/*0*/
	"BAD",	"rpc failure",					/*1*/
	"BAD",	"aci parameter invalid ",			/*2*/
	"TAPE",	"volume not found of this type",		/*3*/
	"DRIVE","drive not in Grau ATL ",			/*4*/
	"DRIVE","the requested drive is in use",		/*5*/
	"TAPE",	"the robot has a physical problem with the volume",	/*6*/
	"BAD",	"an internal error in the AMU ",		/*7*/
	"BAD",	"the DAS was unable to communicate with the AMU",	/*8*/
	"BAD",	"the robotic system is not functioning",	/*9*/
	"BAD",	"the AMU was unable to communicate with the robot",	/*10*/
	"BAD",	"the DAS system is not active ",		/*11*/
	"DRIVE","the drive did not contain an unloaded volume",	/*12*/
	"BAD",	"invalid registration",				/*13*/
	"BAD",	"invalid hostname or ip address",		/*14*/
	"BAD",	"the area name does not exist ",		/*15*/
	"BAD",	"the client is not authorized to make this request",	/*16*/
	"BAD",	"the dynamic area became full, insertion stopped",	/*17*/
	"DRIVE","the drive is currently available to another client",	/*18*/
	"BAD",	"the client does not exist ",			/*19*/
	"BAD",	"the dynamic area does not exist",		/*20*/
	"BAD",	"no request exists with this number",		/*21*/
	"BAD",	"retry attempts exceeded",			/*22*/
	"TAPE",	"requested volser is not mounted",		/*23*/
	"TAPE",	"requested volser is in use ",			/*24*/
	"BAD",	"no space availble to add range",		/*25*/
	"BAD",	"the range or object was not found",		/*26*/
	"BAD",	"the request was cancelled by aci_cancel()",	/*27*/
	"BAD",	"internal DAS error",				/*28*/
	"BAD",	"internal ACI error",				/*29*/
	"BAD",	"for a query more data are available",		/*30*/
	"BAD",	"things don't match together",			/*31*/
	"TAPE",	"volser is still in another pool",	 	/*32*/
	"DRIVE","drive in cleaning",				/*33*/
	"BAD",	"The aci request timed out",			/*34*/
	"DRIVE","the robot has a problem with handling the device"	/*35*/
    };


/**************************************************************************************
        mount -
        arguements
                vol - cartridge id
                drive - drive name
                media_type_s - mediatype
        Returns
                char* - status returned by robot
                int - canonical status 0=> ok, 1=> unknown error, 2=>drive problem, 3=>media problem
                char* text desciption of error
*/

static PyObject* mount(PyObject *self, PyObject *args)
{
  char *vol;
  char *drive;
  char *media_type_s;
  enum aci_media media_type;
  int stat;
  char *sc;
  struct aci_vol_desc desc;

  if (!PyArg_ParseTuple(args, "sss", &vol, &drive, &media_type_s))     /* get args */ 
  	return (NULL);
  if (!(media_type = stoi_mediatype(media_type_s)))        /* convert media type to aci code */
      return(Py_BuildValue("sis",
		status_table[ENOVOLUME].status,
		ENOVOLUME, 
		status_table[ENOVOLUME].status_descrip));
  if (!(stat = get_view(vol,media_type,&desc)) && desc.attrib=='O') /* check if volume is 'occupied' */
  {
     if (EMASS_SKIPINITIALIZEB4ALLCMD || !(stat=do_initialize()))
     {
        if  (stat = aci_mount(vol,media_type,drive))		   /* call aci mount routine */
        {					/* if error */
             stat=d_errno;			/* gett err code */
             if (sizeof(status_table) < stat)	/* if invalid err code */
                 stat= EDASINT;
        }
     }
  }					/* return result */
  return(Py_BuildValue("sis", status_table[stat].status, stat, status_table[stat].status_descrip));
}
/**************************************************************************************
	dismount - see mount parameters and return values
*/
static PyObject* dismount(PyObject *self, PyObject *args)
{
  char *vol;
  char *drive;
  char *media_type_s;
  enum aci_media media_type;
  int stat;
  char *sc;

  /*
        Get the arguements
  */
  if (!PyArg_ParseTuple(args, "sss", &vol, &drive, &media_type_s))            /* get args */ 
  	return (NULL);
/*         aci_dismount takes vol and media_type_s;  force 
  if (!(media_type = stoi_mediatype(media_type_s)))
      return(Py_BuildValue("iis",ENOVOLUME, err_string[ENOVOLUME], err_string[ENOVOLUME]));
      return(Py_BuildValue("sis",
		status_table[ENOVOLUME].status,
		ENOVOLUME, 
		status_table[ENOVOLUME].status_descrip));
  if  (stat = aci_dismount(vol,media_type))
*/
  if (EMASS_SKIPINITIALIZEB4ALLCMD || !(stat=do_initialize()))
  {
     if  (stat = aci_force(drive))
     {
        stat=d_errno;
        if (sizeof(status_table) < stat)
            stat= EDASINT;
     }
  }
  return(Py_BuildValue("sis", status_table[stat].status, stat, status_table[stat].status_descrip));
}
/* #ifdef 0 */
/**************************************************************************************
       get_view -- local wrapper for aci_view()
*/
int get_view(char *vol, enum aci_media media_type, struct aci_vol_desc *desc)
{
  int stat;
  /*
        Initialize struct desc
  */
  desc->coord[0] = '\0';
  desc->owner = '\0';
  desc->attrib = '\0';
  desc->type = '\0';
  desc->volser[0] = '\0';
  desc->vol_owner = '\0';
  desc->use_count = 0;
  desc->crash_count = 0;

  if (EMASS_SKIPINITIALIZEB4ALLCMD || !(stat=do_initialize()))
  {
     if  (stat = aci_view(vol,media_type,desc))
     {					/* if error */
        stat=d_errno;			/* gett err code */
        if (sizeof(status_table) < stat)	/* if invalid err code, return internal DAS S/W error */
            stat= EDASINT;
     } 
  }
  /*
         return result : status index via returned parameter; coordinate, robot owner, volume attribute,
                         volser media type, volume name, volume owner, use count and crash count via
                         listed parameter
  */
  return stat;
}
/* #endif */
/**************************************************************************************
	view - get mount parameters and return values
*/
static PyObject* view(PyObject *self, PyObject *args)
{
  char *vol;
  char *media_type_s;
  enum aci_media media_type;
  int stat;
  char *sc;
  struct aci_vol_desc desc;

  /*
        Initialize struct desc
  */
  desc.coord[0] = '\0';
  desc.owner = '\0';
  desc.attrib = '\0';
  desc.type = '\0';
  desc.volser[0] = '\0';
  desc.vol_owner = '\0';
  desc.use_count = 0;
  desc.crash_count = 0;
  /*
        Get the arguements
  */
  if (!PyArg_ParseTuple(args, "ss", &vol, &media_type_s))            /* get args */ 
  	return (NULL);
  /*         aci_view needs vol and media_type_s, returns error status and database entry in
                    aci_volume_desc struct
  */
  if (!(media_type = stoi_mediatype(media_type_s)))
      /* if error, return that requested media type not found */
      return(Py_BuildValue("sisscccscii",
		status_table[ENOVOLUME].status, ENOVOLUME, 
	        status_table[ENOVOLUME].status_descrip,desc.coord, desc.owner, desc.attrib, desc.type,
                desc.volser, desc.vol_owner, desc.use_count, desc.crash_count));
  stat = get_view(vol,media_type,&desc);
  /*
         return result : status, status index, status description, coordinate, robot owner, volume attribute,
                         volser media type, volume name, volume owner, use count and crash count
  */
  return(Py_BuildValue("sisscccscii", status_table[stat].status, stat, status_table[stat].status_descrip,
                       desc.coord, desc.owner, desc.attrib, desc.type, desc.volser, desc.vol_owner,
                       desc.use_count, desc.crash_count));
}

/**************************************************************************************
	do_initialize - wrapper to initialize ACI library for use; it does not change drive state
*/
int do_initialize(void)
{
  int stat;

  /*
        Initialize aci client, do not change drive state
  */
  if (stat=aci_initialize())
  {
    stat = EAMU;     /* map -1 error return to EAMU - AMU responded with unknown error */
  } else {
    stat = EOK;     /* map 0 no error return to EOK - no error */
  }
  return stat;
}

#ifdef 0
/**************************************************************************************
	initialize - initialize ACI library for use; it does not change drive state
*/
static PyObject* initialize(PyObject *self, PyObject *args)
{
  int stat;

  stat = do_initialize();
  /*
         return result : status
  */
  return(Py_BuildValue("sis", status_table[stat].status, stat, status_table[stat].status_descrip));
}
#endif

/**************************************************************************************
	home (robot)
*/
static PyObject* home(PyObject *self, PyObject *args)
{
  char *robot;
  int stat;
  char *rv;

  /*
        Get the arguements
  */
  if (!PyArg_ParseTuple(args, "s", &robot))            /* get args */
        return (NULL);
  rv="badhome";
  /*
  if (EMASS_SKIPINITIALIZEB4ALLCMD || !(stat=do_initialize()))
  {
  */
  stat = aci_robhome(robot);
  if (!stat)
     rv="badstart";
     stat=aci_robstat(robot, "START");
     if (!stat)
         rv="ok";
  return(Py_BuildValue("s", rv));
}

/**************************************************************************************
   Module Methods table.

   There is one entry with four items for for each method in the module

   Entry 1 - the method name as used  in python
         2 - the c implementation function
         3 - flags
         4 - method documentation string
*/

static PyMethodDef EMASS_Methods[] = {
  { "mount", mount, 1, Mount_Doc},
  { "dismount", dismount, 1, Dismount_Doc},
  { "home", home, 1, Home_Doc},
  { "view", view, 1, View_Doc},
  {0,     0}        /* Sentinel */
};

/**************************************************************************************
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

void initEMASS()
{
  PyObject *m, *d;

  m=  Py_InitModule4("EMASS", EMASS_Methods, EMASS_Doc,
                        (PyObject*)NULL,PYTHON_API_VERSION);
  d = PyModule_GetDict(m);
  EMASSErrObject = PyErr_NewException("EMASS.error", NULL, NULL);
  if (EMASSErrObject != NULL)
             PyDict_SetItemString(d,"error",EMASSErrObject);
}

