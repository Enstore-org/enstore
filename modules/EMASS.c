#include <Python.h>
#include <aci.h>
#include <derrno.h>
/*
	See media_changer.py 
*/
static PyObject *EMASSErrObject;

int EMASSerr(char *caller, char *location, int status)
{
 printf("EMASS err: %s - %s - code %d \n", caller, location, status);
 return(status);
}

static char EMASS_Doc[] =  "EMASS Robot mount and dismount";
static char Mount_Doc[] =  "mount a tape";
static char Dismount_Doc[] =  "dismount a tape";

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
  return(m->media_enum);		/* rturn "INVALID" */
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


/*
        mount
        arguements
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
  char *media_type_s;
  enum aci_media media_type;
  int stat;
  char *sc;

  if (!PyArg_ParseTuple(args, "sss", &vol, &drive, &media_type_s))     /* get args */ 
  	return (NULL);
  if (!(media_type = stoi_mediatype(media_type_s)))		/* cvt media type to aci code */
      return(Py_BuildValue("sis",
		status_table[ENOVOLUME].status,
		ENOVOLUME, 
		status_table[ENOVOLUME].status_descrip));
  if  (stat = aci_mount(vol,media_type,drive))			/* call aci routine */
  {					/* if error */
     stat=d_errno;			/* gett err code */
     if (sizeof(status_table) < stat)	/* if invalid err code */
         stat= EDASINT;
  }					/* return result */
  return(Py_BuildValue("sis", status_table[stat].status, stat, status_table[stat].status_descrip));
}
/*
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
  if (!(media_type = stoi_mediatype(media_type_s)))
      return(Py_BuildValue("sis",
		status_table[ENOVOLUME].status,
		ENOVOLUME, 
		status_table[ENOVOLUME].status_descrip));
  if  (stat = aci_dismount(vol,media_type))
  {
     stat=d_errno;
     if (sizeof(status_table) < stat)
         stat= EDASINT;
  }
  return(Py_BuildValue("sis", status_table[stat].status, stat, status_table[stat].status_descrip));
}

/*
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

