#include <Python.h>
#include <aci.h>
#include <derrno.h>
#include <mc.h>

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

char err_string[] = {
	"request successful",					/*0*/
	"rpc failure",						/*1*/
	"aci parameter invalid ",				/*2*/
	"volume not found of this type",			/*3*/
	"drive not in Grau ATL ",				/*4*/
	"the requested drive is in use",			/*5*/
	"the robot has a physical problem with the volume",	/*6*/
	"an internal error in the AMU ",			/*7*/
	"the DAS was unable to communicate with the AMU",	/*8*/
	"the robotic system is not functioning",		/*9*/
	"the AMU was unable to communicate with the robot",	/*10*/
	"the DAS system is not active ",			/*11*/
	"the drive did not contain an unloaded volume",		/*12*/
	"invalid registration",					/*13*/
	"invalid hostname or ip address",			/*14*/
	"the area name does not exist ",			/*15*/
	"the client is not authorized to make this request",	/*16*/
	"the dynamic area became full, insertion stopped",	/*17*/
	"the drive is currently available to another client",	/*18*/
	"the client does not exist ",				/*19*/
	"the dynamic area does not exist",			/*20*/
	"no request exists with this number",			/*21*/
	"retry attempts exceeded",				/*22*/
	"requested volser is not mounted",			/*23*/
	"requested volser is in use ",				/*24*/
	"no space availble to add range",			/*25*/
	"the range or object was not found",			/*26*/
	"the request was cancelled by aci_cancel()",		/*27*/
	"internal DAS error",					/*28*/
	"internal ACI error",					/*29*/
	"for a query more data are available",			/*30*/
	"things don't match together",				/*31*/
	"volser is still in another pool",	 		/*32*/
	"drive in cleaning",					/*33*/
	"The aci request timed out",				/*34*/
	"the robot has a problem with handling the device"	/*35*/
	};
int drive_errs[]={EPROBDEV,EUPELSE,EDEVEMPTY,EDRVOCCUPIED,ENODRIVE };
int media_errs[]={EOTHERPOOL,EINUSE,ENOTMOUNTED,ENOVOLUME};

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

  if (!PyArg_ParseTuple(args, "sss", &vol, &drive, &media_type_s))              /* get args */ 
  	return (NULL);
  if (!(media_type = stoi_mediatype(media_type_s)))				/* cvt media type to aci code */
      return(Py_BuildValue("iis",ENOVOLUME, MC_MEDIA, err_string[ENOVOLUME]));
  if  (stat = aci_mount(vol,media_type,drive))					/* call aci routine */
  {										/* if error */
     stat=d_errno;									/* gett err code */
     if (sizeof(err_string) < stat)							/* if invalid err code */
         stat= EDASINT;
  }										/* return result */
  return(Py_BuildValue("iis",stat, status_class(stat, drive_errs, media_errs ), err_string[stat]));
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
  /*
        Get the arguements
  */
  if (!PyArg_ParseTuple(args, "sss", &vol, &drive, &media_type_s))            /* get args */ 
  	return (NULL);
  if (!(media_type = stoi_mediatype(media_type_s)))
      return(Py_BuildValue("iis",ENOVOLUME, err_string[ENOVOLUME], err_string[ENOVOLUME]));
  if  (stat = aci_dismount(vol,media_type))
  {
     stat=d_errno;
     if (sizeof(err_string) < stat)
         stat= EDASINT;
  }
  return(Py_BuildValue("iis",stat, status_class(stat, drive_errs, media_errs ), err_string[stat]));
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

