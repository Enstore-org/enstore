#include <Python.h>
#include <aci.h>
#include <derrno.h>

static PyObject *EMASSErrObject;

int EMASSerr(char *caller, char *location, int status)
{
 printf("EMASS err: %s - %s - code %d \n", caller, location, status);
 return(status);
}

static char EMASS_Doc[] =  "EMASS Robot Mount and Dismount";
static char Mount_Doc[] =  "Mount a tape";
static char Dismount_Doc[] =  "Dismount a tape";

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
	EMASS error table - from aci/v1_3_0_c7/inc/derrno.h
	enum e_type a canonical code for enstore
*/
enum e_type {EM_OK=0,		/* mount successful */
		EM_UNKOWN,	/* something wrong with library system */
		EM_DRIVE,	/* a drive problem - retry another drive */
		EM_MEDIA};	/* a cartridge problem - drive ok */

struct err_struct {
  enum e_type	e_code;
  char *    	err_string;
}  err_table[] = {
	EM_OK    ,"request successful",					/*0*/
	EM_UNKOWN,"rpc failure",					/*1*/
	EM_UNKOWN,"aci parameter invalid ",				/*2*/
	EM_MEDIA ,"volume not found of this type",			/*3*/
	EM_DRIVE ,"drive not in Grau ATL ",				/*4*/
	EM_DRIVE ,"the requested drive is in use",			/*5*/
	EM_MEDIA ,"the robot has a physical problem with the volume",	/*6*/
	EM_UNKOWN,"an internal error in the AMU ",			/*7*/
	EM_UNKOWN,"the DAS was unable to communicate with the AMU",	/*8*/
	EM_UNKOWN,"the robotic system is not functioning",		/*9*/
	EM_UNKOWN,"the AMU was unable to communicate with the robot",	/*10*/
	EM_UNKOWN,"the DAS system is not active ",			/*11*/
	EM_DRIVE ,"the drive did not contain an unloaded volume",	/*12*/
	EM_UNKOWN,"invalid registration",				/*13*/
	EM_UNKOWN,"invalid hostname or ip address",			/*14*/
	EM_UNKOWN,"the area name does not exist ",			/*15*/
	EM_UNKOWN,"the client is not authorized to make this request",	/*16*/
	EM_UNKOWN,"the dynamic area became full, insertion stopped",	/*17*/
	EM_DRIVE ,"the drive is currently available to another client",	/*18*/
	EM_UNKOWN,"the client does not exist ",				/*19*/
	EM_UNKOWN,"the dynamic area does not exist",			/*20*/
	EM_UNKOWN,"no request exists with this number",			/*21*/
	EM_UNKOWN,"retry attempts exceeded",				/*22*/
	EM_MEDIA ,"requested volser is not mounted",			/*23*/
	EM_MEDIA ,"requested volser is in use ",			/*24*/
	EM_UNKOWN,"no space availble to add range",			/*25*/
	EM_UNKOWN,"the range or object was not found",			/*26*/
	EM_UNKOWN,"the request was cancelled by aci_cancel()",		/*27*/
	EM_UNKOWN,"internal DAS error",					/*28*/
	EM_UNKOWN,"internal ACI error",					/*29*/
	EM_UNKOWN,"for a query more data are available",		/*30*/
	EM_UNKOWN,"things don't match together",			/*31*/
	EM_MEDIA ,"volser is still in another pool",	 		/*32*/
	EM_DRIVE ,"drive in cleaning",					/*33*/
	EM_UNKOWN,"The aci request timed out",				/*34*/
	EM_DRIVE ,"the robot has a problem with handling the device",	/*35*/
	};

/*
        Mount
        arguements
                vol - cartridge id
                drive - drive name
                media_type_s - mediatype
        Returns
                int - status returned by robot
                int - canonical status 0=> ok, 1=> unknow error, 2=>drive problem, 3=>nedia problem
                char* text desciption of error
*/

static PyObject* Mount(PyObject *self, PyObject *args)
{
  char *vol;
  char *drive;
  char *media_type_s;
  enum aci_media media_type;
  int stat;

  PyArg_ParseTuple(args, "sss", &vol, &drive, &media_type_s);			/* pargs args */
  if (!(media_type = stoi_mediatype(media_type_s)))				/* cvt media type to aci code */
      return(Py_BuildValue("iis",ENOVOLUME, err_table[ENOVOLUME].e_code, err_table[ENOVOLUME].err_string));
  if  (stat = aci_mount(vol,media_type,drive))					/* call aci routine */
  {										/* if error */
     stat=d_errno;									/* gett err code */
     if (sizeof(err_table) < stat)							/* if invalid err code */
         stat= EDASINT;
  }										/* return result */
  return(Py_BuildValue("iis",stat, err_table[stat].e_code, err_table[stat].err_string));
}
/*
	Dismount - see Mount parameters and return values
*/
static PyObject* Dismount(PyObject *self, PyObject *args)
{
  char *vol;
  char *drive;
  char *media_type_s;
  enum aci_media media_type;
  int stat;
  /*
        Get the arguements
  */
  PyArg_ParseTuple(args, "sss", &vol, &drive, &media_type_s);
  if (!(media_type = stoi_mediatype(media_type_s)))
      return(Py_BuildValue("iis",ENOVOLUME, err_table[ENOVOLUME].e_code, err_table[ENOVOLUME].err_string));
  if  (stat = aci_dismount(vol,media_type))
  {
     stat=d_errno;
     if (sizeof(err_table) < stat)
         stat= EDASINT;
  }
  return(Py_BuildValue("iis",stat, err_table[stat].e_code, err_table[stat].err_string));
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
  { "Mount", Mount, 1, Mount_Doc},
  { "Dismount", Dismount, 1, Dismount_Doc},
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

