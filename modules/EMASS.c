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
    printf("%s %s\n",media_type, m->media_string);
    if (strcmp(media_type, m->media_string) == 0) 
      return(m->media_enum);
  }
  return(m->media_enum);		/* rturn "INVALID" */
}

static PyObject* Mount(PyObject *self, PyObject *args)
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
  media_type = stoi_mediatype(media_type_s);
  if  (stat = aci_mount(vol,media_type,drive))
     return(Py_BuildValue("i",d_errno));
  return(Py_BuildValue("i",stat ));
}

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
  media_type = stoi_mediatype(media_type_s);
  if  (stat = aci_dismount(vol,media_type))
     return(Py_BuildValue("i",d_errno));
  return(Py_BuildValue("i",stat ));
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

