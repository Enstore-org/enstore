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

static PyObject* Mount(PyObject *self, PyObject *args)
{
  char *vol;
  char *drive;
  enum aci_media media_type;
  int stat;
  /*
        Get the arguements
  */
  PyArg_ParseTuple(args, "ssi", &vol, &drive, &media_type);
  if  (stat = aci_mount(vol,media_type,drive))
     return(Py_BuildValue("i",d_errno));
  return(Py_BuildValue("i",stat ));
}

static PyObject* Dismount(PyObject *self, PyObject *args)
{
  char *vol;
  enum aci_media media_type;
  int stat;
  /*
        Get the arguements
  */
  PyArg_ParseTuple(args, "ss", &vol, &media_type);
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

