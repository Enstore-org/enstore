#include <Python.h>
#include <ftt.h>

typedef struct {
  ftt_descriptor	ftt_desc;
  char *filename; 
} ET_struct;
/*
  Module description
*/
static char ETape_Doc[] =  "ETape is a module which interfaces to ENSTORE TAPE drives";


/*
   Method implementations
*/
static char ET_CopyToTape_Doc[] = "Copy data from an fd to tape";

static PyObject*
ET_CopyToTape(PyObject *self, PyObject *args)
{
  PyObject *ifob;
  char *iname, *ipathname;

  PyObject *ofob;
  char *oname, *opathname;
  int sts = 0;

  if (!PyArg_ParseTuple(args, "OO", 
                              &PyFile_Type, &ifob))
      return NULL;
  return Py_BuildValue("i",0);
}

static char ET_OpenRead_Doc[] = "Open a tape drive for reading";

static PyObject*
ET_OpenRead(PyObject *self, PyObject *args)
{
  char *iname;
  ftt_descriptor ftt_desc;

  if (!PyArg_ParseTuple(args, "s", &iname))
      return NULL;
    
  ftt_desc = ftt_open(iname, FTT_RDONLY);

  
  return Py_BuildValue("i",0);
}

static char ET_OpenWrite_Doc[] = "Open a tape drive for writing";

static PyObject*
ET_OpenWrite(PyObject *self, PyObject *args)
{
  char *oname;

  int sts;

  if (!PyArg_ParseTuple(args, "s", &oname))
      return NULL;
  return Py_BuildValue("i",0);
}

/*
   Module Methods table. 

   There is one entry with four items for for each method in the module

   Entry 1 - the method name as used  in python
         2 - the c implementation function
         3 - flags 
         4 - method documentation string
*/
static PyMethodDef ETape_Methods[] = {
  { "OpenWrite", ET_OpenWrite, 1, ET_OpenWrite_Doc},
  { "OpenRead", ET_OpenRead, 1, ET_OpenRead_Doc},
  { "CopyToTape", ET_CopyToTape, 1, ET_CopyToTape_Doc},
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

   Third	- a doumentation string for the module
  
   Fourth & Fifth - see Python/modsupport.c

*/
void initETape()
{
  PyObject *m = Py_InitModule4("ETape", ETape_Methods, ETape_Doc, 
                               (PyObject*)NULL,PYTHON_API_VERSION);

}
